"""Candle data + chart UI state API (consumed by the KLineCharts frontend)."""
import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from sqlmodel import Session, select

from quanquant.candles.service import get_candles, get_latest
from quanquant.candles.timeframes import TIMEFRAMES
from quanquant.db.models import ChartState, _utcnow
from quanquant.poller import QuotePoller
from quanquant.web.deps import get_poller, get_session

router = APIRouter()

_MAX_STATE_BYTES = 256 * 1024
_STATE_KINDS = ("indicators", "drawings")


def _validate_tf(tf: str) -> str:
    if tf not in TIMEFRAMES:
        valid = ", ".join(TIMEFRAMES)
        raise HTTPException(status_code=422, detail=f"unknown timeframe {tf!r}; valid: {valid}")
    return tf


# NOTE: candle endpoints are sync `def` on purpose — FastAPI runs them in the
# threadpool, so synchronous SQLite work never blocks the event loop (which also
# serves the SSE quote stream).


@router.get("/api/candles")
def candles_page(
    session: Session = Depends(get_session),
    symbol: str = Query("TXF"),
    tf: str = Query("1m"),
    before: int | None = Query(None, description="exclusive upper bound, epoch ms"),
    limit: int = Query(500, ge=1, le=1000),
):
    page = get_candles(session, symbol, _validate_tf(tf), before=before, limit=limit)
    return JSONResponse(
        {"symbol": symbol, "tf": tf, "bars": page.bars, "hasMore": page.has_more}
    )


@router.get("/api/candles/latest")
def candles_latest(
    session: Session = Depends(get_session),
    symbol: str = Query("TXF"),
    tf: str = Query("1m"),
    since: int = Query(..., description="return bars with timestamp >= since (epoch ms)"),
):
    bars = get_latest(session, symbol, _validate_tf(tf), since=since)
    return JSONResponse({"bars": bars})


# --- chart UI state (indicator configs + drawings) ---


def _get_state(session: Session, symbol: str, kind: str) -> dict | list | None:
    stmt = select(ChartState).where(ChartState.symbol == symbol, ChartState.kind == kind)
    row = session.exec(stmt).first()
    if row is None:
        return None
    try:
        return json.loads(row.payload)
    except json.JSONDecodeError:
        return None


@router.get("/api/chart/state")
def chart_state(session: Session = Depends(get_session), symbol: str = Query("TXF")):
    return JSONResponse(
        {
            "indicators": _get_state(session, symbol, "indicators"),
            "drawings": _get_state(session, symbol, "drawings"),
        }
    )


@router.put("/api/chart/state/{kind}")
async def put_chart_state(
    kind: str,
    request: Request,
    session: Session = Depends(get_session),
    symbol: str = Query("TXF"),
):
    if kind not in _STATE_KINDS:
        raise HTTPException(status_code=404, detail=f"unknown state kind {kind!r}")
    body = await request.body()
    if len(body) > _MAX_STATE_BYTES:
        raise HTTPException(status_code=413, detail="state payload too large")
    try:
        json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=422, detail="payload must be valid JSON") from None

    stmt = select(ChartState).where(ChartState.symbol == symbol, ChartState.kind == kind)
    row = session.exec(stmt).first()
    if row is None:
        row = ChartState(symbol=symbol, kind=kind, payload=body.decode("utf-8"))
    else:
        row.payload = body.decode("utf-8")
        row.updated_at = _utcnow()
    session.add(row)
    session.commit()
    return Response(status_code=204)


# --- health ---


@router.get("/healthz")
async def healthz(poller: QuotePoller | None = Depends(get_poller)):
    age: float | None = None
    if poller is not None and poller.last is not None:
        age = (datetime.now(timezone.utc) - poller.last.fetched_at).total_seconds()
    return JSONResponse({"status": "ok", "last_quote_age_s": age})
