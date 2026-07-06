"""Candle data + chart UI state API (consumed by the KLineCharts frontend)."""
import json
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from sqlmodel import Session, select

from quanquant.auth import service
from quanquant.candles.service import get_candles, get_latest
from quanquant.candles.timeframes import TIMEFRAMES
from quanquant.db.models import User, UserChartState, _utcnow
from quanquant.web.deps import get_current_user, get_session

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
    session_mode: Literal["all", "day", "night"] = Query("all", alias="session"),
):
    page = get_candles(
        session, symbol, _validate_tf(tf), before=before, limit=limit, session_mode=session_mode
    )
    return JSONResponse(
        {"symbol": symbol, "tf": tf, "bars": page.bars, "hasMore": page.has_more}
    )


@router.get("/api/candles/latest")
def candles_latest(
    session: Session = Depends(get_session),
    symbol: str = Query("TXF"),
    tf: str = Query("1m"),
    since: int = Query(..., description="return bars with timestamp >= since (epoch ms)"),
    session_mode: Literal["all", "day", "night"] = Query("all", alias="session"),
):
    bars = get_latest(session, symbol, _validate_tf(tf), since=since, session_mode=session_mode)
    return JSONResponse({"bars": bars})


# --- chart UI state (indicator configs + drawings) ---


def _get_state(session: Session, user_id: int, symbol: str, kind: str) -> dict | list | None:
    stmt = select(UserChartState).where(
        UserChartState.user_id == user_id,
        UserChartState.symbol == symbol,
        UserChartState.kind == kind,
    )
    row = session.exec(stmt).first()
    if row is None:
        return None
    try:
        return json.loads(row.payload)
    except json.JSONDecodeError:
        return None


@router.get("/api/chart/state")
def chart_state(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    symbol: str = Query("TXF"),
):
    return JSONResponse(
        {
            "indicators": _get_state(session, user.id, symbol, "indicators"),
            "drawings": _get_state(session, user.id, symbol, "drawings"),
        }
    )


@router.put("/api/chart/state/{kind}")
async def put_chart_state(
    kind: str,
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
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

    stmt = select(UserChartState).where(
        UserChartState.user_id == user.id,
        UserChartState.symbol == symbol,
        UserChartState.kind == kind,
    )
    row = session.exec(stmt).first()
    if row is None:
        row = UserChartState(user_id=user.id, symbol=symbol, kind=kind, payload=body.decode("utf-8"))
    else:
        row.payload = body.decode("utf-8")
        row.updated_at = _utcnow()
    session.add(row)
    session.commit()
    return Response(status_code=204)


@router.put("/api/user/color-scheme")
async def put_color_scheme(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    try:
        body = await request.json()
    except Exception:
        body = None
    scheme = body.get("scheme") if isinstance(body, dict) else None
    if not isinstance(scheme, str) or not service.set_color_scheme(session, user, scheme):
        raise HTTPException(status_code=422, detail="invalid color scheme")
    return Response(status_code=204)


@router.put("/api/user/theme")
async def put_theme(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    try:
        body = await request.json()
    except Exception:
        body = None
    theme = body.get("theme") if isinstance(body, dict) else None
    if not isinstance(theme, str) or not service.set_theme(session, user, theme):
        raise HTTPException(status_code=422, detail="invalid theme")
    return Response(status_code=204)

