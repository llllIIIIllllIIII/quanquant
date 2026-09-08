"""Candle data + chart UI state API (consumed by the KLineCharts frontend)."""
import json
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, StrictBool, StrictStr
from sqlmodel import Session, select

from quanquant.auth import service
from quanquant.candles.service import get_candles, get_latest
from quanquant.candles.timeframes import TIMEFRAMES
from quanquant.db.models import User, UserChartState, _utcnow
from quanquant.web.deps import get_current_user, get_session

router = APIRouter()

_MAX_STATE_BYTES = 256 * 1024
_STATE_KINDS = ("indicators", "drawings")


# LOW-4（fresh-context 終審修復）：三支 `PUT /api/user/*` 偏好端點改回同步 `def`（FastAPI
# 丟 threadpool 執行，離開 event loop，比照本檔 candle 端點與 orders.py 的既有慣例——
# 原本用 `async def` + `await request.json()` 手動解析，但 sync def 不能 await；改用
# Pydantic body model 當參數（比照 web/routers/alerts.py::AlertCreate 的既有寫法），
# FastAPI 會在呼叫這支 sync 函式之前就非同步解析好 body——JSON 格式錯誤時自動回 422，
# 行為與原本手動 try/except 一致，不需要在函式體內再 await 任何東西。用 Strict* 型別
# （不是裸 str/bool）：Pydantic v2 預設對 bool/str 會寬鬆轉型（如 "yes"/"1" 會被轉成
# True），原本手寫的 `isinstance(x, bool)` 是嚴格型別檢查，換成 Pydantic model 若不強制
# strict 會讓行為變寬鬆（`{"skip": "yes"}` 從原本 422 變成悄悄轉成 True 通過）。
class _ColorSchemeBody(BaseModel):
    scheme: StrictStr


class _ThemeBody(BaseModel):
    theme: StrictStr


class _SkipSimConfirmBody(BaseModel):
    skip: StrictBool


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
def put_color_scheme(
    body: _ColorSchemeBody,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    if not service.set_color_scheme(session, user, body.scheme):
        raise HTTPException(status_code=422, detail="invalid color scheme")
    return Response(status_code=204)


@router.put("/api/user/theme")
def put_theme(
    body: _ThemeBody,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    if not service.set_theme(session, user, body.theme):
        raise HTTPException(status_code=422, detail="invalid theme")
    return Response(status_code=204)


@router.put("/api/user/skip-sim-confirm")
def put_skip_sim_confirm(
    body: _SkipSimConfirmBody,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    """007：sim 下單確認視窗「不再顯示」勾選時，即時（JS fire-and-forget，比照
    QQTheme.toggle 的既有慣例）把偏好存回 User——跨裝置一致，不用 localStorage。"""
    service.set_skip_sim_confirm(session, user, body.skip)
    return Response(status_code=204)

