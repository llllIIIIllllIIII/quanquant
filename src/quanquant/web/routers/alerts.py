"""Alert CRUD, trigger log, and the browser SSE channel for live toasts."""
from decimal import Decimal
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, model_validator
from sqlmodel import Session, select
from sse_starlette.sse import EventSourceResponse

from quanquant.candles.timeframes import TIMEFRAMES
from quanquant.db.models import Alert, AlertEvent, User, _utcnow
from quanquant.web.deps import get_current_user, get_session

router = APIRouter()

Ind = Literal["ma", "wr", "bias"]


class AlertCreate(BaseModel):
    symbol: str = "TXF"
    timeframe: str
    left_kind: Literal["price", "indicator"]
    left_name: Ind | None = None
    left_period: int | None = None
    op: Literal["gte", "lte", "cross_up", "cross_down"]
    right_kind: Literal["const", "indicator"]
    right_value: Decimal | None = None
    right_name: Ind | None = None
    right_period: int | None = None
    fire_once: bool = False

    @model_validator(mode="after")
    def _check(self) -> "AlertCreate":
        if self.timeframe not in TIMEFRAMES:
            raise ValueError(f"unknown timeframe {self.timeframe!r}")
        if self.left_kind == "indicator" and not (self.left_name and (self.left_period or 0) >= 1):
            raise ValueError("indicator left operand needs name + period>=1")
        if self.right_kind == "const":
            if self.right_value is None:
                raise ValueError("const right operand needs right_value")
        elif not (self.right_name and (self.right_period or 0) >= 1):
            raise ValueError("indicator right operand needs name + period>=1")
        return self


class AlertPatch(BaseModel):
    enabled: bool | None = None
    fire_once: bool | None = None


def _alert_dict(a: Alert) -> dict:
    return {
        "id": a.id, "symbol": a.symbol, "timeframe": a.timeframe,
        "left_kind": a.left_kind, "left_name": a.left_name, "left_period": a.left_period,
        "op": a.op,
        "right_kind": a.right_kind,
        "right_value": float(a.right_value) if a.right_value is not None else None,
        "right_name": a.right_name, "right_period": a.right_period,
        "enabled": a.enabled, "fire_once": a.fire_once,
        "last_triggered_at": a.last_triggered_at.isoformat() if a.last_triggered_at else None,
    }


@router.get("/api/alerts")
def list_alerts(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    symbol: str = Query("TXF"),
):
    rows = session.exec(
        select(Alert)
        .where(Alert.symbol == symbol, Alert.user_id == user.id)
        .order_by(Alert.id.desc())  # type: ignore[union-attr]
    ).all()
    return JSONResponse([_alert_dict(a) for a in rows])


@router.post("/api/alerts")
def create_alert(
    body: AlertCreate,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    alert = Alert(**body.model_dump(), user_id=user.id)
    session.add(alert)
    session.commit()
    session.refresh(alert)
    return JSONResponse(_alert_dict(alert), status_code=201)


def _owned_alert(session: Session, alert_id: int, user: User) -> Alert:
    alert = session.get(Alert, alert_id)
    if alert is None or alert.user_id != user.id:
        # 404 (not 403): another user's alert is indistinguishable from a missing one
        raise HTTPException(status_code=404, detail="alert not found")
    return alert


@router.patch("/api/alerts/{alert_id}")
def patch_alert(
    alert_id: int,
    body: AlertPatch,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    alert = _owned_alert(session, alert_id, user)
    data = body.model_dump(exclude_unset=True)
    for key, value in data.items():
        setattr(alert, key, value)
    if data.get("enabled"):
        alert.armed = True  # re-arm when re-enabled
    alert.updated_at = _utcnow()
    session.add(alert)
    session.commit()
    session.refresh(alert)
    return JSONResponse(_alert_dict(alert))


@router.delete("/api/alerts/{alert_id}")
def delete_alert(
    alert_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    alert = _owned_alert(session, alert_id, user)
    session.delete(alert)
    session.commit()
    return Response(status_code=204)


@router.get("/api/alerts/events")
def list_events(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    limit: int = Query(50, ge=1, le=200),
):
    own_alert_ids = select(Alert.id).where(Alert.user_id == user.id)
    events = session.exec(
        select(AlertEvent)
        .where(AlertEvent.alert_id.in_(own_alert_ids))  # type: ignore[union-attr]
        .order_by(AlertEvent.id.desc())  # type: ignore[union-attr]
        .limit(limit)
    ).all()
    return JSONResponse([
        {
            "id": e.id, "alertId": e.alert_id, "barTs": e.bar_ts, "message": e.message,
            "firedAt": e.fired_at.isoformat() if e.fired_at else None,
        }
        for e in events
    ])


@router.get("/alerts/stream")
async def alerts_stream(request: Request):
    notify = getattr(request.app.state, "notify", None)
    if notify is None:
        return EventSourceResponse(iter(()))
    queue = notify.browser.subscribe()

    async def event_generator():
        try:
            while True:
                yield {"data": await queue.get()}
        finally:
            notify.browser.unsubscribe(queue)

    return EventSourceResponse(event_generator())
