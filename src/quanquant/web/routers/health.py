"""Unauthenticated health check (GCP uptime check hits this without credentials)."""
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from quanquant.broker.session_state import OrderSessionState
from quanquant.poller import QuotePoller
from quanquant.web.deps import get_order_session_state, get_poller

router = APIRouter()


@router.get("/healthz")
async def healthz(
    poller: QuotePoller | None = Depends(get_poller),
    order_state: OrderSessionState | None = Depends(get_order_session_state),
):
    age: float | None = None
    if poller is not None and poller.last is not None:
        age = (datetime.now(timezone.utc) - poller.last.fetched_at).total_seconds()

    order_section = None
    if order_state is not None:
        order_section = {
            "ready": order_state.ready,
            "last_error": order_state.last_error,
            "reconnect_attempts": order_state.reconnect_attempts,
        }
    return JSONResponse({
        "status": "ok", "last_quote_age_s": age, "order_subsystem": order_section,
    })
