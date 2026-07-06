"""Dashboard page + live-quote SSE channel.

Candle data for the chart lives in web/routers/candles.py (/api/candles).
"""
import asyncio
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sse_starlette.sse import EventSourceResponse

from quanquant.config import get_settings
from quanquant.market_hours import get_session
from quanquant.models import FuturesSnapshot
from quanquant.poller import QuotePoller
from quanquant.pulse.engine import PulseEngine
from quanquant.web.deps import get_poller, get_pulse
from quanquant.web.templating import SESSION_LABEL, render_partial, templates

router = APIRouter()


def _quote_context(
    snap: FuturesSnapshot | None,
    error: str | None,
    as_of: datetime | None = None,
    pulse: PulseEngine | None = None,
) -> dict:
    session = get_session()
    pulse_level, pulse_state = (0, "silent")
    if pulse is not None:
        pulse_level, pulse_state = pulse.level_at()
    return {
        "snap": snap,
        "error": error,
        "session": session,
        "session_label": SESSION_LABEL.get(session, SESSION_LABEL["closed"]),
        "as_of": as_of,  # "live as of now" clock for the SSE heartbeat; None = use data time
        "pulse_level": pulse_level,
        "pulse_state": pulse_state,
    }


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    settings = get_settings()
    user = request.state.user
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "active": "dashboard",
            "symbol": settings.symbol,
            "color_scheme": (user.chart_color_scheme if user else None) or "green_up",
        },
    )


@router.get("/quote", response_class=HTMLResponse)
async def quote_now(
    request: Request,
    poller: QuotePoller | None = Depends(get_poller),
    pulse: PulseEngine | None = Depends(get_pulse),
):
    snap = poller.last if poller else None
    return HTMLResponse(
        render_partial("partials/quote.html", **_quote_context(snap, None, pulse=pulse))
    )


@router.get("/quote/stream")
async def quote_stream(
    request: Request,
    poller: QuotePoller | None = Depends(get_poller),
    pulse: PulseEngine | None = Depends(get_pulse),
):
    if poller is None:
        return EventSourceResponse(iter(()))

    queue = poller.subscribe()
    min_interval = get_settings().sse_min_interval

    async def event_generator():
        try:
            if poller.last is not None:
                yield {
                    "data": render_partial(
                        "partials/quote.html", **_quote_context(poller.last, None, pulse=pulse)
                    )
                }
            last_emit = time.monotonic()
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=1.0)
                    # Coalesce bursts: wait out the throttle window, then collapse to
                    # the most recent event so the UI never falls behind under streaming.
                    wait = min_interval - (time.monotonic() - last_emit)
                    if wait > 0:
                        await asyncio.sleep(wait)
                    while not queue.empty():
                        event = queue.get_nowait()
                    snap, error, as_of = event.snapshot, event.error, None
                except asyncio.TimeoutError:
                    # Heartbeat: no tick in the last second. Re-emit the latest price
                    # with a "now" clock so the ticker stays visibly live between
                    # trades (no upstream call, no DB write).
                    if poller.last is None:
                        continue
                    snap, error, as_of = poller.last, None, datetime.now(timezone.utc)
                last_emit = time.monotonic()
                yield {
                    "data": render_partial(
                        "partials/quote.html", **_quote_context(snap, error, as_of, pulse=pulse)
                    )
                }
        finally:
            poller.unsubscribe(queue)

    return EventSourceResponse(event_generator())
