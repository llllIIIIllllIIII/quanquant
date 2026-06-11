"""Dashboard page + live-quote SSE channel.

Candle data for the chart lives in web/routers/candles.py (/api/candles).
"""
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sse_starlette.sse import EventSourceResponse

from quanquant.config import get_settings
from quanquant.market_hours import get_session
from quanquant.models import FuturesSnapshot
from quanquant.poller import QuotePoller
from quanquant.web.deps import get_poller
from quanquant.web.templating import SESSION_LABEL, render_partial, templates

router = APIRouter()


def _quote_context(snap: FuturesSnapshot | None, error: str | None) -> dict:
    session = get_session()
    return {
        "snap": snap,
        "error": error,
        "session": session,
        "session_label": SESSION_LABEL.get(session, SESSION_LABEL["closed"]),
    }


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    settings = get_settings()
    tv_url = f"https://www.tradingview.com/symbols/{settings.tv_symbol.replace(':', '-')}/"
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "active": "dashboard",
            "symbol": settings.symbol,
            "tv_symbol": settings.tv_symbol,
            "tv_url": tv_url,
        },
    )


@router.get("/quote", response_class=HTMLResponse)
async def quote_now(request: Request, poller: QuotePoller | None = Depends(get_poller)):
    snap = poller.last if poller else None
    return HTMLResponse(render_partial("partials/quote.html", **_quote_context(snap, None)))


@router.get("/quote/stream")
async def quote_stream(request: Request, poller: QuotePoller | None = Depends(get_poller)):
    if poller is None:
        return EventSourceResponse(iter(()))

    queue = poller.subscribe()

    async def event_generator():
        try:
            if poller.last is not None:
                yield {
                    "data": render_partial(
                        "partials/quote.html", **_quote_context(poller.last, None)
                    )
                }
            while True:
                event = await queue.get()
                yield {
                    "data": render_partial(
                        "partials/quote.html", **_quote_context(event.snapshot, event.error)
                    )
                }
        finally:
            poller.unsubscribe(queue)

    return EventSourceResponse(event_generator())
