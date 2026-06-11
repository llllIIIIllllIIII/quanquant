"""FastAPI application factory.

The shared QuotePoller is started once in the lifespan and exposed on app.state,
so every SSE client (browser tab) is served by a single upstream poll loop.
"""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from sqlmodel import Session

from quanquant.candles.builder import CandleBuilder
from quanquant.candles.repo import prune_quotes, upsert_candles
from quanquant.config import get_settings
from quanquant.db.engine import get_engine, init_db
from quanquant.db.models import Quote
from quanquant.poller import QuotePoller
from quanquant.sources.registry import make_source
from quanquant.web.routers import candles, dashboard, stats, trades
from quanquant.web.templating import STATIC_DIR


async def _persist_market_data(poller: QuotePoller, symbol: str) -> None:
    """Subscribe to the poller; store each raw quote AND build/upsert 1m candles.

    Only the web server persists (the CLI stays read-only). Writes are cheap and
    independent of request sessions; SQLite WAL handles the concurrency.
    """
    builder = CandleBuilder(symbol)
    queue = poller.subscribe()
    try:
        while True:
            event = await queue.get()
            if event.snapshot is None:
                continue
            snap = event.snapshot
            try:
                with Session(get_engine()) as session:
                    session.add(
                        Quote(
                            symbol=snap.symbol,
                            price=snap.price,
                            volume=snap.volume,
                            fetched_at=snap.fetched_at.replace(tzinfo=None),
                        )
                    )
                    session.commit()
                rows = builder.on_snapshot(snap)
                if rows:
                    with Session(get_engine()) as session:
                        upsert_candles(session, rows)
            except Exception:
                pass  # never let a write error stop the market-data feed
    finally:
        poller.unsubscribe(queue)


async def _prune_quotes_loop() -> None:
    """Prune old raw quotes at startup and then every 24h (candles are canonical)."""
    retention_days = get_settings().quote_retention_days
    if retention_days <= 0:
        return
    while True:
        try:
            cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
                days=retention_days
            )
            with Session(get_engine()) as session:
                prune_quotes(session, cutoff)
        except Exception:
            pass
        await asyncio.sleep(24 * 3600)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    settings = get_settings()
    source = make_source(settings.source)
    poller = QuotePoller(source, settings.symbol, settings.poll_interval_seconds)
    app.state.poller = poller
    tasks = [
        asyncio.create_task(poller.run()),
        asyncio.create_task(_persist_market_data(poller, settings.symbol)),
        asyncio.create_task(_prune_quotes_loop()),
    ]
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        await source.close()


def create_app() -> FastAPI:
    app = FastAPI(title="QuanQuant", lifespan=lifespan)
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    app.include_router(dashboard.router)
    app.include_router(trades.router)
    app.include_router(stats.router)
    app.include_router(candles.router)
    return app


def run() -> None:
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "quanquant.web.app:create_app",
        factory=True,
        host=settings.host,
        port=settings.port,
        reload=False,
    )
