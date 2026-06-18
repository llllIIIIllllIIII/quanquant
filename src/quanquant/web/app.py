"""FastAPI application factory.

The shared QuotePoller is started once in the lifespan and exposed on app.state,
so every SSE client (browser tab) is served by a single upstream poll loop.
"""
import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from sqlmodel import Session

from quanquant.alerts.engine import run_alert_engine
from quanquant.candles.builder import CandleBuilder
from quanquant.candles.repo import prune_quotes, upsert_candles
from quanquant.config import get_settings
from quanquant.db.engine import get_engine, init_db
from quanquant.db.models import Quote
from quanquant.notify import build_notify
from quanquant.poller import QuoteEvent, QuotePoller
from quanquant.sources.registry import make_source
from quanquant.web.routers import alerts, candles, dashboard, stats, trades
from quanquant.web.templating import STATIC_DIR

log = logging.getLogger(__name__)


async def _persist_market_data(
    poller: QuotePoller, symbol: str, *, quote_write_min_interval: float = 0.0
) -> None:
    """Subscribe to the poller; store each raw quote AND build/upsert 1m candles.

    Only the web server persists (the CLI stays read-only). Writes are cheap and
    independent of request sessions; SQLite WAL handles the concurrency.

    Under streaming (multi-tick/sec) the candle builder still consumes EVERY tick
    (so 1m OHLCV stays accurate and crash-safe), but raw Quote-row inserts are
    throttled to `quote_write_min_interval` — those rows are pruned samples, the
    candles are canonical, so writing one per tick is pure bloat.
    """
    builder = CandleBuilder(symbol)
    queue = poller.subscribe()
    last_quote_write = 0.0
    try:
        while True:
            event = await queue.get()
            if event.snapshot is None:
                continue
            snap = event.snapshot
            try:
                rows = builder.on_snapshot(snap)  # every tick → accurate OHLCV
                if rows:
                    with Session(get_engine()) as session:
                        upsert_candles(session, rows)
                now = time.monotonic()
                if now - last_quote_write >= quote_write_min_interval:
                    last_quote_write = now
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
            except Exception:
                pass  # never let a write error stop the market-data feed
    finally:
        poller.unsubscribe(queue)


async def _fallback_poll_loop(
    poller: QuotePoller, source, symbol: str, interval: float, stale_threshold: float
) -> None:
    """Poll TAIFEX MIS as a fallback, publishing only while the primary stream is
    silent (login pending, disconnect, or market closed). When Shioaji ticks are
    flowing, `is_stale` is False and this loop touches nothing — no double feed."""
    while True:
        if poller.is_stale(stale_threshold):
            try:
                snap = await source.fetch_snapshot(symbol)
                poller.publish(QuoteEvent(snapshot=snap, error=None, at=snap.fetched_at))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                poller.publish(
                    QuoteEvent(snapshot=None, error=str(exc), at=datetime.now(timezone.utc))
                )
        await asyncio.sleep(interval)


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
    notify = build_notify(settings)
    app.state.notify = notify

    use_shioaji = bool(
        settings.source == "shioaji"
        and settings.shioaji_api_key
        and settings.shioaji_secret_key
    )
    # MIS is the live source when polling, and the fallback when streaming.
    source = make_source("taifex" if use_shioaji else settings.source)
    poller = QuotePoller(source, settings.symbol, settings.poll_interval_seconds)
    app.state.poller = poller

    tasks = [
        asyncio.create_task(_persist_market_data(
            poller, settings.symbol,
            quote_write_min_interval=settings.quote_write_min_interval,
        )),
        asyncio.create_task(_prune_quotes_loop()),
        asyncio.create_task(run_alert_engine(poller, notify, settings.symbol)),
    ]
    streamer = None
    if use_shioaji:
        from quanquant.sources.shioaji_stream import ShioajiStreamer

        streamer = ShioajiStreamer(
            settings.shioaji_api_key, settings.shioaji_secret_key,
            settings.symbol, poller, asyncio.get_running_loop(),
        )
        tasks.append(asyncio.create_task(streamer.run()))
        tasks.append(asyncio.create_task(_fallback_poll_loop(
            poller, source, settings.symbol,
            settings.poll_interval_seconds, settings.shioaji_stale_seconds,
        )))
        log.info(
            "live source: Shioaji streaming (MIS fallback after %.0fs silence)",
            settings.shioaji_stale_seconds,
        )
    else:
        tasks.append(asyncio.create_task(poller.run()))

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
    app.include_router(alerts.router)
    return app


def run() -> None:
    import uvicorn

    # Surface app-level logs (Shioaji connect/reconnect, live-source choice) on the
    # dev entrypoint; uvicorn's own config doesn't add a root handler.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = get_settings()
    uvicorn.run(
        "quanquant.web.app:create_app",
        factory=True,
        host=settings.host,
        port=settings.port,
        reload=False,
    )
