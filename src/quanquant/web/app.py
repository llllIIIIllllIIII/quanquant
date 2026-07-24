"""FastAPI application factory.

The shared QuotePoller is started once in the lifespan and exposed on app.state,
so every SSE client (browser tab) is served by a single upstream poll loop.
"""
import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import Depends, FastAPI
from fastapi.staticfiles import StaticFiles
from sqlmodel import Session

from quanquant.alerts.engine import run_alert_engine
from quanquant.broker.lifecycle import run_confirm_token_cleanup, shutdown_order_subsystem
from quanquant.broker.preflight import order_subsystem_preflight
from quanquant.broker.session_state import OrderSessionState
from quanquant.broker.watchdog import run_order_watchdog
from quanquant.candles.builder import CandleBuilder
from quanquant.candles.repo import prune_quotes, upsert_candles
from quanquant.config import Settings, get_settings
from quanquant.db.engine import get_engine, init_db
from quanquant.db.models import Quote
from quanquant.notify import TelegramNotifier, build_notify
from quanquant.poller import QuoteEvent, QuotePoller
from quanquant.pulse.engine import PulseEngine, run_pulse_engine
from quanquant.pulse.prefs import load_telegram_enabled
from quanquant.sources.registry import make_source
from quanquant.web.deps import get_current_user
from quanquant.web.routers import alerts, candles, dashboard, health, stats, trades
from quanquant.web.routers import admin as admin_routes
from quanquant.web.routers import auth as auth_routes
from quanquant.web.routers import pulse as pulse_routes
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


async def _start_order_subsystem(app: FastAPI, settings: Settings, tasks: list) -> None:
    """Task 8 readiness gate：ORDER_MODE 拼錯（`order_subsystem_preflight` raise
    RuntimeError）只讓下單子系統停用並反映在 `/healthz`——**不**讓整個 app 起不來，行情/日誌
    等其餘系統仍正常啟動。其餘軟性停用原因（缺 key/owner/CA）同樣只停用下單子系統。

    `app.state.order_service` 只有在 `adapter.connect()` 真正成功後才會被 publish（fail
    closed）；connect 失敗直接標 unhealthy 並 return，不留 detached task 吞例外。
    """
    order_state = OrderSessionState()
    app.state.order_session_state = order_state
    app.state.order_service = None
    app.state.order_risk_guard = None
    app.state.order_inbox_worker = None

    try:
        order_enabled, order_disabled_reason = order_subsystem_preflight(settings)
    except RuntimeError as exc:
        order_state.mark_unhealthy(str(exc))
        log.error("下單子系統設定錯誤，下單子系統停用（app 其餘功能正常）: %s", exc)
        return

    if not order_enabled:
        order_state.mark_unhealthy(order_disabled_reason or "下單子系統未啟用")
        log.info("下單子系統未啟用: %s", order_disabled_reason)
        return

    from decimal import Decimal

    from quanquant.broker.inbox_worker import RawInboxWorker
    from quanquant.broker.risk import RiskGuard, parse_owner_ids, parse_whitelist
    from quanquant.broker.shioaji_adapter import ShioajiAdapter
    from quanquant.broker.supervisor import BrokerSupervisor

    supervisor = BrokerSupervisor()

    def _order_session() -> Session:
        return Session(get_engine())

    risk_guard = RiskGuard(
        session_factory=_order_session, secret=settings.session_secret or "dev-only-insecure",
        owner_user_ids=parse_owner_ids(settings.order_owner_user_ids),
        symbol_whitelist=parse_whitelist(settings.order_symbol_whitelist),
        max_qty_per_order=settings.order_max_qty_per_order,
        max_qty_per_day=settings.order_max_qty_per_day,
        max_orders_per_day=settings.order_max_orders_per_day,
        confirm_token_ttl_seconds=settings.order_confirm_token_ttl_seconds,
        kill_switch_initial=settings.order_kill_switch_initial,
    )
    adapter = ShioajiAdapter(
        api_key=settings.shioaji_trade_api_key, secret_key=settings.shioaji_trade_secret_key,
        ca_path=settings.shioaji_ca_path or None, ca_passwd=settings.shioaji_ca_passwd or None,
        person_id=settings.shioaji_person_id or None, symbol=settings.symbol,
        mode=settings.order_mode, session_factory=_order_session, supervisor=supervisor,
        risk_guard=risk_guard, sim_fee_per_lot=Decimal(settings.order_sim_fee_per_lot),
    )
    inbox_worker = RawInboxWorker(
        session_factory=_order_session, supervisor=supervisor,
        deal_mapper=adapter._map_deal_report, order_report_mapper=adapter._map_order_report,
    )

    try:
        await adapter.connect()
    except Exception as exc:
        # readiness gate（round3）：connect 未成功不 publish app.state.order_service，
        # fail closed，不留 detached task 吞例外。
        order_state.mark_unhealthy(f"connect 失敗，下單子系統停用: {exc}")
        log.error("下單子系統 connect 失敗（fail closed）: %s", exc)
        return

    order_state.mark_ready()
    app.state.order_service = adapter
    app.state.order_risk_guard = risk_guard
    app.state.order_inbox_worker = inbox_worker
    tasks.append(asyncio.create_task(inbox_worker.run()))
    tasks.append(asyncio.create_task(run_order_watchdog(
        adapter, order_state, interval=settings.order_watchdog_interval_seconds,
        login_min_interval=settings.order_login_min_interval_seconds,
        unquarantine_after_seconds=settings.order_unquarantine_after_seconds,
        unknown_reconcile_grace_seconds=settings.order_unknown_reconcile_grace_seconds,
    )))
    tasks.append(asyncio.create_task(run_confirm_token_cleanup(
        _order_session, interval=settings.order_confirm_token_cleanup_interval_seconds,
    )))
    log.info("下單子系統就緒：mode=%s account=%s", adapter.mode, adapter.account)


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

    # Market Pulse: classify tick velocity off the un-coalesced poller stream;
    # the level is stamped onto the quote SSE, Telegram fires on entering Extreme.
    pulse = None
    if settings.pulse_enabled:
        pulse = PulseEngine(
            settings.symbol,
            telegram=TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id),
            telegram_level=settings.pulse_telegram_level,
            telegram_cooldown=settings.pulse_telegram_cooldown,
        )
    if pulse is not None:
        # Telegram on/off: persisted web toggle wins; else the config default.
        with Session(get_engine()) as db:
            saved = load_telegram_enabled(db, settings.symbol)
        pulse.telegram_enabled = (
            settings.pulse_telegram_enabled if saved is None else saved
        )
    app.state.pulse = pulse

    tasks = [
        asyncio.create_task(_persist_market_data(
            poller, settings.symbol,
            quote_write_min_interval=settings.quote_write_min_interval,
        )),
        asyncio.create_task(_prune_quotes_loop()),
        asyncio.create_task(run_alert_engine(poller, notify, settings.symbol)),
    ]
    if pulse is not None:
        tasks.append(asyncio.create_task(run_pulse_engine(poller, pulse)))
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

    await _start_order_subsystem(app, settings, tasks)

    try:
        yield
    finally:
        # round3 #17：shutdown sentinel——原子關 ingress（先登出，斷線後底層才不會再有新的
        # native callback 落地）→ 等 RawInboxWorker 把已落地的 batch 真正處理完，這一步必須
        # 在下面「一次性 cancel 所有背景 task」之前完成，否則 worker.run() 的迴圈可能被砍在
        # 一半、drain 就沒有意義了。逾時只會標 unhealthy，不影響其餘系統的正常關閉。
        order_ok = await shutdown_order_subsystem(
            order_service=getattr(app.state, "order_service", None),
            inbox_worker=getattr(app.state, "order_inbox_worker", None),
            state=getattr(app.state, "order_session_state", None),
            timeout=5.0,
        )
        if not order_ok:
            log.error("下單子系統 shutdown 未完全成功（已標 unhealthy；資料仍安全留在 DB，未遺失）")
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

    app.include_router(auth_routes.router)    # public: /login, /logout
    app.include_router(health.router)         # public: /healthz

    protected = [Depends(get_current_user)]
    app.include_router(dashboard.router, dependencies=protected)
    app.include_router(trades.router, dependencies=protected)
    app.include_router(stats.router, dependencies=protected)
    app.include_router(candles.router, dependencies=protected)
    app.include_router(alerts.router, dependencies=protected)
    app.include_router(pulse_routes.router, dependencies=protected)
    app.include_router(admin_routes.router)   # self-guarded: require_admin
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
