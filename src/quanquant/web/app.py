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
from quanquant.broker.order_events import OrderEventHub
from quanquant.broker.preflight import order_subsystem_preflight
from quanquant.broker.redaction import redact_secrets
from quanquant.broker.session_state import OrderSessionState
from quanquant.broker.watchdog import run_order_watchdog
from quanquant.candles.builder import CandleBuilder
from quanquant.candles.market_calendar import session_now
from quanquant.candles.repo import prune_quotes, upsert_candles
from quanquant.config import Settings, get_settings
from quanquant.db.engine import get_engine, init_db
from quanquant.db.models import Quote
from quanquant.market_hours import CST
from quanquant.notify import TelegramNotifier, build_notify
from quanquant.notify.ops_alerter import build_ops_alerter
from quanquant.poller import QuoteEvent, QuotePoller
from quanquant.pulse.engine import PulseEngine, run_pulse_engine
from quanquant.pulse.prefs import load_telegram_enabled
from quanquant.sources.registry import make_source
from quanquant.web.deps import get_current_user
from quanquant.web.routers import alerts, candles, dashboard, health, stats, trades
from quanquant.web.routers import admin as admin_routes
from quanquant.web.routers import agent_ws as agent_ws_routes
from quanquant.web.routers import auth as auth_routes
from quanquant.web.routers import orders as orders_routes
from quanquant.web.routers import pulse as pulse_routes
from quanquant.web.templating import STATIC_DIR

log = logging.getLogger(__name__)


def _write_market_rows(rows, quote_row) -> None:
    """同步 DB 寫入（candle upsert + 可選 Quote insert），設計成在 threadpool 執行——讓
    event loop 在等 SQLite 寫鎖時仍能 fan-out tick / 推報價 SSE，避免「K 線與價格都停住」。
    保留原本兩段各自的寫入語意（upsert_candles 內部自行 commit；Quote 另開 session commit）。"""
    if rows:
        with Session(get_engine()) as session:
            upsert_candles(session, rows)
    if quote_row is not None:
        with Session(get_engine()) as session:
            session.add(quote_row)
            session.commit()


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
    last_write_error_log = 0.0
    try:
        while True:
            event = await queue.get()
            if event.snapshot is None:
                continue
            snap = event.snapshot
            try:
                rows = builder.on_snapshot(snap)  # every tick → accurate OHLCV（CPU，留在 loop）
                now = time.monotonic()
                quote_row = None
                if now - last_quote_write >= quote_write_min_interval:
                    last_quote_write = now
                    quote_row = Quote(
                        symbol=snap.symbol,
                        price=snap.price,
                        volume=snap.volume,
                        fetched_at=snap.fetched_at.replace(tzinfo=None),
                    )
                if rows or quote_row is not None:
                    # DB 寫入丟 threadpool：SQLite 寫鎖等待不再凍住 event loop（tick fan-out /
                    # 報價 SSE 續跑）。await 之後才取下一筆，故寫入仍依 tick 順序序列化、K 棒正確。
                    await asyncio.to_thread(_write_market_rows, rows, quote_row)
            except Exception:
                # 不讓寫入錯誤停掉行情 feed（韌性），但不再靜默吞掉——節流每 30s 記一次完整
                # traceback，否則正準 candle store 寫入失敗會全無觀測性（Tier0 C#3）。
                now = time.monotonic()
                if now - last_write_error_log >= 30.0:
                    last_write_error_log = now
                    log.exception("市場資料寫入失敗，已略過此筆（每 30s 記一次）")
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


def _feed_stale_check(poller, ops_alerter, threshold: float, session_open_fn) -> None:
    """單次判定（抽出以利測試，不必真跑無限 loop）：**僅在交易時段**且 poller 報價停滯逾
    `threshold` 秒時，才發 feed_stale 告警。休市（週末/夜盤收盤後/盤間）時 `session_open_fn()`
    回 False → 直接返回，避免每晚對著關閉的市場狂噴告警（本項驗收重點）。"""
    if poller is None or ops_alerter is None:
        return
    if not session_open_fn():
        return
    if poller.is_stale(threshold):
        ops_alerter.feed_stale(age_seconds=poller.seconds_since_snapshot())


async def run_feed_watchdog(
    poller, ops_alerter, *, threshold: float, interval: float = 30.0, session_open_fn
) -> None:
    """盤中報價停滯 watchdog（T0.3）：每 `interval` 秒做一次 `_feed_stale_check`。判定/告警
    任何例外一律吞掉並記 log，不讓 watchdog 自己掛掉。"""
    while True:
        try:
            _feed_stale_check(poller, ops_alerter, threshold, session_open_fn)
        except Exception:
            log.exception("feed watchdog 判定失敗（已吞，續跑）")
        await asyncio.sleep(interval)


async def _scan_orphan_orders_once(session_factory, ops_alerter) -> None:
    """T0.2：surface 孤兒委託（pending/sending + 無券商識別碼）——process 曾在送單落地前崩潰，
    券商端**可能已收單/成交**，故不自動改狀態/釋放配額（會少算曝險→過度交易），只明顯記錄
    交人工/reconcile 對照券商端後收尾。開機當下任何 pending+NULL 都是前次執行殘留的孤兒。

    in-process／agent 兩種通道共用（Task 8）：in-process 在 connect 成功後原位呼叫；
    agent 分支沒有 native connect 時機，改在 wiring 完成時排一次同樣的 one-shot task。
    """
    try:
        from quanquant.broker import repository as _brepo
        with session_factory() as _s:
            orphans = _brepo.list_pending_orphans_older_than(
                _s, older_than=datetime.now(timezone.utc).replace(tzinfo=None)
            )
        if orphans:
            log.warning(
                "偵測到 %d 筆孤兒委託（送單落地前崩潰，券商端可能已成交，未自動處理）：client_order_id=%s"
                "——請對照券商端後手動收尾其保留配額",
                len(orphans), [o.client_order_id for o in orphans],
            )
    except Exception:
        log.exception("孤兒委託掃描失敗")


async def _start_agent_channel_subsystem(
    app: FastAPI, settings: Settings, tasks: list, order_state: OrderSessionState, ops_alerter,
) -> None:
    """ORDER_CHANNEL=agent 分支：Shioaji I/O 交給使用者本機 broker agent，經 `/ws/agent`
    上下行；server 端仍是唯一決策者（風控/冪等/配額全在這裡，未變）。

    Increment 0 硬限制：僅支援 `ORDER_MODE=sim`（agent 端目前只做模擬撮合，real 走 CA 簽署
    尚未實作）；未設定 owner id 是刻意停用（非故障，/healthz 仍 200）。D2：agent WS 連線
    驗證改為 per-user DB opaque token（`auth/agent_tokens.py`），不再有站台層級的靜態密鑰
    需要在這裡檢查——沒 owner id 就沒有人能被判定為 owner，故仍需 owner id 檢查。

    Task 6（D10）：wiring 前先呼叫 `backfill_account_bindings`——用既有 `Order` 歷史灌
    `agent_account_bindings` 初始資料；衝突（同帳號歷史上屬於多個 user）→ 子系統拒啟
    （fail closed，見下方呼叫處註解）。

    Task 7（D1/D9）：Inc0 的單一全域 `AgentChannel`/`ShioajiAdapter`/`OrderSessionState`
    拆成 per-owner 的 `UserAgentSlot`——`app.state.agent_registry` 取代
    `app.state.agent_channel`；`RiskGuard` 仍是單一共享實例（D3：kill switch 的 per-user
    狀態只是它內部一個 `dict`，不需要拆實例）；每個 slot 各自一份
    `BrokerSupervisor`/`AgentChannel`/`AgentNativeGateway`/`ShioajiAdapter`/
    `OrderSessionState`/`RawInboxWorker`/agent watchdog——跨 user 完全無共享可變 runtime
    狀態（I8：A 的 offline/quarantine/慢 reconcile 不影響 B）。healthz 語意改變（D9）：
    這裡的 `order_state`（全站唯一，`app.state.order_session_state`）現在只代表「wiring
    有沒有完成」，不再跟著任何一個使用者的連線狀態切換 ready/disabled——wiring 一旦成功就
    `mark_ready()`，個別 slot 的連線狀態改進 `slot.session_state`（只餵 UI／
    `orders_agent_status`，不再進 /healthz 判定，見 web/routers/health.py 不需要因此改動）。
    """
    from decimal import Decimal

    from quanquant.broker.agent_channel import AgentChannel, AgentNativeGateway
    from quanquant.broker.agent_registry import AgentRegistry, UserAgentSlot
    from quanquant.broker.inbox_worker import RawInboxWorker
    from quanquant.broker.lifecycle import run_confirm_token_cleanup
    from quanquant.broker.repository import BackfillConflictError, backfill_account_bindings
    from quanquant.broker.risk import RiskGuard, parse_owner_ids, parse_whitelist
    from quanquant.broker.shioaji_adapter import ShioajiAdapter
    from quanquant.broker.supervisor import BrokerSupervisor
    from quanquant.broker.watchdog import run_agent_watchdog

    if settings.order_mode != "sim":
        order_state.mark_unhealthy("agent 通道 Increment 0 僅支援 ORDER_MODE=sim")
        return
    owner_ids = parse_owner_ids(settings.order_owner_user_ids)
    if not owner_ids:
        order_state.mark_disabled("order_owner_user_ids 未設定，下單子系統未啟用")
        return

    def _order_session() -> Session:
        return Session(get_engine())

    try:
        backfill_account_bindings(_order_session)
    except BackfillConflictError as exc:
        # D10/R1-8：既有 Order 歷史 ownership 衝突——不能讓「先綁先贏」隨機覆蓋，這是真的
        # 資料衝突需要人工裁決，但不是「app 起不來」等級的故障（比照既有 preflight 軟停用
        # vs 拒啟語意）：order_state 標 disabled（不是 mark_unhealthy）＋記明確錯誤，讓下單
        # 子系統拒啟（fail closed，不繼續往下 wiring registry/slot），其餘
        # 子系統（行情/日誌/一般路由）仍正常啟動，/healthz 仍可回 200，不崩整站。
        order_state.mark_disabled(f"帳號綁定 backfill 衝突，agent 下單子系統拒啟: {exc}")
        log.error("agent 通道帳號綁定 backfill 衝突，子系統拒啟（app 其餘功能正常）: %s", exc)
        return

    # D3：RiskGuard 單一共享實例——kill switch/quota/confirm token 全站一份（per-user 只有
    # KillSwitchState.per_user 這個 dict 帶 per-user 概念，見 risk.py），所有 slot 的
    # adapter 共用同一個 instance。
    risk_guard = RiskGuard(
        session_factory=_order_session,
        secret=settings.session_secret or "dev-only-insecure",
        owner_user_ids=owner_ids,
        symbol_whitelist=parse_whitelist(settings.order_symbol_whitelist),
        max_qty_per_order=settings.order_max_qty_per_order,
        max_qty_per_day=settings.order_max_qty_per_day,
        max_orders_per_day=settings.order_max_orders_per_day,
        confirm_token_ttl_seconds=settings.order_confirm_token_ttl_seconds,
        kill_switch_initial=settings.order_kill_switch_initial,
    )

    registry = AgentRegistry()
    inbox_workers: list[RawInboxWorker] = []
    for uid in sorted(owner_ids):
        slot_supervisor = BrokerSupervisor()
        channel = AgentChannel()
        gateway = AgentNativeGateway(channel,
                                     timeout_seconds=settings.agent_command_timeout_seconds)
        adapter = ShioajiAdapter(
            api_key="", secret_key="", ca_path=None, ca_passwd=None, person_id=None,
            symbol=settings.symbol, mode="sim", session_factory=_order_session,
            supervisor=slot_supervisor, risk_guard=risk_guard,
            sim_fee_per_lot=Decimal(settings.order_sim_fee_per_lot),
            ops_alerter=ops_alerter, remote_gateway=gateway,
            agent_user_id=uid,  # D5：per-slot scope 蓋章——沒有這行 _stage_reconcile_results/
            # _persist_raw 落地的 RawInbox.user_id 恆為 None，per-slot RawInboxWorker（WHERE
            # user_id=slot.user_id）永遠認領不到，資料孤兒化（Task 7 docstring 早已宣稱會傳，
            # 但實際程式碼漏掉，見 task-8-report.md「已知落差」#4，本次補上）。
            agent_command_expiry_seconds=settings.agent_command_expiry_seconds,  # Task 10：
            # 沒有這行 ShioajiAdapter 會退回模組層預設常數（120，與設定預設值恰好相同，但
            # 不會隨部署端調整設定而改變），見 config.py 該欄位註解。
        )
        session_state = OrderSessionState()
        session_state.mark_disabled("agent 未連線")     # 等這個 user 的 agent 上線
        slot_inbox_worker = RawInboxWorker(
            session_factory=_order_session, supervisor=slot_supervisor,
            deal_mapper=adapter._map_deal_report,
            order_report_mapper=adapter._map_order_report,
            order_events=getattr(app.state, "order_events", None),
            ops_alerter=ops_alerter, user_id=uid,   # D6：批次查詢 WHERE user_id = uid
        )
        slot = UserAgentSlot(
            user_id=uid, channel=channel, gateway=gateway, adapter=adapter,
            session_state=session_state, supervisor=slot_supervisor, tasks=[],
        )
        slot.tasks.append(asyncio.create_task(slot_inbox_worker.run()))
        slot.tasks.append(asyncio.create_task(run_agent_watchdog(
            adapter, user_id=uid,
            unquarantine_after_seconds=settings.order_unquarantine_after_seconds,
        )))
        registry.add(slot)
        inbox_workers.append(slot_inbox_worker)
        tasks.extend(slot.tasks)

    app.state.agent_registry = registry
    app.state.agent_inbox_workers = inbox_workers   # 供 lifespan shutdown 逐 slot drain（Task 7）
    app.state.order_risk_guard = risk_guard
    app.state.order_session_factory = _order_session
    # D9：healthz 語意——子系統 wiring 完成即 ready（200）；個別 slot 的連線狀態不再進這裡，
    # 只反映在 slot.session_state（UI／orders_agent_status）。
    order_state.mark_ready()
    # D6：confirm-token 清理是純 DB 全域工作（不分 user），agent 模式 Inc0 漏掉了一個，
    # 這裡補一個全域 task（in-process 分支本來就有，見 `_start_order_subsystem` 對應行）。
    tasks.append(asyncio.create_task(run_confirm_token_cleanup(
        _order_session, interval=settings.order_confirm_token_cleanup_interval_seconds,
    )))
    # T0.2：孤兒委託掃描本就是全站一次性、跨所有 owner 的 Order 可視性動作，不需要按 slot
    # 各跑一次；agent 分支沒有 native connect 時機可以掛「connect 成功後跑一次」，故在
    # wiring 完成時直接排一次 one-shot 掃描。
    tasks.append(asyncio.create_task(_scan_orphan_orders_once(_order_session, ops_alerter)))
    log.info("agent 通道下單子系統已配線（%d 位 owner，各自獨立 slot），等待各自的本機 broker "
             "agent 連線", len(owner_ids))


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
    ops_alerter = getattr(app.state, "ops_alerter", None)  # T0.3：lifespan 已建好，這裡取用傳遞

    if settings.order_channel not in ("inprocess", "agent"):
        order_state.mark_unhealthy(f"ORDER_CHANNEL 設定錯誤: {settings.order_channel!r}")
        return
    if settings.order_channel == "agent":
        await _start_agent_channel_subsystem(app, settings, tasks, order_state, ops_alerter)
        return

    try:
        order_enabled, order_disabled_reason = order_subsystem_preflight(settings)
    except RuntimeError as exc:
        order_state.mark_unhealthy(str(exc))
        log.error("下單子系統設定錯誤，下單子系統停用（app 其餘功能正常）: %s", exc)
        return

    if not order_enabled:
        # 刻意停用（缺 key/owner/CA）：非故障，/healthz 仍算健康（200）。設定錯（上面的
        # RuntimeError 分支）才是 mark_unhealthy → /healthz 回 503。
        order_state.mark_disabled(order_disabled_reason or "下單子系統未啟用")
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
        ops_alerter=ops_alerter,
    )
    inbox_worker = RawInboxWorker(
        session_factory=_order_session, supervisor=supervisor,
        deal_mapper=adapter._map_deal_report, order_report_mapper=adapter._map_order_report,
        order_events=getattr(app.state, "order_events", None),
        ops_alerter=ops_alerter,
    )

    try:
        await adapter.connect()
    except Exception as exc:
        # readiness gate（round3）：connect 未成功不 publish app.state.order_service，
        # fail closed，不留 detached task 吞例外。
        # F8：connect() 內部呼叫 shioaji login/activate_ca，例外原文可能夾帶
        # api_key/secret_key/ca_passwd/person_id——這則訊息會存進 order_state.last_error，
        # 經 /healthz（未認證公開端點，見 web/routers/health.py）直接回顯給任何呼叫者，
        # redact 是這裡不可省略的一步（不只 log，還包括對外回應）。
        message = redact_secrets(
            f"connect 失敗，下單子系統停用: {exc}", secrets=getattr(adapter, "secrets_to_redact", [])
        )
        order_state.mark_unhealthy(message)
        log.error("下單子系統 connect 失敗（fail closed）: %s", message)
        # T0.3 告警（純疊加）：務必用已 redact 的 message（原始 exc 可能夾帶
        # api_key/ca_passwd/person_id），不可用原始 exc。
        if ops_alerter is not None:
            ops_alerter.connect_failed(message)
        return

    order_state.mark_ready()
    app.state.order_service = adapter
    app.state.order_risk_guard = risk_guard
    app.state.order_inbox_worker = inbox_worker
    tasks.append(asyncio.create_task(inbox_worker.run()))
    # T0.2：開機做一次 best-effort reconcile——原本 reconcile 只在斷線重連後才跑（watchdog），
    # 正常開機不會補回停機期間券商端的委託/狀態變更。失敗不擋啟動（watchdog 後續仍會補）；
    # sim 下 list_trades() 空、no-op。
    try:
        await adapter.reconcile()
    except Exception as exc:
        log.warning(
            "開機 reconcile 失敗（不擋啟動，watchdog 後續重試）: %s",
            redact_secrets(str(exc), secrets=getattr(adapter, "secrets_to_redact", [])),
        )
    # T0.2：surface 孤兒委託（Task 8 抽成模組級 helper，agent 分支共用）。
    await _scan_orphan_orders_once(_order_session, ops_alerter)
    tasks.append(asyncio.create_task(run_order_watchdog(
        adapter, order_state, interval=settings.order_watchdog_interval_seconds,
        login_min_interval=settings.order_login_min_interval_seconds,
        unquarantine_after_seconds=settings.order_unquarantine_after_seconds,
        unknown_reconcile_grace_seconds=settings.order_unknown_reconcile_grace_seconds,
        ops_alerter=ops_alerter,
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

    # T0.3 營運告警管道（獨立 dev chat，與 3 人共用的價格警示分離；未設定則整體 no-op）。
    ops_alerter = build_ops_alerter(settings, mode=settings.order_mode)
    ops_alerter.attach_loop(asyncio.get_running_loop())
    app.state.ops_alerter = ops_alerter

    use_shioaji = bool(
        settings.source == "shioaji"
        and settings.shioaji_api_key
        and settings.shioaji_secret_key
    )
    # MIS is the live source when polling, and the fallback when streaming.
    source = make_source("taifex" if use_shioaji else settings.source)
    poller = QuotePoller(source, settings.symbol, settings.poll_interval_seconds)
    app.state.poller = poller
    # 委託/成交/部位變動的 SSE ping hub：無條件建立（即使下單子系統停用，/orders/stream
    # 端點也有 hub 可訂閱，只是永不觸發），供 RawInboxWorker 發布、/orders/stream 訂閱。
    app.state.order_events = OrderEventHub()

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

    # T0.3 盤中報價停滯 watchdog：session_open_fn 複用 calendar-aware 的 `session_now`
    # （market_calendar），休市（週末/假日/夜盤收盤後/盤間）一律回 None→False，不誤噴告警。
    tasks.append(asyncio.create_task(run_feed_watchdog(
        poller, ops_alerter, threshold=settings.feed_stale_alert_seconds,
        session_open_fn=lambda: session_now(datetime.now(CST)) is not None,
    )))

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
            # Task 7：agent 模式改用 registry——沒有單一 order_service/inbox_worker 可關，
            # 逐 slot 各自 close()/drain（見 shutdown_order_subsystem 的 registry/
            # inbox_workers 參數）。兩組互斥（in-process 用前兩個 kwarg，agent 用這兩個），
            # 但同時傳兩組彼此不影響。
            registry=getattr(app.state, "agent_registry", None),
            inbox_workers=getattr(app.state, "agent_inbox_workers", None),
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
    app.include_router(agent_ws_routes.router)  # public: /ws/agent（token 認證，非 cookie）

    protected = [Depends(get_current_user)]
    app.include_router(dashboard.router, dependencies=protected)
    app.include_router(trades.router, dependencies=protected)
    app.include_router(stats.router, dependencies=protected)
    app.include_router(candles.router, dependencies=protected)
    app.include_router(alerts.router, dependencies=protected)
    app.include_router(pulse_routes.router, dependencies=protected)
    app.include_router(orders_routes.router, dependencies=protected)
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
