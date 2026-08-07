"""下單子系統 watchdog（Task 8）：序列化 health probe → 掛了就重連（login 節流 + 指數
backoff）→ 重連成功後 `adapter.reconcile()` 對帳（拉券商委託補回 RawInbox，走 adapter 內
的持久 cursor 通道，round3 #2）。較慢週期額外做兩件事：

1. `unquarantine_stale_raw_inbox`：給舊 quarantine 列一次補救重試機會。
2. 「quota unknown reconcile」：`Order.status == "unknown"`（送單/改單結果不明，見
   ShioajiAdapter.place/update 的 except 分支）且卡了超過 grace period 的委託——若從未拿到
   任何券商識別碼（ordno/broker_order_id 皆 NULL），代表 native 呼叫幾乎確定沒送達券商，
   判定失敗並釋放對應的 QuotaReservation；委託身分已知（有 ordno/broker_order_id，多半是
   update 造成的 unknown，委託本身早已存在）時，委託本身狀態仍留給 reconcile() 的
   order_report pipeline 自然解決（不猜測狀態），**但**這裡會另外收尾 update-path 為 delta
   建立的保留列（round3 獨立驗收殘留1）：查出該委託目前仍 `reserved` 的 update 保留列
   （`repository.list_reserved_update_reservations`），呼叫
   `adapter._query_order_qty_blocking` 直接向券商查詢這筆委託目前的真實口數，與「改單前
   口數」（`order.qty`，因為 unknown 分支不會覆寫它）/「改單後目標口數」（`order.qty +
   reservation.qty`）比對：吻合改單後目標 → 改單其實生效，`confirm_quota`；吻合改單前
   原值 → 改單其實沒生效，`release_quota`；兩者皆不符（含查無此委託、adapter 未實作查詢
   能力）一律不猜測，留到下一輪 grace period 後再試。`release_quota`/`confirm_quota` 皆是
   `UPDATE ... WHERE state='reserved'` 的原子一次性轉移，重複呼叫本身就是安全的 no-op，
   「不重複釋放/確認」不需要 watchdog 額外加鎖判斷。

round3 #10：health probe 用 `adapter.health_probe()`（序列化探測底層連線，不只看
`_api is not None`）；為相容沒有實作 `health_probe` 的極簡假 adapter（測試用），找不到這個
方法時 fallback 回舊行為。
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

from quanquant.broker import agent_commands
from quanquant.broker import repository as brepo
from quanquant.broker.redaction import redact_secrets
from quanquant.db.models import AgentCommand

log = logging.getLogger(__name__)

_MAX_BACKOFF_SECONDS = 300.0


async def _probe_healthy(adapter) -> bool:
    probe = getattr(adapter, "health_probe", None)
    if probe is None:
        # 相容沒有實作 health_probe 的極簡假 adapter（僅供測試）；正式 ShioajiAdapter 一律有
        # health_probe（round3 #10），這個分支不應該在正式環境走到。
        return getattr(adapter, "_api", None) is not None
    try:
        return bool(await probe())
    except Exception as exc:
        message = redact_secrets(str(exc), secrets=getattr(adapter, "secrets_to_redact", []))
        log.warning("watchdog health_probe 呼叫本身失敗（視為不健康）: %s", message)
        return False


async def run_order_watchdog(
    adapter,
    state,
    *,
    interval: float,
    login_min_interval: float,
    unquarantine_after_seconds: float = 300.0,
    unknown_reconcile_grace_seconds: float = 300.0,
    ops_alerter=None,
) -> None:
    backoff = interval
    last_login_monotonic = 0.0
    last_unquarantine_monotonic = 0.0
    last_unknown_reconcile_monotonic = 0.0

    while True:
        await asyncio.sleep(interval)
        healthy = await _probe_healthy(adapter)

        if healthy:
            backoff = interval
            state.mark_ready()
        else:
            since_last_login = time.monotonic() - last_login_monotonic
            if since_last_login < login_min_interval:
                await asyncio.sleep(login_min_interval - since_last_login)
            state.mark_unhealthy("connection lost, reconnecting")
            try:
                await adapter.connect()
                last_login_monotonic = time.monotonic()
                await adapter.reconcile()
                state.mark_ready()
                backoff = interval
            except Exception as exc:
                # F8：exc 可能是 shioaji login/activate_ca 拋出、原文夾帶 api_key/ca_passwd/
                # person_id 的例外——這裡的訊息會存進 state.last_error，經 /healthz（未認證
                # 公開端點）直接回顯，redact 不可省略。
                message = redact_secrets(str(exc), secrets=getattr(adapter, "secrets_to_redact", []))
                log.warning("watchdog 重連失敗: %s", message)
                state.mark_unhealthy(message)
                # T0.3 告警（純疊加）：重連失敗通知，訊息已 redact（可能夾帶 login/activate_ca
                # 的 api_key/ca_passwd/person_id）。OpsAlerter.connect_failed 自帶節流+吞錯。
                if ops_alerter is not None:
                    ops_alerter.connect_failed(message)
                state.reconnect_attempts += 1
                backoff = min(backoff * 2, _MAX_BACKOFF_SECONDS)
                await asyncio.sleep(backoff)
                continue

        now_monotonic = time.monotonic()
        if now_monotonic - last_unquarantine_monotonic >= max(unquarantine_after_seconds, interval):
            last_unquarantine_monotonic = now_monotonic
            try:
                await _retry_quarantined(adapter, unquarantine_after_seconds)
            except Exception as exc:
                log.warning(
                    "watchdog unquarantine 失敗: %s",
                    redact_secrets(str(exc), secrets=getattr(adapter, "secrets_to_redact", [])),
                )

        if now_monotonic - last_unknown_reconcile_monotonic >= max(unknown_reconcile_grace_seconds, interval):
            last_unknown_reconcile_monotonic = now_monotonic
            try:
                await _reconcile_unknown_quota(adapter, unknown_reconcile_grace_seconds)
            except Exception as exc:
                log.warning(
                    "watchdog unknown quota reconcile 失敗: %s",
                    redact_secrets(str(exc), secrets=getattr(adapter, "secrets_to_redact", [])),
                )


async def run_agent_watchdog(
    adapter, *, user_id: int, unquarantine_after_seconds: float, gateway=None,
) -> None:
    """agent 通道模式的精簡 watchdog：只做 DB-only 背景工作（+G3 的 query_qty round-trip）。
    Inc1 D6/D9：per-slot 跑一份（lifespan 對每個 owner 各起一個 task），`user_id` 是這個
    slot 的 owner——quarantine 解除嚴格 scope 到這個 user（`unquarantine_stale_raw_inbox` 的
    `RawInbox.user_id == user_id` 精確比對＋既有 reason=association_pending 篩選，見
    repository.py），不會誤解除/誤重試其他 user 的殘留，跨 user 完全互不影響（I8）。

    連線/重連/健康是 agent 端與 WS 端點的責任。`gateway`（Task 11）是這個 slot 的
    `AgentNativeGateway`：`_reconcile_unknown_quota_agent` 用它下行 `DownQueryQty` 收斂
    update unknown-resolver；`gateway=None`（呼叫端未接線，理論上不應該發生於正式部署，
    只是防禦性容錯）時整段 G3 收斂略過，維持 Inc0 保守後果（unknown 委託的配額維持保留、
    不會超賣）。supervisor.lock 在 agent 模式背後沒有 native → 即決策 5 的「獨立 server
    鎖」，且 Task 7 起這顆鎖是每個 slot 各自一份，A 卡住不會佔用 B 的鎖。
    """
    while True:
        await asyncio.sleep(unquarantine_after_seconds)
        try:
            await _retry_quarantined(adapter, unquarantine_after_seconds, user_id=user_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("agent watchdog：retry_quarantined 失敗（user_id=%s）", user_id)

        if gateway is None:
            continue
        try:
            await _reconcile_unknown_quota_agent(adapter, gateway, user_id=user_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("agent watchdog：unknown quota resolver 失敗（user_id=%s）", user_id)


async def _retry_quarantined(adapter, older_than_seconds: float, *, user_id: int | None = None) -> None:
    async with adapter.supervisor.lock:
        await asyncio.to_thread(_retry_quarantined_blocking, adapter, older_than_seconds, user_id)


def _retry_quarantined_blocking(adapter, older_than_seconds: float, user_id: int | None = None) -> None:
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=older_than_seconds)
    with adapter._session_factory() as session:
        n = brepo.unquarantine_stale_raw_inbox(session, older_than=cutoff, user_id=user_id)
        session.commit()
        if n:
            log.info("watchdog 解除 %d 筆 quarantine raw_inbox 待重試（user_id=%s）", n, user_id)


async def _reconcile_unknown_quota(adapter, grace_seconds: float) -> None:
    async with adapter.supervisor.lock:
        await asyncio.to_thread(_reconcile_unknown_quota_blocking, adapter, grace_seconds)


def _reconcile_unknown_quota_blocking(adapter, grace_seconds: float) -> None:
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=grace_seconds)
    query_qty = getattr(adapter, "_query_order_qty_blocking", None)
    with adapter._session_factory() as session:
        stuck = brepo.list_unknown_orders_older_than(session, older_than=cutoff)
        resolved = 0
        for order in stuck:
            if order.ordno is None and order.broker_order_id is None:
                # 從未拿到任何券商識別碼——native 呼叫幾乎確定沒送達，判定失敗並釋放配額。
                # release_quota 對已 confirmed/released 的列一律安全 no-op（不重複釋放）。
                brepo.mark_order_status(session, order, status="failed")
                brepo.release_quota(session, reservation_id=order.client_order_id)
                resolved += 1
                continue
            # 已有 ordno/broker_order_id：委託身分已知（多半是 update 造成的 unknown），
            # 委託本身狀態留給 reconcile() 的 order_report pipeline 自然解決，這裡不猜測。
            # 但要收尾 update-path 為 delta 建立的保留列（round3 殘留1）——否則會永遠卡
            # reserved，當日配額被逾時改單靜默侵蝕。沒有 ordno（只有裸 broker_order_id）
            # 或 adapter 沒實作查詢能力時，同樣不猜測，跳過留待下一輪。
            if order.ordno is None or query_qty is None:
                continue
            pending = brepo.list_reserved_update_reservations(session, client_order_id=order.client_order_id)
            if not pending:
                continue
            real_qty = query_qty(order.ordno)
            if real_qty is None:
                continue  # 券商目前清單查無此委託，無法判斷，不猜測
            for reservation in pending:
                target_qty = order.qty + reservation.qty  # order.qty 是改單前原值（unknown 分支未覆寫）
                if real_qty == target_qty:
                    # 改單其實生效（口數已是改單後目標值）→ 這筆 delta 保留永久計入已用配額。
                    if brepo.confirm_quota(session, reservation_id=reservation.reservation_id):
                        resolved += 1
                elif real_qty == order.qty:
                    # 改單其實沒生效（口數仍是改單前原值）→ 退還這筆 delta 保留。
                    if brepo.release_quota(session, reservation_id=reservation.reservation_id):
                        resolved += 1
                # 其餘：口數既非改單前也非改單後，無法判斷是哪次改單造成，不猜測，留待下一輪。
        session.commit()
        if resolved:
            log.info("watchdog 判定 %d 筆送單/改單結果不明的委託（或其保留列）完成配額收尾", resolved)


# ---------------------------------------------------------------------------
# agent 模式 G3 unknown-resolver（D8，Task 11）
# ---------------------------------------------------------------------------
#
# in-process 版（上面 `_reconcile_unknown_quota_blocking`）鍵在 `Order.status == "unknown"`
# 直接同步查 native；agent 模式沒有本機 native 可查（server 端這個 adapter instance 的
# `_native` 只是佔位 stub，真正的 Shioaji 連線在使用者本機的 agent 子程序），必須改鍵在
# `agent_commands` ledger 列（`resolved_at IS NULL`）並經 `gateway.query_qty()` 下行
# `DownQueryQty` 才能問到真實口數——這是兩套邏輯刻意分開、不共用同一個 blocking 函式的
# 原因（spec D8「in-process 模式行為不變」，agent 版邏輯只掛 agent watchdog）。
#
# place 的 outcome=unknown 且無 broker ID（D4/D8）：resolver 一律不碰、永不自動 release——
# 下面兩個查詢（`list_unresolved_unknown_updates`/`list_unresolved_cancels`）只認
# `kind IN ('update','cancel')`，`kind='place'` 的列天生不會出現在任何一個結果集裡，place
# 因此以「結構上不可能被觸碰」的方式滿足這條規則，不需要額外的排除判斷。


async def _reconcile_unknown_quota_agent(adapter, gateway, *, user_id: int) -> None:
    """per-slot watchdog 週期跑：slot 未 ready（agent 未連線/未登入）直接跳過本輪——沒有
    gateway 可查詢，硬查只會製造逾時噪音，且 D8 收尾前置規則本就要求「未 resolved 就不
    confirm/不 release」，跳過本輪不會有任何錯誤收尾風險。`adapter.supervisor.lock` 持鎖
    範圍涵蓋整輪（DB 查詢＋query_qty round-trip＋DB 寫回），與既有 in-process
    `_reconcile_unknown_quota`／agent 模式 `_retry_quarantined` 用同一顆鎖序列化這個 slot
    的背景工作一致（per-slot 各自一份，I8 不受影響；這顆鎖背後在 agent 模式沒有 native
    呼叫，不會與 WS 收訊迴圈的 `channel.resolve_query_result` 產生死鎖——見 module 頂部
    `run_agent_watchdog` docstring）。"""
    if not gateway.ready:
        return
    async with adapter.supervisor.lock:
        update_cmd_ids, cancel_cmd_ids = await asyncio.to_thread(
            _list_unknown_resolver_cmd_ids_blocking, adapter, user_id
        )
        resolved = 0
        for cmd_id in update_cmd_ids:
            if await _resolve_unknown_update_cmd(adapter, gateway, cmd_id):
                resolved += 1
        for cmd_id in cancel_cmd_ids:
            if await asyncio.to_thread(_resolve_unresolved_cancel_cmd_blocking, adapter, cmd_id):
                resolved += 1
        if resolved:
            log.info(
                "agent watchdog（user_id=%s）G3 unknown-resolver 完成 %d 筆收斂", user_id, resolved,
            )


def _list_unknown_resolver_cmd_ids_blocking(adapter, user_id: int) -> tuple[list[str], list[str]]:
    with adapter._session_factory() as session:
        updates = agent_commands.list_unresolved_unknown_updates(session, user_id=user_id)
        cancels = agent_commands.list_unresolved_cancels(session, user_id=user_id)
        return [r.cmd_id for r in updates], [r.cmd_id for r in cancels]


async def _resolve_unknown_update_cmd(adapter, gateway, cmd_id: str) -> bool:
    """update unknown-resolver 一列的完整處理：先讀 ordno（不含鎖外副作用的快速查詢）→
    `await gateway.query_qty(ordno)`（唯一離開本機的 I/O）→ 帶著查詢結果重新進 DB 交易做
    讀-判-寫（`resolve_update_via_query_qty` 是核心比對，見 agent_commands.py）。ordno 前後
    兩次各自開獨立 session/交易——`gateway.query_qty` 是 await 邊界，不能讓一個 SQLAlchemy
    Session 橫跨這段（ORM session 不是 async-safe 的長生命週期物件，同檔其餘函式一貫的
    『開 session→做完→關閉』慣例）。"""
    ordno = await asyncio.to_thread(_load_unresolved_update_ordno_blocking, adapter, cmd_id)
    if ordno is None:
        return False  # 已被別的路徑收斂（applier 搶先/row 消失），或列本身沒有 ordno（防禦性）
    real_qty = await gateway.query_qty(ordno)
    return await asyncio.to_thread(_apply_update_resolution_blocking, adapter, cmd_id, real_qty)


def _load_unresolved_update_ordno_blocking(adapter, cmd_id: str) -> str | None:
    with adapter._session_factory() as session:
        row = session.get(AgentCommand, cmd_id)
        if row is None or row.resolved_at is not None or row.ordno is None:
            return None
        return row.ordno


def _apply_update_resolution_blocking(adapter, cmd_id: str, real_qty: int | None) -> bool:
    with adapter._session_factory() as session:
        row = session.get(AgentCommand, cmd_id)
        if row is None or row.resolved_at is not None:
            return False  # 已被別的路徑收斂（同 watchdog 上一輪/applier），no-op
        order = brepo.find_order_by_ordno(
            session, broker=row.broker, account=row.account, mode=row.mode, ordno=row.ordno
        )
        if order is None:
            return False
        action = agent_commands.resolve_update_via_query_qty(
            session, row=row, order=order, real_qty=real_qty
        )
        if action == "left_pending":
            return False
        session.add(row)
        session.commit()
        return True


def _resolve_unresolved_cancel_cmd_blocking(adapter, cmd_id: str) -> bool:
    with adapter._session_factory() as session:
        row = session.get(AgentCommand, cmd_id)
        if row is None or row.resolved_at is not None or row.ordno is None:
            return False
        order = brepo.find_order_by_ordno(
            session, broker=row.broker, account=row.account, mode=row.mode, ordno=row.ordno
        )
        if order is None:
            return False
        applied = agent_commands.resolve_unresolved_cancel_via_report(session, row=row, order=order)
        if not applied:
            return False
        session.add(row)
        session.commit()
        return True
