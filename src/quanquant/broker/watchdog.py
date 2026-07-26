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

from quanquant.broker import repository as brepo
from quanquant.broker.redaction import redact_secrets

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


async def _retry_quarantined(adapter, older_than_seconds: float) -> None:
    async with adapter.supervisor.lock:
        await asyncio.to_thread(_retry_quarantined_blocking, adapter, older_than_seconds)


def _retry_quarantined_blocking(adapter, older_than_seconds: float) -> None:
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=older_than_seconds)
    with adapter._session_factory() as session:
        n = brepo.unquarantine_stale_raw_inbox(session, older_than=cutoff)
        session.commit()
        if n:
            log.info("watchdog 解除 %d 筆 quarantine raw_inbox 待重試", n)


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
