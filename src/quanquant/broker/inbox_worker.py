"""RawInboxWorker：從 RawInbox（durable spool）拉未處理列，逐列一交易完成
驗證→委託關聯解析（複合 scope）→ Deal 去重 → PositionTracker → Order 聚合更新 → processed=true。

零丟單（V3-2，修 BLOCKER#2）：`commit_raw_callback` 是給 callback thread（或任何排程協程）
呼叫的同步落地函式——用獨立 Session 把 raw payload 落地到 RawInbox 並立即 commit，**返回前
保證落地**，不能改成「排程一個協程晚點才 commit」的形式（那正是 round3 BLOCKER#2 指出的
漏洞：loop 未就緒/排程後 crash/等 supervisor lock 時 payload 仍會遺失）。callback 只負責
呼叫這個函式落地，之後 RawInboxWorker 才做真正的業務處理；沒有任何 volatile
`asyncio.Queue` 存在於這條路徑上，「QueueFull 丟單」這個攻擊面在架構上就不存在。

例外分類（重要）：
  - ValueError / PositionMismatchError：業務邏輯性失敗（payload 不合法、委託關聯解不到、
    Cover 缺對應開倉部位、Auto 歧義）→ rollback 該列的交易 → 單獨 quarantine
    （reason="association_pending"，可重試），不留部分寫入。
  - RawInboxDeadLetterError（Inc1 D5/R2-6）：scope/payload/user 蓋章驗證失敗——**確定**這列
    永遠不會因為重試而變成合法（帳號冒用/回報與蓋章帳號矛盾/委託歸屬不符本人），不是暫時性
    問題 → rollback 該列的交易 → quarantine 且 reason 為 `scope_violation`/`payload_mismatch`/
    `user_mismatch` 三者之一 → 永久 dead-letter（`quarantine_raw_inbox` 內部一併把
    processed=True，退出換帳號 guard 的 unprocessed 計數與 unquarantine 重試迴圈）。
  - 其餘任何例外：視為 transient（DB 短暫故障等）→ 該列保持 processed=False、quarantine=False，
    不判死刑，留給下一輪 batch 自然重試——這是「零丟單」的關鍵：與其在不確定時猜測，
    不如什麼都不做，讓下次重試決定。

PositionTracker.apply_fill 內部已經會在 order 可解析時呼叫 brepo.apply_order_fill 更新
filled_qty/avg_fill_price/status（round3 #12，見 broker/position_tracker.py），本 worker
不重複呼叫該函式，否則 filled_qty 會被同一筆 fill 累加兩次。
"""
import asyncio
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, replace as _dc_replace
from typing import TYPE_CHECKING

from sqlmodel import Session

from quanquant.broker import agent_commands
from quanquant.broker import repository as brepo
from quanquant.broker.position_tracker import PositionMismatchError, PositionTracker
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import Fill
from quanquant.db.models import Order, RawInbox

if TYPE_CHECKING:
    from quanquant.broker.order_events import OrderEventHub

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class OrderReport:
    """委託回報（FuturesOrder callback）；範圍窄於 Fill，只用來更新 Order.status。"""

    broker: str
    # Inc1 D5/S3：account 改吃列上蓋章值，直接呼叫（未帶 scope 的既有單元測試/舊路徑）時
    # 可能是 None——型別放寬以反映這個合法的執行期狀態（`_resolve_order_report_order` 傳給
    # `find_order_by_ordno`/`find_order_by_broker_id` 時 None 會被當成一般查詢條件，不會炸）。
    account: str | None
    mode: str
    ordno: str | None
    broker_order_id: str | None
    status: str


DealMapper = Callable[..., Fill]  # (payload, *, account=...) -> Fill（Inc1 D5/S3：帳號改吃列上蓋章）
OrderReportMapper = Callable[..., OrderReport]  # (payload, *, account=...) -> OrderReport（同上）


class RawInboxDeadLetterError(Exception):
    """Inc1 D5/R2-6：代表這列 raw_inbox 應該**永久** dead-letter，不進 `association_pending`
    的 unquarantine 重試迴圈——reason 必須是 `repository.DEAD_LETTER_QUARANTINE_REASONS`
    三者之一（`scope_violation`/`payload_mismatch`/`user_mismatch`）。"""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


def _validate_report_scope(session: Session, *, user_id: int, broker: str, account: str) -> bool:
    """D5 逐訊息 scope 驗證（codex R1-5）：查 `agent_account_bindings` 是否已有該
    `(broker,account)` 的綁定列——有列且指向別的 user → False（scope_violation）；查無列 →
    先放行（True）。

    Task 6 收口說明：`agent_ws` 的 UpLogin 現在會在 `mark_logged_in` 之前呼叫
    `repository.bind_account`（先綁先贏，見 `agent_ws._check_uplogin`）——**登入必綁**，故
    走這條路徑（已登入連線送出的 UpReport）理論上不應該再查到 `binding is None`。這裡仍刻意
    保留「查無放行」而非改成 fail closed：涵蓋（a）尚未跑過 Task 6 backfill 的舊資料庫／環境、
    （b）理論上的競態或人工介入清掉綁定列——這些情況若改成 fail closed，會把合法回報誤判成
    違規、永久 dead-letter 掉，代價比維持 fail-open 更差。獨立回歸覆蓋見
    `test_inbox_worker.py::test_commit_raw_callback_no_binding_yet_permits_staging`（直接呼叫
    `commit_raw_callback`，不經過 WS/login，因此不受「登入必綁」影響）。`user_id`/`account`
    皆為 None 的呼叫端（in-process）不會走到這個函式，見 `stage_scoped_raw_inbox` 的呼叫
    guard。"""
    binding = brepo.find_account_binding(session, broker=broker, account=account)
    if binding is None:
        return True
    return binding.user_id == user_id


def stage_scoped_raw_inbox(
    session: Session,
    *,
    kind: str,
    broker: str,
    payload: str,
    user_id: int | None,
    account: str | None,
    mode: str | None,
    ops_alerter=None,
) -> RawInbox:
    """Inc1 D5（R1-6）：**唯一** scoped staging 邏輯——供 `commit_raw_callback`（單筆事件、
    自己開 session 並立即 commit）與 reconcile 批次落列（`ShioajiAdapter._stage_reconcile_results`，
    需要與 cursor 推進包在同一交易，因此自己管理 session/commit）共用同一份驗證＋落地邏輯，
    不重複實作兩份。呼叫端負責 session 生命週期／commit，本函式只 add/flush。

    逐訊息 scope 驗證（codex R1-5）：只在 `user_id`/`account` 皆非 None 時才查
    `agent_account_bindings`（in-process 呼叫端一律傳 `user_id=None`，不會觸發這個檢查，
    行為與現行完全一致）——違規時仍然把這列**落地**（不是丟棄，保留稽核證據），並在同一次
    `add/flush` 內直接標成 `quarantine=True, quarantine_reason="scope_violation"`（經
    `repository.quarantine_raw_inbox` 一併把 `processed=True`，永久 dead-letter），commit
    後仍會照常送 DownReportAck（I1/I4：commit-then-ack 不因為驗證失敗而破例），並嘗試發一次
    OpsAlerter 告警（`ops_alerter` 為 None 時跳過；呼叫失敗吞掉，絕不反噬落地流程）。"""
    if user_id is not None and account is not None and not _validate_report_scope(
        session, user_id=user_id, broker=broker, account=account
    ):
        row = brepo.stage_raw_inbox(
            session, kind=kind, broker=broker, payload=payload,
            user_id=user_id, account=account, mode=mode,
        )
        error = f"帳號 {account!r} 不屬於 user_id={user_id} 的綁定（scope_violation，fail closed）"
        brepo.quarantine_raw_inbox(session, row, error=error, reason="scope_violation")
        if ops_alerter is not None:
            try:
                ops_alerter.quarantine(row_id=row.id, kind=kind, error=error)
            except Exception:
                log.exception("scope_violation quarantine 告警失敗（已吞，不影響落地流程）")
        return row
    return brepo.stage_raw_inbox(
        session, kind=kind, broker=broker, payload=payload,
        user_id=user_id, account=account, mode=mode,
    )


def commit_raw_callback(
    session_factory: Callable[[], Session],
    *,
    kind: str,
    broker: str,
    payload: dict,
    user_id: int | None,
    account: str | None,
    mode: str | None,
    ops_alerter=None,
) -> None:
    """callback thread（或排程協程）呼叫的同步落地函式（round3 BLOCKER#2）：用獨立 Session
    把 raw payload 落地到 RawInbox 並立即 commit——**返回前保證落地**。呼叫端（ShioajiAdapter
    callback／agent_ws UpReport handler）只需呼叫這一個函式就完成 durable 保證，不可只排程
    協程而不等 commit 完成就返回。

    Inc1 D5（R1-6）：三個呼叫端全部**必須**明確帶 scope（沒有預設值，強迫呼叫端表態，不會有
    「忘記蓋章」的呼叫路徑）：
      - in-process `ShioajiAdapter._persist_raw` → `(user_id=None, account=self.account,
        mode=self.mode)`（in-process 的 user 歸屬本就由 ordno 匹配 Order 決定，不變）。
      - `agent_ws` UpReport handler → `(user_id=<連線認證 user_id>, account=msg.account,
        mode=msg.mode)`。
      - remote reconcile 落列（`ShioajiAdapter._stage_reconcile_results`）改走
        `stage_scoped_raw_inbox`（不是本函式——那裡需要與 cursor 推進同一交易，見該函式
        docstring），語意仍是同一套驗證邏輯。
    """
    with session_factory() as session:
        stage_scoped_raw_inbox(
            session, kind=kind, broker=broker, payload=json.dumps(payload),
            user_id=user_id, account=account, mode=mode, ops_alerter=ops_alerter,
        )
        session.commit()


class RawInboxWorker:
    def __init__(
        self,
        session_factory: Callable[[], Session],
        *,
        supervisor: BrokerSupervisor,
        deal_mapper: DealMapper,
        order_report_mapper: OrderReportMapper,
        tracker: PositionTracker | None = None,
        idle_interval: float = 1.0,
        batch_limit: int = 50,
        order_events: "OrderEventHub | None" = None,
        ops_alerter=None,
        user_id: int | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._supervisor = supervisor
        self._deal_mapper = deal_mapper
        self._order_report_mapper = order_report_mapper
        self._tracker = tracker or PositionTracker()
        self._idle_interval = idle_interval
        self._batch_limit = batch_limit
        self._order_events = order_events
        self._ops = ops_alerter  # T0.3：回報進 quarantine 時發營運告警（fire-and-forget，純疊加）
        # Inc1 D6：`user_id=None`（預設）＝in-process 單一 worker，批次不篩 user（既有行為
        # 位元級不變）；agent 模式每個 UserAgentSlot 各自一個 worker，傳自己的 slot.user_id
        # ——批次查詢只認領這個 user 蓋章的列（`repository.list_unprocessed_raw_inbox` 的
        # `RawInbox.user_id == user_id` 精確比對），跨 user 完全無共享可變狀態（I8）。
        self._user_id = user_id
        self._stop = asyncio.Event()

    async def run(self) -> None:
        while not self._stop.is_set():
            async with self._supervisor.lock:
                handled = await asyncio.to_thread(self.process_batch_once)
            # 有列真的落地了（成交/委託狀態變更）→ 推 SSE，瀏覽器據此重抓委託/部位（取代盲輪詢）。
            # 發布點在 to_thread 回來後、已回到 event loop 執行緒，故可直接呼叫、不需 call_soon_threadsafe。
            if handled and self._order_events is not None:
                self._order_events.publish()
            if handled == 0:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self._idle_interval)
                except TimeoutError:
                    pass

    async def stop_and_drain(self, timeout: float = 5.0) -> bool:
        """設停止旗標；等目前持鎖中的 batch 結束（拿得到鎖代表沒有 batch 在跑）或逾時。

        round3 #17：回傳 True/False 讓呼叫端（Task 8 shutdown sentinel）能明確知道是否真的
        drain 完成——逾時只代表「不再等了」，不代表背景 to_thread 已經真的停下來（asyncio
        無法砍掉正在跑的 native thread），呼叫端不得把逾時當成功處理，DB 裡已落地的
        RawInbox 資料本身不受影響，仍安全保留供下次啟動時的 worker 撿起繼續處理。"""
        self._stop.set()
        try:
            async with asyncio.timeout(timeout):
                async with self._supervisor.lock:
                    pass
            return True
        except TimeoutError:
            log.warning("RawInboxWorker.stop_and_drain 逾時（%.1fs），可能仍有 batch 在跑", timeout)
            return False

    def process_batch_once(self) -> int:
        """同步、可直接測試。回傳「確定處理完」（processed 或 quarantine）的列數；
        非預期例外的列不計入（見模組 docstring 的例外分類）。"""
        with self._session_factory() as scan_session:
            row_ids = [
                r.id for r in brepo.list_unprocessed_raw_inbox(
                    scan_session, limit=self._batch_limit, user_id=self._user_id,
                )
            ]
        handled = 0
        for row_id in row_ids:
            try:
                self._process_one(row_id)
                handled += 1
            except Exception:
                log.exception("raw_inbox id=%s 處理時發生未預期例外，留待下一輪重試（零丟單）", row_id)
        return handled

    def _process_one(self, row_id: int) -> None:
        with self._session_factory() as session:
            row = session.get(RawInbox, row_id)
            if row is None or row.processed or row.quarantine:
                return  # 防禦性：理論上單一序列化通道內不會重複排到同一列
            try:
                payload = self._decode_payload(row.payload)
                if row.kind == "deal_report":
                    self._process_deal(session, row, payload)
                elif row.kind == "order_report":
                    self._process_order_report(session, row, payload)
                else:
                    raise ValueError(f"未知 raw_inbox.kind: {row.kind!r}")
            except (RawInboxDeadLetterError, ValueError, PositionMismatchError) as exc:
                session.rollback()
                row = session.get(RawInbox, row_id)
                kind = row.kind if row is not None else "?"  # commit 前先取（expire_on_commit 後不再讀 detached row）
                # R2-6：RawInboxDeadLetterError 攜帶明確 reason（scope_violation/payload_mismatch/
                # user_mismatch，永久 dead-letter）；既有 ValueError/PositionMismatchError 一律維持
                # 原本語意，reason="association_pending"（可重試，quarantine_raw_inbox 預設值）。
                reason = exc.reason if isinstance(exc, RawInboxDeadLetterError) else "association_pending"
                brepo.quarantine_raw_inbox(session, row, error=str(exc), reason=reason)
                session.commit()
                # T0.3 告警（純疊加）：quarantine 落地後才通知；取值/呼叫包 try/except 吞掉，
                # 告警絕不能反噬處理流程（此列已成功 quarantine，DB 狀態不受告警影響）。
                if self._ops is not None:
                    try:
                        self._ops.quarantine(row_id=row_id, kind=kind, error=str(exc))
                    except Exception:
                        log.exception("quarantine 告警失敗（已吞，不影響處理流程）")

    @staticmethod
    def _decode_payload(raw: str) -> dict:
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError(f"payload 非合法 JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("payload 必須是 JSON object")
        return payload

    def _process_deal(self, session: Session, row: RawInbox, payload: dict) -> None:
        # Inc1 D5/S3：mapper 一律以列上蓋章的 account 為權威（不信任 payload 自帶值可能是
        # 冒用/矛盾）；payload.account_id 與 row.account 不符時，mapper 內部 raise
        # RawInboxDeadLetterError(reason="payload_mismatch")——只在 row.account 非 None 時檢查
        # （row.account 為 None 的舊列/未蓋章列跳過，保持既有位元級行為，同 R2-2 的 user_id
        # NULL 放行原則）。payload 本身不合法（缺欄位/型別錯）→ mapper 內部 raise ValueError（既有行為）。
        fill = self._deal_mapper(payload, account=row.account)

        order = None
        if fill.ordno:
            order = brepo.find_order_by_ordno(
                session, broker=fill.broker, account=fill.account, mode=fill.mode, ordno=fill.ordno
            )
        if order is None and fill.broker_order_id:
            order = brepo.find_order_by_broker_id(
                session, broker=fill.broker, account=fill.account, mode=fill.mode,
                broker_order_id=fill.broker_order_id,
            )
        if order is None:
            raise ValueError(
                f"無法解析委託關聯（broker={fill.broker!r},account={fill.account!r},mode={fill.mode!r},"
                f"ordno={fill.ordno!r},broker_order_id={fill.broker_order_id!r}），quarantine 待重建"
            )
        # R2-2：只在 row.user_id 非 None（agent 模式蓋章列）才強制比對——in-process 的 NULL 列
        # 沿用現行「委託歸屬完全由 ordno/broker_order_id 複合鍵解析決定」行為，位元級不變。
        if row.user_id is not None and order.user_id != row.user_id:
            raise RawInboxDeadLetterError(
                "user_mismatch",
                f"deal_report 解析到的委託 user_id={order.user_id} 與列蓋章 user_id={row.user_id} "
                f"不符（fill_id={fill.fill_id!r}），fail closed",
            )

        # 真實成交回報（FuturesDealEvent）沒有 octype 欄位（見
        # ShioajiAdapter._map_deal_report 說明）——mapper 回傳的 fill.octype 此刻只是滿足
        # Fill.__post_init__ 型別驗證的占位值，正確值一律用上面剛解析到的對應 Order 當初下單
        # 時存的 octype 覆蓋。解不到對應 Order 的情況已經在上面 raise ValueError quarantine
        # 掉，不會走到這裡（「解不到就 quarantine，不亂猜」）。
        #
        # symbol 同理覆蓋（部位顯示 bug 收尾）：真實 FuturesDealEvent 的 `code` 欄位是具體
        # 月合約代碼（如 "TXFH6"），mapper（_map_deal_report）照原樣帶出；但 Order.symbol／
        # ShioajiAdapter.symbol／positions() 查詢一律用通用商品代碼（如 "TXF"，見
        # config.Settings.symbol）。若直接採用 mapper 給的具體合約代碼寫入
        # BrokerPosition.symbol，會跟 `list_open_positions(symbol=self.symbol)` 的過濾條件
        # 對不起來——部位明明已入帳，`positions()` 卻永遠查不到。一律以解析到的
        # Order.symbol（通用代碼）為準，不信任 mapper 給的具體合約代碼。
        fill = _dc_replace(fill, octype=order.octype, symbol=order.symbol)

        trading_day = brepo.trading_day_for(fill.ts)
        deal = brepo.stage_deal(
            session, broker=fill.broker, account=fill.account, mode=fill.mode, trading_day=trading_day,
            fill_id=fill.fill_id, ordno=fill.ordno, broker_order_id=fill.broker_order_id,
            order_id=order.id, user_id=order.user_id,
            symbol=fill.symbol, action=fill.action, price=fill.price, qty=fill.qty, fee=fill.fee,
            octype=fill.octype, ts=fill.ts, raw_inbox_id=row.id,
        )
        if deal is None:
            # 重播（watchdog 對帳補進同一筆 fill_id）：冪等，只補 processed，不重跑帳務。
            brepo.mark_raw_inbox_processed(session, row)
            session.commit()
            return

        resolved_fill = fill if fill.user_id is not None else Fill(
            broker=fill.broker, fill_id=fill.fill_id, ordno=fill.ordno, broker_order_id=fill.broker_order_id,
            symbol=fill.symbol, action=fill.action, price=fill.price, qty=fill.qty, fee=fill.fee,
            octype=fill.octype, ts=fill.ts, account=fill.account, mode=fill.mode, user_id=order.user_id,
        )
        # PositionTracker.apply_fill 內部會自行以複合 scope 重新解析 order 並在成功時呼叫
        # brepo.apply_order_fill 更新 filled_qty/avg_fill_price/status（round3 #12）；
        # 這裡不再額外呼叫一次 apply_order_fill，避免同一筆 fill 的 filled_qty 被累加兩次。
        self._tracker.apply_fill(session, resolved_fill, user_id=order.user_id)

        deal.processed = True
        session.add(deal)
        brepo.mark_raw_inbox_processed(session, row)
        session.commit()

    def _process_order_report(self, session: Session, row: RawInbox, payload: dict) -> None:
        # Inc1 D5/S3 拆除：mapper 改吃列上蓋章的 account，不再讀 adapter.account（那是 mutable
        # 單例，多 agent 會互相污染帳號對映）——見 ShioajiAdapter._map_order_report。
        report = self._order_report_mapper(payload, account=row.account)
        order = self._resolve_order_report_order(session, report)
        if order is None:
            raise ValueError(
                f"委託回報無法解析關聯（ordno={report.ordno!r}, broker_order_id={report.broker_order_id!r}）"
            )
        # R2-2：同 _process_deal，只在 row.user_id 非 None 才強制比對；NULL 列（in-process）
        # 位元級行為不變。
        if row.user_id is not None and order.user_id != row.user_id:
            raise RawInboxDeadLetterError(
                "user_mismatch",
                f"order_report 解析到的委託 user_id={order.user_id} 與列蓋章 user_id={row.user_id} "
                f"不符（ordno={report.ordno!r}），fail closed",
            )
        brepo.mark_order_status(session, order, status=report.status)
        if row.user_id is not None:
            # Inc1 D4 終態 resolver 的 worker 掛載點（Task 11 修復回合 1，發現 2）：agent 模式
            # 下這筆回報若把 Order 推進終態，同一交易內立刻收斂它掛著的 update-unknown／cancel
            # 指令列，把收斂延遲從 watchdog 週期上限（`unknown_reconcile_grace_seconds`，預設
            # 300s）壓到與這筆回報同一次處理。純 in-process（row.user_id is None）完全不受
            # 影響——原路徑零變更。
            self._resolve_agent_commands_after_terminal(session, order)
        brepo.mark_raw_inbox_processed(session, row)
        session.commit()

    @staticmethod
    def _resolve_agent_commands_after_terminal(session: Session, order: Order) -> None:
        """D4 終態 resolver 的第二個掛載點（第一個是 watchdog 週期掃描，見 watchdog.py 的
        `_reconcile_unknown_quota_agent`）：只在 Order 剛好落在終態（cancelled/failed/
        filled）才有東西可做；`order.ordno is None` 時無法關聯任何 ledger 列，直接跳過
        （呼叫端已保證 `row.user_id is not None`，這裡不重複判斷 agent/in-process）。

        update 一律以 `real_qty=None` 呼叫 `agent_commands.resolve_one_unresolved_update`——
        這裡是同步 DB 交易，沒有 `gateway.query_qty` round-trip 能力，天然只能走 D8「不可得」
        保守 confirm 分支（不猜、不 release，低估風險比高估風險小，同 watchdog 終態分支的既有
        哲學）；cancel 走既有 state-based report resolver。兩者都經 Task 11 修復回合 1（發現
        1）修好的原子 CAS，與 watchdog 下一輪或遲到 ack 互不覆寫——誰先誰贏，輸家 no-op，不會
        重複套效果（同一委託理論上可能同時被 watchdog 這輪與這裡搶著收斂，靠 CAS 保證恰一次）。
        """
        if order.ordno is None or order.status not in agent_commands.ORDER_TERMINAL_FOR_UNKNOWN_RESOLVER:
            return
        updates = agent_commands.list_unresolved_unknown_updates_for_ordno(
            session, user_id=order.user_id, ordno=order.ordno
        )
        for cmd_id in [r.cmd_id for r in updates]:
            agent_commands.resolve_one_unresolved_update(session, cmd_id=cmd_id, real_qty=None)
        cancels = agent_commands.list_unresolved_cancels_for_ordno(
            session, user_id=order.user_id, ordno=order.ordno
        )
        for cmd_id in [r.cmd_id for r in cancels]:
            agent_commands.resolve_one_unresolved_cancel(session, cmd_id=cmd_id)

    @staticmethod
    def _resolve_order_report_order(session: Session, report: OrderReport) -> Order | None:
        """比照 PositionTracker._resolve_order（round3 #3-new）的雙鍵交叉驗證範式：deal_report
        路徑早已對 ordno/broker_order_id 各自 scoped 查詢、矛盾即 fail closed，
        order_report 路徑先前只有 ordno 命中即用、broker_order_id 純 fallback，完全沒驗
        第二鍵是否指向同一張 Order（獨立驗收殘留2）。這裡補上同款交叉驗證：兩把鍵各自
        scoped 查詢，若都有命中但指向不同 Order，矛盾一律 raise（呼叫端 quarantine，不任選
        其一）；只有其中一把鍵、或兩者查詢結果一致時正常處理，不退化既有單鍵 fallback 行為。"""
        by_ordno = (
            brepo.find_order_by_ordno(
                session, broker=report.broker, account=report.account, mode=report.mode, ordno=report.ordno
            )
            if report.ordno
            else None
        )
        by_broker_id = (
            brepo.find_order_by_broker_id(
                session, broker=report.broker, account=report.account, mode=report.mode,
                broker_order_id=report.broker_order_id,
            )
            if report.broker_order_id
            else None
        )
        if by_ordno is not None and by_broker_id is not None and by_ordno.id != by_broker_id.id:
            raise ValueError(
                f"order_report 的 ordno={report.ordno!r} 與 broker_order_id={report.broker_order_id!r} "
                f"分別指向不同 Order（id={by_ordno.id} vs id={by_broker_id.id}），矛盾，fail closed 進 quarantine"
            )
        return by_ordno or by_broker_id
