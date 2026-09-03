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
from datetime import datetime, timezone
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


def _order_report_event_payload(order: Order) -> dict:
    """007（批次 B-1）：`order-report` SSE 事件的 payload——`_process_deal`（Deal 觸發
    apply_order_fill 造成的部分成交/全部成交）與 `_process_order_report`（券商 callback
    的已送出/已取消/失敗等）共用同一個 shape，前端不需要分兩種格式解析。

    價格取 `avg_fill_price`（已有成交時，均價比原始掛單價更有意義）、缺值時 fallback 回
    `order.price`（尚未成交的委託，如剛送出/被取消）。`error_message` 目前固定 None——
    `Order`/`OrderReport` 資料模型都沒有回報失敗原因的欄位（券商 callback 只給狀態碼，
    見 ShioajiAdapter._map_order_report），留著這個欄位是為了讓 payload shape 穩定、
    B-3 前端不用等後續補欄位就能先接線；同步下單失敗（RiskError/OrderError）的訊息走既有
    HTTP 回應本身，不經這條 SSE 路徑，見本檔模組 docstring 與 007 規格「不要動到的部分」。

    LOW-6（fresh-context 終審修復）：補 `price_type`——MKT 委託未成交時 `avg_fill_price`
    為 None、`order.price` 恆為 0（`_parse_order_price` 送單當下就定的，同 confirm_dialog
    的 CRITICAL-1），沒有這個欄位時前端（static/banners.js）只看得到裸的 "0" 這個字串，
    會顯示成「@ 0」（`"0"` 是非空字串，truthy）；帶上 price_type 讓前端能比照
    confirm_dialog.html 的判斷式改顯示「市價」。
    """
    price = order.avg_fill_price if order.avg_fill_price is not None else order.price
    return {
        "symbol": order.symbol, "action": order.action, "qty": order.qty,
        "filled_qty": order.filled_qty, "price": str(price), "price_type": order.price_type,
        "status": order.status, "octype": order.octype, "broker_order_id": order.broker_order_id,
        "error_message": None,
    }


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
    on_committed: Callable[[], None] | None = None,
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

    事件喚醒（RawInboxWorker 從 idle_interval 純逾時輪詢改事件喚醒補的掛載點）：
    `on_committed` 是 best-effort 喚醒 hook，只在 `session.commit()` 真正成功**之後**才呼叫
    ——durable 保證（「落地 commit 成功才 return」）完全不受影響，喚醒只是錦上添花。呼叫端
    （`ShioajiAdapter._persist_raw`／`agent_ws` UpReport handler）各自傳自己對應那個
    `RawInboxWorker.request_wake`；未接線（預設 None）行為與現行完全一致——不呼叫任何東西。
    hook 本身若拋例外一律吞掉（不得反噬已經成功的落地），worker 端仍有 idle_interval 逾時
    輪詢當 fallback（喚醒遺失不等於資料遺失）。
    """
    with session_factory() as session:
        stage_scoped_raw_inbox(
            session, kind=kind, broker=broker, payload=json.dumps(payload),
            user_id=user_id, account=account, mode=mode, ops_alerter=ops_alerter,
        )
        session.commit()
    if on_committed is not None:
        try:
            on_committed()
        except Exception:
            log.exception("commit_raw_callback: on_committed 喚醒 hook 失敗（已吞，不影響落地）")


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
        # F1（opus 終審發現，本分支修復）：battery-guard——語意「本 scope 可能存在
        # association_pending 隔離列」。`unquarantine_stale_raw_inbox` 對無索引的
        # `quarantine` 欄全表掃，健康常態（零隔離列）下每個落地批次都白付一次 O(n)，且發生在
        # `supervisor.lock` 內（成交越密→下單越被擋）。保守初始化 True：重啟後可能還有舊的
        # 隔離列，開機後第一個落地批次付一次 O(n) 查詢就會歸位（見
        # `_retry_association_pending_once`：released=0 才關閉；`_process_one` 把列 quarantine
        # 成 association_pending 時撥回 True）。執行緒紀律：本 worker 單一實例，批次在
        # `process_batch_once` 內、且呼叫端（`run()`）序列化於 `supervisor.lock` 之下，同一時間
        # 只有一個批次在跑，所以是普通屬性即可，不需要鎖。
        self._maybe_assoc_pending = True
        # N1（opus 二輪複審發現，本輪修復，MEDIUM）：`self._maybe_assoc_pending` 光看
        # `released == 0` 無法擋住「永不可解的孤兒 association_pending 列」（如券商官方 App
        # 下的單，對應委託永遠不存在）——每個落地批次都會把它釋放、重試、失敗、重新隔離，
        # `_process_one` 的隔離分支又把 flag 撥回 True，guard 形同虛設（opus 實測：連五個
        # 健康落地批次，`unquarantine_stale_raw_inbox` 累計呼叫 1→5）。`_last_retry_released_ids`
        # 記錄「上一次呼叫 `_retry_association_pending_once` 時，重掃到的列 id 集合」（`None`＝
        # 尚無記錄／這個 scope 剛回到完全乾淨狀態），供零進展偵測比對，見該函式 docstring。
        self._last_retry_released_ids: frozenset[int] | None = None
        self._stop = asyncio.Event()
        # 事件喚醒：新列落地後（commit_raw_callback 的 on_committed hook）可以立刻喚醒本
        # worker，把「委託/成交回報顯示延遲」從 idle_interval 純逾時輪詢壓到近零；喚醒遺失
        # （下面兩者的邊界情況）一律靠 idle_interval 逾時輪詢兜底，故 idle_interval 的預設值
        # 與既有語意不變、不可移除（見 `request_wake`/`run` docstring）。
        self._wake = asyncio.Event()
        # `run()` 開始跑時才捕捉目前的 event loop，供 `request_wake()` 從任意執行緒（含沒有
        # loop 的 Shioaji SDK callback thread）安全地 `call_soon_threadsafe` 回這個 loop；
        # worker 尚未啟動或已經停止時這裡是 None，`request_wake()` 據此判斷安靜 no-op。
        self._loop: asyncio.AbstractEventLoop | None = None
        # CRITICAL-1（fresh-context opus 終審修復，2026-09）：`process_batch_once()`
        # 整批在 `asyncio.to_thread()` 裡執行（見 `run()`）——`_process_deal`/
        # `_process_order_report` 因此可能跑在 worker thread 上，不能直接呼叫
        # `self._order_events.publish_deal`/`publish_order_report`（內部是
        # `asyncio.Queue.put_nowait`，不是 thread-safe，`loop.set_debug(True)` 下非本
        # 執行緒操作會直接 RuntimeError；非 debug 模式下則是隱性資料競態，且該例外會被
        # `process_batch_once` 逐列 `except Exception` 吞掉，該列不計 handled、SSE 靜默
        # 降級，見 `test_deal_landing_publish_runs_on_loop_thread_safe_under_asyncio_
        # debug_mode` 的重現）。改成：commit 成功後只把 (kind, user_id, payload) 三元組
        # append 進這個 list（純 Python list.append，哪個執行緒呼叫都安全）；真正呼叫 hub
        # 的動作留給 `_flush_pending_events()`，只在 event loop 執行緒上呼叫（`run()` 在
        # `await asyncio.to_thread(...)` 回來後那一刻，或測試直接呼叫）。單一 worker
        # 實例、批次序列化於 `supervisor.lock`，同一時間只有一個批次在累積／清空這個
        # list，不需要額外的鎖。
        self._pending_events: list[tuple[str, int, dict]] = []

    async def run(self) -> None:
        self._loop = asyncio.get_running_loop()
        try:
            while not self._stop.is_set():
                # 醒來（或本來就還沒睡）一律先 clear 再掃表：任何在 clear 之前已經
                # commit 成功的列，這次的 process_batch_once 掃描必然涵蓋得到（commit
                # 早於 request_wake() 呼叫，clear 又早於掃描）；clear 之後才落地的新列會
                # 讓 wake 重新被設起來，留給下一輪處理，不會遺失（見模組頂部/
                # request_wake docstring 的競態說明）。
                self._wake.clear()
                async with self._supervisor.lock:
                    handled = await asyncio.to_thread(self.process_batch_once)
                # CRITICAL-1：to_thread 回來的這一刻已經確定回到 event loop 執行緒——
                # `_flush_pending_events()` 把批次期間（可能在 worker thread 上）累積的
                # 帶 payload 事件，在這裡才真正送進 hub（asyncio.Queue.put_nowait 只能在
                # 本執行緒呼叫）。「先 flush scoped、後發 ping」的順序是**刻意且 load-bearing
                # 的**：queue 滿時採 drop-oldest（見 order_events.py `_put_evicting_oldest`），
                # ping 最後入列才保證它不會被同一批 >8 筆的 scoped 洪峰擠掉——ping 一掉，
                # 委託/成交/部位三頁該批次就不刷新。對調這兩行會讓 MEDIUM-4 原樣復活
                # （反向測試釘在 test_order_events.py::test_ping_published_before_burst_gets_evicted）。
                self._flush_pending_events()
                # 有列真的落地了（成交/委託狀態變更）→ 推 SSE，瀏覽器據此重抓委託/部位（取代盲輪詢）。
                # 發布點在 to_thread 回來後、已回到 event loop 執行緒，故可直接呼叫、不需 call_soon_threadsafe。
                if handled and self._order_events is not None:
                    self._order_events.publish()
                if handled == 0:
                    await self._wait_for_stop_or_wake()
        finally:
            self._loop = None

    def _flush_pending_events(self) -> None:
        """CRITICAL-1：把 `_pending_events` 清空並逐一送進 hub——呼叫端必須保證這是在
        event loop 執行緒上執行（`run()` 在 `await asyncio.to_thread(...)` 回來後呼叫；
        直接呼叫 `process_batch_once()` 的測試單執行緒跑，呼叫這個方法一樣安全）。先把
        list 換成新的空 list 再迭代舊內容，避免迭代期間又有人 append 進同一個 list
        （目前的呼叫方式不會發生，但這樣寫本身就不依賴這個假設）。`self._order_events`
        為 None（下單子系統停用／測試未接線）時單純清空、不呼叫任何東西。"""
        pending, self._pending_events = self._pending_events, []
        if self._order_events is None:
            return
        for kind, owner_id, payload in pending:
            if kind == "deal":
                self._order_events.publish_deal(user_id=owner_id, payload=payload)
            else:
                self._order_events.publish_order_report(user_id=owner_id, payload=payload)

    async def _wait_for_stop_or_wake(self) -> None:
        """等 `_stop` 或 `_wake` 任一被 set，逾時 `self._idle_interval` 秒即返回——取代原本
        單純的 `wait_for(self._stop.wait(), timeout=idle_interval)`。事件喚醒只是讓這次等待
        提早結束，`idle_interval` 逾時 fallback 完全保留（喚醒遺失時最慢還是這個時間內會被
        撿起來，不會真的丟單）。"""
        stop_task = asyncio.ensure_future(self._stop.wait())
        wake_task = asyncio.ensure_future(self._wake.wait())
        try:
            await asyncio.wait(
                {stop_task, wake_task}, timeout=self._idle_interval,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in (stop_task, wake_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(stop_task, wake_task, return_exceptions=True)

    def request_wake(self) -> None:
        """事件喚醒：commit 成功之後才可能被呼叫的 best-effort 喚醒——跨執行緒安全，
        Shioaji SDK 原生 callback 執行緒（沒有 event loop）與任何其他執行緒皆可安全呼叫，
        絕不 raise（callback 執行緒若因此炸掉，會連帶丟掉這筆券商回報，比晚一輪
        idle_interval 才處理嚴重得多）。

        worker 尚未啟動（`run()` 還沒開始跑、`_loop` 仍是 None）或已經停止（`run()` 已返回、
        `_loop` 被 finally 清空）時安靜 no-op——不喚醒任何人，既有 idle_interval 逾時輪詢
        仍是唯一且足夠的 fallback，不視為錯誤。"""
        loop = self._loop
        if loop is None:
            return
        try:
            loop.call_soon_threadsafe(self._wake.set)
        except RuntimeError:
            # loop 已關閉（shutdown 競態下可能發生）——喚醒本來就只是 best-effort，
            # 安靜吞掉即可，不影響任何 durable 保證。
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
        非預期例外的列不計入（見模組 docstring 的例外分類）。

        批次內快速重試（association_pending 隔離列，本分支新增）：本批若有列真正「成功
        落地」（`_process_one` 回傳 True，非單純 quarantine），代表有新資訊進來（典型是
        遲到的 order ack）——對本 worker scope 內 reason=association_pending 的隔離列做
        「一次」unquarantine→重新處理（見 `_retry_association_pending_once`），把原本得等
        watchdog `_retry_quarantined`（`order_unquarantine_after_seconds`，預設 300s）的
        尾端延遲，壓到與觸發批次同一次或緊接下一次呼叫。只用「成功落地」（不含單純
        quarantine）當觸發條件，是刻意避免本批自己剛產生的 quarantine 立刻自我重試（那不
        代表任何新資訊出現，也會讓既有「quarantine 也算確定處理完」的 handled 計數斷言失真
        ——見 tests/test_inbox_worker.py 既有回歸測試）。重試結果（無論再次成功落地或再次
        quarantine）併入回傳值，讓呼叫端（`run()`）照既有邏輯視 handled>0 發一次 SSE
        publish，不需要新增判斷分支。

        F1 in-memory guard（opus 終審發現，本分支修復）：`landed` 之外還要
        `self._maybe_assoc_pending` 為 True 才觸發重試查詢——健康常態（零隔離列）下第一次
        觸發會把 flag 關掉，之後的落地批次不再白付 `unquarantine_stale_raw_inbox` 的 O(n)
        全表掃，直到又有新的 association_pending 隔離列出現（`_process_one` 撥回 True）才
        重新開啟，見 `__init__`/`_retry_association_pending_once` docstring。

        F2：快速重試是 best-effort 疊加功能，不是零丟單的必要路徑（watchdog 300s fallback
        仍在）——包 try/except 避免它內部的暫時性 DB 例外（UPDATE+commit）一路穿出 `run()`
        （迴圈本體無 except）害 worker task 靜默死掉、回報管線停擺。例外時刻意不動
        `_maybe_assoc_pending`（保守維持目前值，通常是 True——下一個落地批次還會再試一次，
        不因為這次失敗被誤判成「沒有隔離列」而永久關掉）。"""
        with self._session_factory() as scan_session:
            row_ids = [
                r.id for r in brepo.list_unprocessed_raw_inbox(
                    scan_session, limit=self._batch_limit, user_id=self._user_id,
                )
            ]
        handled = 0
        landed = 0  # 本批「成功落地」（非 quarantine）的列數，見上方 docstring
        for row_id in row_ids:
            try:
                if self._process_one(row_id):
                    landed += 1
                handled += 1
            except Exception:
                log.exception("raw_inbox id=%s 處理時發生未預期例外，留待下一輪重試（零丟單）", row_id)
        if landed and self._maybe_assoc_pending:
            try:
                handled += self._retry_association_pending_once()
            except Exception:
                log.exception(
                    "批次內快速重試發生未預期例外（best-effort，不影響本批已處理的列）："
                    "留給下一個落地批次或 watchdog 300s fallback 重試",
                )
        return handled

    def _retry_association_pending_once(self) -> int:
        """批次內快速重試：只在 `process_batch_once` 內、且只在本批有新列真正成功落地時被
        呼叫「一次」（不遞迴呼叫自己）——重用 watchdog 同一套機制
        （`repository.unquarantine_stale_raw_inbox`），只是 cutoff 傳「現在」而非 300 秒前：
        任何已落地的隔離列 `received_at` 必然早於這個當下時刻，等同「不篩年齡、把 scope 內
        現有的 association_pending 隔離列全部解除」；scope（`user_id=self._user_id`）與
        reason 篩選（association_pending／NULL，跳過永久 dead-letter 三種 reason）完全沿用
        該函式既有邏輯，不另造一份。

        解除後的列會回到一般 unprocessed 佇列，用同一個 scoped 查詢
        （`list_unprocessed_raw_inbox`）重新掃描並照常呼叫 `_process_one` 逐列處理——與
        `run()` 平常撿到新列的路徑完全相同，沒有另外的處理邏輯。

        天然節流：孤兒列（ack 永遠不來）重試失敗後原地重新 quarantine，等下一個有落地的
        批次或 watchdog 的 300 秒 fallback，不會在同一批次內反覆重試造成熱迴圈——本函式
        每次 `process_batch_once` 呼叫最多執行一次，內部也沒有任何迴圈或遞迴會再次觸發它。

        F1 guard：`released == 0`（本 scope 目前沒有任何 association_pending 隔離列）時把
        `self._maybe_assoc_pending` 關成 False，讓呼叫端（`process_batch_once`）之後的落地
        批次不再呼叫這個函式，直到 `_process_one` 因為新的 association_pending quarantine
        把 flag 撥回 True。`released > 0` 時不動 flag（維持 True 不需要每次都重複賦值，語意
        上也對稱：還有隔離列在，下一批繼續保持警覺）。

        N1（opus 二輪複審發現，本輪修復，MEDIUM）：`released > 0` 不代表有進展——永不可解的
        孤兒 association_pending 列（如券商官方 App 下的單，對應委託永遠不存在）每個落地
        批次都會被這裡釋放、重掃、`_process_one` 重新隔離（那個分支會把
        `self._maybe_assoc_pending` 撥回 True，見其 docstring），若只看 `released` 是否為
        0，guard 永遠不會關閉——O(n) 全表掃＋整輪重試成本對著同一批孤兒每個落地批次白跑一次
        （opus 實測：連五個健康落地批次，`unquarantine_stale_raw_inbox` 累計呼叫 1→5）。

        零進展偵測（**嚴格語意**，opus 三輪複審修正——見下方「為何不能用 None 當萬用匹配」）：
        `self._last_retry_released_ids` 記錄「上一次呼叫本函式時，重掃到的列 id 集合」
        （`None`＝尚無記錄，或上一次是「這個 scope 完全沒有隔離列」的乾淨狀態——見下方
        `not released` 分支）。這次重試迴圈跑完之後，只有在①沒有任何一列真正成功落地
        （`retry_landed == 0`）**且**②`self._last_retry_released_ids` 不是 `None`（代表這不是
        本函式第一次處理這個 id 集合，先前已經給過一次機會）**且**③這次重掃到的 id 集合與
        上次呼叫時完全相同——三者同時成立才關閉 guard，交還 watchdog 300s 慢速路徑照顧（語意
        回歸「快速重試只服務暫態 deal-before-ack 競態」：只有觸發批次當下就成功落地，或是
        「同一批隔離列已經連續兩輪都沒有任何進展」才不繼續投入）。只要這次重試有任何一列成功
        落地、這次的集合裡出現了上次沒有的新列，或這是第一次遇到這個集合，一律維持/恢復
        True，正常參與下一個落地批次的快速重試。

        **為何不能用「`None` 視為與任何集合相符」（上一輪設計，已修正的 BUG）**：deal D 先到、
        無對應委託 quarantine(association_pending)；**不相關**的另一筆回報接著落地觸發本函式
        第一次呼叫（`_last_retry_released_ids` 還是 `None`）——D 的委託 ack 還沒到，重試失敗
        原地重新隔離（`retry_landed == 0`）。若把「`None`」視同「與這次集合相符」直接關閉
        guard，D 自己委託的 ack 緊接著下一批落地時，guard 已經是 False、快速重試被整批跳過，
        D 退回 watchdog 300 秒 fallback——這正是批次內快速重試要消滅的核心情境（deal-before-
        ack 競態），在「多筆委託併發、有不相關回報插隊」的忙碌時段反而失效（見
        `test_first_failed_retry_does_not_disable_guard_before_own_ack_gets_a_chance`）。現在
        改成嚴格比對：`None` 一律不關閉（只記錄這次的集合），必須連續兩輪重試都是同一個集合、
        且都零進展，才判定為解不開、關閉 guard——純孤兒情境代價是多付一輪觀察（第 2 個落地
        批次才關閉），可忽略；換來的是「同一批隔離列連續兩輪」才會被判死，不會因為運氣不好
        被不相關批次抽到一次就提前放棄。

        **順序依賴**：迴圈內 `_process_one` 對每一列失敗都會把 `self._maybe_assoc_pending`
        撥回 True（見該函式 docstring）——零進展判定必須放在這個迴圈**跑完之後**才執行，寫在
        迴圈中間或之前都會被迴圈本身的副作用蓋掉，見下方程式碼順序。

        選擇「重掃時比對 id 集合」而非修改 `unquarantine_stale_raw_inbox` 回傳型別：後者會
        動到 watchdog.py 既有呼叫端（現在只認 `int` 筆數，見 `_retry_quarantined_blocking`），
        侵入面更大；重掃本來就會撈出剛解除隔離的那批列（見上方既有 docstring：「解除後的列
        會回到一般 unprocessed 佇列，用同一個 scoped 查詢重新掃描」），直接拿這次的 id 集合
        當「本輪實際處理的列」的忠實代理，不需要額外查詢或改動 repository 層。"""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with self._session_factory() as session:
            released = brepo.unquarantine_stale_raw_inbox(session, older_than=now, user_id=self._user_id)
            session.commit()
        if not released:
            self._maybe_assoc_pending = False
            # 這個 scope 目前完全沒有隔離列——乾淨狀態，回到「尚無記錄」，讓未來第一次遇到
            # 的孤兒／隔離列一樣享有「連續兩輪零進展才關閉」的一致行為（見上方 docstring）。
            self._last_retry_released_ids = None
            return 0
        # F7（opus 終審發現，LOW）：措辭比照 watchdog.py `_retry_quarantined_blocking` 的既有
        # 訊息風格，標明來源是「批次內快速重試」而非 watchdog 週期性掃描。
        log.info("批次內快速重試解除 %d 筆 quarantine raw_inbox 待重試（user_id=%s）", released, self._user_id)
        with self._session_factory() as scan_session:
            retry_ids = [
                r.id for r in brepo.list_unprocessed_raw_inbox(
                    scan_session, limit=self._batch_limit, user_id=self._user_id,
                )
            ]
        retry_id_set = frozenset(retry_ids)
        retried = 0
        retry_landed = 0
        for row_id in retry_ids:
            try:
                if self._process_one(row_id):
                    retry_landed += 1
                retried += 1
            except Exception:
                log.exception(
                    "raw_inbox id=%s 批次內快速重試時發生未預期例外，留待下一輪重試（零丟單）", row_id,
                )
        # N1 零進展偵測（嚴格語意）：務必在上面的迴圈跑完之後才判斷（見本函式 docstring 的
        # 順序依賴說明），否則會被迴圈內 `_process_one` 的隔離分支把 flag 撥回 True 的副作用
        # 蓋掉。`self._last_retry_released_ids is None`（第一次遇到這個集合）一律不關閉——只
        # 有連續兩輪都是同一個集合、且都零進展，才判定解不開。
        same_as_last_round = (
            self._last_retry_released_ids is not None and retry_id_set == self._last_retry_released_ids
        )
        if retry_landed == 0 and same_as_last_round:
            self._maybe_assoc_pending = False
        self._last_retry_released_ids = retry_id_set
        return retried

    def _process_one(self, row_id: int) -> bool:
        """處理單一 raw_inbox 列。回傳 True＝這次呼叫真正成功處理落地（非 quarantine、非
        防禦性 no-op）；False＝quarantine 或防禦性 no-op（row 已經是終態，理論上不會發生）。
        呼叫端（`process_batch_once`）用這個信號區分「有新資訊真的落地」與「純
        quarantine」，只有前者才觸發批次內快速重試。"""
        with self._session_factory() as session:
            row = session.get(RawInbox, row_id)
            if row is None or row.processed or row.quarantine:
                return False  # 防禦性：理論上單一序列化通道內不會重複排到同一列
            try:
                payload = self._decode_payload(row.payload)
                if row.kind == "deal_report":
                    self._process_deal(session, row, payload)
                elif row.kind == "order_report":
                    self._process_order_report(session, row, payload)
                else:
                    raise ValueError(f"未知 raw_inbox.kind: {row.kind!r}")
                return True
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
                if reason == "association_pending":
                    # F1 guard：這個 scope 剛剛真的多了一筆可重試的隔離列——把 flag 撥回
                    # True，讓下一個落地批次會再跑一次批次內快速重試查詢（同一個分支天然
                    # 涵蓋「快速重試路徑中重試失敗、原地重新 quarantine」的情況，不需要在
                    # `_retry_association_pending_once` 另外處理）。
                    self._maybe_assoc_pending = True
                # T0.3 告警（純疊加）：quarantine 落地後才通知；取值/呼叫包 try/except 吞掉，
                # 告警絕不能反噬處理流程（此列已成功 quarantine，DB 狀態不受告警影響）。
                if self._ops is not None:
                    try:
                        self._ops.quarantine(row_id=row_id, kind=kind, error=str(exc))
                    except Exception:
                        log.exception("quarantine 告警失敗（已吞，不影響處理流程）")
                return False

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
        # 注意：`apply_fill` 內部（`PositionTracker._resolve_order`）用同一個 `session` 以
        # 複合鍵重新查詢同一張 Order——SQLAlchemy identity map 保證回傳同一個 Python 物件，
        # 故下面直接讀這個 scope 內的 `order` 變數就能看到 apply_order_fill 剛寫入的最新
        # filled_qty/avg_fill_price/status，不需要再查一次。
        self._tracker.apply_fill(session, resolved_fill, user_id=order.user_id)

        deal.processed = True
        session.add(deal)
        brepo.mark_raw_inbox_processed(session, row)

        # 007（批次 B-1）：一筆 Deal 落地＝一個 user-scoped 'deal' 事件（滑價一價一橫幅天然
        # 成立，不聚合）；同一次落地也順帶發一個 'order-report'，反映這筆 fill 之後委託的
        # 最新狀態（部分成交/全部成交這兩種狀態轉換只會由這裡的 apply_order_fill 觸發，不會
        # 經過 _process_order_report 那條路徑，兩者互補才涵蓋完整的「委託狀態變化」）。事件
        # payload 在 commit 前就地取值（避免 commit 後 expire_on_commit 觸發多餘的重新查詢），
        # 但 append 進 `_pending_events` 的動作留到 commit 成功之後才做——落地保證與既有其餘
        # 分支一致，事件只是錦上添花的即時提示，不影響 durable 保證。重播（下方
        # `deal is None` 的早退路徑）不會走到這裡，天然不會重複發送。CRITICAL-1：這裡只
        # append 進 list（純 Python 操作，這個函式可能跑在 worker thread 上，見
        # `process_batch_once`/`run()`），真正呼叫 hub 送進 asyncio.Queue 的動作交給
        # `_flush_pending_events()`，只在 event loop 執行緒上執行。
        deal_payload = {
            "symbol": deal.symbol, "action": deal.action, "qty": deal.qty,
            "price": str(deal.price), "octype": deal.octype, "ts": deal.ts,
            "fee": str(deal.fee) if deal.fee is not None else None,
        }
        order_report_payload = _order_report_event_payload(order)
        owner_id = order.user_id
        session.commit()
        if self._order_events is not None:
            self._pending_events.append(("deal", owner_id, deal_payload))
            self._pending_events.append(("order-report", owner_id, order_report_payload))

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
        # 007（批次 B-1）：委託回報（券商 callback：已送出/已取消/失敗等）落地即發一個
        # user-scoped 'order-report' 事件。`mark_order_status` 內部受狀態偏序守衛，晚到/
        # 重播的低序回報會 no-op（`order.status` 不變）——這裡不特判是否真的有變化，一律
        # 反映當下的最新狀態；重複收到同一狀態頂多讓前端重繪同一條橫幅，不算誤導。
        # CRITICAL-1：同 `_process_deal`，只 append 進 `_pending_events`，真正送進 hub
        # 的動作交給 `_flush_pending_events()`（只在 event loop 執行緒上執行）。
        order_report_payload = _order_report_event_payload(order)
        owner_id = order.user_id
        session.commit()
        if self._order_events is not None:
            self._pending_events.append(("order-report", owner_id, order_report_payload))

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
