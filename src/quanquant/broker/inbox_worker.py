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
    Cover 缺對應開倉部位、Auto 歧義）→ rollback 該列的交易 → 單獨 quarantine，不留部分寫入。
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
    account: str
    mode: str
    ordno: str | None
    broker_order_id: str | None
    status: str


DealMapper = Callable[[dict], Fill]
OrderReportMapper = Callable[[dict], OrderReport]


def commit_raw_callback(
    session_factory: Callable[[], Session], *, kind: str, broker: str, payload: dict
) -> None:
    """callback thread（或排程協程）呼叫的同步落地函式（round3 BLOCKER#2）：用獨立 Session
    把 raw payload 落地到 RawInbox 並立即 commit——**返回前保證落地**。呼叫端（Task 6 的
    ShioajiAdapter callback）只需呼叫這一個函式就完成 durable 保證，不可只排程協程而不等
    commit 完成就返回。"""
    with session_factory() as session:
        brepo.stage_raw_inbox(session, kind=kind, broker=broker, payload=json.dumps(payload))
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
            row_ids = [r.id for r in brepo.list_unprocessed_raw_inbox(scan_session, limit=self._batch_limit)]
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
            except (ValueError, PositionMismatchError) as exc:
                session.rollback()
                row = session.get(RawInbox, row_id)
                kind = row.kind if row is not None else "?"  # commit 前先取（expire_on_commit 後不再讀 detached row）
                brepo.quarantine_raw_inbox(session, row, error=str(exc))
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
        fill = self._deal_mapper(payload)  # 不合法 → mapper 內部 raise ValueError（Task 6 嚴格驗證）

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
        report = self._order_report_mapper(payload)
        order = self._resolve_order_report_order(session, report)
        if order is None:
            raise ValueError(
                f"委託回報無法解析關聯（ordno={report.ordno!r}, broker_order_id={report.broker_order_id!r}）"
            )
        brepo.mark_order_status(session, order, status=report.status)
        brepo.mark_raw_inbox_processed(session, row)
        session.commit()

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
