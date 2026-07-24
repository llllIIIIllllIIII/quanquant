"""ShioajiAdapter：OrderService 的 Shioaji 落地。

lazy-import shioaji（比照 sources/shioaji_stream.py），一般測試/CLI import 這個模組不會
拉進原生 client。所有 native 呼叫（connect/close/place/cancel/update/positions）一律經
`BrokerSupervisor.run()` 這個單一 command executor（round3 #11）——**禁止**呼叫端各自
`async with supervisor.lock:`，否則 watchdog reconnect（Task 8）可能與 place 交錯替換
`_api`。鎖內、native 呼叫前最後一次 `_send_gate()` 檢查 kill switch（V3-2，修
BLOCKER#13 TOCTOU）；取消單刻意不檢查 kill switch（spec 明文：取消單仍允許）。

冪等（V3-3）：place 先以 client_order_id 查既有 Order，命中且 request_hash 相符 → 直接
回既有狀態，不重跑風控、不消費 confirm token、不再送單；不符 → 拒絕（同鍵不同 payload）；
命中但 owner 不符 → 拒絕（round3 開放清單 #6：非 owner 猜到 client_order_id+payload 不得
在授權前拿到他人 OrderAck）。canonical_payload_hash 一律用 self.mode/self.account
（server-side 真相），不接受外部輸入——`OrderRequest`（Task 2）結構上就沒有 mode 欄位，
外部混入 mode 在建構當下就會被 dataclass 拒絕（TypeError）。

callback-before-ack：place 在**送出 native 呼叫之前**就已經把 Order（client_order_id→
user_id/mode，ordno/broker_order_id 皆為 NULL 佔位）commit 進 DB，所以即使成交回報早於
ack 抵達（callback 在獨立執行緒，與本協程的 native 呼叫完成順序無關），RawInboxWorker
（Task 5）事後仍能透過 ordno/broker_order_id 補齊時解析到這筆委託（一開始解不到就
quarantine，等 ack 補上 ordno 後由 watchdog unquarantine 重試，不會遺失）。

callback → durable（round3 BLOCKER#2）：`set_order_callback` 的 handler
（`_on_order_cb`，跑在 Solace/.NET 背景執行緒）**只呼叫 `commit_raw_callback`**——
獨立 Session、同步、返回前保證落地，不做 `loop.call_soon_threadsafe` + `ensure_future`
排程協程晚點才 commit 的作法（那正是 round3 抓到的漏洞：loop 未就緒/排程後
crash/等 supervisor lock 時 payload 仍會遺失）；callback 內**不**碰部位/DB 業務邏輯，
那是 RawInboxWorker 之後才做的事。

**注意**：Shioaji SDK 的確切回呼欄位名稱（`order.id`/`order.seqno`/`status.id` 等）以
官方文件公開語意為準，本檔用 `getattr`/`payload.get` 防禦性存取（比照
`shioaji_stream.py::_dec` 既有慣例）；實作時若已安裝套件的 type stub 顯示不同屬性名，
只需局部調整以下三個函式內的欄位鍵，不影響本檔其餘架構：
`_ack_fields_from_trade` / `_map_deal_report` / `_map_order_report`。
"""
import asyncio
import logging
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from typing import Protocol

from sqlmodel import Session

from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, OrderError, RiskError
from quanquant.broker.inbox_worker import OrderReport, commit_raw_callback
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import Fill, Mode, OrderAck, OrderRequest, Position, canonical_payload_hash
from quanquant.db.models import Order

log = logging.getLogger(__name__)


class _RiskGuardLike(Protocol):
    """Task 7 RiskGuard 的結構型別（避免對 Task 7 模組的 import-time 相依）。"""

    kill_switch: bool

    def assert_owner(self, actor_user_id: int) -> None: ...
    def check_place(self, session: Session, req: OrderRequest, **kw) -> Order: ...
    def check_update(self, session: Session, order: Order, **kw) -> None: ...


class ShioajiAdapter:
    def __init__(
        self,
        *,
        api_key: str,
        secret_key: str,
        ca_path: str | None,
        ca_passwd: str | None,
        person_id: str | None,
        symbol: str,
        mode: Mode,
        session_factory: Callable[[], Session],
        supervisor: BrokerSupervisor,
        risk_guard: "_RiskGuardLike | None" = None,
        broker: str = "shioaji",
        sim_fee_per_lot: Decimal | None = None,
    ) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._ca_path = ca_path
        self._ca_passwd = ca_passwd
        self._person_id = person_id
        self.symbol = symbol
        self.mode: Mode = mode
        self.broker = broker
        self.account = ""
        self._session_factory = session_factory
        self._supervisor = supervisor
        self._risk_guard = risk_guard
        self._sim_fee_per_lot = sim_fee_per_lot  # A6：sim 成交 fee 缺值時依口數估算，不留 None/0
        self._api = None
        self._contract = None
        self._fill_handler: Callable[[Fill], None] | None = None

    # ---- 連線生命週期（round3 #11：connect/close 都經 supervisor.run，同一通道） ----

    async def connect(self) -> None:
        await self._supervisor.run(lambda: asyncio.to_thread(self._connect_blocking))

    def _connect_blocking(self) -> None:
        import shioaji as sj  # lazy：一般 import 這個模組不拉原生 client（比照 shioaji_stream.py）

        api = sj.Shioaji(simulation=(self.mode == "sim"))
        api.login(self._api_key, self._secret_key, fetch_contract=True, subscribe_trade=True)
        if self.mode == "real":
            api.activate_ca(ca_path=self._ca_path, ca_passwd=self._ca_passwd, person_id=self._person_id)
        api.set_order_callback(self._on_order_cb)
        self._api = api
        self.account = api.futopt_account.account_id
        self._contract = self._contract_for(self.symbol)

    async def close(self) -> None:
        async def _do_close() -> None:
            api, self._api = self._api, None
            if api is None:
                return
            try:
                await asyncio.to_thread(api.logout)
            except Exception:
                pass

        await self._supervisor.run(_do_close)

    def _contract_for(self, symbol: str):
        """比照 shioaji_stream.py::_front_contract：取最近未到期的具體月合約。"""
        import datetime as _dt

        category = getattr(self._api.Contracts.Futures, symbol)
        today = _dt.datetime.now().strftime("%Y/%m/%d")
        months = [c for c in category if "R" not in c.code[len(symbol):]]
        if not months:
            raise OrderError(f"找不到 {symbol} 的月合約")
        active = [c for c in months if (c.delivery_date or "") >= today]
        return min(active or months, key=lambda c: c.delivery_date or "9999/99/99")

    def on_fill(self, handler: Callable[[Fill], None]) -> None:
        self._fill_handler = handler  # 目前無呼叫端；保留供未來 push 通知擴充

    # ---- send gate（V3-2，鎖內、native 呼叫前的最後線性化點） ----

    async def _send_gate(self) -> None:
        if self._api is None:
            raise OrderError("下單 session 尚未就緒")
        if self._risk_guard is not None and self._risk_guard.kill_switch:
            raise RiskError("kill switch 已啟動，拒絕送出")

    # ---- place ----

    async def place(
        self, req: OrderRequest, *, actor_user_id: int, confirm_token: str | None = None
    ) -> OrderAck:
        request_hash = canonical_payload_hash(
            symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
            price_type=req.price_type, order_type=req.order_type, octype=req.octype,
            account=self.account, mode=self.mode,
        )
        with self._session_factory() as session:
            existing = brepo.find_order_by_client_order_id(session, req.client_order_id)
            if existing is not None:
                if existing.request_hash != request_hash:
                    raise OrderError(f"client_order_id={req.client_order_id!r} 已存在但 payload 不同")
                if existing.user_id != actor_user_id:
                    # round3 開放清單 #6：既有列命中只驗 request_hash 不夠——非 owner 猜到
                    # 別人的 client_order_id+完全相同 payload 不得在授權前拿到他人 OrderAck。
                    raise AuthorizationError("client_order_id 已被其他使用者的委託佔用")
                return self._ack_from_order(existing)  # 冪等：不重跑風控、不燒 token、不再送單

            if self._risk_guard is not None:
                order = self._risk_guard.check_place(
                    session, req, actor_user_id=actor_user_id, mode=self.mode, broker=self.broker,
                    account=self.account, request_hash=request_hash, confirm_token=confirm_token,
                )
            else:
                trading_day = brepo.trading_day_for(int(_now_ms()))
                order = brepo.create_order(
                    session, client_order_id=req.client_order_id, request_hash=request_hash,
                    user_id=actor_user_id, mode=self.mode, broker=self.broker, account=self.account,
                    symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
                    price_type=req.price_type, order_type=req.order_type, octype=req.octype,
                    trading_day=trading_day,
                )
                session.commit()
            # callback-before-ack：Order 此刻已 commit（client_order_id→user_id/mode 就位，
            # ordno/broker_order_id 仍是 NULL 佔位），即使成交回報早於下面的 native 呼叫完成，
            # RawInboxWorker 之後仍能靠 ordno/broker_order_id 補齊後解析到這筆委託。
            order_id, client_order_id = order.id, order.client_order_id

        async def _do_place():
            try:
                await self._send_gate()
                return await asyncio.to_thread(self._place_blocking, req)
            except RiskError:
                # send gate 擋下（如 kill switch）：確定沒送出，直接標 failed，不留在
                # pending 卡死（V3-2 修 BLOCKER#13 TOCTOU 的收尾）。quota reservation 的
                # 建立/釋放歸 risk_guard（Task 7）自己的職責——Task 6 這裡沒有
                # reservation_id 可用，不猜測呼叫 brepo.release_quota。
                with self._session_factory() as fail_session:
                    fail_order = fail_session.get(Order, order_id)
                    brepo.mark_order_status(fail_session, fail_order, status="failed")
                    fail_session.commit()
                raise
            except Exception as exc:
                with self._session_factory() as fail_session:
                    fail_order = fail_session.get(Order, order_id)
                    brepo.mark_order_status(fail_session, fail_order, status="unknown")
                    fail_session.commit()
                raise OrderError(f"送單失敗，委託標記 unknown 待 reconcile：{exc}") from exc

        ack_fields = await self._supervisor.run(_do_place)

        with self._session_factory() as session:
            brepo.set_order_ack(
                session, order_id, broker_order_id=ack_fields["broker_order_id"],
                ordno=ack_fields["ordno"], status="submitted",
            )
            session.commit()
        return OrderAck(
            client_order_id=client_order_id, broker_order_id=ack_fields["broker_order_id"],
            ordno=ack_fields["ordno"], status="submitted",
        )

    def _place_blocking(self, req: OrderRequest) -> dict:
        native_order = self._api.Order(
            action=req.action, price=float(req.price), quantity=req.qty,
            price_type=req.price_type, order_type=req.order_type, octype=req.octype,
            account=self._api.futopt_account,
        )
        trade = self._api.place_order(self._contract, native_order)
        return self._ack_fields_from_trade(trade)

    @staticmethod
    def _ack_fields_from_trade(trade) -> dict:
        order = getattr(trade, "order", None)
        ordno = getattr(order, "id", None) if order else None
        broker_order_id = (getattr(order, "seqno", None) if order else None) or ordno
        return {"ordno": ordno, "broker_order_id": broker_order_id}

    @staticmethod
    def _ack_from_order(order: Order) -> OrderAck:
        return OrderAck(
            client_order_id=order.client_order_id, broker_order_id=order.broker_order_id or "",
            ordno=order.ordno, status=order.status,
        )

    # ---- cancel ----

    async def cancel(self, broker_order_id: str, *, actor_user_id: int) -> OrderAck:
        with self._session_factory() as session:
            order = brepo.find_order_by_broker_id(
                session, broker=self.broker, account=self.account, mode=self.mode,
                broker_order_id=broker_order_id,
            )
            if order is None:
                raise OrderError(f"找不到委託 broker_order_id={broker_order_id!r}")
            if self._risk_guard is not None:
                self._risk_guard.assert_owner(actor_user_id)
            elif order.user_id != actor_user_id:
                raise AuthorizationError("非委託所有人不得取消")
            order_id, ordno, client_order_id = order.id, order.ordno, order.client_order_id

        async def _do_cancel() -> None:
            # kill switch 不擋取消單（spec 明文：取消單仍允許），仍檢查 session 就緒
            if self._api is None:
                raise OrderError("下單 session 尚未就緒")
            await asyncio.to_thread(self._cancel_blocking, ordno)

        await self._supervisor.run(_do_cancel)

        with self._session_factory() as session:
            order = session.get(Order, order_id)
            brepo.mark_order_status(session, order, status="cancelled")
            session.commit()
        return OrderAck(client_order_id=client_order_id, broker_order_id=broker_order_id, ordno=ordno, status="cancelled")

    def _cancel_blocking(self, ordno: str) -> None:
        self._api.cancel_order(ordno)

    # ---- update ----

    async def update(
        self,
        broker_order_id: str,
        *,
        actor_user_id: int,
        price=None,
        qty=None,
        confirm_token: str | None = None,
    ) -> OrderAck:
        with self._session_factory() as session:
            order = brepo.find_order_by_broker_id(
                session, broker=self.broker, account=self.account, mode=self.mode,
                broker_order_id=broker_order_id,
            )
            if order is None:
                raise OrderError(f"找不到委託 broker_order_id={broker_order_id!r}")
            new_price = price if price is not None else order.price
            new_qty = qty if qty is not None else order.qty
            request_hash = canonical_payload_hash(
                symbol=order.symbol, action=order.action, qty=new_qty, price=new_price,
                price_type=order.price_type, order_type=order.order_type, octype=order.octype,
                account=self.account, mode=self.mode,
            )
            if self._risk_guard is not None:
                self._risk_guard.check_update(
                    session, order, actor_user_id=actor_user_id, new_qty=new_qty, new_price=new_price,
                    request_hash=request_hash, confirm_token=confirm_token,
                )
            elif order.user_id != actor_user_id:
                raise AuthorizationError("非委託所有人不得改單")
            order_id, ordno, client_order_id = order.id, order.ordno, order.client_order_id

        async def _do_update() -> None:
            await self._send_gate()
            await asyncio.to_thread(self._update_blocking, ordno, new_price, new_qty)

        await self._supervisor.run(_do_update)

        with self._session_factory() as session:
            order = session.get(Order, order_id)
            order.price, order.qty = new_price, new_qty
            brepo.mark_order_status(session, order, status="submitted")
            session.commit()
        return OrderAck(client_order_id=client_order_id, broker_order_id=broker_order_id, ordno=ordno, status="submitted")

    def _update_blocking(self, ordno: str, price, qty: int) -> None:
        self._api.update_order(ordno, price=float(price), qty=qty)

    # ---- positions（純讀 DB，不呼叫 native API——BrokerPosition 是唯一真相來源；
    #      仍經 supervisor.run 走同一通道，避免與 connect/reconnect 交錯讀到半新半舊狀態） ----

    async def positions(self, *, actor_user_id: int) -> list[Position]:
        def _do_positions() -> list[Position]:
            with self._session_factory() as session:
                if self._risk_guard is not None:
                    self._risk_guard.assert_owner(actor_user_id)
                rows = brepo.list_open_positions(
                    session, user_id=actor_user_id, broker=self.broker, account=self.account,
                    mode=self.mode, symbol=self.symbol,
                )
                return [
                    Position(
                        symbol=r.symbol, direction=r.direction,
                        qty=brepo.remaining_qty(r), avg_price=brepo.avg_entry_price(r),
                    )
                    for r in rows
                ]

        return await self._supervisor.run(_do_positions)

    # ---- callback（背景執行緒）：只落地 RawInbox，不直接處理業務邏輯（round3 BLOCKER#2） ----

    def _on_order_cb(self, stat, msg) -> None:
        """set_order_callback 的 handler，跑在 Solace/.NET 背景執行緒。round3 修正：只呼叫
        `commit_raw_callback`（獨立 Session、同步、返回前保證落地），不做
        `loop.call_soon_threadsafe`+`ensure_future` 排程協程晚點才 commit 的作法——那個
        作法在 loop 未就緒/排程後 crash/等 supervisor lock 時 payload 仍會遺失（round3
        BLOCKER#2）。callback 內不碰部位/DB 業務邏輯，那是 RawInboxWorker 之後才做的事。
        """
        kind = "deal_report" if str(stat).endswith("Deal") else "order_report"
        payload = self._json_safe(msg)
        commit_raw_callback(self._session_factory, kind=kind, broker=self.broker, payload=payload)

    @staticmethod
    def _json_safe(msg) -> dict:
        if isinstance(msg, dict):
            return msg
        if hasattr(msg, "to_dict"):
            try:
                return msg.to_dict()
            except Exception:
                pass
        return {"raw": str(msg)}

    # ---- Task 5 DealMapper / OrderReportMapper 實作（V3-4 嚴格驗證） ----

    def _map_deal_report(self, payload: dict) -> Fill:
        try:
            fill_id = payload["deal_id"]
            if not fill_id:
                raise ValueError("deal_id 為空")
            action = payload["action"]
            octype = payload["octype"]
            qty = int(payload["quantity"])
            price = Decimal(str(payload["price"]))
            ts = int(payload["ts"])
            account = payload["account_id"]
            ordno = payload.get("order_id")
            broker_order_id = payload.get("seqno") or ordno
            fee_raw = payload.get("fee")
            fee = Decimal(str(fee_raw)) if fee_raw not in (None, "") else None
            symbol = payload.get("code") or self.symbol
        except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
            raise ValueError(f"deal_report payload 缺值或格式不合法: {exc}") from exc

        if fee is None and self.mode == "sim" and self._sim_fee_per_lot is not None:
            # A6：sim 模擬單成交 fee 常缺值/零，依設定的「每口」估算，按 qty 分批累計時自然正確
            # （每筆 fill 各自算 qty*sim_fee_per_lot，PositionTracker 累加 open_fee_total/close_fee_total
            # 時就是「已成交口數 * 每口 fee」的正確累計，不需要另外處理批次）。real 模式缺值一律
            # 保持 None（round3 HIGH：不得靜默記 0，那會讓正式 PnL 永久低估成本）。
            fee = self._sim_fee_per_lot * qty

        return Fill(
            broker=self.broker, fill_id=str(fill_id), ordno=ordno, broker_order_id=broker_order_id,
            symbol=symbol, action=action, price=price, qty=qty, fee=fee, octype=octype, ts=ts,
            account=account, mode=self.mode, user_id=None,
        )

    _STATUS_MAP = {
        "Cancelled": "cancelled", "Failed": "failed", "PartFilled": "partfilled",
        "Filled": "filled", "PendingSubmit": "sending", "Submitted": "submitted",
    }

    def _map_order_report(self, payload: dict) -> OrderReport:
        status_raw = payload.get("status")
        ordno = payload.get("order_id")
        broker_order_id = payload.get("seqno") or ordno
        if not ordno and not broker_order_id:
            raise ValueError("order_report 缺 order_id/seqno，無法關聯委託")
        status = self._STATUS_MAP.get(str(status_raw))
        if status is None:
            raise ValueError(f"未知委託狀態: {status_raw!r}")
        return OrderReport(
            broker=self.broker, account=self.account, mode=self.mode,
            ordno=ordno, broker_order_id=broker_order_id, status=status,
        )


def _now_ms() -> float:
    import time

    return time.time() * 1000
