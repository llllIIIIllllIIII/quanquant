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
import json
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Protocol

from sqlmodel import Session

from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, OrderError, RiskError
from quanquant.broker.inbox_worker import OrderReport, commit_raw_callback
from quanquant.broker.redaction import redact_secrets as _redact_secrets
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
        # F8（round3 中央化 redaction）：這個 adapter instance 認得的所有秘密值，供
        # place/update/health_probe 的例外訊息、以及 watchdog/lifespan/HTTP 路由（經
        # `secrets_to_redact` 這個 public property 讀取）呼叫 broker.redaction.redact_secrets
        # 時使用，確保任何可能夾帶這些值的上游例外文字落地/回顯前一律先過濾。
        self._secrets_to_redact = [s for s in (api_key, secret_key, ca_passwd, person_id) if s]
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

    @property
    def supervisor(self) -> BrokerSupervisor:
        """Task 8 watchdog 需要直接拿鎖做 DB-only 背景工作（unquarantine/unknown reconcile），
        不經過 `.run()`（那是給 native shioaji API 呼叫用的通道，round3 #11）。"""
        return self._supervisor

    @property
    def secrets_to_redact(self) -> list[str]:
        """F8：watchdog/lifespan/HTTP 路由沒有直接持有 api_key/secret_key/ca_passwd/
        person_id，但持有這個 adapter instance——經這個 property 取得同一份秘密清單，統一
        呼叫 `broker.redaction.redact_secrets`，不需要各自另外接收/保存一份秘密。回傳
        copy（不是內部 list 的參照），避免呼叫端意外修改到 adapter 內部狀態。"""
        return list(self._secrets_to_redact)

    # ---- health probe（round3 #10：序列化 broker health probe，不只看 `_api is not None`） ----

    async def health_probe(self) -> bool:
        """探測底層連線是否還活著。`self._api` 是 python object，就算底層 TCP/會話已經斷線，
        object 本身通常還在——只檢查 `_api is not None` 會讓 watchdog 永遠以為連線健康、
        永不重連（round3 #10 抓到的漏洞）。這裡經 supervisor.run() 同一通道做一次輕量、
        無副作用的 native 呼叫，失敗（含任何例外）一律視為不健康。"""

        async def _do_probe() -> bool:
            if self._api is None:
                return False
            try:
                await asyncio.to_thread(self._probe_blocking)
                return True
            except Exception as exc:
                log.warning(
                    "health_probe 失敗（視為不健康，觸發重連）: %s",
                    _redact_secrets(str(exc), secrets=self._secrets_to_redact),
                )
                return False

        return await self._supervisor.run(_do_probe)

    def _probe_blocking(self) -> None:
        """SDK 確切探測 API 待實機驗證，以 getattr 防禦性存取（同檔一貫慣例，見模組頂部
        說明）：優先用 `list_accounts()`（存在即代表 session 仍能來回一次 native 呼叫）；
        找不到時 fallback 讀 `futopt_account`（純屬性存取，至少能驗證 client 物件仍持有
        登入後才會有的狀態，AttributeError 會被呼叫端當成探測失敗）。"""
        probe_fn = getattr(self._api, "list_accounts", None)
        if callable(probe_fn):
            probe_fn()
            return
        _ = self._api.futopt_account

    # ---- reconcile（round3 #2：重連後對帳，持久 cursor + 分辨委託/成交，不只 retry 本地 quarantine） ----

    async def reconcile(self) -> None:
        """重連後對帳：拉券商目前委託回報補回 RawInbox（kind="order_report"，不是 round3 覆核前
        草稿版本把所有列都當 deal_report 那個 bug）。用持久 `BrokerReconcileCursor` watermark
        只補「上次對帳後有新進展」的委託——重啟後從 DB 讀回 cursor 續接，不會每次都重新灌一次
        全量 snapshot，也不會因為重啟就遺失對帳進度。

        已知限制（誠實記錄，非本檔可單方面解決）：Shioaji 的 `list_trades()` 只回傳委託層級的
        彙總狀態（含 `deal_quantity` 累計數），不含逐筆真實 deal_id；V3-4 明文禁止用
        ordno/seqno 等 fallback 冒充 fill_id 建構 Fill（見 broker/types.py），因此本函式刻意
        不嘗試從這裡重建成交明細——成交回報一律只信任 durable callback
        （`_on_order_cb`→`commit_raw_callback`，已保證同步落地不遺失，見模組頂部說明）。若
        真的發生「斷線期間券商 callback 完全沒送達」的成交缺口，需要券商提供逐筆歷史回放
        API 才能完整補齊，超出目前高階 SDK 介面下可靠實作的範圍。
        """
        await self._supervisor.run(lambda: asyncio.to_thread(self._reconcile_blocking))

    def _reconcile_blocking(self) -> None:
        if self._api is None:
            return
        with self._session_factory() as session:
            cursor = brepo.get_reconcile_cursor(session, broker=self.broker, account=self.account, mode=self.mode)

        list_trades = getattr(self._api, "list_trades", None)
        trades = list_trades() if callable(list_trades) else []

        newest = cursor
        staged: list[dict] = []
        for trade in trades:
            watermark = self._trade_watermark(trade)
            if cursor is not None and watermark is not None and watermark <= cursor:
                continue  # 已對帳過，週期補洞只補新進展
            order = getattr(trade, "order", None)
            status = getattr(trade, "status", None)
            ordno = getattr(order, "id", None) if order is not None else None
            seqno = (getattr(order, "seqno", None) if order is not None else None) or ordno
            if not ordno and not seqno:
                continue  # 無法關聯到任何委託，略過（不硬塞垃圾進 RawInbox）
            status_raw = getattr(status, "status", None) if status is not None else None
            staged.append({
                "order_id": ordno, "seqno": seqno,
                "status": str(status_raw) if status_raw is not None else None,
            })
            if watermark is not None and (newest is None or watermark > newest):
                newest = watermark

        if not staged and newest == cursor:
            return  # 沒有新東西，連 cursor 都不動（避免每次 watchdog 週期都無意義地寫 DB）

        with self._session_factory() as session:
            for payload in staged:
                brepo.stage_raw_inbox(session, kind="order_report", broker=self.broker, payload=json.dumps(payload))
            brepo.upsert_reconcile_cursor(
                session, broker=self.broker, account=self.account, mode=self.mode,
                at=newest if newest is not None else _utcnow_naive(),
            )
            session.commit()

    @staticmethod
    def _trade_watermark(trade) -> datetime | None:
        """防禦性抽取 Trade 的時間戳（欄位名稱待實機 SDK 驗證）；抽不到就回 None（呼叫端視為
        「無法判斷新舊」，保守地一律納入這次對帳，最多是重複補一次——下游 order_report
        處理本身是冪等/單調的，重複補不會造成錯誤，只是白工）。"""
        status = getattr(trade, "status", None)
        raw = getattr(status, "order_datetime", None) if status is not None else None
        if raw is None:
            return None
        if isinstance(raw, datetime):
            return raw
        try:
            return datetime.fromisoformat(str(raw))
        except (TypeError, ValueError):
            return None

    def _query_order_qty_blocking(self, ordno: str) -> int | None:
        """Task 8 watchdog「quota unknown reconcile」收尾用（round3 殘留1）：查詢券商目前對
        這筆委託回報的口數（quantity），供 watchdog 判斷 update 逾時（unknown）後這次改單
        究竟是否真的生效——比對這個回傳值與「改單前口數」/「改單後目標口數」，才能決定
        對應的 delta QuotaReservation 該 confirm 還是 release（見 watchdog.py
        `_reconcile_unknown_quota_blocking`）。

        **呼叫端責任（避免鎖重入死結）**：本函式刻意設計成單純同步、自己不取
        `supervisor` 鎖——watchdog 對這批「DB-only 背景工作」是直接
        `async with adapter.supervisor.lock:` 整段包住（見本檔 `supervisor` property 說明、
        watchdog.py 模組頂部），本函式若再呼叫 `supervisor.run()` 會在同一顆
        `asyncio.Lock` 上重入，永久卡死；因此只能在「呼叫端已經持有鎖」的前提下直接呼叫。

        找不到這筆委託（已從 `list_trades()` 目前清單消失，例如已完全結案）回 None，
        呼叫端視為無法判斷、不猜測。欄位名稱（`order.id`/`order.quantity`）待實機 SDK
        驗證（同檔一貫 getattr 防禦性慣例，見模組頂部說明）。"""
        if self._api is None:
            return None
        list_trades = getattr(self._api, "list_trades", None)
        trades = list_trades() if callable(list_trades) else []
        for trade in trades:
            order = getattr(trade, "order", None)
            if order is not None and getattr(order, "id", None) == ordno:
                qty = getattr(order, "quantity", None)
                return int(qty) if qty is not None else None
        return None

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
                # pending 卡死（V3-2 修 BLOCKER#13 TOCTOU 的收尾）。round3 #4 收尾：確定
                # 沒送出 → 退還這筆保留的配額（reservation_id 就是 client_order_id，與
                # RiskGuard.check_place 建立保留列時用的同一把鍵——見 repository.reserve_quota
                # 的呼叫端冪等鍵慣例）；沒有 risk_guard 時本來就沒有保留列可退。
                with self._session_factory() as fail_session:
                    fail_order = fail_session.get(Order, order_id)
                    brepo.mark_order_status(fail_session, fail_order, status="failed")
                    if self._risk_guard is not None:
                        brepo.release_quota(fail_session, reservation_id=req.client_order_id)
                    fail_session.commit()
                raise
            except Exception as exc:
                # 結果不明（可能已經送到券商、只是回應逾時/連線中斷）：不 release、不
                # confirm，保留列維持 reserved，留給 Task 8 watchdog reconcile 決議
                # （round3 #4：unknown 不得立即 release，否則若其實已送達會讓配額被
                # 誤退還、變相突破日限）。
                with self._session_factory() as fail_session:
                    fail_order = fail_session.get(Order, order_id)
                    brepo.mark_order_status(fail_session, fail_order, status="unknown")
                    fail_session.commit()
                raise OrderError(
                    f"送單失敗，委託標記 unknown 待 reconcile："
                    f"{_redact_secrets(str(exc), secrets=self._secrets_to_redact)}"
                ) from exc

        ack_fields = await self._supervisor.run(_do_place)

        with self._session_factory() as session:
            brepo.set_order_ack(
                session, order_id, broker_order_id=ack_fields["broker_order_id"],
                ordno=ack_fields["ordno"], status="submitted",
            )
            if self._risk_guard is not None:
                # 送出成功 → 這筆保留的配額永久計入今日已用（round3 #4 收尾）。
                brepo.confirm_quota(session, reservation_id=req.client_order_id)
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
            # round3 #6：owner allowlist 過了不代表這張委託是這個 owner 的——多 owner
            # 情境下若只驗 assert_owner 就直接放行，owner A 能取消 owner B 的委託。這裡
            # 一律再以 (user_id,broker,mode,broker_order_id) 驗真正委託所有權（先前版本
            # 這行只在沒有 risk_guard 時的 elif 分支才會跑，risk_guard 存在時被跳過）。
            if order.user_id != actor_user_id:
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

        # round3 #4/#11 收尾：這次改單「若有」保留的 delta 配額（RiskGuard.check_update 只在
        # new_qty 較原本增加時才會建立這列，見 repository.reservation_id_for_update），送出
        # 成功/失敗後在這裡 confirm/release；沒有 risk_guard 就沒有保留列，不猜測呼叫。
        reservation_id = (
            brepo.reservation_id_for_update(client_order_id=client_order_id, request_hash=request_hash)
            if self._risk_guard is not None else None
        )

        async def _do_update() -> None:
            await self._send_gate()
            await asyncio.to_thread(self._update_blocking, ordno, new_price, new_qty)

        try:
            await self._supervisor.run(_do_update)
        except RiskError:
            # send gate 擋下（如 kill switch 剛好在改單當下被打開）：確定沒送出，退還這次
            # 改單嘗試「若有」保留的 delta 配額（沒有保留列時 release_quota 是 no-op）；
            # 委託本身的狀態不變（改單失敗不代表委託本身壞了，不比照 place 標 failed）。
            if reservation_id is not None:
                with self._session_factory() as fail_session:
                    brepo.release_quota(fail_session, reservation_id=reservation_id)
                    fail_session.commit()
            raise
        except Exception as exc:
            # 結果不明：不 release、不 confirm，留給 Task 8 watchdog reconcile 決議
            # （同 place 的 round3 #4 收尾邏輯）。
            with self._session_factory() as fail_session:
                fail_order = fail_session.get(Order, order_id)
                brepo.mark_order_status(fail_session, fail_order, status="unknown")
                fail_session.commit()
            raise OrderError(
                f"改單失敗，委託標記 unknown 待 reconcile："
                f"{_redact_secrets(str(exc), secrets=self._secrets_to_redact)}"
            ) from exc

        with self._session_factory() as session:
            order = session.get(Order, order_id)
            order.price, order.qty = new_price, new_qty
            brepo.mark_order_status(session, order, status="submitted")
            if reservation_id is not None:
                brepo.confirm_quota(session, reservation_id=reservation_id)
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


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
