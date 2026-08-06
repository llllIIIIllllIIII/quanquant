"""ShioajiNativeClient：對 Shioaji SDK 的所有直接呼叫封裝於此（Task 1，Inc0 第一步）。

從 `shioaji_adapter.py` 逐字抽出所有直接碰 Shioaji SDK 的程式碼，零 DB/repository/session
相依，供之後在使用者本機 broker agent 內嵌執行——**本檔是全 broker 子系統唯一允許 import
shioaji 的模組；不得 import 任何 DB/repository/session 相關符號**。

所有方法皆為同步阻塞呼叫，不含 `asyncio`/`BrokerSupervisor` 序列化——序列化責任交由呼叫端
（例如 Task 2 的 `ShioajiAdapter` 經 `BrokerSupervisor.run()` 呼叫本檔方法，或本機 agent
自行串行化）。

`on_raw`（建構子注入的 `Callable[[str, dict], None]`）取代原本 `_on_order_cb` 直呼
`commit_raw_callback` 的落地方式：落地責任移交呼叫端，本檔只負責「kind 判斷 + JSON 安全化
+ 呼叫 on_raw」，degradation（`on_raw` 或 `json_safe` 例外時退化重試帶 `_unparsed` 標記的
payload）邏輯與原本等價保留。

**注意**：Shioaji SDK 的確切回呼欄位名稱（`order.id`/`order.seqno`/`status.status` 等）以
官方文件公開語意為準，本檔用 `getattr`/`payload.get` 防禦性存取（比照
`shioaji_stream.py::_dec`/`shioaji_adapter.py` 既有慣例）。
"""
import logging
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal

from quanquant.broker.base import OrderError, TradeNotFoundError
from quanquant.broker.types import Mode

log = logging.getLogger(__name__)


class ShioajiNativeClient:
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
        on_raw: Callable[[str, dict], None],
    ) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._ca_path = ca_path
        self._ca_passwd = ca_passwd
        self._person_id = person_id
        self.symbol = symbol
        self.mode: Mode = mode
        self.account = ""
        self._on_raw = on_raw
        self.api = None
        self.contract = None

    # ---- 連線生命週期 ----

    def connect(self) -> str:
        import shioaji as sj  # lazy：一般 import 這個模組不拉原生 client（比照 shioaji_stream.py）

        api = sj.Shioaji(simulation=(self.mode == "sim"))
        api.login(self._api_key, self._secret_key, fetch_contract=True, subscribe_trade=True)
        if self.mode == "real":
            api.activate_ca(ca_path=self._ca_path, ca_passwd=self._ca_passwd, person_id=self._person_id)
        api.set_order_callback(self._on_order_cb)
        self.api = api
        self.account = api.futopt_account.account_id
        self.contract = self._contract_for(self.symbol)
        return self.account

    def close(self) -> None:
        api, self.api = self.api, None
        if api is None:
            return
        try:
            api.logout()
        except Exception:
            pass

    def _contract_for(self, symbol: str):
        """比照 shioaji_stream.py::_front_contract：取最近未到期的具體月合約。"""
        import datetime as _dt

        category = getattr(self.api.Contracts.Futures, symbol)
        today = _dt.datetime.now().strftime("%Y/%m/%d")
        months = [c for c in category if "R" not in c.code[len(symbol):]]
        if not months:
            raise OrderError(f"找不到 {symbol} 的月合約")
        active = [c for c in months if (c.delivery_date or "") >= today]
        return min(active or months, key=lambda c: c.delivery_date or "9999/99/99")

    # ---- health probe ----

    def probe(self) -> None:
        """SDK 確切探測 API 待實機驗證，以 getattr 防禦性存取（同檔一貫慣例，見模組頂部
        說明）：優先用 `list_accounts()`（存在即代表 session 仍能來回一次 native 呼叫）；
        找不到時 fallback 讀 `futopt_account`（純屬性存取，至少能驗證 client 物件仍持有
        登入後才會有的狀態，AttributeError 會被呼叫端當成探測失敗）。"""
        probe_fn = getattr(self.api, "list_accounts", None)
        if callable(probe_fn):
            probe_fn()
            return
        _ = self.api.futopt_account

    # ---- reconcile（trades snapshot：純 SDK 讀取，watermark 過濾/持久化交由呼叫端） ----

    def trades_snapshot(self, after: datetime | None) -> tuple[list[dict], datetime | None]:
        """回傳（本次篩選出的委託進展 payload 清單, 這批資料中最新的 watermark）；
        `api` 尚未連線（None）時回 `([], None)`。只做「讀 SDK + 依 watermark 過濾」，不含
        任何 cursor 持久化/DB 寫入（那是呼叫端 `_reconcile_blocking` 的責任，本檔零 DB 相依）。
        """
        if self.api is None:
            return [], None

        list_trades = getattr(self.api, "list_trades", None)
        trades = list_trades() if callable(list_trades) else []

        newest = after
        staged: list[dict] = []
        for trade in trades:
            watermark = self.trade_watermark(trade)
            if after is not None and watermark is not None and watermark <= after:
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
        return staged, newest

    @staticmethod
    def trade_watermark(trade) -> "datetime | None":
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

    def query_order_qty(self, ordno: str) -> int | None:
        """查詢券商目前對這筆委託回報的口數（quantity）——供呼叫端判斷 update 逾時
        （unknown）後這次改單究竟是否真的生效。找不到這筆委託（已從 `list_trades()` 目前
        清單消失，例如已完全結案）回 None，呼叫端視為無法判斷、不猜測。欄位名稱
        （`order.id`/`order.quantity`）待實機 SDK 驗證（同檔一貫 getattr 防禦性慣例）。"""
        if self.api is None:
            return None
        list_trades = getattr(self.api, "list_trades", None)
        trades = list_trades() if callable(list_trades) else []
        for trade in trades:
            order = getattr(trade, "order", None)
            if order is not None and getattr(order, "id", None) == ordno:
                qty = getattr(order, "quantity", None)
                return int(qty) if qty is not None else None
        return None

    # ---- place ----

    def place(
        self, *, action: str, price: Decimal, qty: int, price_type: str, order_type: str, octype: str
    ) -> dict:
        # 防禦（bug 2）：MKT 不需要價格，顯式送 0.0，不信任呼叫端傳入 price 當下的值。
        sendable_price = 0.0 if price_type == "MKT" else float(price)
        native_order = self.api.Order(
            action=action, price=sendable_price, quantity=qty,
            price_type=price_type, order_type=order_type, octype=octype,
            account=self.api.futopt_account,
        )
        trade = self.api.place_order(self.contract, native_order)
        return self.ack_fields_from_trade(trade)

    @staticmethod
    def ack_fields_from_trade(trade) -> dict:
        order = getattr(trade, "order", None)
        ordno = getattr(order, "id", None) if order else None
        broker_order_id = (getattr(order, "seqno", None) if order else None) or ordno
        return {"ordno": ordno, "broker_order_id": broker_order_id}

    # ---- cancel/update 共用：委託刷新 + 比對 ----

    def _refresh_and_list_trades(self) -> list:
        """cancel/update 前的委託狀態刷新（bug 2/3）：依 `_core.pyi` 真實簽章，
        `update_status(account=...)` 是純 side-effect 呼叫（回傳 `None`，不是 list——刷新
        SDK 內部快取），刷新完才呼叫 `list_trades()` 取回目前的 `Trade` 物件清單。
        `update_status`/`list_trades` 皆防禦性 `getattr`（比照本檔一貫慣例）：假
        client／舊版 SDK 沒有這個方法時直接跳過，不影響後續清單（測試用的假 client 資料
        本來就是即時的，不需要真的刷新）。"""
        update_status = getattr(self.api, "update_status", None)
        if callable(update_status):
            update_status(getattr(self.api, "futopt_account", None))
        list_trades = getattr(self.api, "list_trades", None)
        return list_trades() if callable(list_trades) else []

    def _find_trade_by_ordno(self, ordno: str | None):
        """cancel/update 前先刷新 + `list_trades()` 比對回真正的 `Trade` 物件（bug 2/3
        根因收尾）：`cancel_order`/`update_order` 依 `_core.pyi` 真實簽章要收 `Trade`
        物件，不是委託單號字串——先前直接塞 `ordno` 字串進去，在真實 SDK 會炸
        `argument 'trade': 'str' object is not an instance of 'Trade'`。

        比對鍵沿用 `ack_fields_from_trade`/`trades_snapshot` 既有慣例：我方存的
        `ordno` ← Shioaji `OrderResult.id`（同一組欄位，兩處對得起來才能正確關聯）。找不到
        （已成交/已刪/跨日等，`list_trades()` 目前清單裡已經沒有這筆委託）回 `None`，
        呼叫端（`cancel`/`update`）視為無法判斷、拒絕盲目操作，不得猜測著把字串硬塞給
        native API。"""
        for trade in self._refresh_and_list_trades():
            order = getattr(trade, "order", None)
            if order is not None and getattr(order, "id", None) == ordno:
                return trade
        return None

    # ---- cancel ----

    def cancel(self, ordno: str) -> None:
        trade = self._find_trade_by_ordno(ordno)
        if trade is None:
            raise OrderError(
                f"找不到券商對應委託（ordno={ordno!r}），可能已成交/已刪除/跨日，"
                "拒絕在無法確認對應委託的情況下送出取消"
            )
        self.api.cancel_order(trade)

    # ---- update ----

    def update(self, ordno: str, *, price, qty: int, price_type: str | None = None) -> None:
        # bug 2/3：先刷新 + list_trades() 比對回真正的 Trade 物件——update_order 依
        # _core.pyi 真實簽章要收 Trade（見 _find_trade_by_ordno 說明），不是 ordno 字串。
        trade = self._find_trade_by_ordno(ordno)
        if trade is None:
            raise TradeNotFoundError(ordno)
        # 防禦（bug 2），同 place：MKT 顯式送 0.0，不信任呼叫端算出的 price 當下的值。
        sendable_price = 0.0 if price_type == "MKT" else float(price)
        self.api.update_order(trade, price=sendable_price, qty=qty)

    # ---- callback（背景執行緒）：只做 kind 判斷 + json_safe，落地責任交給呼叫端 ----

    def _on_order_cb(self, stat, msg) -> None:
        """`set_order_callback` 的 handler，跑在 Solace/.NET 背景執行緒。與原本
        `ShioajiAdapter._on_order_cb` 的差異：原本直呼 `commit_raw_callback`落地，這裡改呼叫
        `self._on_raw(kind, payload)`——落地責任移交呼叫端（本檔零 DB 相依）。degradation
        邏輯等價保留：`on_raw` 或 `json_safe` 例外時，退化重試帶 `_unparsed` 標記與原始
        `repr` 的 payload，再失敗只記錄，不把例外拋回 callback thread（券商回報是真錢關鍵
        路徑，序列化/落地失敗絕不能靜默丟單，也絕不能殺掉整條回報通道）。"""
        kind = "deal_report" if str(stat).endswith("Deal") else "order_report"
        try:
            payload = self.json_safe(msg)
            self._on_raw(kind, payload)
        except Exception:
            log.exception("成交/委託回報落地失敗，改以退化 payload 保存（kind=%s）", kind)
            try:
                self._on_raw(kind, {"_unparsed": True, "repr": repr(msg)})
            except Exception:
                log.exception("退化 payload 也落地失敗，回報恐遺失（kind=%s）", kind)

    @staticmethod
    def json_safe(msg) -> dict:
        """把 shioaji callback 的 `msg` 轉成保證 JSON 可序列化的巢狀 dict/list（供呼叫端
        `commit_raw_callback` 之類的落地邏輯 `json.dumps` 使用）。

        依 `_core.pyi` 實測確認（bug 1(b) 根因之一）：真實 callback 的 `msg` 是
        `OrderEventDict`——有 `keys()`/`__getitem__`/`items()` 等 Mapping 協定方法，但**不是**
        `dict` 子類、也**沒有** `to_dict()`；先前只判斷 `isinstance(dict)`/`hasattr(to_dict)`，
        兩者都不中，直接落到 `{"raw": str(msg)}`，整包結構全部遺失。改用 `dict(msg)`（dict
        constructor 認得任何有 `.keys()`+`__getitem__` 的 mapping）。

        `FuturesOrderEvent` 是巢狀結構（`operation`/`order`/`status`/`contract` 各自可能也是
        同款 Mapping-only 物件，不是原生 dict）——只轉最外層不夠，`json.dumps` 遇到巢狀的
        非原生型別一樣會炸；因此這裡遞迴轉換每一層，確保回傳值全部由原生
        dict/list/str/int/float/bool/None 組成。
        """

        def _convert(value):
            if isinstance(value, dict):
                return {k: _convert(v) for k, v in value.items()}
            if isinstance(value, (list, tuple)):
                return [_convert(v) for v in value]
            if hasattr(value, "to_dict"):
                try:
                    return _convert(value.to_dict())
                except Exception:
                    pass
            if hasattr(value, "keys") and hasattr(value, "__getitem__"):
                try:
                    return {k: _convert(value[k]) for k in value.keys()}
                except Exception:
                    pass
            # 保底（真錢丟單防線）：走到這裡代表無法結構化轉換。JSON 原生型別原封保留，
            # 其餘一律 str()——確保下游 json.dumps 永不炸、整包回報不會因序列化失敗而遺失
            # （datetime/Decimal/enum/未知 SDK 物件等葉節點皆轉字串）。
            if value is None or isinstance(value, (str, int, float, bool)):
                return value
            return str(value)

        converted = _convert(msg)
        return converted if isinstance(converted, dict) else {"raw": str(msg)}
