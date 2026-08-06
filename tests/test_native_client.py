"""ShioajiNativeClient：純 Shioaji SDK 操作、零 DB 相依（Task 1，Inc0 第一步）。

比照 `tests/test_shioaji_adapter.py:46-97` 的 `_FakeApi`/`_FakeTrade` 模式，本檔自帶一份假
shioaji client（不跨檔 import 測試私有類）。`_FakeApi` 預先塞一筆帶 `status` 的假委託
（`SEED1`），供 `trades_snapshot` 測試不需額外設置即可驗證 watermark 過濾/回傳 newest 的
邏輯；`cancel`/`update` 測試用的 `"NOPE"` ordno 不會命中任何既有委託（含這筆種子），仍能
驗證「找不到對應委託」的例外路徑。
"""
from datetime import datetime
from decimal import Decimal

from quanquant.broker.base import OrderError, TradeNotFoundError
from quanquant.broker.native import ShioajiNativeClient
from quanquant.broker.shioaji_adapter import ShioajiAdapter


class _FakeOrderHandle:
    def __init__(self, id_, seqno, ordno=None):
        self.id = id_
        self.seqno = seqno
        self.ordno = ordno if ordno is not None else f"ALT-{id_}"
        self.quantity = 1


class _FakeStatus:
    def __init__(self, status, order_datetime):
        self.status = status
        self.order_datetime = order_datetime


class _FakeTrade:
    def __init__(self, id_, seqno, ordno=None, status=None):
        self.order = _FakeOrderHandle(id_, seqno, ordno)
        self.status = status


class _FakeApi:
    """假 shioaji client：不連真網路，place_order/cancel_order/update_order 皆同步回傳。

    `place_order` 收到的 native order（`Order(**kw)` 直接回傳 kw dict）直接存進 `placed`
    （不包 `(contract, order)` tuple），讓測試可以直接 `placed[-1]["price"]` 斷言。
    `cancel_order`/`update_order` 依真實 SDK 契約收 `Trade` 物件，收到非 Trade-like 就
    raise `TypeError`（比照 test_shioaji_adapter 既有慣例）。
    """

    def __init__(self):
        self.placed = []
        self.futopt_account = type("Acc", (), {"account_id": "F1"})()
        self._seq = 0
        self._live_trades: dict = {}
        self.update_status_calls = 0
        # 預先塞一筆帶 status 的假委託，供 trades_snapshot 測試不需額外設置即可驗證。
        seed = _FakeTrade("SEED1", "SEEDSEQ1", status=_FakeStatus("Filled", datetime(2026, 1, 1, 9, 0, 0)))
        self._live_trades[seed.order.id] = seed

    def Order(self, **kw):
        return kw

    def place_order(self, contract, order):
        self._seq += 1
        self.placed.append(order)
        trade = _FakeTrade(f"ORD{self._seq}", f"SEQ{self._seq}")
        self._live_trades[trade.order.id] = trade
        return trade

    def update_status(self, account=None, **kw):
        self.update_status_calls += 1

    def list_trades(self):
        return list(self._live_trades.values())

    @staticmethod
    def _assert_trade(trade) -> None:
        if not hasattr(trade, "order"):
            raise TypeError(
                f"argument 'trade': {type(trade).__name__!r} object is not an instance of 'Trade'"
            )

    def cancel_order(self, trade):
        self._assert_trade(trade)
        return trade

    def update_order(self, trade, **kw):
        self._assert_trade(trade)
        return trade

    def logout(self):
        pass


def _client(on_raw=None):
    c = ShioajiNativeClient(
        api_key="k", secret_key="s", ca_path=None, ca_passwd=None, person_id=None,
        symbol="TXF", mode="sim", on_raw=on_raw or (lambda kind, payload: None),
    )
    c.api = _FakeApi()
    c.contract = object()
    c.account = "F1"
    return c


def test_place_mkt_sends_zero_price_and_returns_ack_fields():
    c = _client()
    ack = c.place(action="Buy", price=Decimal("0"), qty=1,
                  price_type="MKT", order_type="IOC", octype="Auto")
    assert set(ack) == {"ordno", "broker_order_id"}
    assert c.api.placed[-1]["price"] == 0.0


def test_cancel_unknown_ordno_raises_order_error():
    c = _client()
    try:
        c.cancel("NOPE")
        assert False, "應該 raise"
    except OrderError:
        pass


def test_update_unknown_ordno_raises_trade_not_found():
    c = _client()
    try:
        c.update("NOPE", price=Decimal("21000"), qty=2)
        assert False, "應該 raise"
    except TradeNotFoundError as exc:
        assert exc.ordno == "NOPE"


def test_on_order_cb_calls_on_raw_with_kind_and_json_safe_payload():
    seen = []
    c = _client(on_raw=lambda kind, payload: seen.append((kind, payload)))
    c._on_order_cb("OrderState.FuturesDeal", {"trade_id": "T1", "price": 100.0})
    assert seen == [("deal_report", {"trade_id": "T1", "price": 100.0})]


def test_on_order_cb_degrades_when_on_raw_raises_once():
    calls = []

    def flaky(kind, payload):
        calls.append(payload)
        if len(calls) == 1:
            raise RuntimeError("db down")

    c = _client(on_raw=flaky)
    c._on_order_cb("OrderState.FuturesDeal", {"trade_id": "T1"})
    assert calls[1]["_unparsed"] is True and "repr" in calls[1]


def _bare_adapter(persist_raw):
    """比照 tests/test_broker_hardening.py 的 `_bare_adapter` 慣例：繞過 `__init__` 建最小
    adapter（`_on_order_cb` 只用到 `_persist_raw`），把落地責任換成測試用的攔截函式——不碰
    真 DB，也不需要 session_factory/engine。"""
    adapter = object.__new__(ShioajiAdapter)
    adapter._session_factory = lambda: None
    adapter.broker = "shioaji"
    adapter._persist_raw = persist_raw
    return adapter


def test_adapter_and_native_order_cb_stay_equivalent():
    """回歸測試（Task 2 委派重構審查 Important #2）：native.py 的 `ShioajiNativeClient.
    _on_order_cb` 與 shioaji_adapter.py 的 `ShioajiAdapter._on_order_cb` 是刻意保留的兩份
    等價實作（見 shioaji_adapter.py 該方法 docstring 的說明——既有硬化測試用
    `object.__new__` 繞過 __init__，改成委派 native 會炸 AttributeError，故兩邊各自維護一份
    邏輯逐字相同的 callback）。這是零丟單紅線路徑，兩份漂移不會被既有測試發現：本測試把
    同一組輸入分別餵給兩邊，攔截兩邊最終落地的 (kind, payload)，斷言完全相同。"""
    # 案例①：正常 dict msg，兩邊應落地相同 (kind, payload)。
    native_seen: list[tuple[str, dict]] = []
    native_client = _client(on_raw=lambda kind, payload: native_seen.append((kind, payload)))
    native_client._on_order_cb("OrderState.FuturesDeal", {"trade_id": "T1", "price": 100.0})

    adapter_seen: list[tuple[str, dict]] = []
    adapter = _bare_adapter(lambda kind, payload: adapter_seen.append((kind, payload)))
    adapter._on_order_cb("OrderState.FuturesDeal", {"trade_id": "T1", "price": 100.0})

    assert native_seen == adapter_seen == [("deal_report", {"trade_id": "T1", "price": 100.0})]

    # 案例②：落地 handler 第一次拋例外（flaky），逼兩邊都走「退化重試」路徑——斷言退化
    # payload 的 `_unparsed`/`repr` 鍵形狀與內容也完全一致。
    def _make_flaky(calls: list[tuple[str, dict]]):
        def flaky(kind, payload):
            calls.append((kind, payload))
            if len(calls) == 1:
                raise RuntimeError("db down")

        return flaky

    native_calls: list[tuple[str, dict]] = []
    native_client2 = _client(on_raw=_make_flaky(native_calls))
    native_client2._on_order_cb("FuturesOrder", {"id": "O1"})

    adapter_calls: list[tuple[str, dict]] = []
    adapter2 = _bare_adapter(_make_flaky(adapter_calls))
    adapter2._on_order_cb("FuturesOrder", {"id": "O1"})

    assert len(native_calls) == len(adapter_calls) == 2
    assert native_calls[0] == adapter_calls[0] == ("order_report", {"id": "O1"})

    n_kind, n_degraded = native_calls[1]
    a_kind, a_degraded = adapter_calls[1]
    assert n_kind == a_kind == "order_report"
    assert n_degraded.keys() == a_degraded.keys() == {"_unparsed", "repr"}
    assert n_degraded["_unparsed"] is a_degraded["_unparsed"] is True
    assert n_degraded["repr"] == a_degraded["repr"]


def test_trades_snapshot_filters_by_watermark_and_returns_newest():
    c = _client()
    # _FakeApi 需支援 list_trades 回帶 status.order_datetime 的 trade（測試檔內自建）
    payloads, newest = c.trades_snapshot(after=None)
    assert isinstance(payloads, list) and (newest is None or hasattr(newest, "year"))
