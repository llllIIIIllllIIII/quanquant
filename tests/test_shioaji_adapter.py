"""ShioajiAdapter：place 冪等前置查詢（同 client_order_id+相符 request_hash 回既有 Order，
不重送不燒 token；owner 不符則拒絕，round3 開放清單 #6）、canonical hash 用
self.mode/self.account（不可信任外部 mode——OrderRequest 結構上就沒有 mode 欄位）、
send gate 在鎖內最後一次擋 kill switch、callback 只呼叫 commit_raw_callback 落地 RawInbox
（不做 call_soon_threadsafe+ensure_future、不直接動 DB 業務邏輯，round3 BLOCKER#2）、
callback-before-ack（送單前 pending order correlation 已落地）、connect/close/place 全部經
BrokerSupervisor.run() 同一個 command executor 序列化（round3 #11）、_map_deal_report 嚴格
驗證缺值。用假 shioaji client（`_FakeApi`）不連真網路。"""
import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.broker import shioaji_adapter as shioaji_adapter_module
from quanquant.broker import watchdog as watchdog_module
from quanquant.broker.base import AuthorizationError, OrderError, RiskError
from quanquant.broker.risk import RiskGuard
from quanquant.broker.shioaji_adapter import ShioajiAdapter
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import OrderRequest
from quanquant.db.models import Order, QuotaReservation, RawInbox


class _FakeOrderHandle:
    def __init__(self, id_, seqno):
        self.id = id_
        self.seqno = seqno


class _FakeTrade:
    def __init__(self, id_, seqno):
        self.order = _FakeOrderHandle(id_, seqno)


class _FakeApi:
    """假 shioaji client：不連真網路，place_order/cancel_order/update_order 皆同步回傳。

    bug 2/3 修正後比照真實 SDK 契約（`.venv/.../shioaji/_core.pyi`）：`cancel_order(trade)`／
    `update_order(trade, price=, qty=)` 一律收 `Trade` 物件——本 fake 收到非 Trade-like
    （沒有 `.order` 屬性，例如呼叫端誤傳 ordno 字串）就 raise `TypeError`，模擬真實 SDK 對
    型別的要求（先前直接塞字串在真實 SDK 會炸
    `argument 'trade': 'str' object is not an instance of 'Trade'`）。`place_order` 送出後
    把回傳的 Trade 記進 `_live_trades`（以 `order.id` 為 key，比照真實 SDK `list_trades()`
    語意——已送出的委託會出現在列表中），`ShioajiAdapter._find_trade_by_ordno` 呼叫
    `update_status()` + `list_trades()` 才找得到對應 Trade 物件。"""

    def __init__(self):
        self.placed = []
        self.futopt_account = type("Acc", (), {"account_id": "F1"})()
        self._seq = 0
        self._live_trades: dict = {}
        self.update_status_calls = 0

    def Order(self, **kw):
        return kw

    def place_order(self, contract, order):
        self._seq += 1
        self.placed.append((contract, order))
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


def _adapter(engine, *, mode="sim", risk_guard=None):
    a = ShioajiAdapter(
        api_key="k", secret_key="s", ca_path=None, ca_passwd=None, person_id=None,
        symbol="TXF", mode=mode, session_factory=lambda: Session(engine),
        supervisor=BrokerSupervisor(), risk_guard=risk_guard,
    )
    a._api = _FakeApi()  # 跳過 connect()（不做網路呼叫），直接注入假 client
    a._contract = object()
    a.account = "F1"
    return a


def _req(**over):
    base = dict(
        client_order_id="C1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", user_id=1,
    )
    base.update(over)
    return OrderRequest(**base)


# ---- place：冪等前置查詢 / owner 檢查 / 失敗分類 ----

def test_place_returns_ack_and_persists_order(engine):
    adapter = _adapter(engine)
    ack = asyncio.run(adapter.place(_req(), actor_user_id=1))
    assert ack.status == "submitted" and ack.ordno == "ORD1"
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.mode == "sim" and order.account == "F1"  # server-side 值，非表單


def test_place_idempotent_same_client_order_id_does_not_resend(engine):
    adapter = _adapter(engine)
    first = asyncio.run(adapter.place(_req(), actor_user_id=1))
    second = asyncio.run(adapter.place(_req(), actor_user_id=1))  # 同 client_order_id + 同內容
    assert second.ordno == first.ordno
    assert len(adapter._api.placed) == 1  # 沒有重送


def test_place_same_client_order_id_different_payload_rejected(engine):
    adapter = _adapter(engine)
    asyncio.run(adapter.place(_req(), actor_user_id=1))
    with pytest.raises(OrderError):
        asyncio.run(adapter.place(_req(qty=99), actor_user_id=1))
    assert len(adapter._api.placed) == 1  # 竄改的那次沒有送出


def test_place_idempotent_hit_rejects_non_owner(engine):
    """round3 開放清單 #6：既有列命中只驗 request_hash 不夠，非 owner 猜到別人的
    client_order_id + 完全相同 payload 不得在授權前拿到他人 OrderAck。"""
    adapter = _adapter(engine)
    asyncio.run(adapter.place(_req(), actor_user_id=1))
    with pytest.raises(AuthorizationError):
        asyncio.run(adapter.place(_req(), actor_user_id=2))
    assert len(adapter._api.placed) == 1  # 沒有因為冒充而重送


def test_place_broker_failure_marks_order_unknown_not_blind_resend(engine):
    """模稜兩可（逾時/連線中斷，無法辨識的例外，沒有券商明確拒絕訊號）：結果不明，
    維持 unknown fail-safe，不得誤判成 failed。"""
    adapter = _adapter(engine)

    def _boom(contract, order):
        raise RuntimeError("網路逾時")

    adapter._api.place_order = _boom
    with pytest.raises(OrderError):
        asyncio.run(adapter.place(_req(), actor_user_id=1))
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "unknown"  # 不盲送、待 reconcile，不是直接標 failed


def test_place_broker_explicit_rejection_marks_order_failed(engine):
    """本次精進：券商明確拒絕（Shioaji 例外訊息帶 HTTP 式 4xx 回應碼，真實案例為
    `place_order: ... code: 406, detail: Please sign ... first.`）代表委託確定沒送出，
    不必等 watchdog reconcile，應直接標 failed。"""
    adapter = _adapter(engine)

    def _boom(contract, order):
        raise Exception("place_order: SubAccount not found. code: 406, detail: Please sign agreement first.")

    adapter._api.place_order = _boom
    with pytest.raises(OrderError):
        asyncio.run(adapter.place(_req(), actor_user_id=1))
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "failed"  # 明確拒絕，確定沒送出，不是 unknown


def test_classify_place_failure_boundary_cases():
    """分類器單元測試：只有能辨識出 4xx 明確拒絕訊號才判 failed，其餘（含 5xx、無法辨識
    的一般例外）一律 unknown——寧可漏判成 unknown，不可誤判成 failed。"""
    classify = shioaji_adapter_module._classify_place_failure
    assert classify(Exception("place_order: ... code: 406, detail: Please sign ... first.")) == "failed"
    assert classify(RuntimeError("網路逾時")) == "unknown"
    assert classify(Exception("connection reset by peer")) == "unknown"
    assert classify(Exception("internal server error, code: 500")) == "unknown"  # 5xx 非明確拒絕


def test_place_persists_pending_correlation_before_native_call(engine):
    """callback-before-ack：place 送單前，client_order_id→user_id/mode 的 pending
    correlation 已經 commit（ordno/broker_order_id 是 NULL 佔位），即使成交回報早於
    ack 抵達，RawInboxWorker 之後仍能靠 ordno/broker_order_id 補齊後解析到這筆委託。"""
    adapter = _adapter(engine)
    seen = {}
    orig_place_order = adapter._api.place_order

    def spy_place_order(contract, order):
        with Session(engine) as s:
            seen["order_before_send"] = brepo.find_order_by_client_order_id(s, "C1")
        return orig_place_order(contract, order)

    adapter._api.place_order = spy_place_order
    asyncio.run(adapter.place(_req(), actor_user_id=1))

    pending = seen["order_before_send"]
    assert pending is not None
    assert pending.user_id == 1 and pending.mode == "sim" and pending.status == "pending"
    assert pending.ordno is None and pending.broker_order_id is None  # 佔位，ack 後才補上


# ---- bug 2：市價單（MKT）不再因 price 而報 decimal.ConversionSyntax / 被 price>0 擋 ----

def test_place_mkt_order_succeeds_and_forces_zero_price_to_native_api(engine):
    """MKT 委託（price=0, order_type=IOC）走完整 place 流程（表單→OrderRequest→adapter）
    應成功送出，且 native Order 收到的 price 一律是 0.0（防禦，見 _place_blocking）。"""
    adapter = _adapter(engine)
    req = _req(price_type="MKT", order_type="IOC", price=Decimal("0"))
    ack = asyncio.run(adapter.place(req, actor_user_id=1))
    assert ack.status == "submitted"
    _contract, native_order = adapter._api.placed[0]
    assert native_order["price"] == 0.0


def test_place_mkt_order_forces_zero_price_even_if_req_price_nonzero(engine):
    """型別層只保證 MKT 時 price>=0，不強制一定是 0——_place_blocking 仍必須顯式送 0.0，
    不信任 req.price 當下殘留的值（本次精進的防禦線，見模組頂部說明）。"""
    adapter = _adapter(engine)
    req = _req(price_type="MKT", order_type="IOC", price=Decimal("100"))
    asyncio.run(adapter.place(req, actor_user_id=1))
    _contract, native_order = adapter._api.placed[0]
    assert native_order["price"] == 0.0


def test_update_mkt_order_forces_zero_price_to_native_api(engine):
    adapter = _adapter(engine)
    req = _req(price_type="MKT", order_type="IOC", price=Decimal("0"), qty=2)
    ack = asyncio.run(adapter.place(req, actor_user_id=1))

    calls = []
    orig_update_order = adapter._api.update_order

    def spy_update_order(trade, **kw):
        calls.append(kw)
        return orig_update_order(trade, **kw)

    adapter._api.update_order = spy_update_order
    asyncio.run(adapter.update(ack.broker_order_id, actor_user_id=1, qty=5))
    assert calls[0]["price"] == 0.0


# ---- bug 2/3：cancel/update 依 _core.pyi 真實簽章傳 Trade 物件，不是 ordno 字串 ----

def test_cancel_passes_trade_object_not_ordno_string_to_native_cancel_order(engine):
    """bug 3 回歸：真實 Shioaji `cancel_order(trade: Trade, ...)` 收 Trade 物件——先前
    `_cancel_blocking` 直接塞 ordno 字串，在真實 SDK 會炸
    `argument 'trade': 'str' object is not an instance of 'Trade'`（`_FakeApi.cancel_order`
    現在會對非 Trade-like 輸入 raise TypeError，模擬這個真實行為，見 `_FakeApi` 說明）。"""
    adapter = _adapter(engine)
    ack = asyncio.run(adapter.place(_req(), actor_user_id=1))

    received = []
    orig_cancel_order = adapter._api.cancel_order

    def spy_cancel_order(trade):
        received.append(trade)
        return orig_cancel_order(trade)

    adapter._api.cancel_order = spy_cancel_order
    ack2 = asyncio.run(adapter.cancel(ack.broker_order_id, actor_user_id=1))
    assert ack2.status == "cancelled"
    assert len(received) == 1
    assert hasattr(received[0], "order")  # Trade-like，不是裸 ordno 字串
    assert received[0].order.id == ack.ordno


def test_update_passes_trade_object_not_ordno_string_to_native_update_order(engine):
    """bug 2/3 回歸：真實 Shioaji `update_order(trade: Trade, price=, qty=, ...)` 收 Trade
    物件，同 cancel 一樣不能是 ordno 字串。"""
    adapter = _adapter(engine)
    ack = asyncio.run(adapter.place(_req(qty=2), actor_user_id=1))

    received = []
    orig_update_order = adapter._api.update_order

    def spy_update_order(trade, **kw):
        received.append(trade)
        return orig_update_order(trade, **kw)

    adapter._api.update_order = spy_update_order
    ack2 = asyncio.run(adapter.update(ack.broker_order_id, actor_user_id=1, qty=5))
    assert ack2.status == "submitted"
    assert len(received) == 1
    assert hasattr(received[0], "order")  # Trade-like，不是裸 ordno 字串
    assert received[0].order.id == ack.ordno


def test_cancel_refreshes_status_before_listing_trades(engine):
    """cancel 前應先 update_status() 刷新，再 list_trades() 取回目前狀態（見
    `_refresh_and_list_trades`），比對得到才把 Trade 物件傳給 native cancel_order。"""
    adapter = _adapter(engine)
    ack = asyncio.run(adapter.place(_req(), actor_user_id=1))
    asyncio.run(adapter.cancel(ack.broker_order_id, actor_user_id=1))
    assert adapter._api.update_status_calls == 1


def test_cancel_raises_clear_error_when_no_matching_trade_found_not_blind_string(engine):
    """找不到對應 Trade（例如已成交/已刪/跨日，list_trades() 目前清單已經沒有這筆委託）
    ——明確 raise OrderError，不得盲目把 ordno 字串塞給 native cancel_order。委託本身狀態
    不變（沒有被誤標 cancelled）。"""
    adapter = _adapter(engine)
    ack = asyncio.run(adapter.place(_req(), actor_user_id=1))
    adapter._api.list_trades = lambda: []  # 模擬券商端已無這筆委託

    with pytest.raises(OrderError):
        asyncio.run(adapter.cancel(ack.broker_order_id, actor_user_id=1))
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "submitted"  # 沒有被誤標 cancelled


def test_update_raises_clear_error_and_releases_delta_quota_when_no_matching_trade_found(engine):
    """找不到對應 Trade——update() 明確 raise，且委託狀態不誤標 unknown（根本沒有送出任何
    native 呼叫，不是「結果不明」的 unknown fail-safe 範疇，見 `_TradeNotFoundError`
    docstring）；這次改單嘗試「若有」保留的 delta 配額確定沒被使用，應立即釋放。"""
    guard = _real_guard(engine)
    adapter = _adapter(engine, risk_guard=guard)
    ack = asyncio.run(adapter.place(_req(qty=2), actor_user_id=1))
    adapter._api.list_trades = lambda: []  # 模擬券商端已無這筆委託

    with pytest.raises(OrderError):
        asyncio.run(adapter.update(ack.broker_order_id, actor_user_id=1, qty=5))
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "submitted"  # 不誤標 unknown
        rows = list(s.exec(select(QuotaReservation)))
        update_row = next(r for r in rows if r.reservation_id != "C1")
        assert update_row.state == "released"  # 確定沒生效，delta 配額立即釋放


def test_order_request_has_no_mode_field_structural_rejection():
    """mode 僅 server-side：OrderRequest 結構上就沒有 mode 欄位，外部混入一律在
    建構當下被 dataclass 拒絕（TypeError），adapter 一律用 self.mode 蓋入。"""
    with pytest.raises(TypeError):
        OrderRequest(
            client_order_id="C1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
            price_type="LMT", order_type="ROD", octype="New", user_id=1, mode="real",
        )


# ---- send gate ----

def test_send_gate_blocks_when_kill_switch_on():
    class _Guard:
        kill_switch = True

        def assert_owner(self, actor_user_id):
            pass

        def check_place(self, session, req, **kw):
            order = brepo.create_order(
                session, client_order_id=req.client_order_id, request_hash="H",
                user_id=kw["actor_user_id"], mode=kw["mode"], broker=kw["broker"], account=kw["account"],
                symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
                price_type=req.price_type, order_type=req.order_type, octype=req.octype,
                trading_day="2026-06-16",
            )
            session.commit()  # 比照真正 RiskGuard.check_place：quota reserve + create_order 同一交易提交
            return order

    from sqlalchemy.pool import StaticPool
    from sqlmodel import create_engine, SQLModel
    eng = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(eng)

    adapter = _adapter(eng, risk_guard=_Guard())
    with pytest.raises(RiskError):
        asyncio.run(adapter.place(_req(), actor_user_id=1))
    assert len(adapter._api.placed) == 0  # send gate 在 native 呼叫前擋下
    with Session(eng) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "failed"  # 不留在 pending 卡死（V3-2 收尾）


# ---- callback：只落地 RawInbox（round3 BLOCKER#2） ----

def test_callback_only_calls_commit_raw_callback_and_persists_raw_inbox(engine, monkeypatch):
    adapter = _adapter(engine)
    calls = []
    original = shioaji_adapter_module.commit_raw_callback

    def spy(session_factory, *, kind, broker, payload):
        calls.append((kind, broker, payload))
        return original(session_factory, kind=kind, broker=broker, payload=payload)

    monkeypatch.setattr(shioaji_adapter_module, "commit_raw_callback", spy)

    # 真實 FuturesDealEvent 欄位名稱（見 _core.pyi）：trade_id/ordno，沒有 octype/order_id。
    adapter._on_order_cb("FuturesDeal", {"trade_id": "D1", "action": "Buy",
                                          "quantity": 1, "price": "18000", "ts": 1_780_000_000.0,
                                          "account_id": "F1", "ordno": "O1"})

    assert len(calls) == 1
    kind, broker, payload = calls[0]
    assert kind == "deal_report" and broker == "shioaji" and payload["trade_id"] == "D1"
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row is not None and row.kind == "deal_report"
        assert json.loads(row.payload)["trade_id"] == "D1"
        # 只落地 RawInbox，不直接處理業務邏輯：沒有 Order/Deal/BrokerPosition 被動到
        assert s.exec(select(Order)).first() is None


def test_callback_order_report_uses_order_report_kind(engine):
    adapter = _adapter(engine)
    # 真實 FuturesOrderEvent 是巢狀結構（operation/order/status/contract，見 _core.pyi）。
    adapter._on_order_cb("FuturesOrder", {
        "operation": {"op_type": "Cancel", "op_code": "00", "op_msg": ""},
        "order": {"id": "O1", "seqno": "B1"}, "status": {}, "contract": {},
    })
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row is not None and row.kind == "order_report"


class _FakeMapping:
    """比照真實 shioaji `OrderEventDict` 的最小複製品：有 keys()/__getitem__/items() 等
    Mapping 協定方法，但不是 dict 子類、也沒有 to_dict()（見 _json_safe 的 docstring 與
    _core.pyi 的 `OrderEventDict` 類別定義）。"""

    def __init__(self, data: dict):
        self._data = data

    def keys(self):
        return list(self._data.keys())

    def __getitem__(self, key):
        return self._data[key]

    def __contains__(self, key):
        return key in self._data

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)


def test_json_safe_converts_non_dict_mapping_without_to_dict():
    """bug 1(b) 根因之一：真實 callback msg 是 mapping（有 keys()/__getitem__）但不是 dict
    子類、無 to_dict——先前落到 {"raw": str(msg)} 整包結構遺失，改用 dict(msg)。"""
    msg = _FakeMapping({"trade_id": "D1", "action": "Buy"})
    result = shioaji_adapter_module.ShioajiAdapter._json_safe(msg)
    assert result == {"trade_id": "D1", "action": "Buy"}
    assert isinstance(result, dict) and not isinstance(result, _FakeMapping)


def test_json_safe_recursively_converts_nested_mapping_objects():
    """FuturesOrderEvent 是巢狀結構，operation/order/status/contract 各自也可能是同款
    Mapping-only 物件——只轉最外層不夠，json.dumps 遇到巢狀非原生型別一樣會炸。"""
    msg = _FakeMapping({
        "operation": _FakeMapping({"op_type": "Cancel"}),
        "order": _FakeMapping({"id": "O1", "seqno": "B1"}),
    })
    result = shioaji_adapter_module.ShioajiAdapter._json_safe(msg)
    assert result == {"operation": {"op_type": "Cancel"}, "order": {"id": "O1", "seqno": "B1"}}
    json.dumps(result)  # 不得拋例外——必須是保證可 JSON 序列化的原生型別


# ---- round3 #11：connect/close/place 全部經同一個 BrokerSupervisor.run() 通道 ----

def test_supervisor_run_shares_lock_with_raw_lock_usage():
    """新加的 run() command executor 必須跟 Task 5 RawInboxWorker 既有的裸 `async with
    supervisor.lock:` 用法共用同一顆鎖，兩種呼叫方式彼此互斥，不能各自獨立。"""
    sup = BrokerSupervisor()
    events = []

    async def hold_lock_raw():
        async with sup.lock:
            events.append(("raw", "start"))
            await asyncio.sleep(0.02)
            events.append(("raw", "end"))

    async def hold_lock_via_run():
        async def _inner():
            events.append(("run", "start"))
            await asyncio.sleep(0.02)
            events.append(("run", "end"))
        await sup.run(_inner)

    async def scenario():
        await asyncio.gather(hold_lock_raw(), hold_lock_via_run())

    asyncio.run(scenario())

    starts = [e for e in events if e[1] == "start"]
    ends = [e for e in events if e[1] == "end"]
    assert len(starts) == 2 and len(ends) == 2
    first_label = starts[0][0]
    # 第一個開始的那段必須先結束，第二段才能開始——不可交錯。
    assert events.index((first_label, "end")) < events.index((starts[1][0], "start"))


def test_connect_and_place_serialize_through_same_supervisor_channel(engine):
    """round3 #11：connect 與 place 必須經同一個 BrokerSupervisor command executor，不得
    各自 `async with lock:`——用一個共享『目前是否在臨界區』旗標偵測交錯；若序列化通道
    失效，臨界區內的 assert 會在 to_thread 背景執行緒炸開並經 asyncio 傳回主協程使測試失敗。"""
    adapter = _adapter(engine)
    state = {"busy": False}

    def _guard(label):
        assert state["busy"] is False, f"{label} 與另一操作交錯，序列化通道失效"
        state["busy"] = True
        time.sleep(0.03)
        state["busy"] = False

    def fake_connect_blocking():
        _guard("connect")

    orig_place_blocking = adapter._place_blocking

    def fake_place_blocking(req):
        _guard("place")
        return orig_place_blocking(req)

    adapter._connect_blocking = fake_connect_blocking
    adapter._place_blocking = fake_place_blocking

    async def scenario():
        await asyncio.gather(adapter.connect(), adapter.place(_req(), actor_user_id=1))

    asyncio.run(scenario())


def test_close_goes_through_supervisor_and_clears_api(engine):
    adapter = _adapter(engine)
    asyncio.run(adapter.close())
    assert adapter._api is None


# ---- Task 7 round3 #4：quota reservation confirm/release 收尾（真 RiskGuard，不用假 guard，
#      驗證 adapter 與 RiskGuard 兩端算出的 reservation_id 一致、confirm/release 真的命中） ----

def _real_guard(engine, **over):
    base = dict(
        secret="s", owner_user_ids=frozenset({1, 2}), symbol_whitelist=frozenset({"TXF"}),
        max_qty_per_order=10, max_qty_per_day=50, max_orders_per_day=50,
    )
    base.update(over)
    return RiskGuard(session_factory=lambda: Session(engine), **base)


def test_place_confirms_quota_reservation_on_successful_send(engine):
    guard = _real_guard(engine)
    adapter = _adapter(engine, risk_guard=guard)
    asyncio.run(adapter.place(_req(), actor_user_id=1))
    with Session(engine) as s:
        row = s.exec(select(QuotaReservation)).first()
        assert row is not None
        assert row.reservation_id == "C1"  # place 用 client_order_id 當 reservation_id
        assert row.state == "confirmed"  # 送出成功 → 收尾 confirm


def test_place_releases_quota_reservation_when_send_gate_raises_riskerror(engine):
    """round3 #4 收尾：send gate（如 kill switch TOCTOU 窗口）擋下時，確定沒送出，
    退還這筆保留的配額，不留死配額。"""
    guard = _real_guard(engine)
    adapter = _adapter(engine, risk_guard=guard)

    async def _blocked_gate():
        raise RiskError("kill switch 已啟動，拒絕送出")

    adapter._send_gate = _blocked_gate
    with pytest.raises(RiskError):
        asyncio.run(adapter.place(_req(), actor_user_id=1))
    assert len(adapter._api.placed) == 0  # 送單前就被擋，沒有送出
    with Session(engine) as s:
        row = s.exec(select(QuotaReservation)).first()
        assert row is not None and row.state == "released"
        order = s.exec(select(Order)).first()
        assert order.status == "failed"


def test_place_leaves_quota_reservation_reserved_when_send_outcome_unknown(engine):
    """round3 #4：native 呼叫結果不明（例如網路逾時，不確定券商是否已收到）不得擅自
    release——若其實已送達，誤退還配額會變相突破日限，留給 Task 8 watchdog reconcile 決議。"""
    guard = _real_guard(engine)
    adapter = _adapter(engine, risk_guard=guard)

    def _boom(contract, order):
        raise RuntimeError("網路逾時")

    adapter._api.place_order = _boom
    with pytest.raises(OrderError):
        asyncio.run(adapter.place(_req(), actor_user_id=1))
    with Session(engine) as s:
        row = s.exec(select(QuotaReservation)).first()
        assert row is not None and row.state == "reserved"
        order = s.exec(select(Order)).first()
        assert order.status == "unknown"


def test_place_releases_quota_reservation_on_broker_explicit_rejection(engine):
    """本次精進，對照上一個 unknown 測試：券商明確拒絕（確定沒送出）不必等 watchdog
    reconcile，應立即釋放保留的配額。"""
    guard = _real_guard(engine)
    adapter = _adapter(engine, risk_guard=guard)

    def _boom(contract, order):
        raise Exception("place_order: ... code: 406, detail: Please sign ... first.")

    adapter._api.place_order = _boom
    with pytest.raises(OrderError):
        asyncio.run(adapter.place(_req(), actor_user_id=1))
    with Session(engine) as s:
        row = s.exec(select(QuotaReservation)).first()
        assert row is not None and row.state == "released"  # 明確拒絕，立即退還配額
        order = s.exec(select(Order)).first()
        assert order.status == "failed"


def test_update_confirms_delta_quota_reservation_on_successful_send(engine):
    guard = _real_guard(engine)
    adapter = _adapter(engine, risk_guard=guard)
    ack = asyncio.run(adapter.place(_req(qty=2), actor_user_id=1))
    ack2 = asyncio.run(adapter.update(ack.broker_order_id, actor_user_id=1, qty=5))
    assert ack2.status == "submitted"
    with Session(engine) as s:
        rows = list(s.exec(select(QuotaReservation)))
        assert len(rows) == 2
        place_row = next(r for r in rows if r.reservation_id == "C1")
        update_row = next(r for r in rows if r.reservation_id != "C1")
        assert place_row.state == "confirmed"
        assert update_row.state == "confirmed"
        assert update_row.qty == 3  # delta = 5-2


def test_update_releases_delta_quota_reservation_when_send_gate_raises_riskerror(engine):
    guard = _real_guard(engine)
    adapter = _adapter(engine, risk_guard=guard)
    ack = asyncio.run(adapter.place(_req(qty=2), actor_user_id=1))

    async def _blocked_gate():
        raise RiskError("kill switch 已啟動，拒絕送出")

    adapter._send_gate = _blocked_gate
    with pytest.raises(RiskError):
        asyncio.run(adapter.update(ack.broker_order_id, actor_user_id=1, qty=5))
    with Session(engine) as s:
        rows = list(s.exec(select(QuotaReservation)))
        update_row = next(r for r in rows if r.reservation_id != "C1")
        assert update_row.state == "released"
        order = s.exec(select(Order)).first()
        assert order.status == "submitted"  # 改單失敗不影響委託本身既有狀態（仍是 place 時的狀態）


def test_update_marks_order_unknown_without_touching_reservation_when_send_outcome_unknown(engine):
    """round3 獨立驗收殘留1：update 逾時當下（native 呼叫結果不明）不得擅自 release/confirm——
    這一刻仍然「不知道」券商到底有沒有收到，reservation 必須維持 reserved（若其實已送達，
    誤退還配額會變相突破日限；若其實沒送達，誤確認會讓當日配額被靜默侵蝕，兩者都不可以）。

    但「當下不猜測」不等於「永遠卡 reserved」——過了 grace period，watchdog 的 unknown quota
    reconcile 應該主動向券商查詢這筆委託目前真實口數，依查到的結果 confirm（改單其實生效）
    或 release（改單其實沒生效）該筆 delta 保留，不讓配額被永久侵蝕。以下延續同一個
    adapter/order/reservation，模擬 watchdog 過了 grace period 後查到「改單其實生效」的
    情境，驗證 reservation 最終會被 confirm，不是永遠停在 reserved。"""
    guard = _real_guard(engine)
    adapter = _adapter(engine, risk_guard=guard)
    ack = asyncio.run(adapter.place(_req(qty=2), actor_user_id=1))

    def _boom(trade, **kw):
        raise RuntimeError("網路逾時")

    adapter._api.update_order = _boom
    with pytest.raises(OrderError):
        asyncio.run(adapter.update(ack.broker_order_id, actor_user_id=1, qty=5))
    with Session(engine) as s:
        rows = list(s.exec(select(QuotaReservation)))
        update_row = next(r for r in rows if r.reservation_id != "C1")
        update_row_id = update_row.id
        assert update_row.state == "reserved"  # 結果不明，當下不擅自 release/confirm
        order = s.exec(select(Order)).first()
        assert order.status == "unknown"

    # 過了 grace period：watchdog 向券商查詢，真實口數已是改單後目標值（2+3=5）——代表這次
    # 改單其實生效，只是 ack 逾時沒收到。
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        order.updated_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=9999)
        s.add(order)
        s.commit()
        ordno, broker_order_id = order.ordno, order.broker_order_id
    adapter._api.list_trades = lambda: [_FakeTrade2(ordno, broker_order_id, "Submitted", quantity=5)]

    asyncio.run(watchdog_module._reconcile_unknown_quota(adapter, grace_seconds=60))

    with Session(engine) as s:
        update_row = s.exec(
            select(QuotaReservation).where(QuotaReservation.id == update_row_id)
        ).first()
        assert update_row.state == "confirmed"  # 改單其實生效，watchdog 收尾 confirm，不再永遠 reserved


def test_update_releases_delta_quota_reservation_on_broker_explicit_rejection(engine):
    """本次精進，對稱套用同一分類器：改單遭券商明確拒絕（確定沒生效）不必等 watchdog
    reconcile，應立即釋放「若有」保留的 delta 配額；委託本身狀態不變（同既有 RiskError
    分支原則——改單失敗不代表委託本身壞了，不比照 place 標 failed）。"""
    guard = _real_guard(engine)
    adapter = _adapter(engine, risk_guard=guard)
    ack = asyncio.run(adapter.place(_req(qty=2), actor_user_id=1))

    def _boom(trade, **kw):
        raise Exception("update_order: ... code: 406, detail: Please sign ... first.")

    adapter._api.update_order = _boom
    with pytest.raises(OrderError):
        asyncio.run(adapter.update(ack.broker_order_id, actor_user_id=1, qty=5))
    with Session(engine) as s:
        rows = list(s.exec(select(QuotaReservation)))
        update_row = next(r for r in rows if r.reservation_id != "C1")
        assert update_row.state == "released"  # 明確拒絕，確定沒生效，立即退還 delta 配額
        order = s.exec(select(Order)).first()
        assert order.status == "submitted"  # 委託本身狀態不變（同 RiskError 分支既有原則）


def test_watchdog_reconcile_releases_update_reservation_when_broker_shows_update_did_not_take_effect(engine):
    """殘留1 對照情境：watchdog 向券商查到的真實口數仍是改單前原值（2），代表這次改單其實
    沒生效（native 呼叫真的沒送達，不是 ack 遺失）——delta 保留應該 release，退還配額，
    不讓它跟著委託一起被永久誤判為已用。"""
    guard = _real_guard(engine)
    adapter = _adapter(engine, risk_guard=guard)
    ack = asyncio.run(adapter.place(_req(qty=2), actor_user_id=1))

    def _boom(trade, **kw):
        raise RuntimeError("網路逾時")

    adapter._api.update_order = _boom
    with pytest.raises(OrderError):
        asyncio.run(adapter.update(ack.broker_order_id, actor_user_id=1, qty=5))

    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        order.updated_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=9999)
        s.add(order)
        s.commit()
        update_row_id = next(
            r.id for r in s.exec(select(QuotaReservation)) if r.reservation_id != "C1"
        )
        ordno, broker_order_id = order.ordno, order.broker_order_id
    adapter._api.list_trades = lambda: [_FakeTrade2(ordno, broker_order_id, "Submitted", quantity=2)]

    asyncio.run(watchdog_module._reconcile_unknown_quota(adapter, grace_seconds=60))

    with Session(engine) as s:
        update_row = s.exec(select(QuotaReservation).where(QuotaReservation.id == update_row_id)).first()
        assert update_row.state == "released"  # 改單其實沒生效，watchdog 收尾 release，退還配額


# ---- Task 7 round3 #6：cancel 除了 assert_owner，仍要驗真正委託所有權 ----

def test_cancel_rejects_non_owner_of_order_even_when_actor_is_a_different_owner(engine):
    """多 owner 情境下，owner A 的委託不能被 owner B 取消——先前版本只在沒有 risk_guard
    時才會驗 order.user_id，risk_guard 存在時被跳過，是個真正的跨人越權漏洞。"""
    guard = _real_guard(engine)  # owner_user_ids={1,2}
    adapter = _adapter(engine, risk_guard=guard)
    ack = asyncio.run(adapter.place(_req(), actor_user_id=1))
    with pytest.raises(AuthorizationError):
        asyncio.run(adapter.cancel(ack.broker_order_id, actor_user_id=2))
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "submitted"  # 沒有被取消


# ---- mapper：嚴格驗證 ----

def _adapter_stub_for_mapper(*, sim_fee_per_lot=None, mode="sim"):
    return ShioajiAdapter(
        api_key="k", secret_key="s", ca_path=None, ca_passwd=None, person_id=None,
        symbol="TXF", mode=mode, session_factory=lambda: None,
        supervisor=BrokerSupervisor(), sim_fee_per_lot=sim_fee_per_lot,
    )


def test_map_deal_report_rejects_missing_trade_id():
    """真實 FuturesDealEvent 用 trade_id（不是先前假設的 deal_id），且沒有 octype 欄位。"""
    adapter = _adapter_stub_for_mapper()
    with pytest.raises(ValueError):
        adapter._map_deal_report({"action": "Buy", "quantity": 1, "price": "1",
                                  "ts": 1.0, "account_id": "F1"})  # 缺 trade_id


def test_map_deal_report_fills_sim_fee_from_setting_when_missing():
    """A6：sim 模擬單成交常缺 fee，依設定 sim_fee_per_lot * qty 估算，不留 None/0。"""
    adapter = _adapter_stub_for_mapper(sim_fee_per_lot=Decimal("20"))
    fill = adapter._map_deal_report({
        "trade_id": "D1", "action": "Buy", "quantity": 3, "price": "18000",
        "ts": 1_780_000_000.0, "account_id": "F1", "ordno": "O1",
    })  # payload 無 "fee" 欄位、無 "octype"（真實 FuturesDealEvent 沒有這個欄位）
    assert fill.fee == Decimal("60")  # 20 * 3 口
    assert fill.ts == 1_780_000_000_000  # 秒→毫秒


def test_map_deal_report_real_missing_fee_stays_none_no_sim_substitution():
    adapter = _adapter_stub_for_mapper(sim_fee_per_lot=Decimal("20"), mode="real")
    fill = adapter._map_deal_report({
        "trade_id": "D1", "action": "Buy", "quantity": 1, "price": "18000",
        "ts": 1_780_000_000.0, "account_id": "F1", "ordno": "O1",
    })
    assert fill.fee is None  # real 一律取 broker 回報，缺就是缺，不用 sim 設定頂替


def test_map_deal_report_converts_epoch_seconds_ts_to_epoch_ms_for_correct_trading_day():
    """真實 FuturesDealEvent.ts 是 epoch 秒(float)，Deal.ts/Fill.ts 需要 epoch-ms——沒有 ×1000
    會讓 trading_day_for 算出 1970 年（epoch 秒當 ms 用，值小了 1000 倍）。"""
    adapter = _adapter_stub_for_mapper()
    fill = adapter._map_deal_report({
        "trade_id": "D1", "action": "Buy", "quantity": 1, "price": "18000",
        "ts": 1_780_000_000.0, "account_id": "F1", "ordno": "O1", "fee": "20",
    })
    assert fill.ts == 1_780_000_000_000
    trading_day = brepo.trading_day_for(fill.ts)
    assert trading_day.startswith("2026-")  # 不是 1970 年（沒 ×1000 會算出 1970-01-21 附近）


def test_map_deal_report_octype_is_placeholder_overridden_by_inbox_worker():
    """成交回報沒有 octype 欄位——mapper 只能給一個滿足 Fill.__post_init__ 型別驗證的占位值
    （"Auto"），真正生效前一律會被 RawInboxWorker._process_deal 用解析到的 Order.octype
    覆蓋（見該檔/本檔下面的端到端契約測試）。"""
    adapter = _adapter_stub_for_mapper()
    fill = adapter._map_deal_report({
        "trade_id": "D1", "action": "Buy", "quantity": 1, "price": "18000",
        "ts": 1_780_000_000.0, "account_id": "F1", "ordno": "O1", "fee": "20",
    })
    assert fill.octype == "Auto"


def test_map_order_report_rejects_missing_ordno_and_broker_order_id_flat_reconcile_shape():
    """_reconcile_blocking 自建的扁平 payload（order_id/seqno，非即時 callback 巢狀結構）。"""
    adapter = _adapter_stub_for_mapper()
    with pytest.raises(ValueError):
        adapter._map_order_report({"status": "Cancelled"})  # order_id/seqno 都缺


def test_map_order_report_rejects_missing_ordno_in_nested_real_callback_shape():
    """真實 FuturesOrderEvent 是巢狀結構——order 內沒有 id/seqno 時同樣要拒絕。"""
    adapter = _adapter_stub_for_mapper()
    with pytest.raises(ValueError):
        adapter._map_order_report({"operation": {"op_type": "Cancel"}, "order": {}, "status": {}})


def test_map_order_report_handles_reconcile_flat_shape_from_list_trades():
    """_reconcile_blocking 自建的扁平 payload（來自 list_trades() 的 OrderStatusInfo.status，
    非即時 callback 巢狀結構）仍須能被 _map_order_report 正確解析——兩種來源共用同一個
    kind="order_report" 佇列與同一個 mapper。"""
    adapter = _adapter_stub_for_mapper()
    report = adapter._map_order_report({"order_id": "O1", "seqno": "B1", "status": "Cancelled"})
    assert report.ordno == "O1" and report.broker_order_id == "B1" and report.status == "cancelled"


def test_map_order_report_maps_nested_real_callback_cancel_to_cancelled():
    """真實 FuturesOrderEvent 巢狀結構：ordno/seqno 在 order 內（比照既有 _ack_fields_from_trade
    慣例，order.id→我方 Order.ordno、order.seqno→我方 Order.broker_order_id），操作型態在
    operation.op_type（不是一個現成的 status 字串——見 shioaji_adapter.py 模組內說明）。"""
    adapter = _adapter_stub_for_mapper()
    report = adapter._map_order_report({
        "operation": {"op_type": "Cancel", "op_code": "00", "op_msg": ""},
        "order": {
            "id": "O1", "seqno": "B1", "ordno": "ALT", "account": {}, "action": "Buy",
            "price": 18000.0, "quantity": 1, "order_type": "ROD", "price_type": "LMT",
            "market_type": "Day", "oc_type": "New", "subaccount": "", "combo": False,
        },
        "status": {"id": "O1", "exchange_ts": 1_780_000_000.0, "modified_price": 0.0,
                   "cancel_quantity": 1, "order_quantity": 1, "web_id": "web"},
        "contract": {},
    })
    assert report.ordno == "O1" and report.broker_order_id == "B1" and report.status == "cancelled"


@pytest.mark.parametrize("op_type,expected_status", [
    ("New", "submitted"), ("UpdatePrice", "submitted"), ("UpdateQty", "submitted"),
    ("Cancel", "cancelled"), ("Reject", "failed"),
])
def test_map_order_report_op_type_status_table(op_type, expected_status):
    adapter = _adapter_stub_for_mapper()
    report = adapter._map_order_report({
        "operation": {"op_type": op_type}, "order": {"id": "O1", "seqno": "B1"}, "status": {},
    })
    assert report.status == expected_status


# ---- 以 _core.pyi 真實結構為藍本的端到端契約測試（bug 1(b)）：
#      真實 FuturesDeal/FuturesOrder callback（非 dict、無 to_dict 的 mapping）
#      → _json_safe → commit_raw_callback（JSON 落地）→ RawInboxWorker 解碼 → mapper
#      → 對應到已存 Order（取得 octype）→ PositionTracker 開/平倉。 ----

def test_futures_deal_event_contract_end_to_end_opens_position_using_resolved_order_octype(engine):
    from quanquant.broker.inbox_worker import RawInboxWorker
    from quanquant.db.models import BrokerPosition, Deal

    adapter = _adapter(engine)
    with Session(engine) as s:
        order = brepo.create_order(
            s, client_order_id="C1", request_hash="H1", user_id=1, mode="sim", broker="shioaji",
            account="F1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
            price_type="LMT", order_type="ROD", octype="New", trading_day="2026-06-16",
        )
        brepo.set_order_ack(s, order.id, broker_order_id="B1", ordno="O1", status="submitted")
        s.commit()

    # 真實 FuturesDealEvent 結構（見 _core.pyi）：非 dict、無 to_dict 的 mapping，沒有 octype。
    raw_event = _FakeMapping({
        "trade_id": "D1", "seqno": "SEQ1", "ordno": "O1", "exchange_seq": "EX1",
        "broker_id": "F002000", "account_id": "F1", "action": "Buy", "code": "TXF",
        "full_code": "TXFG6", "price": 18500.0, "quantity": 1, "subaccount": "",
        "security_type": "FUT", "delivery_month": "202607", "strike_price": 0.0,
        "option_right": "Future", "market_type": "Day", "combo": False,
        "ts": 1_780_000_000.0,  # epoch 秒 (float)
    })
    adapter._on_order_cb("FuturesDeal", raw_event)

    worker = RawInboxWorker(
        session_factory=lambda: Session(engine), supervisor=BrokerSupervisor(),
        deal_mapper=adapter._map_deal_report, order_report_mapper=adapter._map_order_report,
    )
    handled = worker.process_batch_once()
    assert handled == 1

    with Session(engine) as s:
        deal = s.exec(select(Deal)).first()
        assert deal is not None
        assert deal.ts == 1_780_000_000_000  # 秒→毫秒
        assert brepo.trading_day_for(deal.ts).startswith("2026-")  # 不是 1970 年

        pos = s.exec(select(BrokerPosition)).first()
        assert pos is not None and pos.direction == "long" and pos.total_opened_qty == 1

        refreshed_order = s.exec(select(Order)).first()
        assert refreshed_order.filled_qty == 1
        row = s.exec(select(RawInbox)).first()
        assert row.processed is True and row.quarantine is False


def test_futures_deal_event_contract_unresolvable_order_quarantines_not_dropped(engine):
    """解不到對應 Order → quarantine，不亂猜 octype（「解不到就 quarantine」的設計要求）。"""
    from quanquant.broker.inbox_worker import RawInboxWorker
    from quanquant.db.models import Deal

    adapter = _adapter(engine)
    raw_event = _FakeMapping({
        "trade_id": "D-GHOST", "seqno": "SEQ-GHOST", "ordno": "GHOST", "exchange_seq": "EX1",
        "broker_id": "F002000", "account_id": "F1", "action": "Buy", "code": "TXF",
        "full_code": "TXFG6", "price": 18500.0, "quantity": 1, "subaccount": "",
        "security_type": "FUT", "delivery_month": "202607", "strike_price": 0.0,
        "option_right": "Future", "market_type": "Day", "combo": False,
        "ts": 1_780_000_000.0,
    })
    adapter._on_order_cb("FuturesDeal", raw_event)

    worker = RawInboxWorker(
        session_factory=lambda: Session(engine), supervisor=BrokerSupervisor(),
        deal_mapper=adapter._map_deal_report, order_report_mapper=adapter._map_order_report,
    )
    handled = worker.process_batch_once()
    assert handled == 1  # quarantine 也算「確定處理完」

    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.quarantine is True and row.processed is False
        assert s.exec(select(Deal)).first() is None  # 沒有部分寫入


def test_futures_order_event_contract_end_to_end_cancel_updates_order_status(engine):
    """真實 FuturesOrderEvent 巢狀 callback（非 dict、無 to_dict 的 mapping）走完整
    callback → RawInbox → worker → mapper 流程，operation.op_type=Cancel 應標記委託 cancelled。"""
    from quanquant.broker.inbox_worker import RawInboxWorker

    adapter = _adapter(engine)
    with Session(engine) as s:
        order = brepo.create_order(
            s, client_order_id="C1", request_hash="H1", user_id=1, mode="sim", broker="shioaji",
            account="F1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
            price_type="LMT", order_type="ROD", octype="New", trading_day="2026-06-16",
        )
        brepo.set_order_ack(s, order.id, broker_order_id="B1", ordno="O1", status="submitted")
        s.commit()

    raw_event = _FakeMapping({
        "operation": _FakeMapping({"op_type": "Cancel", "op_code": "00", "op_msg": ""}),
        "order": _FakeMapping({
            "id": "O1", "seqno": "B1", "ordno": "ALT-ORDNO", "account": {}, "action": "Buy",
            "price": 18000.0, "quantity": 1, "order_type": "ROD", "price_type": "LMT",
            "market_type": "Day", "oc_type": "New", "subaccount": "", "combo": False,
        }),
        "status": _FakeMapping({"id": "O1", "exchange_ts": 1_780_000_000.0, "modified_price": 0.0,
                                 "cancel_quantity": 1, "order_quantity": 1, "web_id": "web"}),
        "contract": _FakeMapping({"security_type": "FUT", "code": "TXF", "exchange": "TAIFEX",
                                   "delivery_month": "202607", "full_code": "TXFG6",
                                   "delivery_date": "2026/07/15", "strike_price": 0.0,
                                   "option_right": "Future"}),
    })
    adapter._on_order_cb("FuturesOrder", raw_event)

    worker = RawInboxWorker(
        session_factory=lambda: Session(engine), supervisor=BrokerSupervisor(),
        deal_mapper=adapter._map_deal_report, order_report_mapper=adapter._map_order_report,
    )
    handled = worker.process_batch_once()
    assert handled == 1

    with Session(engine) as s:
        refreshed = s.exec(select(Order)).first()
        assert refreshed.status == "cancelled"


# ---- Task 8：supervisor property / health_probe（round3 #10）/ reconcile（round3 #2） ----

class _FakeOrderHandle2:
    def __init__(self, id_, seqno, quantity=None):
        self.id = id_
        self.seqno = seqno
        self.quantity = quantity


class _FakeTradeStatus:
    def __init__(self, status, order_datetime=None):
        self.status = status
        self.order_datetime = order_datetime


class _FakeTrade2:
    def __init__(self, id_, seqno, status, order_datetime=None, quantity=None):
        self.order = _FakeOrderHandle2(id_, seqno, quantity=quantity)
        self.status = _FakeTradeStatus(status, order_datetime=order_datetime)


def test_supervisor_property_returns_injected_instance(engine):
    sup = BrokerSupervisor()
    adapter = ShioajiAdapter(
        api_key="k", secret_key="s", ca_path=None, ca_passwd=None, person_id=None,
        symbol="TXF", mode="sim", session_factory=lambda: Session(engine), supervisor=sup,
    )
    assert adapter.supervisor is sup


def test_health_probe_false_when_api_none(engine):
    adapter = _adapter(engine)
    adapter._api = None
    assert asyncio.run(adapter.health_probe()) is False


def test_health_probe_true_via_list_accounts(engine):
    adapter = _adapter(engine)
    adapter._api.list_accounts = lambda: []
    assert asyncio.run(adapter.health_probe()) is True


def test_health_probe_false_when_probe_raises_even_though_api_object_still_present(engine):
    """round3 #10 核心斷言：`_api` 物件還在（不是 None）但底層連線其實已死時，探測必須
    回 False——不能只憑 `_api is not None` 判斷健康。"""
    adapter = _adapter(engine)

    def _boom():
        raise RuntimeError("連線已死")

    adapter._api.list_accounts = _boom
    assert adapter._api is not None
    assert asyncio.run(adapter.health_probe()) is False


def test_health_probe_true_via_futopt_account_fallback_when_no_list_accounts(engine):
    adapter = _adapter(engine)
    assert not hasattr(adapter._api, "list_accounts")  # _FakeApi 本來就沒有這個方法
    assert asyncio.run(adapter.health_probe()) is True  # 退回讀 futopt_account 屬性驗證存活


def test_reconcile_stages_order_report_rows_not_deal_report_and_persists_cursor(engine):
    adapter = _adapter(engine)
    t1 = datetime(2026, 6, 16, 9, 0)
    t2 = datetime(2026, 6, 16, 9, 5)
    adapter._api.list_trades = lambda: [
        _FakeTrade2("ORD1", "SEQ1", "Submitted", order_datetime=t1),
        _FakeTrade2("ORD2", "SEQ2", "Cancelled", order_datetime=t2),
    ]
    asyncio.run(adapter.reconcile())

    with Session(engine) as s:
        rows = list(s.exec(select(RawInbox)))
        assert len(rows) == 2
        assert all(r.kind == "order_report" for r in rows)  # round3 #2：不是全部塞 deal_report
        cursor = brepo.get_reconcile_cursor(s, broker="shioaji", account="F1", mode="sim")
        assert cursor == t2  # watermark 推進到最新委託時間戳


def test_reconcile_second_call_skips_trades_already_covered_by_cursor(engine):
    adapter = _adapter(engine)
    t1 = datetime(2026, 6, 16, 9, 0)
    adapter._api.list_trades = lambda: [_FakeTrade2("ORD1", "SEQ1", "Submitted", order_datetime=t1)]
    asyncio.run(adapter.reconcile())
    with Session(engine) as s:
        assert len(list(s.exec(select(RawInbox)))) == 1

    # 同一批舊委託，第二次對帳（模擬 watchdog 週期呼叫）不該重複塞 RawInbox（重啟續接/週期補洞）
    asyncio.run(adapter.reconcile())
    with Session(engine) as s:
        assert len(list(s.exec(select(RawInbox)))) == 1

    # 有新委託（時間戳晚於 cursor）才會補
    t2 = datetime(2026, 6, 16, 9, 10)
    adapter._api.list_trades = lambda: [
        _FakeTrade2("ORD1", "SEQ1", "Submitted", order_datetime=t1),
        _FakeTrade2("ORD2", "SEQ2", "Filled", order_datetime=t2),
    ]
    asyncio.run(adapter.reconcile())
    with Session(engine) as s:
        assert len(list(s.exec(select(RawInbox)))) == 2


def test_reconcile_noop_when_api_none_does_not_raise(engine):
    adapter = _adapter(engine)
    adapter._api = None
    asyncio.run(adapter.reconcile())  # 不應拋例外（尚未連線時 watchdog 也可能呼叫到）
    with Session(engine) as s:
        assert list(s.exec(select(RawInbox))) == []
