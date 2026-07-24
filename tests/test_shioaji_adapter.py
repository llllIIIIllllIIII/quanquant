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
from decimal import Decimal

import pytest
from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.broker import shioaji_adapter as shioaji_adapter_module
from quanquant.broker.base import AuthorizationError, OrderError, RiskError
from quanquant.broker.shioaji_adapter import ShioajiAdapter
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import OrderRequest
from quanquant.db.models import Order, RawInbox


class _FakeOrderHandle:
    def __init__(self, id_, seqno):
        self.id = id_
        self.seqno = seqno


class _FakeTrade:
    def __init__(self, id_, seqno):
        self.order = _FakeOrderHandle(id_, seqno)


class _FakeApi:
    """假 shioaji client：不連真網路，place_order/cancel_order/update_order 皆同步回傳。"""

    def __init__(self):
        self.placed = []
        self.futopt_account = type("Acc", (), {"account_id": "F1"})()
        self._seq = 0

    def Order(self, **kw):
        return kw

    def place_order(self, contract, order):
        self._seq += 1
        self.placed.append((contract, order))
        return _FakeTrade(f"ORD{self._seq}", f"SEQ{self._seq}")

    def cancel_order(self, ordno):
        return _FakeTrade(ordno, f"SEQ-{ordno}")

    def update_order(self, ordno, **kw):
        return _FakeTrade(ordno, f"SEQ-{ordno}")

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
    adapter = _adapter(engine)

    def _boom(contract, order):
        raise RuntimeError("網路逾時")

    adapter._api.place_order = _boom
    with pytest.raises(OrderError):
        asyncio.run(adapter.place(_req(), actor_user_id=1))
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "unknown"  # 不盲送、待 reconcile，不是直接標 failed


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

    adapter._on_order_cb("FuturesDeal", {"deal_id": "D1", "action": "Buy", "octype": "New",
                                          "quantity": 1, "price": "18000", "ts": 1_780_000_000_000,
                                          "account_id": "F1", "order_id": "O1"})

    assert len(calls) == 1
    kind, broker, payload = calls[0]
    assert kind == "deal_report" and broker == "shioaji" and payload["deal_id"] == "D1"
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row is not None and row.kind == "deal_report"
        assert json.loads(row.payload)["deal_id"] == "D1"
        # 只落地 RawInbox，不直接處理業務邏輯：沒有 Order/Deal/BrokerPosition 被動到
        assert s.exec(select(Order)).first() is None


def test_callback_order_report_uses_order_report_kind(engine):
    adapter = _adapter(engine)
    adapter._on_order_cb("FuturesOrder", {"order_id": "O1", "status": "Cancelled", "account_id": "F1"})
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row is not None and row.kind == "order_report"


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


# ---- mapper：嚴格驗證 ----

def _adapter_stub_for_mapper(*, sim_fee_per_lot=None, mode="sim"):
    return ShioajiAdapter(
        api_key="k", secret_key="s", ca_path=None, ca_passwd=None, person_id=None,
        symbol="TXF", mode=mode, session_factory=lambda: None,
        supervisor=BrokerSupervisor(), sim_fee_per_lot=sim_fee_per_lot,
    )


def test_map_deal_report_rejects_missing_deal_id():
    adapter = _adapter_stub_for_mapper()
    with pytest.raises(ValueError):
        adapter._map_deal_report({"action": "Buy", "octype": "New", "quantity": 1, "price": "1",
                                  "ts": 1, "account_id": "F1"})  # 缺 deal_id


def test_map_deal_report_fills_sim_fee_from_setting_when_missing():
    """A6：sim 模擬單成交常缺 fee，依設定 sim_fee_per_lot * qty 估算，不留 None/0。"""
    adapter = _adapter_stub_for_mapper(sim_fee_per_lot=Decimal("20"))
    fill = adapter._map_deal_report({
        "deal_id": "D1", "action": "Buy", "octype": "New", "quantity": 3, "price": "18000",
        "ts": 1_780_000_000_000, "account_id": "F1", "order_id": "O1",
    })  # payload 無 "fee" 欄位
    assert fill.fee == Decimal("60")  # 20 * 3 口


def test_map_deal_report_real_missing_fee_stays_none_no_sim_substitution():
    adapter = _adapter_stub_for_mapper(sim_fee_per_lot=Decimal("20"), mode="real")
    fill = adapter._map_deal_report({
        "deal_id": "D1", "action": "Buy", "octype": "New", "quantity": 1, "price": "18000",
        "ts": 1_780_000_000_000, "account_id": "F1", "order_id": "O1",
    })
    assert fill.fee is None  # real 一律取 broker 回報，缺就是缺，不用 sim 設定頂替


def test_map_order_report_rejects_missing_ordno_and_broker_order_id():
    adapter = _adapter_stub_for_mapper()
    with pytest.raises(ValueError):
        adapter._map_order_report({"status": "Cancelled"})  # order_id/seqno 都缺
