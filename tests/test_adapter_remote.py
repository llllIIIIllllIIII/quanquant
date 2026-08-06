"""本機 broker agent（WS 通道）例外分類測試 + ShioajiAdapter `remote_gateway` 三段切行為矩陣。

`AgentUnavailableError`（指令送出前 agent 即不在線，保證未送達券商）
→ `_classify_place_failure` 判 `"failed"`（可安全退配額）。
`AgentCommandTimeoutError`（指令可能已送達但未收到 ack）
→ `_classify_place_failure` 判 `"unknown"`（保守保留配額）。
既有 `code: 4xx` 券商拒單規則不變。

下半段（Inc0 Task 6）：`ShioajiAdapter.__init__(remote_gateway=...)` 三段切——DB 決策/寫回留
server，native 呼叫改經 `_NativeGatewayLike` gateway 下行；Tier0 硬化語意（配額/失敗分類/kill
switch）必須跨網路後原樣保存，見任務簡報行為矩陣。
"""
from datetime import datetime
from decimal import Decimal

import pytest
from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.broker.base import (
    AgentCommandTimeoutError,
    AgentUnavailableError,
    OrderError,
    RiskError,
    TradeNotFoundError,
)
from quanquant.broker.risk import RiskGuard
from quanquant.broker.shioaji_adapter import ShioajiAdapter, _classify_place_failure
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import OrderRequest
from quanquant.db.models import Order, QuotaReservation, RawInbox


def test_agent_unavailable_classified_failed():
    assert _classify_place_failure(AgentUnavailableError("agent 未連線")) == "failed"


def test_agent_timeout_classified_unknown():
    assert _classify_place_failure(AgentCommandTimeoutError("ack 逾時")) == "unknown"


def test_broker_reject_code_still_failed():
    assert _classify_place_failure(Exception("code: 406 not signed")) == "failed"


def test_agent_timeout_wrapping_broker_code_message_still_unknown():
    """結構性早退（非靠訊息不含 code: 4xx 的隱含保證）：即使底層例外訊息被
    `AgentChannel.request` 包成含 `code: 4xx` 字樣，型別判斷仍優先於字串內容，
    保持 `AgentCommandTimeoutError` 一律 unknown。"""
    exc = AgentCommandTimeoutError("底層錯誤 code: 404 xxx")
    assert _classify_place_failure(exc) == "unknown"


# ---- Task 6: remote_gateway 三段切行為矩陣 ----


class _FakeGateway:
    def __init__(self):
        self.ready = True
        self.place_calls, self.cancel_calls = [], []
        self.result = {"ordno": "101AA1", "broker_order_id": "101AA1"}
        self.raise_exc = None
        self.snapshot = ([], None)

    async def place(self, req):
        self.place_calls.append(req)
        if self.raise_exc:
            raise self.raise_exc
        return self.result

    async def cancel(self, ordno):
        self.cancel_calls.append(ordno)
        if self.raise_exc:
            raise self.raise_exc

    async def update(self, ordno, *, price, qty, price_type=None):
        if self.raise_exc:
            raise self.raise_exc

    async def trades_snapshot(self, after):
        return self.snapshot


def _guard(engine):
    return RiskGuard(session_factory=lambda: Session(engine), secret="s",
                     owner_user_ids=frozenset({1}), symbol_whitelist=frozenset({"TXF"}),
                     max_qty_per_order=5, max_qty_per_day=20, max_orders_per_day=20)


def _adapter(engine, gw, guard=None):
    a = ShioajiAdapter(api_key="", secret_key="", ca_path=None, ca_passwd=None,
                       person_id=None, symbol="TXF", mode="sim",
                       session_factory=lambda: Session(engine),
                       supervisor=BrokerSupervisor(), risk_guard=guard,
                       sim_fee_per_lot=Decimal("20"), remote_gateway=gw)
    a.account = "F1"
    return a


def _req(cid="c-1"):
    return OrderRequest(client_order_id=cid, symbol="TXF", action="Buy", qty=1,
                        price=Decimal("21500"), price_type="LMT", order_type="ROD",
                        octype="Auto", user_id=1)


async def test_remote_place_success_submitted_and_quota_confirmed(engine):
    gw = _FakeGateway()
    a = _adapter(engine, gw, _guard(engine))
    ack = await a.place(_req(), actor_user_id=1)
    assert ack.status == "submitted" and ack.ordno == "101AA1"
    with Session(engine) as s:
        order = s.exec(select(Order)).one()
        assert order.status == "submitted" and order.ordno == "101AA1"
        assert s.exec(select(QuotaReservation)).one().state == "confirmed"


async def test_remote_place_timeout_unknown_and_quota_reserved(engine):
    gw = _FakeGateway()
    gw.raise_exc = AgentCommandTimeoutError("ack 逾時")
    a = _adapter(engine, gw, _guard(engine))
    with pytest.raises(AgentCommandTimeoutError):
        await a.place(_req(), actor_user_id=1)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "unknown"
        assert s.exec(select(QuotaReservation)).one().state == "reserved"


async def test_remote_place_unavailable_midflight_failed_and_quota_released(engine):
    gw = _FakeGateway()
    gw.raise_exc = AgentUnavailableError("斷線")
    a = _adapter(engine, gw, _guard(engine))
    with pytest.raises(AgentUnavailableError):
        await a.place(_req(), actor_user_id=1)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "failed"
        assert s.exec(select(QuotaReservation)).one().state == "released"


async def test_remote_place_offline_fails_fast_no_db_rows(engine):
    gw = _FakeGateway()
    gw.ready = False
    a = _adapter(engine, gw, _guard(engine))
    with pytest.raises(OrderError):
        await a.place(_req(), actor_user_id=1)
    with Session(engine) as s:
        assert s.exec(select(Order)).all() == []
        assert s.exec(select(QuotaReservation)).all() == []


async def test_remote_place_idempotent_replay_served_while_offline(engine):
    """Fix Round 1（審查 Important #1）：offline fail-fast 必須排在冪等查找之後——已成功
    送出的委託，client 用同一 client_order_id 重送時（例如網路逾時重試），即使 agent 此刻
    恰好離線，也要能命中冪等 shortcut 回快取 ack，不得被 fail-fast 攔截、也不能真的再送
    一次單。"""
    gw = _FakeGateway()
    a = _adapter(engine, gw, _guard(engine))
    req = _req(cid="c-replay")
    first = await a.place(req, actor_user_id=1)
    assert first.status == "submitted" and first.ordno == "101AA1"

    gw.ready = False
    replay = await a.place(req, actor_user_id=1)
    assert replay.status == "submitted"
    assert replay.ordno == "101AA1"
    with Session(engine) as s:
        assert len(s.exec(select(Order)).all()) == 1
    assert len(gw.place_calls) == 1


async def test_kill_switch_blocks_before_gateway_called(engine):
    gw = _FakeGateway()
    guard = _guard(engine)
    guard.set_kill_switch(True)
    a = _adapter(engine, gw, guard)
    with pytest.raises(RiskError):
        await a.place(_req(), actor_user_id=1)
    assert gw.place_calls == []


class _KillSwitchBlindGuard:
    """驗收者發現：`test_kill_switch_blocks_before_gateway_called` 用的真 `RiskGuard.
    check_place` 本身就會查 kill switch 並提早擋下——那支測試其實只驗到 check_place 這層，
    `_send_gate()`（鎖內、native/remote gateway 呼叫前的最後線性化點，見 shioaji_adapter.py
    `_send_gate` docstring）在 remote 路徑上從未被單獨驗證過（套套邏輯）。這個 fake guard
    比照 tests/test_shioaji_adapter.py 的 `test_send_gate_blocks_when_kill_switch_on`
    手法：`check_place` 完全不看 kill_switch、直接放行建單，把「擋下」的責任完全留給
    `_send_gate()` 自己的 `self._risk_guard.kill_switch` 檢查——這樣才是 `_send_gate`
    這道深度防禦真正被獨立驗證，而不是被上層 check_place 順便擋掉。"""

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


async def test_send_gate_blocks_kill_switch_in_remote_path_when_check_place_blind(engine):
    """Fix 6（順修）：remote gateway 路徑上，`_send_gate()` 自己的 kill switch 檢查要在
    鎖內、gateway.place() 呼叫之前獨立擋下——不依賴 check_place 是否也查了 kill switch。"""
    gw = _FakeGateway()
    a = _adapter(engine, gw, _KillSwitchBlindGuard())
    with pytest.raises(RiskError):
        await a.place(_req(), actor_user_id=1)
    assert gw.place_calls == []  # _send_gate 在 gateway.place() 之前就擋下，gateway 從未被呼叫
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "failed"  # 不留在 pending 卡死（同 V3-2 收尾原則）


# ---- Fix Round 1（審查 Important #2）：remote cancel/update 專屬測試補齊 ----
# cancel 的 `_do_cancel`、update 的 `_do_update` 實作在 Task 6 原始交付就已存在（gateway.ready
# 檢查、TradeNotFoundError/AgentUnavailableError/AgentCommandTimeoutError 傳遞），當時只缺
# 專屬測試覆蓋，這裡補上。


async def _placed_order(engine, gw, guard):
    """建立一筆已成功送出（status=submitted）的委託，供以下 cancel/update 測試操作。"""
    a = _adapter(engine, gw, guard)
    ack = await a.place(_req(), actor_user_id=1)
    return a, ack


async def test_remote_cancel_success_marks_cancelled(engine):
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    result = await a.cancel(ack.broker_order_id, actor_user_id=1)
    assert result.status == "cancelled"
    assert gw.cancel_calls == ["101AA1"]
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "cancelled"


async def test_remote_cancel_offline_rejected_state_unchanged(engine):
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    gw.ready = False
    with pytest.raises(OrderError):
        await a.cancel(ack.broker_order_id, actor_user_id=1)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "submitted"


async def test_remote_cancel_trade_not_found_propagates(engine):
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    gw.raise_exc = TradeNotFoundError("101AA1")
    with pytest.raises(OrderError):
        await a.cancel(ack.broker_order_id, actor_user_id=1)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "submitted"


async def test_remote_update_timeout_marks_unknown_keeps_quota(engine):
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    gw.raise_exc = AgentCommandTimeoutError("逾時")
    with pytest.raises(AgentCommandTimeoutError):
        await a.update(ack.broker_order_id, actor_user_id=1, qty=3)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "unknown"
        rows = list(s.exec(select(QuotaReservation)))
        update_row = next(r for r in rows if r.reservation_id != ack.client_order_id)
        assert update_row.state == "reserved"  # 結果不明，watchdog reconcile 前不擅自 release


async def test_remote_update_unavailable_releases_delta_quota(engine):
    """讀碼結論（`update()` 的 `except Exception` 分支，見 shioaji_adapter.py ~707-747 行）：
    update 失敗分支語意與 place 不同——`classification=="failed"`（`AgentUnavailableError`
    正是一例）只會 release「若有」保留的 delta 配額，**不**把委託本身標成 failed；委託
    status 維持呼叫前的既有值（這裡是 place 留下的 "submitted"），因為「改單失敗不代表
    委託本身壞了」（同 in-process 版 test_update_releases_delta_quota_reservation_on_broker_
    explicit_rejection / ...when_send_gate_raises_riskerror 的既有原則）。只有
    classification=="unknown" 才會把委託標成 "unknown"（見上一個測試）。兩支分支最後都
    re-raise 原始例外型別（不包成 OrderError），故這裡 pytest.raises 抓的是
    AgentUnavailableError 本身。"""
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    gw.raise_exc = AgentUnavailableError("斷線")
    with pytest.raises(AgentUnavailableError):
        await a.update(ack.broker_order_id, actor_user_id=1, qty=3)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "submitted"  # 改單失敗不影響委託本身狀態
        rows = list(s.exec(select(QuotaReservation)))
        update_row = next(r for r in rows if r.reservation_id != ack.client_order_id)
        assert update_row.state == "released"  # 明確判定失敗，立即釋放 delta 配額


async def test_remote_reconcile_stages_payloads_and_returns_count(engine):
    gw = _FakeGateway()
    gw.snapshot = ([{"order_id": "101AA1", "seqno": "101AA1", "status": "Filled"}],
                   datetime(2026, 8, 4, 9, 0))
    a = _adapter(engine, gw, _guard(engine))
    await a.reconcile()
    with Session(engine) as s:
        rows = s.exec(select(RawInbox)).all()
        assert len(rows) == 1 and rows[0].kind == "order_report"
