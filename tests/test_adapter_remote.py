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

from quanquant.broker.base import (
    AgentCommandTimeoutError,
    AgentUnavailableError,
    OrderError,
    RiskError,
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


async def test_kill_switch_blocks_before_gateway_called(engine):
    gw = _FakeGateway()
    guard = _guard(engine)
    guard.set_kill_switch(True)
    a = _adapter(engine, gw, guard)
    with pytest.raises(RiskError):
        await a.place(_req(), actor_user_id=1)
    assert gw.place_calls == []


async def test_remote_reconcile_stages_payloads_and_returns_count(engine):
    gw = _FakeGateway()
    gw.snapshot = ([{"order_id": "101AA1", "seqno": "101AA1", "status": "Filled"}],
                   datetime(2026, 8, 4, 9, 0))
    a = _adapter(engine, gw, _guard(engine))
    await a.reconcile()
    with Session(engine) as s:
        rows = s.exec(select(RawInbox)).all()
        assert len(rows) == 1 and rows[0].kind == "order_report"
