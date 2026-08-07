"""Task 7（D1/D9 骨架）：AgentRegistry／UserAgentSlot／lifespan／healthz。

涵蓋 brief 指定的驗收清單（S#8 隔離骨架/13）：
  - UserAgentSlot/AgentRegistry 基本行為（get/slots）。
  - 兩個 slot 各自連線互不干擾：A offline 不影響 B 下單；A 的 reconcile 卡住不會排隊 B 的
    place（各自一份 BrokerSupervisor 鎖，用可控 fake gateway 直接證明——不需要真 WS）。
  - agent 模式 wiring 完成後 /healthz 恆 200，且與個別 slot 的連線狀態無關（D9 語意變更）。
  - deps 層 user-aware 解析（`get_order_service`/`get_agent_slot`）：agent 模式依 registry
    找 slot；in-process／registry 不存在時 fallback 回舊的單例路徑，零改動。
  - `orders_agent_status` per-user 化：每個 user 只看得到自己 slot 的連線狀態。
"""
import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from sqlmodel import Session
from starlette.testclient import TestClient

from quanquant.auth import service as auth_service
from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.broker.agent_registry import AgentRegistry, UserAgentSlot
from quanquant.broker.base import OrderError
from quanquant.broker.risk import RiskGuard
from quanquant.broker.session_state import OrderSessionState
from quanquant.broker.shioaji_adapter import ShioajiAdapter
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import OrderRequest
from quanquant.config import Settings, get_settings
from quanquant.web.app import _start_agent_channel_subsystem, create_app
from quanquant.web.deps import get_agent_slot, get_order_service, get_poller, get_session

# 專案 pytest 設定 asyncio_mode="auto"（pyproject.toml）——async def 測試自動被
# pytest-asyncio 收集，不需要額外標記。


# ---------------------------------------------------------------------------
# UserAgentSlot / AgentRegistry：基本行為
# ---------------------------------------------------------------------------

def _bare_slot(user_id: int) -> UserAgentSlot:
    return UserAgentSlot(
        user_id=user_id, channel=None, gateway=None, adapter=None,
        session_state=OrderSessionState(), supervisor=BrokerSupervisor(), tasks=[],
    )


def test_registry_get_returns_none_when_no_slot():
    registry = AgentRegistry()
    assert registry.get(1) is None


def test_registry_add_get_and_slots_roundtrip():
    registry = AgentRegistry()
    slot1, slot2 = _bare_slot(1), _bare_slot(2)
    registry.add(slot1)
    registry.add(slot2)

    assert registry.get(1) is slot1
    assert registry.get(2) is slot2
    assert registry.get(3) is None
    assert {s.user_id for s in registry.slots()} == {1, 2}


def test_registry_add_same_user_id_replaces_slot():
    registry = AgentRegistry()
    old, new = _bare_slot(1), _bare_slot(1)
    registry.add(old)
    registry.add(new)
    assert registry.get(1) is new
    assert list(registry.slots()) == [new]


# ---------------------------------------------------------------------------
# S#8/13 隔離骨架：兩個 slot 各自一份 supervisor/gateway/adapter，互不干擾
# ---------------------------------------------------------------------------

class _FakeGateway:
    """比照 test_adapter_remote.py 的 `_FakeGateway`，額外加 `snapshot_block`——一個
    `asyncio.Event`，`trades_snapshot` 會在這裡卡住，用來模擬「慢 reconcile」。"""

    def __init__(self, *, ready: bool = True) -> None:
        self.ready = ready
        self.place_calls: list = []
        self.snapshot_calls = 0
        self.snapshot_block: asyncio.Event | None = None
        self.snapshot = ([], None)
        self.result = {"ordno": "O1", "broker_order_id": "B1"}

    @property
    def admission_ready(self) -> bool:
        # C2：測試替身鏡射 ready，讓既有 gw_ready 寫法對 place/update 的 admission 檢查
        # （現在改查 admission_ready）仍然生效。
        return self.ready

    async def place(self, req, *, cmd_id=None, expires_at=None):
        self.place_calls.append(req)
        return self.result

    async def cancel(self, ordno, *, cmd_id=None, expires_at=None):
        pass

    async def update(self, ordno, *, price, qty, price_type=None, cmd_id=None, expires_at=None):
        pass

    async def trades_snapshot(self, after):
        self.snapshot_calls += 1
        if self.snapshot_block is not None:
            await self.snapshot_block.wait()
        return self.snapshot


def _req(cid: str, *, user_id: int) -> OrderRequest:
    return OrderRequest(client_order_id=cid, symbol="TXF", action="Buy", qty=1,
                        price=Decimal("21500"), price_type="LMT", order_type="ROD",
                        octype="Auto", user_id=user_id)


def _shared_guard(engine, *, owners) -> RiskGuard:
    """D3：RiskGuard 是全站單一共享實例，兩個 slot 的 adapter 共用同一個——本測試刻意共用
    一個 guard，貼近 `_start_agent_channel_subsystem` 的真實 wiring 形狀。"""
    return RiskGuard(session_factory=lambda: Session(engine), secret="s",
                     owner_user_ids=frozenset(owners), symbol_whitelist=frozenset({"TXF"}),
                     max_qty_per_order=5, max_qty_per_day=20, max_orders_per_day=20)


def _make_slot(engine, *, user_id: int, account: str, guard: RiskGuard, gw_ready: bool = True):
    supervisor = BrokerSupervisor()
    gw = _FakeGateway(ready=gw_ready)
    adapter = ShioajiAdapter(api_key="", secret_key="", ca_path=None, ca_passwd=None,
                             person_id=None, symbol="TXF", mode="sim",
                             session_factory=lambda: Session(engine), supervisor=supervisor,
                             risk_guard=guard, sim_fee_per_lot=Decimal("20"), remote_gateway=gw,
                             agent_user_id=user_id)
    adapter.account = account
    state = OrderSessionState()
    state.mark_ready()
    slot = UserAgentSlot(user_id=user_id, channel=None, gateway=gw, adapter=adapter,
                         session_state=state, supervisor=supervisor, tasks=[])
    return slot, gw


async def test_slot_a_offline_does_not_affect_slot_b_place(engine):
    """S#8：A offline → B 下單不受影響。"""
    guard = _shared_guard(engine, owners={1, 2})
    slot_a, gw_a = _make_slot(engine, user_id=1, account="ACC-A", guard=guard, gw_ready=False)
    slot_b, gw_b = _make_slot(engine, user_id=2, account="ACC-B", guard=guard, gw_ready=True)
    registry = AgentRegistry()
    registry.add(slot_a)
    registry.add(slot_b)

    with pytest.raises(OrderError):
        await registry.get(1).adapter.place(_req("a-1", user_id=1), actor_user_id=1)

    ack = await registry.get(2).adapter.place(_req("b-1", user_id=2), actor_user_id=2)
    assert ack.status == "submitted"
    assert len(gw_b.place_calls) == 1
    assert gw_a.place_calls == []  # A 完全沒被呼叫到（offline fail-fast，不進 native）


async def test_slot_a_slow_reconcile_does_not_queue_slot_b_place(engine):
    """S#8：A 的 reconcile 卡住（拿著 A 自己的 supervisor 鎖）不會讓 B 的 place 排隊——
    每個 slot 各自一份 BrokerSupervisor，這是 I8 跨 user 隔離的核心機制。"""
    guard = _shared_guard(engine, owners={1, 2})
    slot_a, gw_a = _make_slot(engine, user_id=1, account="ACC-A", guard=guard)
    slot_b, gw_b = _make_slot(engine, user_id=2, account="ACC-B", guard=guard)
    registry = AgentRegistry()
    registry.add(slot_a)
    registry.add(slot_b)

    gw_a.snapshot_block = asyncio.Event()  # 永久不 set：A 的 reconcile 卡住不放
    reconcile_task = asyncio.create_task(registry.get(1).adapter.reconcile())
    for _ in range(50):  # 等 reconcile 真的進入卡住狀態（已拿到 supervisor_a 鎖）
        await asyncio.sleep(0.01)
        if gw_a.snapshot_calls >= 1:
            break
    assert gw_a.snapshot_calls >= 1 and not reconcile_task.done()

    try:
        ack = await asyncio.wait_for(
            registry.get(2).adapter.place(_req("b-2", user_id=2), actor_user_id=2), timeout=1.0,
        )
    finally:
        gw_a.snapshot_block.set()
        await asyncio.wait_for(reconcile_task, timeout=1.0)

    assert ack.status == "submitted"
    assert len(gw_b.place_calls) == 1


# ---------------------------------------------------------------------------
# healthz：agent 模式 wiring 完成即 200，恆與個別 slot 狀態無關（D9）
# ---------------------------------------------------------------------------

def _agent_settings(**over) -> Settings:
    base = dict(order_channel="agent", order_mode="sim", order_owner_user_ids="1,2",
               session_secret="s")
    base.update(over)
    return Settings(**base)


async def _wire(engine, monkeypatch) -> tuple[FastAPI, OrderSessionState, list]:
    monkeypatch.setattr("quanquant.web.app.get_engine", lambda: engine)
    app = SimpleNamespace(state=SimpleNamespace(order_events=None))
    state, tasks = OrderSessionState(), []
    await _start_agent_channel_subsystem(app, _agent_settings(), tasks, state, None)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    return app, state, tasks


async def test_agent_mode_healthz_state_ready_regardless_of_slot_states(engine, monkeypatch):
    app, state, _ = await _wire(engine, monkeypatch)
    assert state.ready is True  # wiring 完成即 ready，尚未有任何 agent 連線

    # 把兩個 slot 分別撥成「已連線」「離線」「（模擬異常）unhealthy」等各種狀態，全站的
    # order_session_state（healthz 真正讀的那個）都不應該因此改變——它只在 wiring 這個
    # 呼叫本身失敗時才會是別的值（見 test_agent_app_wiring.py 的 disabled/unhealthy 案例）。
    slot1, slot2 = app.state.agent_registry.get(1), app.state.agent_registry.get(2)
    slot1.session_state.mark_ready()
    slot2.session_state.mark_unhealthy("模擬異常，只影響這個 slot")
    assert state.ready is True

    slot1.session_state.mark_disabled("agent 離線")
    assert state.ready is True


def test_agent_mode_healthz_endpoint_returns_200_via_http(engine, user, monkeypatch):
    """端到端經 /healthz：agent 模式即使沒有任何 slot 連線也回 200（沿用既有
    web/routers/health.py 邏輯，只讀全站 order_session_state，不迭代 registry）。"""
    from quanquant.web.deps import get_order_session_state

    monkeypatch.setenv("ORDER_CHANNEL", "agent")
    get_settings.cache_clear()
    app = create_app()
    state = OrderSessionState()
    state.mark_ready()  # 模擬 _start_agent_channel_subsystem 已成功 wiring
    app.dependency_overrides[get_order_session_state] = lambda: state
    app.dependency_overrides[get_poller] = lambda: None
    client = TestClient(app)
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["order_subsystem"]["status"] == "ready"
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# deps 層 user-aware 解析：agent 模式依 registry；in-process/未 wiring fallback 回單例
# ---------------------------------------------------------------------------

def _fake_request(**state_kwargs):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(**state_kwargs)))


def test_get_order_service_falls_back_to_singleton_when_no_registry():
    """in-process 模式（或 agent 模式尚未成功 wiring）：agent_registry 不存在，一律
    fallback 回 app.state.order_service，零改動。"""
    sentinel = object()
    request = _fake_request(order_service=sentinel)
    user = SimpleNamespace(id=1)
    assert get_order_service(request, user) is sentinel


def test_get_order_service_returns_none_fallback_when_neither_set():
    request = _fake_request()
    user = SimpleNamespace(id=1)
    assert get_order_service(request, user) is None


def test_get_order_service_resolves_via_registry_slot_for_owner():
    registry = AgentRegistry()
    slot = _bare_slot(1)
    slot.adapter = "ADAPTER-FOR-1"
    registry.add(slot)
    request = _fake_request(agent_registry=registry)
    user = SimpleNamespace(id=1)
    assert get_order_service(request, user) == "ADAPTER-FOR-1"


def test_get_order_service_none_for_non_owner_when_registry_present():
    """registry 存在但這個 user 沒有 slot（非 owner）——回 None，沿用既有「下單子系統
    未啟用」下游語意，不會誤把其他 user 的 adapter 借給他用。"""
    registry = AgentRegistry()
    registry.add(_bare_slot(1))
    request = _fake_request(agent_registry=registry)
    user = SimpleNamespace(id=999)
    assert get_order_service(request, user) is None


def test_get_agent_slot_none_when_no_registry():
    request = _fake_request()
    user = SimpleNamespace(id=1)
    assert get_agent_slot(request, user) is None


def test_get_agent_slot_returns_own_slot_only():
    registry = AgentRegistry()
    slot1, slot2 = _bare_slot(1), _bare_slot(2)
    registry.add(slot1)
    registry.add(slot2)
    request = _fake_request(agent_registry=registry)
    assert get_agent_slot(request, SimpleNamespace(id=1)) is slot1
    assert get_agent_slot(request, SimpleNamespace(id=2)) is slot2
    assert get_agent_slot(request, SimpleNamespace(id=3)) is None


# ---------------------------------------------------------------------------
# orders_agent_status：per-user 化——每個 user 只看得到自己 slot 的狀態
# ---------------------------------------------------------------------------

def _logged_in_client(engine, user_id: int, *, token_version: int = 0) -> TestClient:
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    client = TestClient(app)
    client.cookies.set(SESSION_COOKIE, sign_session(user_id, token_version))
    return client, app


def test_orders_agent_status_per_user_badge_isolated(engine, monkeypatch):
    """兩個 owner 各自登入，各自打 /orders/agent-status：A 看到自己已連線，B 仍看到自己
    未連線——不是全站共用一份（Task 7 之前的行為）。"""
    monkeypatch.setenv("ORDER_CHANNEL", "agent")
    get_settings.cache_clear()
    with Session(engine) as s:
        owner_a = auth_service.create_user(s, "owner-a", "pw", role="admin")
        owner_b = auth_service.create_user(s, "owner-b", "pw", role="admin")
        a_id, b_id = owner_a.id, owner_b.id

    registry = AgentRegistry()
    slot_a, slot_b = _bare_slot(a_id), _bare_slot(b_id)
    slot_a.session_state.mark_ready()
    slot_b.session_state.mark_disabled("agent 未連線")
    registry.add(slot_a)
    registry.add(slot_b)

    client_a, app_a = _logged_in_client(engine, a_id)
    app_a.state.agent_registry = registry
    body_a = client_a.get("/orders/agent-status").text
    assert "agent 已連線" in body_a

    client_b, app_b = _logged_in_client(engine, b_id)
    app_b.state.agent_registry = registry
    body_b = client_b.get("/orders/agent-status").text
    assert "agent 未連線" in body_b
    get_settings.cache_clear()
