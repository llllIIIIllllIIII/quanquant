import time
import pytest
from sqlmodel import Session, select
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from quanquant.broker.agent_channel import AgentChannel
from quanquant.broker.session_state import OrderSessionState
from quanquant.config import get_settings
from quanquant.db.models import RawInbox
from quanquant.web.app import create_app
from quanquant.web.deps import get_session


class _FakeHub:
    def __init__(self):
        self.publishes = 0
    def publish(self):
        self.publishes += 1


class _FakeAdapter:
    def __init__(self):
        self.account = ""
        self.reconcile_calls = 0
        self.block = None            # asyncio.Event 時卡住 reconcile（測非 inline）
    async def reconcile(self):
        self.reconcile_calls += 1
        if self.block is not None:
            await self.block.wait()


class _SpyChannel(AgentChannel):
    def __init__(self):
        super().__init__()
        self.acks = []
    def resolve_ack(self, ack):
        self.acks.append(ack)
        super().resolve_ack(ack)


def _wait(cond, timeout=2.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.02)
    return False


@pytest.fixture
def ws_env(engine, monkeypatch):
    monkeypatch.setenv("AGENT_WS_TOKEN", "tok")
    get_settings.cache_clear()
    app = create_app()

    def _session_override():
        with Session(engine) as s:
            yield s

    app.dependency_overrides[get_session] = _session_override
    app.state.agent_channel = _SpyChannel()
    app.state.order_session_state = OrderSessionState()
    app.state.order_events = _FakeHub()
    app.state.order_service = _FakeAdapter()
    app.state.order_session_factory = lambda: Session(engine)
    yield app
    get_settings.cache_clear()


def test_bad_token_closed(ws_env):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "wrong"}) as ws:
        with pytest.raises(WebSocketDisconnect):
            ws.receive_json()


def test_login_marks_ready_sets_account_schedules_reconcile(ws_env):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
        assert _wait(lambda: ws_env.state.order_session_state.ready)
        assert ws_env.state.order_service.account == "F1"
        assert _wait(lambda: ws_env.state.order_service.reconcile_calls == 1)
        assert ws_env.state.order_events.publishes >= 1
    assert _wait(lambda: ws_env.state.order_session_state.disabled)  # 斷線 → disabled


def test_report_staged_then_acked(ws_env, engine):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        ws.send_json({"type": "report", "event_id": 7, "kind": "deal_report",
                      "payload": {"trade_id": "T1"}})
        assert ws.receive_json() == {"type": "report_ack", "event_id": 7}
    with Session(engine) as s:
        rows = s.exec(select(RawInbox)).all()
        assert len(rows) == 1 and rows[0].kind == "deal_report"


def test_duplicate_report_resend_both_staged_and_acked(ws_env, engine):
    # at-least-once：staging 層允許重複列，去重由既有 Deal 層 uq_deal_fill 吸收
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        for _ in range(2):
            ws.send_json({"type": "report", "event_id": 7, "kind": "deal_report",
                          "payload": {"trade_id": "T1"}})
            assert ws.receive_json()["event_id"] == 7
    with Session(engine) as s:
        assert len(s.exec(select(RawInbox)).all()) == 2


def test_cmd_ack_routed_to_channel(ws_env):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        ws.send_json({"type": "cmd_ack", "cmd_id": "c9", "ok": True, "result": {}})
        assert _wait(lambda: len(ws_env.state.agent_channel.acks) == 1)
        assert ws_env.state.agent_channel.acks[0].cmd_id == "c9"


def test_login_reconcile_not_inline_receive_loop_stays_responsive(ws_env, engine):
    import asyncio
    adapter = ws_env.state.order_service
    adapter.block = asyncio.Event()   # reconcile 永久卡住
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
        ws.send_json({"type": "report", "event_id": 1, "kind": "order_report",
                      "payload": {"k": 1}})
        # reconcile 卡住時 report 仍被處理 → 證明 login 用 create_task 非 inline await
        assert ws.receive_json() == {"type": "report_ack", "event_id": 1}
    adapter.block.set()


def test_invalid_frame_ignored_connection_survives(ws_env):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        ws.send_json({"type": "evil"})
        ws.send_json({"type": "report", "event_id": 2, "kind": "order_report",
                      "payload": {}})
        assert ws.receive_json()["event_id"] == 2


def test_ws_closes_when_wiring_incomplete_missing_session_factory(engine, monkeypatch):
    # Task 8 附加需求 1：channel.attach 前務必讀完 order_session_state/order_service/
    # order_session_factory——缺任一個就拒絕連線，channel 不能卡在 attached 態洩漏。
    monkeypatch.setenv("AGENT_WS_TOKEN", "tok")
    get_settings.cache_clear()
    app = create_app()

    def _session_override():
        with Session(engine) as s:
            yield s

    app.dependency_overrides[get_session] = _session_override
    channel = AgentChannel()
    app.state.agent_channel = channel
    app.state.order_session_state = OrderSessionState()
    app.state.order_events = _FakeHub()
    app.state.order_service = _FakeAdapter()
    # 故意不設定 app.state.order_session_factory —— 模擬 wiring 未完成

    client = TestClient(app)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws.receive_json()
    assert exc_info.value.code == 1011
    assert not channel.connected  # 未 attach，不會卡在 attached 態
    get_settings.cache_clear()


def test_ws_rejects_when_token_unset_default_empty(engine, monkeypatch):
    # Task 8 附加需求 2：不設 AGENT_WS_TOKEN（預設空字串）時一律拒絕連線——驗 production
    # 預設安全（漏設 env 不會意外開放無認證下單通道）。
    monkeypatch.delenv("AGENT_WS_TOKEN", raising=False)
    get_settings.cache_clear()
    app = create_app()

    def _session_override():
        with Session(engine) as s:
            yield s

    app.dependency_overrides[get_session] = _session_override
    app.state.agent_channel = AgentChannel()
    app.state.order_session_state = OrderSessionState()

    client = TestClient(app)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ""}) as ws:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws.receive_json()
    assert exc_info.value.code == 1008
    get_settings.cache_clear()


def test_receive_loop_unexpected_exception_logged_and_reraised(ws_env, monkeypatch, caplog):
    # Task 8 附加需求 3：非 WebSocketDisconnect 的例外要 log.exception 後 re-raise（觀測用，
    # 不改變既有中斷語意——finally 仍會跑，連線仍會斷）。
    import quanquant.web.routers.agent_ws as agent_ws_module

    def _boom(_data):
        raise RuntimeError("boom")

    monkeypatch.setattr(agent_ws_module, "parse_uplink", _boom)
    caplog.set_level("ERROR", logger="quanquant.web.routers.agent_ws")
    client = TestClient(ws_env)
    # TestClient 的 websocket 連線在背景 thread 跑 app；非 WebSocketDisconnect 的例外會在
    # `with` 區塊結束（背景 task join）時於前景重新拋出——這正是「re-raise、不吞例外」要
    # 驗的行為，只是在這個測試工具下顯現的位置是 context manager 出口而非 receive_json()。
    with pytest.raises(RuntimeError, match="boom"):
        with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
            ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
            ws.receive_json()
    assert "agent WS 處理上行訊息失敗" in caplog.text
    assert _wait(lambda: ws_env.state.order_session_state.disabled)
