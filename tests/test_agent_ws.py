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
    # codex round3 fix2：UpReport 分支現在要求 channel.logged_in——先 login 才能送 report。
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
        ws.send_json({"type": "report", "event_id": 7, "kind": "deal_report",
                      "payload": {"trade_id": "T1"}})
        assert ws.receive_json() == {"type": "report_ack", "event_id": 7}
    with Session(engine) as s:
        rows = s.exec(select(RawInbox)).all()
        assert len(rows) == 1 and rows[0].kind == "deal_report"


def test_duplicate_report_resend_both_staged_and_acked(ws_env, engine):
    # at-least-once：staging 層允許重複列，去重由既有 Deal 層 uq_deal_fill 吸收
    # codex round3 fix2：UpReport 分支現在要求 channel.logged_in——先 login 才能送 report。
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
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
    # codex round3 fix2：UpReport 分支現在要求 channel.logged_in——先 login 才能送 report。
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
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


# ---- codex round1 fix2（HIGH）：新連線 attach 後，舊連線較晚才跑到的 finally 不該把
# 新連線拆掉、誤標 offline；舊連線收到的訊息也不該再改動 channel 狀態。----

def test_stale_connection_message_ignored_after_superseded(ws_env):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws_old:
        ws_old.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
        assert _wait(lambda: ws_env.state.order_service.account == "F1")

        with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws_new:
            ws_new.send_json({"type": "login", "account": "F2", "mode": "sim", "protocol": 1})
            assert _wait(lambda: ws_env.state.order_service.account == "F2")

            # 舊連線的 socket 仍開著；重送一次 login（F1）——若舊 handler 沒被 generation
            # 擋下，會被當成合法上行訊息處理，把 account 改回 F1，蓋掉新連線剛登入的 F2。
            ws_old.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
            time.sleep(0.1)

            assert ws_env.state.order_service.account == "F2"     # 未被舊連線的訊息改回去
            assert ws_env.state.agent_channel.account == "F2"


def test_stale_connection_finally_does_not_disable_new_connection(ws_env):
    client = TestClient(ws_env)
    old_cm = client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"})
    ws_old = old_cm.__enter__()
    ws_old.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
    assert _wait(lambda: ws_env.state.order_service.account == "F1")

    new_cm = client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"})
    ws_new = new_cm.__enter__()
    try:
        ws_new.send_json({"type": "login", "account": "F2", "mode": "sim", "protocol": 1})
        assert _wait(lambda: ws_env.state.order_service.account == "F2")

        old_cm.__exit__(None, None, None)   # 手動關閉舊連線（觸發它的 finally），新連線仍開著

        assert _wait(lambda: ws_env.state.agent_channel.connected is True)
        assert ws_env.state.order_session_state.disabled is False
        assert ws_env.state.agent_channel.account == "F2"
    finally:
        new_cm.__exit__(None, None, None)


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


# ---- codex round2 fix2：server 端擋「未處理 RawInbox + 換帳號」視窗——agent 端的
# tripwire（buffer.assert_account）只擋得住「尚未送出」的列；server 已經 commit RawInbox
# 並 ack、worker 尚未處理的列不受保護。此時若換帳號登入，worker 之後映射 order_report 用
# 的是 mutable adapter.account（已被新帳號覆蓋），舊帳號的回報會被錯配。----

def test_login_account_switch_rejected_when_unprocessed_raw_inbox_pending(ws_env, engine):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws1:
        ws1.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
        assert _wait(lambda: ws_env.state.order_service.account == "F1")
        assert _wait(lambda: ws_env.state.order_session_state.ready)

    # server 已 commit 但 worker 尚未處理的一筆 RawInbox（processed=False, quarantine=False）。
    with Session(engine) as s:
        s.add(RawInbox(kind="deal_report", broker="shioaji", payload="{}"))
        s.commit()

    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws2:
        ws2.send_json({"type": "login", "account": "F2", "mode": "sim", "protocol": 1})
        time.sleep(0.2)   # 給 server 足夠時間處理（若未擋下，account 會被改成 F2）
        assert ws_env.state.order_service.account == "F1"          # 沒被換掉
        assert ws_env.state.order_session_state.ready is False     # 這次 login 未生效


def test_login_account_switch_allowed_when_no_unprocessed_raw_inbox(ws_env, engine):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws1:
        ws1.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
        assert _wait(lambda: ws_env.state.order_service.account == "F1")

    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws2:
        ws2.send_json({"type": "login", "account": "F2", "mode": "sim", "protocol": 1})
        assert _wait(lambda: ws_env.state.order_service.account == "F2")   # 無未處理列：放行
        assert _wait(lambda: ws_env.state.order_session_state.ready)


# ---- codex round3（HIGH，第三輪唯一殘留）：round2 fix2 的兩條實證繞過。
#   繞過1：login 被拒（換帳號＋有未處理 RawInbox）後只 continue，socket 還開著；agent 端
#          的 pump 不等 login 確認就送 report；UpReport 分支未檢查 channel.logged_in →
#          被拒帳號的 report 照樣 commit+ack，之後 worker 用仍是舊帳號的 adapter.account
#          處理 → 跨帳號錯配。
#   繞過2（TOCTOU）：舊連線的 report commit 正在 to_thread 飛行中，新連線的未處理列 count
#          查詢看到 0 → 放行換帳號；舊 report 之後才 commit 完成，落在新帳號狀態下。
# 修法：login 被拒即關閉連線（不再 continue）；UpReport 分支未登入/舊 generation 一律不
# commit 不 ack；AgentChannel.inbox_lock 序列化「report commit」與「login 的 count 查詢+
# 決策+mark_logged_in+adapter.account 設定」。----

def test_login_rejected_closes_connection(ws_env, engine):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws1:
        ws1.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
        assert _wait(lambda: ws_env.state.order_service.account == "F1")

    # 未處理列（processed=False, quarantine=False）殘留 → 觸發換帳號 guard。
    with Session(engine) as s:
        s.add(RawInbox(kind="deal_report", broker="shioaji", payload="{}"))
        s.commit()

    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws2:
        ws2.send_json({"type": "login", "account": "F2", "mode": "sim", "protocol": 1})
        with pytest.raises(WebSocketDisconnect):
            ws2.receive_json()   # 連線被關閉（1008）——不再是半開態

    assert ws_env.state.order_service.account == "F1"        # 帳號未被換掉
    assert ws_env.state.agent_channel.logged_in is False      # 未 mark_logged_in
    with Session(engine) as s:
        rows = s.exec(select(RawInbox)).all()
        # 繞過1：拒收後連線已關，不會再有機會讓 F2 的 report 被 commit——維持原本那 1 筆。
        assert len(rows) == 1


def test_report_before_login_not_staged_not_acked(ws_env, engine):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        ws.send_json({"type": "report", "event_id": 99, "kind": "deal_report", "payload": {}})
        ws.send_json({"type": "health"})   # 確認迴圈仍活著（report 被忽略不代表連線掛了）
        assert _wait(lambda: ws_env.state.agent_channel.last_heartbeat is not None)
    with Session(engine) as s:
        assert s.exec(select(RawInbox)).all() == []   # 未登入的 report 沒有被 staged


def test_toctou_report_commit_serializes_against_login_switch(ws_env, engine, monkeypatch):
    import threading

    import quanquant.web.routers.agent_ws as agent_ws_module

    entered = threading.Event()
    release = threading.Event()
    original_commit = agent_ws_module.commit_raw_callback

    def _blocking_commit(*args, **kwargs):
        entered.set()
        release.wait(timeout=5)
        return original_commit(*args, **kwargs)

    monkeypatch.setattr(agent_ws_module, "commit_raw_callback", _blocking_commit)

    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws_old:
        ws_old.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
        assert _wait(lambda: ws_env.state.order_service.account == "F1")

        ws_old.send_json({"type": "report", "event_id": 1, "kind": "deal_report", "payload": {}})
        assert entered.wait(timeout=2), "commit 應已進入（卡在 blocking commit 中）"

        with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws_new:
            ws_new.send_json({"type": "login", "account": "F2", "mode": "sim", "protocol": 1})

            # 舊 report 的 commit 仍卡住（inbox_lock 未釋放）：F2 login 的 count 查詢必須被
            # 序列化在 commit 完成之後才判定——輪詢一段時間內帳號都不該被換成 F2。
            assert not _wait(lambda: ws_env.state.order_service.account == "F2", timeout=0.3)

            release.set()   # 放行卡住的 commit

            assert ws_old.receive_json() == {"type": "report_ack", "event_id": 1}
            # commit 完成後 RawInbox 多一筆未處理列 → F2 login 的 count 查詢看到它 → 拒絕、
            # 連線被關（繞過2：TOCTOU 已被 inbox_lock 消除）。
            with pytest.raises(WebSocketDisconnect):
                ws_new.receive_json()

    assert ws_env.state.order_service.account == "F1"   # F2 login 被拒，帳號未換
