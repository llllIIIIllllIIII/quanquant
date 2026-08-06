import datetime as dt
import time
from datetime import timedelta

import pytest
from sqlmodel import Session, select
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from quanquant.auth import service as auth_service
from quanquant.auth.agent_tokens import issue_token
from quanquant.broker.agent_channel import AgentChannel
from quanquant.broker.session_state import OrderSessionState
from quanquant.config import get_settings
from quanquant.db.models import AgentToken, RawInbox
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


class _FakeRiskGuard:
    """D2 測試替身：只需要 `is_owner`（WS 握手唯一用到的介面），不拉進完整
    `RiskGuard`（session_factory/secret/quota 等與本檔測試焦點無關）。"""
    def __init__(self, owner_ids):
        self._owner_ids = set(owner_ids)
    def is_owner(self, user_id):
        return user_id in self._owner_ids


def _wait(cond, timeout=2.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.02)
    return False


@pytest.fixture
def ws_env(engine, monkeypatch):
    # D2：連線驗證改為 per-user DB opaque token——不再需要 AGENT_WS_TOKEN 這個站台層級
    # 靜態密鑰；改為先建一個 owner 使用者、幫它簽發一枚真的 agent token，WS 測試一律帶
    # 這枚 token（`ws_env.state.agent_test_token`）連線。
    get_settings.cache_clear()
    app = create_app()

    def _session_override():
        with Session(engine) as s:
            yield s

    app.dependency_overrides[get_session] = _session_override
    with Session(engine) as s:
        owner = auth_service.create_user(s, "agent-owner", "pw", role="admin")
        owner_id = owner.id  # 讀出來存純值——owner 本身在 with 區塊結束後就會 detach
        token = issue_token(s, user_id=owner_id, ttl_days=30)
    app.state.agent_channel = _SpyChannel()
    app.state.order_session_state = OrderSessionState()
    app.state.order_events = _FakeHub()
    app.state.order_service = _FakeAdapter()
    app.state.order_session_factory = lambda: Session(engine)
    app.state.order_risk_guard = _FakeRiskGuard({owner_id})
    app.state.agent_test_token = token
    app.state.agent_test_owner_id = owner_id
    yield app
    get_settings.cache_clear()


def test_bad_token_closed(ws_env):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "wrong"}) as ws:
        with pytest.raises(WebSocketDisconnect):
            ws.receive_json()


def test_login_marks_ready_sets_account_schedules_reconcile(ws_env):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
        assert _wait(lambda: ws_env.state.order_session_state.ready)
        assert ws_env.state.order_service.account == "F1"
        assert _wait(lambda: ws_env.state.order_service.reconcile_calls == 1)
        assert ws_env.state.order_events.publishes >= 1
    assert _wait(lambda: ws_env.state.order_session_state.disabled)  # 斷線 → disabled


def test_report_staged_then_acked(ws_env, engine):
    # codex round3 fix2：UpReport 分支現在要求 channel.logged_in——先 login 才能送 report。
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
        ws.send_json({"type": "report", "event_id": 7, "kind": "deal_report",
                      "account": "F1", "mode": "sim", "payload": {"trade_id": "T1"}})
        assert ws.receive_json() == {"type": "report_ack", "event_id": 7}
    with Session(engine) as s:
        rows = s.exec(select(RawInbox)).all()
        assert len(rows) == 1 and rows[0].kind == "deal_report"


def test_duplicate_report_resend_both_staged_and_acked(ws_env, engine):
    # at-least-once：staging 層允許重複列，去重由既有 Deal 層 uq_deal_fill 吸收
    # codex round3 fix2：UpReport 分支現在要求 channel.logged_in——先 login 才能送 report。
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
        for _ in range(2):
            ws.send_json({"type": "report", "event_id": 7, "kind": "deal_report",
                          "account": "F1", "mode": "sim", "payload": {"trade_id": "T1"}})
            assert ws.receive_json()["event_id"] == 7
    with Session(engine) as s:
        assert len(s.exec(select(RawInbox)).all()) == 2


def test_report_ack_only_after_commit_success(ws_env, engine, monkeypatch, caplog):
    # 零丟單紅線：UpReport 分支必須「RawInbox commit 成功後才回 report_ack」——commit 失敗
    # 時，client 絕不能收到 ack（否則 agent 端會誤判該筆回報已落地而不再重送，造成靜默丟單）。
    # 驗收者實證：把 send_json(DownReportAck) 搬到 commit_raw_callback 之前，既有 19 個
    # 相關測試全綠也擋不住這個回歸——本測試補上這條紅線的直接覆蓋。
    import quanquant.web.routers.agent_ws as agent_ws_module

    def _boom(*args, **kwargs):
        raise RuntimeError("commit boom")

    monkeypatch.setattr(agent_ws_module, "commit_raw_callback", _boom)
    caplog.set_level("ERROR", logger="quanquant.web.routers.agent_ws")
    client = TestClient(ws_env)
    received = []
    try:
        with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws:
            ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
            ws.send_json({"type": "report", "event_id": 5, "kind": "deal_report",
                         "account": "F1", "mode": "sim", "payload": {}})
            # 現行例外語意（Task 8 附加需求 3）：commit 失敗會讓 receive 迴圈 log.exception
            # 後 re-raise、連線斷線——不論客端在哪個時點觀察到例外（receive_json() 當場，或
            # with 區塊結束時背景 task join 再拋），照實接住，只驗證關鍵事實：沒有 ack。
            received.append(ws.receive_json())
    except Exception:
        pass
    assert not any(isinstance(m, dict) and m.get("type") == "report_ack" for m in received), (
        "commit 失敗仍收到 report_ack——commit-before-ack 順序被破壞"
    )
    assert "agent WS 處理上行訊息失敗" in caplog.text
    with Session(engine) as s:
        assert s.exec(select(RawInbox)).all() == []


def test_cmd_ack_routed_to_channel(ws_env):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws:
        ws.send_json({"type": "cmd_ack", "cmd_id": "c9", "event_id": 1, "ok": True,
                     "result": {}})
        assert _wait(lambda: len(ws_env.state.agent_channel.acks) == 1)
        assert ws_env.state.agent_channel.acks[0].cmd_id == "c9"


def test_login_reconcile_not_inline_receive_loop_stays_responsive(ws_env, engine):
    import asyncio
    adapter = ws_env.state.order_service
    adapter.block = asyncio.Event()   # reconcile 永久卡住
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
        ws.send_json({"type": "report", "event_id": 1, "kind": "order_report",
                      "account": "F1", "mode": "sim", "payload": {"k": 1}})
        # reconcile 卡住時 report 仍被處理 → 證明 login 用 create_task 非 inline await
        assert ws.receive_json() == {"type": "report_ack", "event_id": 1}
    adapter.block.set()


def test_invalid_frame_ignored_connection_survives(ws_env):
    # codex round3 fix2：UpReport 分支現在要求 channel.logged_in——先 login 才能送 report。
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
        ws.send_json({"type": "evil"})
        ws.send_json({"type": "report", "event_id": 2, "kind": "order_report",
                      "account": "F1", "mode": "sim", "payload": {}})
        assert ws.receive_json()["event_id"] == 2


def test_ws_closes_when_wiring_incomplete_missing_session_factory(engine, monkeypatch):
    # Task 8 附加需求 1（D2 更新：wiring 完整性檢查現在也涵蓋 order_risk_guard，因為 token
    # 驗證的 owner 判定需要它——但這裡缺的是更早讀取的 order_session_factory，所以不論
    # risk_guard 有沒有設都一樣落在這個分支）：channel.attach 前務必讀完
    # order_session_state/order_service/order_session_factory/order_risk_guard——缺任一個
    # 就拒絕連線，channel 不能卡在 attached 態洩漏。token 驗證本身依賴 session_factory，
    # 缺席時連驗證都做不了，所以 wiring 檢查必須先於 token 驗證——這裡帶什麼 token 字串
    # 都不影響結果（1011 而非 1008）。
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
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "irrelevant"}) as ws:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws.receive_json()
    assert exc_info.value.code == 1011
    assert not channel.connected  # 未 attach，不會卡在 attached 態
    get_settings.cache_clear()


def test_ws_closes_when_wiring_incomplete_missing_risk_guard(engine):
    # D2 新增：order_risk_guard 是 owner 判定的必要依賴，缺席同樣算 wiring 不完整（1011），
    # 不是「驗證失敗」（1008）——即使帶的是合法 token 也一樣，因為根本沒有東西可以判斷
    # is_owner。
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
    app.state.order_session_factory = lambda: Session(engine)
    # 故意不設定 app.state.order_risk_guard —— 模擬 wiring 未完成

    client = TestClient(app)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "irrelevant"}) as ws:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws.receive_json()
    assert exc_info.value.code == 1011
    assert not channel.connected
    get_settings.cache_clear()


def test_non_ascii_token_rejected_not_crashed(ws_env):
    # D2 更新：舊機制的 `secrets.compare_digest` 對非 ASCII str 直接 raise TypeError；新機制
    # 改成 DB hash 查表（`raw.encode("utf-8")` 餵給 hashlib，對任何 Unicode 字串都合法），
    # 不再有那個崩潰疑慮——本測試改驗證「規則仍然安全」：Starlette 依 ASGI spec 用 latin-1
    # 解碼 header，理論上可帶非 ASCII 字元；httpx 的 TestClient 對 str header 值本身強制
    # ascii-only（client 端送不出去），所以這裡直接用 latin-1 編碼過的 bytes 當 header
    # value，繞過 client 端限制、重現「server 收到非 ASCII token」的情境——查無此 hash，
    # 照樣安全回 1008，不崩潰。
    client = TestClient(ws_env)
    with client.websocket_connect(
        "/ws/agent", headers={"x-agent-token": "é".encode("latin-1")}
    ) as ws:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws.receive_json()
    assert exc_info.value.code == 1008


def test_ws_rejects_when_token_header_missing_or_empty(ws_env):
    # D2 改寫（原「不設 AGENT_WS_TOKEN 預設空字串」測試已隨該設定移除失去意義）：沒有
    # x-agent-token header（或空字串）時 raw_token 落地成空字串，`validate_token` 對空字串
    # 直接回 None——驗 production 預設安全（漏帶 header 不會意外放行），即使其餘 wiring
    # 完整、有真正 owner 的合法 token 存在也一樣拒絕。
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ""}) as ws:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws.receive_json()
    assert exc_info.value.code == 1008


# ---- codex round1 fix2（HIGH）：新連線 attach 後，舊連線較晚才跑到的 finally 不該把
# 新連線拆掉、誤標 offline；舊連線收到的訊息也不該再改動 channel 狀態。----

def test_stale_connection_message_ignored_after_superseded(ws_env):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws_old:
        ws_old.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
        assert _wait(lambda: ws_env.state.order_service.account == "F1")

        with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws_new:
            ws_new.send_json({"type": "login", "account": "F2", "mode": "sim", "protocol": 2, "health_epoch": 0})
            assert _wait(lambda: ws_env.state.order_service.account == "F2")

            # 舊連線的 socket 仍開著；重送一次 login（F1）——若舊 handler 沒被 generation
            # 擋下，會被當成合法上行訊息處理，把 account 改回 F1，蓋掉新連線剛登入的 F2。
            ws_old.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
            time.sleep(0.1)

            assert ws_env.state.order_service.account == "F2"     # 未被舊連線的訊息改回去
            assert ws_env.state.agent_channel.account == "F2"


def test_stale_connection_finally_does_not_disable_new_connection(ws_env):
    client = TestClient(ws_env)
    old_cm = client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token})
    ws_old = old_cm.__enter__()
    ws_old.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
    assert _wait(lambda: ws_env.state.order_service.account == "F1")

    new_cm = client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token})
    ws_new = new_cm.__enter__()
    try:
        ws_new.send_json({"type": "login", "account": "F2", "mode": "sim", "protocol": 2, "health_epoch": 0})
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
        with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws:
            ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
            ws.receive_json()
    assert "agent WS 處理上行訊息失敗" in caplog.text
    assert _wait(lambda: ws_env.state.order_session_state.disabled)


# ---- codex round2 fix2：server 端擋「未處理 RawInbox + 換帳號」視窗——agent 端的
# tripwire（buffer.assert_account）只擋得住「尚未送出」的列；server 已經 commit RawInbox
# 並 ack、worker 尚未處理的列不受保護。此時若換帳號登入，worker 之後映射 order_report 用
# 的是 mutable adapter.account（已被新帳號覆蓋），舊帳號的回報會被錯配。----

def test_login_account_switch_rejected_when_unprocessed_raw_inbox_pending(ws_env, engine):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws1:
        ws1.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
        assert _wait(lambda: ws_env.state.order_service.account == "F1")
        assert _wait(lambda: ws_env.state.order_session_state.ready)

    # server 已 commit 但 worker 尚未處理的一筆 RawInbox（processed=False, quarantine=False）。
    with Session(engine) as s:
        s.add(RawInbox(kind="deal_report", broker="shioaji", payload="{}"))
        s.commit()

    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws2:
        ws2.send_json({"type": "login", "account": "F2", "mode": "sim", "protocol": 2, "health_epoch": 0})
        time.sleep(0.2)   # 給 server 足夠時間處理（若未擋下，account 會被改成 F2）
        assert ws_env.state.order_service.account == "F1"          # 沒被換掉
        assert ws_env.state.order_session_state.ready is False     # 這次 login 未生效


def test_login_account_switch_allowed_when_no_unprocessed_raw_inbox(ws_env, engine):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws1:
        ws1.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
        assert _wait(lambda: ws_env.state.order_service.account == "F1")

    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws2:
        ws2.send_json({"type": "login", "account": "F2", "mode": "sim", "protocol": 2, "health_epoch": 0})
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
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws1:
        ws1.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
        assert _wait(lambda: ws_env.state.order_service.account == "F1")

    # 未處理列（processed=False, quarantine=False）殘留 → 觸發換帳號 guard。
    with Session(engine) as s:
        s.add(RawInbox(kind="deal_report", broker="shioaji", payload="{}"))
        s.commit()

    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws2:
        ws2.send_json({"type": "login", "account": "F2", "mode": "sim", "protocol": 2, "health_epoch": 0})
        with pytest.raises(WebSocketDisconnect):
            ws2.receive_json()   # 連線被關閉（1008）——不再是半開態

    assert ws_env.state.order_service.account == "F1"        # 帳號未被換掉
    assert ws_env.state.agent_channel.logged_in is False      # 未 mark_logged_in
    with Session(engine) as s:
        rows = s.exec(select(RawInbox)).all()
        # 繞過1：拒收後連線已關，不會再有機會讓 F2 的 report 被 commit——維持原本那 1 筆。
        assert len(rows) == 1


def test_login_account_switch_blocked_by_quarantined_rows(ws_env, engine):
    # codex round4（HIGH，唯一新發現）：count 查詢原本同時濾 processed==False 且
    # quarantine==False，漏放了「被隔離但仍未處理」的列——run_agent_watchdog 的
    # _retry_quarantined 之後會自動解除 quarantine 讓 worker 重新處理；若那時帳號已切到
    # F2，舊 F1 的 order_report 會用 F2 的 mutable adapter.account 映射，造成延遲跨帳號
    # 錯配。換帳號 guard 必須擋下「所有」processed==False 列，不論 quarantine 與否。
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws1:
        ws1.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
        assert _wait(lambda: ws_env.state.order_service.account == "F1")

    # 一筆已被隔離、但仍未處理的 RawInbox（quarantine=True）——之後 watchdog 的
    # _retry_quarantined 會解除隔離讓 worker 重新處理，此列在那之前仍算「未處理」。
    with Session(engine) as s:
        s.add(RawInbox(kind="deal_report", broker="shioaji", payload="{}", quarantine=True))
        s.commit()

    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws2:
        ws2.send_json({"type": "login", "account": "F2", "mode": "sim", "protocol": 2, "health_epoch": 0})
        with pytest.raises(WebSocketDisconnect):
            ws2.receive_json()   # 連線被關閉（1008）——quarantined 列也要擋下換帳號

    assert ws_env.state.order_service.account == "F1"          # 帳號未被換掉
    assert ws_env.state.agent_channel.logged_in is False        # 未 mark_logged_in


def test_report_before_login_not_staged_not_acked(ws_env, engine):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws:
        ws.send_json({"type": "report", "event_id": 99, "kind": "deal_report",
                     "account": "F1", "mode": "sim", "payload": {}})
        ws.send_json({"type": "health", "status": "ok", "health_epoch": 0})
        # 確認迴圈仍活著（report 被忽略不代表連線掛了）
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
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws_old:
        ws_old.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
        assert _wait(lambda: ws_env.state.order_service.account == "F1")

        ws_old.send_json({"type": "report", "event_id": 1, "kind": "deal_report",
                         "account": "F1", "mode": "sim", "payload": {}})
        assert entered.wait(timeout=2), "commit 應已進入（卡在 blocking commit 中）"

        with client.websocket_connect("/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}) as ws_new:
            ws_new.send_json({"type": "login", "account": "F2", "mode": "sim", "protocol": 2, "health_epoch": 0})

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


# ---- D2（Task 4）：per-user agent token 簽發/驗證＋WS 換發（S#11） ----
# 這一段直接對照設計文件 D2 的握手流程：`x-agent-token` → validate_token（sha256 查表，
# 過期/撤銷回 None）→ 載 User 驗 is_active → RiskGuard.is_owner → 得 user_id。任一步失敗
# close(1008)。issue_token/validate_token 本身的單元測試在 tests/test_agent_tokens.py；
# 這裡驗證的是「這條鏈在 WS 握手上真的被接起來」。

def test_issued_token_handshake_succeeds_and_updates_last_used_at(ws_env, engine):
    owner_id = ws_env.state.agent_test_owner_id
    with Session(engine) as s:
        before = s.exec(select(AgentToken).where(AgentToken.user_id == owner_id)).one()
        assert before.last_used_at is None

    client = TestClient(ws_env)
    with client.websocket_connect(
        "/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}
    ) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
        assert _wait(lambda: ws_env.state.order_session_state.ready)

    with Session(engine) as s:
        after = s.exec(select(AgentToken).where(AgentToken.user_id == owner_id)).one()
        assert after.last_used_at is not None


def test_expired_token_closed(ws_env, engine):
    owner_id = ws_env.state.agent_test_owner_id
    with Session(engine) as s:
        row = s.exec(select(AgentToken).where(AgentToken.user_id == owner_id)).one()
        row.expires_at = dt.datetime.utcnow() - timedelta(seconds=1)
        s.add(row)
        s.commit()

    client = TestClient(ws_env)
    with client.websocket_connect(
        "/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}
    ) as ws:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws.receive_json()
    assert exc_info.value.code == 1008


def test_revoked_token_closed(ws_env, engine):
    owner_id = ws_env.state.agent_test_owner_id
    with Session(engine) as s:
        row = s.exec(select(AgentToken).where(AgentToken.user_id == owner_id)).one()
        row.revoked_at = dt.datetime.utcnow()
        s.add(row)
        s.commit()

    client = TestClient(ws_env)
    with client.websocket_connect(
        "/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}
    ) as ws:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws.receive_json()
    assert exc_info.value.code == 1008


def test_rotation_invalidates_old_token_new_token_still_works(ws_env, engine):
    owner_id = ws_env.state.agent_test_owner_id
    old_token = ws_env.state.agent_test_token
    with Session(engine) as s:
        new_token = issue_token(s, user_id=owner_id, ttl_days=30)
    assert new_token != old_token

    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": old_token}) as ws_old:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws_old.receive_json()
    assert exc_info.value.code == 1008

    with client.websocket_connect("/ws/agent", headers={"x-agent-token": new_token}) as ws_new:
        ws_new.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
        assert _wait(lambda: ws_env.state.order_session_state.ready)


def test_same_user_only_one_valid_token_after_rotation(ws_env, engine):
    # 與 test_agent_tokens.py 的 DB 層測試互補：這裡從 WS 握手的角度直接證明「同一時刻
    # 只有最新那一枚能連得上」——rotation 前後各連一次，只有最後簽發的那枚成功。
    owner_id = ws_env.state.agent_test_owner_id
    with Session(engine) as s:
        raw2 = issue_token(s, user_id=owner_id, ttl_days=30)
    with Session(engine) as s:
        raw3 = issue_token(s, user_id=owner_id, ttl_days=30)

    client = TestClient(ws_env)
    for stale in (ws_env.state.agent_test_token, raw2):
        with client.websocket_connect("/ws/agent", headers={"x-agent-token": stale}) as ws:
            with pytest.raises(WebSocketDisconnect) as exc_info:
                ws.receive_json()
        assert exc_info.value.code == 1008

    with client.websocket_connect("/ws/agent", headers={"x-agent-token": raw3}) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
        assert _wait(lambda: ws_env.state.order_session_state.ready)


def test_non_owner_valid_token_rejected(ws_env, engine):
    # 有效、未過期、未撤銷的 token，但持有者不在 owner 白名單——D2 握手鐵律：任一步失敗
    # （含非 owner）一律 close(1008)，不得放行。
    with Session(engine) as s:
        non_owner = auth_service.create_user(s, "not-owner", "pw", role="user")
        raw = issue_token(s, user_id=non_owner.id, ttl_days=30)

    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": raw}) as ws:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws.receive_json()
    assert exc_info.value.code == 1008


def test_inactive_owner_valid_token_rejected(ws_env, engine):
    # is_active=False（帳號被停用）：即使 token 本身仍有效、使用者仍在 owner 白名單，
    # 也必須拒絕——帳號停用要立即生效，不能靠 token 過期慢慢收斂。
    owner_id = ws_env.state.agent_test_owner_id
    with Session(engine) as s:
        from quanquant.db.models import User
        user_row = s.get(User, owner_id)
        user_row.is_active = False
        s.add(user_row)
        s.commit()

    client = TestClient(ws_env)
    with client.websocket_connect(
        "/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}
    ) as ws:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws.receive_json()
    assert exc_info.value.code == 1008
