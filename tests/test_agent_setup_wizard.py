import logging

import pytest


# ---------------------------------------------------------------------------
# brief 逐字案例（Step 5／5b）
# ---------------------------------------------------------------------------

async def test_step2_validation_error_does_not_echo_credentials(gui_client):
    resp = await gui_client.post("/setup/step2", data={"api_key": "", "secret_key": "SECRET123"})
    assert resp.status_code == 422
    assert "SECRET123" not in resp.text


async def test_setup_responses_are_no_store(gui_client):
    resp = await gui_client.get("/setup")
    assert resp.headers.get("cache-control") == "no-store"


async def test_wizard_flow_logs_do_not_leak_secrets(gui_client, caplog):
    """spec §5.1『掃 access/application log 不含三項秘密』——application log 這一半，
    跑一輪成功＋錯誤路徑的 step2 表單提交，斷言 caplog 全部 record 都不含任何一項秘密
    明文（token 這項用一個假明文樣本模擬，因為本 task 尚未實際持有真 token——Task 10/11
    另外各自針對自己新增的路徑補齊，這裡只保證本 task 引入的程式碼不洩漏）。"""
    caplog.set_level(logging.DEBUG)
    secret_api_key = "SAPI-SUPER-SECRET-KEY-0001"
    secret_secret_key = "SSEC-SUPER-SECRET-VALUE-0002"
    secret_token_sample = "TOKEN-SUPER-SECRET-VALUE-0003"

    await gui_client.post("/setup/step2", data={"api_key": secret_api_key, "secret_key": secret_secret_key})
    await gui_client.post("/setup/step2", data={"api_key": "", "secret_key": secret_secret_key})  # 錯誤路徑（422）

    for record in caplog.records:
        message = record.getMessage()
        assert secret_api_key not in message
        assert secret_secret_key not in message
        assert secret_token_sample not in message


# ---------------------------------------------------------------------------
# 補充：require_gui_session 守門（本 task 新增路由的既有鐵律回歸）
# ---------------------------------------------------------------------------

async def test_setup_rejects_missing_session_cookie():
    import httpx

    from quanquant.agent.gui.coordinator import build_app
    from quanquant.agent.gui.security import GuiSecurityState

    state = GuiSecurityState(port=54322)
    app = build_app(state)
    app.state.site_origin = "https://quant.example"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:54322",
        headers={"Host": "127.0.0.1:54322"},
    ) as client:
        resp = await client.get("/setup")
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# 補充：步驟①（device flow）渲染與單一 in-flight／冪等啟動
# ---------------------------------------------------------------------------

async def test_setup_step1_shows_user_code_once_device_flow_initiates(gui_client):
    resp = await gui_client.get("/setup")
    assert resp.status_code == 200
    # _ensure_device_flow_started 已排入背景 task；讓 event loop 跑一輪把 initiate() 執行完。
    task = gui_client.app.state.device_flow_task
    for _ in range(50):
        client = gui_client.app.state.device_flow_client
        if client is not None and client.user_code is not None:
            break
        import asyncio
        await asyncio.sleep(0)
    assert gui_client.app.state.device_flow_client.user_code == "TEST-CODE"
    assert task is gui_client.app.state.device_flow_task  # 仍是同一顆 task，未重開


async def test_setup_get_does_not_start_a_second_device_flow_task_on_repeat_visits(gui_client):
    await gui_client.get("/setup")
    first_task = gui_client.app.state.device_flow_task
    await gui_client.get("/setup")
    second_task = gui_client.app.state.device_flow_task
    assert first_task is second_task  # 單一 in-flight：重複造訪不重開


async def test_poll_status_redirects_to_setup_once_approved(gui_client):
    gui_client.app.state.device_flow_result = {
        "token": "tok", "profile_id": "1", "username": "tester",
        "token_expires_at": "2099-01-01T00:00:00",
    }
    resp = await gui_client.get("/setup/poll-status", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/setup"


async def test_setup_shows_step2_once_device_flow_approved(gui_client):
    gui_client.app.state.device_flow_result = {
        "token": "tok", "profile_id": "1", "username": "tester",
        "token_expires_at": "2099-01-01T00:00:00",
    }
    resp = await gui_client.get("/setup")
    assert resp.status_code == 200
    assert "永豐" in resp.text


async def test_setup_step1_start_restarts_after_error(gui_client):
    from quanquant.agent.device_flow_client import DeviceFlowExpiredError

    # 只需要一個非 None 的 sentinel（_ensure_device_flow_started 只檢查是否為 None，
    # 不會真的 await 這顆舊 task）：模擬「上一輪已經跑完並以錯誤終結」的狀態。
    gui_client.app.state.device_flow_task = object()
    gui_client.app.state.device_flow_error = DeviceFlowExpiredError.__name__

    resp = await gui_client.post("/setup/step1/start")
    assert resp.status_code == 200
    assert gui_client.app.state.device_flow_error is None  # 已清掉，重新開始一輪


# ---------------------------------------------------------------------------
# 補充：步驟②（永豐憑證）成功路徑
# ---------------------------------------------------------------------------

async def test_step2_success_stores_credentials_in_memory_and_redirects(gui_client):
    resp = await gui_client.post(
        "/setup/step2",
        data={"api_key": "K1", "secret_key": "S1", "remember_broker": "on"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/setup"
    assert gui_client.app.state.broker_credentials == {"api_key": "K1", "secret_key": "S1"}
    assert gui_client.app.state.remember_broker_credentials is True
    assert gui_client.app.state.remember_device_auth is False


# ---------------------------------------------------------------------------
# 補充：步驟③（確認啟動）
# ---------------------------------------------------------------------------

async def test_step3_launch_rejects_when_prerequisites_missing(gui_client):
    resp = await gui_client.post("/setup/step3/launch")
    assert resp.status_code == 409


async def test_step3_launch_builds_runner_and_attaches_it(gui_client, monkeypatch):
    gui_client.app.state.device_flow_result = {
        "token": "tok", "profile_id": "1", "username": "tester",
        "token_expires_at": "2099-01-01T00:00:00",
    }
    gui_client.app.state.broker_credentials = {"api_key": "K1", "secret_key": "S1"}

    calls = {}

    class _FakeChildHandle:
        def __init__(self, **kwargs):
            calls["child_kwargs"] = kwargs

    class _FakeTransport:
        def __init__(self, url, *, token):
            calls["ws_url"] = url
            calls["token"] = token

    class _FakeRunner:
        def __init__(self, **kwargs):
            calls["runner_kwargs"] = kwargs

    class _FakeBuffer:
        def __init__(self, path):
            calls["buffer_path"] = path

    def _fake_attach_runner(app, runner):
        calls["attached_runner"] = runner
        calls["attached_app"] = app

    import quanquant.agent.buffer as buffer_module
    import quanquant.agent.gui.coordinator as coordinator_module
    import quanquant.agent.runner as runner_module
    import quanquant.agent.ws_client as ws_client_module

    monkeypatch.setattr(buffer_module, "DurableBuffer", _FakeBuffer)
    monkeypatch.setattr(runner_module, "AgentRunner", _FakeRunner)
    monkeypatch.setattr(runner_module, "ChildHandle", _FakeChildHandle)
    monkeypatch.setattr(ws_client_module, "WebsocketsTransport", _FakeTransport)
    monkeypatch.setattr(coordinator_module, "attach_runner", _fake_attach_runner)

    resp = await gui_client.post("/setup/step3/launch", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/status"
    assert calls["ws_url"] == "wss://quant.example/ws/agent"
    assert calls["token"] == "tok"
    assert calls["child_kwargs"]["credentials"] == {"api_key": "K1", "secret_key": "S1"}
    assert isinstance(calls["attached_runner"], _FakeRunner)


@pytest.mark.parametrize("site_origin,expected", [
    ("https://quant.example", "wss://quant.example/ws/agent"),
    ("http://127.0.0.1:8000", "ws://127.0.0.1:8000/ws/agent"),
])
def test_derive_ws_url(site_origin, expected):
    from quanquant.agent.gui.setup_routes import _derive_ws_url

    assert _derive_ws_url(site_origin) == expected


def test_derive_ws_url_rejects_unknown_scheme():
    from quanquant.agent.gui.setup_routes import _derive_ws_url

    with pytest.raises(ValueError):
        _derive_ws_url("ftp://quant.example")
