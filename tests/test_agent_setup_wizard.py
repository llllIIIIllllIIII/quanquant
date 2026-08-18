import asyncio
import logging

import pytest

# ---------------------------------------------------------------------------
# Task 13：本檔所有測試共用的隔離 fixture——沿用 tests/test_agent_profile_registry.py
# 的 _isolated_home 手法，autouse 保護整個檔案：Task 13 起，`/setup/step3/launch` 會真的
# 呼叫 profile_registry.upsert_profile()／check_legacy_buffer_conflict()，沒有這層隔離
# 會誤寫真正的 ~/.quanquant-agent/。
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolated_home(tmp_path, monkeypatch):
    from quanquant.agent import profile_registry
    monkeypatch.setattr(profile_registry, "REGISTRY_PATH", tmp_path / "profiles.json")
    monkeypatch.setattr(profile_registry, "LOCK_PATH", tmp_path / "profiles.lock")
    import quanquant.agent.gui.startup_flow as sf
    monkeypatch.setattr(sf, "LEGACY_DEFAULT_BUFFER", tmp_path / "legacy_outbox.db")
    return tmp_path


# ---------------------------------------------------------------------------
# Step 5b 秘密掃描共用工具（reviewer Important fix a：不只查 record.getMessage()，
# 也要查 logging.Formatter().format(record)——後者才含 exc_info 的完整 traceback
# 文字；log.exception(...) 附帶的 traceback 最後一行固定是
# f"{type(exc).__name__}: {exc}"，只看 getMessage() 完全看不到這段，必須看格式化後的
# 完整輸出才驗得到。）
# ---------------------------------------------------------------------------

_SECRET_API_KEY = "SAPI-SUPER-SECRET-KEY-0001"
_SECRET_SECRET_KEY = "SSEC-SUPER-SECRET-VALUE-0002"
_SECRET_TOKEN_SAMPLE = "TOKEN-SUPER-SECRET-VALUE-0003"


def _assert_no_secret_leak(records) -> None:
    formatter = logging.Formatter()
    for record in records:
        message = record.getMessage()
        formatted = formatter.format(record)  # 含 exc_info 的完整 traceback 文字
        for secret in (_SECRET_API_KEY, _SECRET_SECRET_KEY, _SECRET_TOKEN_SAMPLE):
            assert secret not in message
            assert secret not in formatted


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
    跑一輪成功＋錯誤路徑的 step2 表單提交，斷言 caplog 全部 record（getMessage()＋完整
    格式化文字，見 `_assert_no_secret_leak`）都不含任何一項秘密明文（token 這項用一個假
    明文樣本模擬，因為本 task 尚未實際持有真 token——Task 10/11 另外各自針對自己新增的
    路徑補齊，這裡只保證本 task 引入的程式碼不洩漏）。"""
    caplog.set_level(logging.DEBUG)

    await gui_client.post("/setup/step2", data={"api_key": _SECRET_API_KEY, "secret_key": _SECRET_SECRET_KEY})
    await gui_client.post("/setup/step2", data={"api_key": "", "secret_key": _SECRET_SECRET_KEY})  # 錯誤路徑（422）

    _assert_no_secret_leak(caplog.records)


async def test_device_flow_background_task_exception_log_does_not_leak_secrets(gui_client, caplog, monkeypatch):
    """reviewer Important fix (b)：先前 Step 5b 掃描從未真正驅動 `log.exception`
    （application log 唯一會帶 exc_info/完整 traceback 的路徑，只掃 `record.getMessage()`
    看不到這段文字，等於這條防線從未被驗證過）。這裡用 monkeypatch 讓
    `DeviceFlowClient.initiate()` 拋一般例外，觸發
    `_ensure_device_flow_started()` 背景 task 的
    `except Exception: ... log.exception(...)` 分支：①先斷言這個分支真的被驅動
    （存在 `exc_info is not None` 的 record，否則後面的『沒查到秘密』毫無意義——可能只是
    根本沒東西可查）②同一輪也送出真的秘密（重用 step2 表單）驗證完整格式化輸出（含
    traceback）仍不含任何一項秘密。"""
    from quanquant.agent.device_flow_client import DeviceFlowClient

    caplog.set_level(logging.DEBUG)

    async def _boom(self) -> dict:
        raise RuntimeError("device-code 端點連線逾時（模擬，不含任何秘密）")

    monkeypatch.setattr(DeviceFlowClient, "initiate", _boom)

    await gui_client.post("/setup/step2", data={"api_key": _SECRET_API_KEY, "secret_key": _SECRET_SECRET_KEY})
    resp = await gui_client.post("/setup/step1/start")
    assert resp.status_code == 200

    task = gui_client.app.state.device_flow_task
    for _ in range(200):
        if task.done():
            break
        await asyncio.sleep(0)
    assert task.done()
    assert gui_client.app.state.device_flow_error == "UnexpectedError"

    exception_records = [r for r in caplog.records if r.exc_info is not None]
    assert exception_records, "log.exception 分支必須至少被驅動一次，否則本測試沒有意義"

    _assert_no_secret_leak(caplog.records)


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
# 補充：步驟①（device flow）渲染與單一 in-flight／mutation 只走 POST
# （reviewer Important fix：GET /setup 先前會外呼 device-code 配發，違反
# 「mutation 全 POST」——現在 GET 只渲染，POST /setup/step1/start 才是唯一啟動點。）
# ---------------------------------------------------------------------------

async def test_setup_get_never_triggers_device_flow_mutation(gui_client):
    """GET /setup（含重複造訪）純渲染，不得觸發任何背景 device flow task——
    `app.state.device_flow_task` 應該全程維持 None，直到使用者明確 POST
    /setup/step1/start。"""
    resp = await gui_client.get("/setup")
    assert resp.status_code == 200
    assert "開始授權" in resp.text
    assert getattr(gui_client.app.state, "device_flow_task", None) is None

    await gui_client.get("/setup")  # 重複造訪一樣不觸發
    assert getattr(gui_client.app.state, "device_flow_task", None) is None


async def test_setup_step1_start_is_the_only_mutation_trigger_and_shows_user_code(gui_client):
    resp = await gui_client.post("/setup/step1/start")
    assert resp.status_code == 200
    task = gui_client.app.state.device_flow_task
    assert task is not None
    for _ in range(50):
        client = gui_client.app.state.device_flow_client
        if client is not None and client.user_code is not None:
            break
        await asyncio.sleep(0)
    assert gui_client.app.state.device_flow_client.user_code == "TEST-CODE"
    # GET /setup 造訪不會重開背景 task（單一 in-flight，冪等維持同一顆 task）。
    await gui_client.get("/setup")
    assert task is gui_client.app.state.device_flow_task


async def test_step1_start_repeated_posts_do_not_start_a_second_task(gui_client):
    resp1 = await gui_client.post("/setup/step1/start")
    first_task = gui_client.app.state.device_flow_task
    resp2 = await gui_client.post("/setup/step1/start")
    second_task = gui_client.app.state.device_flow_task
    assert resp1.status_code == 200 and resp2.status_code == 200
    assert first_task is second_task  # 單一 in-flight：重複 POST 不重開


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


async def test_setup_shows_friendly_message_for_gave_up_error(gui_client):
    """reviewer Important fix 2：DeviceFlowGaveUpError 對應到指定文案。"""
    from quanquant.agent.device_flow_client import DeviceFlowGaveUpError

    gui_client.app.state.device_flow_client = None
    gui_client.app.state.device_flow_error = DeviceFlowGaveUpError.__name__

    resp = await gui_client.get("/setup")
    assert resp.status_code == 200
    assert "授權多次交付失敗" in resp.text


# ---------------------------------------------------------------------------
# 補充：步驟①背景 JS 輪詢（UX 改善——同源 JS 取代 meta-refresh 整頁刷新）
# ---------------------------------------------------------------------------

async def test_poll_status_json_reports_waiting_when_not_yet_approved(gui_client):
    """尚未核准、也還沒進入錯誤終態 → "waiting"，不含 next/message。"""
    resp = await gui_client.get("/setup/poll-status.json")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/json")
    data = resp.json()
    assert data == {"state": "waiting", "next": None, "message": None}


async def test_poll_status_json_reports_ready_once_approved(gui_client):
    """已核准（`device_flow_result` 非 None）→ "ready"，`next` 固定 "/setup"（由
    `setup_page` 依 app.state 自動渲染下一步，JSON 端點本身不重複判斷邏輯）。"""
    gui_client.app.state.device_flow_result = {
        "token": "tok", "profile_id": "1", "username": "tester",
        "token_expires_at": "2099-01-01T00:00:00",
    }
    resp = await gui_client.get("/setup/poll-status.json")
    assert resp.status_code == 200
    data = resp.json()
    assert data["state"] == "ready"
    assert data["next"] == "/setup"


async def test_poll_status_json_reports_error_and_reuses_existing_gave_up_message(gui_client):
    """error 狀態沿用既有 device flow 狀態（`DeviceFlowGaveUpError`）與既有
    `_ERROR_MESSAGES` 文案，不自創新語意——與 `GET /setup` 步驟①錯誤頁同一份文案。"""
    from quanquant.agent.device_flow_client import DeviceFlowGaveUpError

    gui_client.app.state.device_flow_client = None
    gui_client.app.state.device_flow_error = DeviceFlowGaveUpError.__name__

    resp = await gui_client.get("/setup/poll-status.json")
    assert resp.status_code == 200
    data = resp.json()
    assert data["state"] == "error"
    assert data["next"] is None
    assert data["message"] == "授權多次交付失敗，請按「開始授權」重試或檢查伺服器狀態。"


async def test_poll_status_json_does_not_trigger_device_flow_mutation(gui_client):
    """比照 `GET /setup`／`GET /setup/poll-status` 的既有鐵律：純渲染，不觸發
    `_ensure_device_flow_started()`。"""
    resp = await gui_client.get("/setup/poll-status.json")
    assert resp.status_code == 200
    assert getattr(gui_client.app.state, "device_flow_task", None) is None


async def test_poll_status_json_requires_gui_session(gui_anon_client):
    resp = await gui_anon_client.get("/setup/poll-status.json")
    assert resp.status_code == 403


async def test_step1_poll_js_is_served_same_origin_as_javascript(gui_client):
    resp = await gui_client.get("/setup/step1-poll.js")
    assert resp.status_code == 200
    assert "javascript" in resp.headers["content-type"]
    assert "poll-status.json" in resp.text
    assert "window.location" in resp.text


async def test_step1_poll_js_requires_gui_session(gui_anon_client):
    resp = await gui_anon_client.get("/setup/step1-poll.js")
    assert resp.status_code == 403


async def test_setup_step1_started_page_uses_background_js_poll_not_meta_refresh(gui_client):
    """核心 UX 斷言：`started=True` 分支（有 user_code）不再靠整頁 `<meta http-equiv=
    "refresh">` 刷新，改引入同源 `step1-poll.js`＋一個可被 JS 更新文字的狀態元素；核准
    連結仍是 `target="_blank"`（開新分頁，不受本次改動影響）。"""
    resp = await gui_client.post("/setup/step1/start")
    assert resp.status_code == 200
    for _ in range(50):
        client = gui_client.app.state.device_flow_client
        if client is not None and client.user_code is not None:
            break
        await asyncio.sleep(0)

    resp = await gui_client.get("/setup")
    assert resp.status_code == 200
    assert 'http-equiv="refresh"' not in resp.text
    assert '<script src="/setup/step1-poll.js"></script>' in resp.text
    assert 'id="qq-poll-status"' in resp.text
    assert 'target="_blank"' in resp.text


async def test_setup_step1_connecting_page_also_uses_background_js_poll(gui_client, monkeypatch):
    """`started=True` 但尚未拿到 user_code（「正在與伺服器建立連線……」子分支）同樣要
    輪詢，比照原本 meta-refresh 的涵蓋範圍（`not error and started`）。"""
    import asyncio as _asyncio

    from quanquant.agent.device_flow_client import DeviceFlowClient

    async def _never_returns(self) -> dict:
        await _asyncio.sleep(3600)
        raise AssertionError("不應該真的等到")

    monkeypatch.setattr(DeviceFlowClient, "initiate", _never_returns)

    resp = await gui_client.post("/setup/step1/start")
    assert resp.status_code == 200
    assert gui_client.app.state.device_flow_client.user_code is None

    resp = await gui_client.get("/setup")
    assert resp.status_code == 200
    assert 'http-equiv="refresh"' not in resp.text
    assert '<script src="/setup/step1-poll.js"></script>' in resp.text
    assert 'id="qq-poll-status"' in resp.text


async def test_setup_step1_error_page_has_no_poll_script(gui_client):
    """error 分支已是終態，不該再輪詢（不引入 step1-poll.js）——與原本 meta-refresh 只在
    `not error and started` 時出現的行為一致。"""
    from quanquant.agent.device_flow_client import DeviceFlowGaveUpError

    gui_client.app.state.device_flow_client = None
    gui_client.app.state.device_flow_error = DeviceFlowGaveUpError.__name__

    resp = await gui_client.get("/setup")
    assert resp.status_code == 200
    assert "step1-poll.js" not in resp.text


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


async def test_step3_launch_builds_runner_and_attaches_it(gui_client, monkeypatch, tmp_path):
    from quanquant.agent.profile_registry import ProfileEntry

    gui_client.app.state.device_flow_result = {
        "token": "tok", "profile_id": "1", "username": "tester",
        "token_expires_at": "2099-01-01T00:00:00",
    }
    gui_client.app.state.broker_credentials = {"api_key": "K1", "secret_key": "S1"}
    # Task 13：step3/launch 唯一讀 app_state.gui_current_profile 取得 profile——正常流程
    # 由 _finalize_approved_profile（核准當下）寫入，這裡直接注入等價狀態，測試不必真的
    # 跑一輪 device flow 背景 task。
    gui_client.app.state.gui_current_profile = ProfileEntry(
        profile_id="1", username="tester", buffer_path=str(tmp_path / "outbox.db"),
        created_at="2020-01-01T00:00:00",
    )

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


async def test_step3_launch_failure_shows_themed_error_page_not_raw_500(gui_client, monkeypatch, tmp_path):
    """reviewer Minor fix：runner 建構/掛載失敗時走 `_render_step3(launch_error=...)`
    主題化錯誤頁，而非未接住裸例外。"""
    from quanquant.agent.profile_registry import ProfileEntry

    gui_client.app.state.device_flow_result = {
        "token": "tok", "profile_id": "1", "username": "tester",
        "token_expires_at": "2099-01-01T00:00:00",
    }
    gui_client.app.state.broker_credentials = {"api_key": "K1", "secret_key": "S1"}
    gui_client.app.state.gui_current_profile = ProfileEntry(
        profile_id="1", username="tester", buffer_path=str(tmp_path / "outbox.db"),
        created_at="2020-01-01T00:00:00",
    )

    import quanquant.agent.runner as runner_module

    def _boom(**kwargs):
        raise RuntimeError("construction failed")

    monkeypatch.setattr(runner_module, "AgentRunner", _boom)

    resp = await gui_client.post("/setup/step3/launch", follow_redirects=False)
    assert resp.status_code == 500
    assert "啟動失敗" in resp.text
    assert "construction failed" not in resp.text  # 例外原始訊息不外洩


# ---------------------------------------------------------------------------
# Task 14 補入項（controller 依 spec §5.3）：InstanceLock 接線——同 profile 禁止第二個
# agent 程序。用一個「搶先持有同一個 buffer_path InstanceLock」的外部 lock 物件模擬
# 已有另一個 agent 程序在跑。
# ---------------------------------------------------------------------------

async def test_step3_launch_blocked_when_instance_lock_already_held(gui_client, tmp_path):
    from quanquant.agent import profile_registry
    from quanquant.agent.profile_registry import ProfileEntry

    buffer_path = tmp_path / "outbox.db"
    gui_client.app.state.device_flow_result = {
        "token": "tok", "profile_id": "1", "username": "tester",
        "token_expires_at": "2099-01-01T00:00:00",
    }
    gui_client.app.state.broker_credentials = {"api_key": "K1", "secret_key": "S1"}
    gui_client.app.state.gui_current_profile = ProfileEntry(
        profile_id="1", username="tester", buffer_path=str(buffer_path),
        created_at="2020-01-01T00:00:00",
    )

    external_lock = profile_registry.InstanceLock(buffer_path)
    external_lock.acquire()
    try:
        resp = await gui_client.post("/setup/step3/launch", follow_redirects=False)
        assert resp.status_code == 409
        assert "此帳號的 Agent 已在執行中" in resp.text
        assert gui_client.app.state.agent_runner is None   # 沒有建構出任何 runner
    finally:
        external_lock.release()


async def test_step3_launch_succeeds_after_conflicting_lock_released(gui_client, tmp_path, monkeypatch):
    """release 後可重取：外部 lock 釋放後，下一次 launch 正常成功並掛上 runner、且把
    自己的 InstanceLock 存進 app.state.instance_lock。"""
    from quanquant.agent import profile_registry
    from quanquant.agent.profile_registry import ProfileEntry

    buffer_path = tmp_path / "outbox.db"
    gui_client.app.state.device_flow_result = {
        "token": "tok", "profile_id": "1", "username": "tester",
        "token_expires_at": "2099-01-01T00:00:00",
    }
    gui_client.app.state.broker_credentials = {"api_key": "K1", "secret_key": "S1"}
    gui_client.app.state.gui_current_profile = ProfileEntry(
        profile_id="1", username="tester", buffer_path=str(buffer_path),
        created_at="2020-01-01T00:00:00",
    )

    external_lock = profile_registry.InstanceLock(buffer_path)
    external_lock.acquire()
    resp_blocked = await gui_client.post("/setup/step3/launch", follow_redirects=False)
    assert resp_blocked.status_code == 409
    external_lock.release()

    class _FakeChildHandle:
        def __init__(self, **kwargs):
            pass

    class _FakeTransport:
        def __init__(self, url, *, token):
            pass

    class _FakeRunner:
        def __init__(self, **kwargs):
            pass

    class _FakeBuffer:
        def __init__(self, path):
            pass

    def _fake_attach_runner(app, runner):
        app.state.agent_runner = runner
        app.state.runner_task = None

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
    assert isinstance(gui_client.app.state.instance_lock, profile_registry.InstanceLock)


async def test_step3_launch_releases_lock_when_runner_construction_fails(gui_client, tmp_path, monkeypatch):
    """建構失敗（既有 reviewer Minor fix 走的錯誤頁）不得洩漏 lock——否則使用者照錯誤頁
    指示重試會被自己先前失敗的那次卡死。"""
    from quanquant.agent import profile_registry
    from quanquant.agent.profile_registry import ProfileEntry

    buffer_path = tmp_path / "outbox.db"
    gui_client.app.state.device_flow_result = {
        "token": "tok", "profile_id": "1", "username": "tester",
        "token_expires_at": "2099-01-01T00:00:00",
    }
    gui_client.app.state.broker_credentials = {"api_key": "K1", "secret_key": "S1"}
    gui_client.app.state.gui_current_profile = ProfileEntry(
        profile_id="1", username="tester", buffer_path=str(buffer_path),
        created_at="2020-01-01T00:00:00",
    )

    import quanquant.agent.runner as runner_module

    def _boom(**kwargs):
        raise RuntimeError("construction failed")

    monkeypatch.setattr(runner_module, "AgentRunner", _boom)

    resp = await gui_client.post("/setup/step3/launch", follow_redirects=False)
    assert resp.status_code == 500

    # lock 已釋放：外部現在可以拿到
    probe_lock = profile_registry.InstanceLock(buffer_path)
    probe_lock.acquire()
    probe_lock.release()


# ---------------------------------------------------------------------------
# Task 13 Step 3b：核准後的 profile 收斂點（_finalize_approved_profile）
# ---------------------------------------------------------------------------

def test_finalize_approved_profile_reuses_existing_when_id_matches_registry(_isolated_home):
    from types import SimpleNamespace

    from quanquant.agent import profile_registry
    from quanquant.agent.gui import setup_routes as sr

    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="7",
                                     username="dave", buffer_path="/x7")
    app_state = SimpleNamespace()
    result = sr._finalize_approved_profile(
        app_state, site_origin="https://q.example", expected_profile=None,
        approved={"profile_id": "7", "username": "dave", "token": "t",
                  "token_expires_at": "2099-01-01T00:00:00"},
    )
    assert result.buffer_path == "/x7"
    assert app_state.gui_current_profile is result


def test_finalize_approved_profile_creates_isolated_new_profile_when_mismatched(_isolated_home):
    from types import SimpleNamespace

    from quanquant.agent.gui import setup_routes as sr
    from quanquant.agent.profile_registry import ProfileEntry

    expected = ProfileEntry(profile_id="1", username="old", buffer_path="/x1", created_at="2020-01-01T00:00:00")
    app_state = SimpleNamespace()
    result = sr._finalize_approved_profile(
        app_state, site_origin="https://q.example", expected_profile=expected,
        approved={"profile_id": "2", "username": "new", "token": "t",
                  "token_expires_at": "2099-01-01T00:00:00"},
    )
    assert result.profile_id == "2" and result.buffer_path != expected.buffer_path


# ---------------------------------------------------------------------------
# Task 13 Step 3c：舊 buffer 防呆只擋『這台機器第一次建立這個 profile』的 launch
# ---------------------------------------------------------------------------

async def test_launch_blocked_when_legacy_buffer_has_unsent_rows_and_profile_is_new(gui_client, tmp_path):
    from quanquant.agent.buffer import DurableBuffer
    from quanquant.agent.gui.setup_routes import _finalize_approved_profile

    legacy = tmp_path / "legacy_outbox.db"  # _isolated_home（autouse）已把 LEGACY_DEFAULT_BUFFER 指到這裡
    DurableBuffer(str(legacy)).append("order_report", {"x": 1})

    approved = {"token": "tok", "profile_id": gui_client.profile_id, "username": "tester",
                "token_expires_at": "2099-01-01T00:00:00"}
    gui_client.app.state.device_flow_result = approved
    gui_client.app.state.broker_credentials = {"api_key": "K1", "secret_key": "S1"}
    _finalize_approved_profile(gui_client.app.state, site_origin=gui_client.site_origin,
                                expected_profile=None, approved=approved)

    resp = await gui_client.post("/setup/step3/launch")
    assert resp.status_code == 409
    assert "不會自動搬移" in resp.text


async def test_launch_not_blocked_when_profile_already_exists_in_registry(gui_client, tmp_path, monkeypatch):
    """既有 profile（reset 或補問憑證流程）不是『首次建立』，即使舊 buffer 有 pending 也
    不擋——這條規則只保護真正的新使用者。"""
    from quanquant.agent import profile_registry
    from quanquant.agent.buffer import DurableBuffer
    from quanquant.agent.gui.setup_routes import _finalize_approved_profile

    legacy = tmp_path / "legacy_outbox.db"
    DurableBuffer(str(legacy)).append("order_report", {"x": 1})
    profile_registry.upsert_profile(site_origin=gui_client.site_origin, profile_id=gui_client.profile_id,
                                     username="tester", buffer_path=str(tmp_path / "existing_outbox.db"))

    approved = {"token": "tok", "profile_id": gui_client.profile_id, "username": "tester",
                "token_expires_at": "2099-01-01T00:00:00"}
    gui_client.app.state.device_flow_result = approved
    gui_client.app.state.broker_credentials = {"api_key": "K1", "secret_key": "S1"}
    _finalize_approved_profile(gui_client.app.state, site_origin=gui_client.site_origin,
                                expected_profile=None, approved=approved)

    import quanquant.agent.gui.coordinator as coordinator_module
    monkeypatch.setattr(coordinator_module, "attach_runner", lambda app, runner: None)

    resp = await gui_client.post("/setup/step3/launch")
    assert resp.status_code != 409


# ---------------------------------------------------------------------------
# Task 13 Step 6：/profiles 選擇頁
# ---------------------------------------------------------------------------

async def test_profiles_page_lists_usernames_and_add_account_link(gui_client):
    from quanquant.agent import profile_registry
    profile_registry.upsert_profile(site_origin=gui_client.site_origin, profile_id="1",
                                     username="alice", buffer_path="/x1")
    profile_registry.upsert_profile(site_origin=gui_client.site_origin, profile_id="2",
                                     username="bob", buffer_path="/x2")

    resp = await gui_client.get("/profiles")

    assert resp.status_code == 200
    assert "alice" in resp.text and "bob" in resp.text
    assert "新增帳號" in resp.text and "/setup" in resp.text


async def test_profiles_page_requires_gui_session(gui_anon_client):
    resp = await gui_anon_client.get("/profiles")
    assert resp.status_code == 403


async def test_select_profile_posts_profile_id_and_redirects_toward_resolved_decision(gui_client):
    from quanquant.agent import profile_registry
    profile_registry.upsert_profile(site_origin=gui_client.site_origin, profile_id="1",
                                     username="alice", buffer_path="/x1")

    resp = await gui_client.post("/profiles/select", data={"profile_id": "1"}, follow_redirects=False)

    assert resp.status_code in (302, 303)
    assert resp.headers["location"] == "/setup"  # 沒有 keyring 憑證 → decision.kind="setup"


async def test_select_profile_direct_branch_reuses_coordinator_launch_direct_and_redirects_to_status(
    gui_client, tmp_path, monkeypatch,
):
    """decision.kind="direct"（keyring 憑證齊備）時，/profiles/select 呼叫
    coordinator.launch_direct（與 run_gui() 的 direct 分支共用同一段實作），連線成功導向
    /status。"""
    from quanquant.agent import keyring_store, profile_registry

    profile_registry.upsert_profile(site_origin=gui_client.site_origin, profile_id="1",
                                     username="alice", buffer_path=str(tmp_path / "outbox.db"))
    monkeypatch.setattr(keyring_store, "load_token", lambda *, site_origin, profile_id: {
        "token": "tok", "expires_at": "2099-01-01T00:00:00", "username": "alice",
    })
    monkeypatch.setattr(keyring_store, "load_broker_credentials", lambda *, site_origin, profile_id: {
        "api_key": "K1", "secret_key": "S1",
    })

    class _FakeSnapshot:
        connection = "connected"

    class _FakeRunner:
        def __init__(self, **kwargs): ...
        async def run_forever(self, *, stop_on_token_reject=False):
            await asyncio.Event().wait()
        async def snapshot(self):
            return _FakeSnapshot()

    import quanquant.agent.buffer as buffer_module
    import quanquant.agent.runner as runner_module
    import quanquant.agent.ws_client as ws_client_module
    monkeypatch.setattr(buffer_module, "DurableBuffer", lambda path: object())
    monkeypatch.setattr(runner_module, "AgentRunner", lambda **kwargs: _FakeRunner())
    monkeypatch.setattr(runner_module, "ChildHandle", lambda **kwargs: object())
    monkeypatch.setattr(ws_client_module, "WebsocketsTransport", lambda url, *, token: object())

    resp = await gui_client.post("/profiles/select", data={"profile_id": "1"}, follow_redirects=False)

    assert resp.status_code in (302, 303)
    assert resp.headers["location"] == "/status"
    task = gui_client.app.state.runner_task
    assert task is not None
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


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
