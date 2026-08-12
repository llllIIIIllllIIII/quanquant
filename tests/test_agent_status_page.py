"""Task 10：`/status` 儀表板測試。手法沿用 Task 9 建立的 `gui_client` fixture 做法
（`ASGITransport` ＋手動種好 `GuiSecurityState.session_token`／cookie／Host header）——
`runner` 用 Task 7 `test_agent_runner_snapshot.py` 的 `_FakeBuffer`/`_FakeChild` 手法組一個
真 `AgentRunner` 掛到 `app.state`，`transport=None`（這些測試只碰 `snapshot()`／`stop()`／
`_child.terminate()`，不會啟動 `run_forever()` 背景迴圈去真的碰 transport）。

四個情境（本檔內用小 helper 組出，不新增 conftest 全域 fixture，避免污染其他測試檔）：
- `gui_status_client`：預設健康狀態（connecting、無 latch、buffer 淨空）。
- `gui_status_client_latched`：sentinel 記著 latch（`_load_persisted_health` 讀入）。
- `gui_status_client_pending_buffer`：buffer 有未送資料。
"""
import asyncio

import httpx
import keyring
import keyring.errors
import pytest

from quanquant.agent import keyring_store, profile_registry
from quanquant.agent.device_flow_client import AGENT_AUTHORIZE_PATH
from quanquant.agent.gui.coordinator import build_app
from quanquant.agent.gui.security import GUI_SESSION_COOKIE, GuiSecurityState
from quanquant.agent.profile_registry import ProfileEntry
from quanquant.agent.runner import AgentRunner

_DEFAULT_PROFILE = ProfileEntry(
    profile_id="42", username="tester",
    buffer_path="/tmp/fake-status-buffer-profile.db", created_at="2026-01-01T00:00:00",
)


class _FakeSecureBackend(keyring.backend.KeyringBackend):
    """比照 `tests/test_agent_keyring_store.py::_FakeSecureBackend`：純記憶體 fake
    backend，測試絕不碰真正的系統 keychain。"""
    priority = 1

    def __init__(self) -> None:
        self._store: dict[tuple[str, str], str] = {}

    def get_password(self, service, key):
        return self._store.get((service, key))

    def set_password(self, service, key, value):
        self._store[(service, key)] = value

    def delete_password(self, service, key):
        if (service, key) not in self._store:
            raise keyring.errors.PasswordDeleteError("not found")
        del self._store[(service, key)]


@pytest.fixture
def fake_keyring_backend(monkeypatch):
    backend = _FakeSecureBackend()
    monkeypatch.setattr(keyring, "get_keyring", lambda: backend)
    monkeypatch.setattr(keyring, "get_password", backend.get_password)
    monkeypatch.setattr(keyring, "set_password", backend.set_password)
    monkeypatch.setattr(keyring, "delete_password", backend.delete_password)
    return backend


@pytest.fixture(autouse=True)
def _isolated_profile_registry(tmp_path, monkeypatch):
    """比照 `tests/test_agent_profile_registry.py`：全檔 autouse，隔離
    `~/.quanquant-agent/profiles.json`，避免任何 delete_profile 測試不小心動到本機真實
    registry。"""
    monkeypatch.setattr(profile_registry, "REGISTRY_PATH", tmp_path / "profiles.json")
    monkeypatch.setattr(profile_registry, "LOCK_PATH", tmp_path / "profiles.lock")


class _FakeBuffer:
    def __init__(self, pending: int = 0, sentinel: dict | None = None) -> None:
        self._pending = pending
        self._sentinel = sentinel
        self.path = "/tmp/fake-status-buffer.db"

    def unsent_count(self) -> int:
        return self._pending

    def read_sentinel(self) -> dict | None:
        return self._sentinel

    def get_health_epoch(self) -> int:
        return 0


class _FakeChild:
    alive = False

    def __init__(self, terminate_result: bool = True) -> None:
        self.terminate_calls = 0
        self._terminate_result = terminate_result

    def start(self) -> str:
        return "F1"

    def terminate(self) -> bool:
        self.terminate_calls += 1
        return self._terminate_result


def _build_app(*, port: int, pending: int = 0, sentinel: dict | None = None,
                terminate_result: bool = True, profile: ProfileEntry | None = _DEFAULT_PROFILE):
    """`profile`：預設帶一個固定 `ProfileEntry`（模擬精靈完成後 `gui_current_profile`
    已寫入的正常狀態，見 `status_routes._current_profile`）——`clear_credential`／
    `delete_profile`／`reauth` 三個 handler 都要靠它定位 keyring/registry 筆。傳
    `profile=None` 模擬『無法定位目前 profile』的防禦分支。"""
    state = GuiSecurityState(port=port)
    state.session_token = f"test-status-session-{port}"
    app = build_app(state)
    app.state.site_origin = "https://quant.example"
    runner = AgentRunner(
        transport=None,
        buffer=_FakeBuffer(pending=pending, sentinel=sentinel),
        child=_FakeChild(terminate_result=terminate_result),
        mode="sim",
    )
    app.state.agent_runner = runner
    if profile is not None:
        app.state.gui_current_profile = profile
    return app, state


def _client_for(app, state) -> httpx.AsyncClient:
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url=f"http://127.0.0.1:{state.port}",
        cookies={GUI_SESSION_COOKIE: state.session_token},
        headers={"Host": f"127.0.0.1:{state.port}"},
    )
    client.app = app
    return client


@pytest.fixture
async def gui_status_client():
    app, state = _build_app(port=54401)
    async with _client_for(app, state) as client:
        yield client


@pytest.fixture
async def gui_status_client_latched():
    app, state = _build_app(
        port=54402, sentinel={"epoch": 2, "detail": "buffer 目錄唯讀"},
    )
    async with _client_for(app, state) as client:
        yield client


@pytest.fixture
async def gui_status_client_pending_buffer():
    app, state = _build_app(port=54403, pending=3)
    async with _client_for(app, state) as client:
        yield client


# ---------------------------------------------------------------------------
# brief 逐字案例（Step 1）
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_status_page_shows_connection_badge_and_pending_count(gui_status_client):
    resp = await gui_status_client.get("/status")
    assert resp.status_code == 200
    assert "connecting" in resp.text or "連線中" in resp.text


@pytest.mark.asyncio
async def test_status_page_shows_failstop_warning_when_latched(gui_status_client_latched):
    resp = await gui_status_client_latched.get("/status")
    assert "buffer 目錄唯讀" in resp.text  # latch_detail 文案出現


@pytest.mark.asyncio
async def test_delete_profile_blocked_when_buffer_has_unsent_rows(gui_status_client_pending_buffer):
    resp = await gui_status_client_pending_buffer.post("/status/delete-profile")
    assert resp.status_code == 409
    assert "尚未送出" in resp.text or "拒絕" in resp.text


@pytest.mark.asyncio
async def test_stop_agent_calls_runner_stop_and_reports_result(gui_status_client):
    resp = await gui_status_client.post("/status/stop")
    assert resp.status_code == 200
    assert gui_status_client.app.state.agent_runner._stopping is True


@pytest.mark.asyncio
async def test_status_responses_are_no_store(gui_status_client):
    resp = await gui_status_client.get("/status")
    assert resp.headers.get("cache-control") == "no-store"


# ---------------------------------------------------------------------------
# 補充：require_gui_session 守門（既有鐵律回歸，比照 test_agent_setup_wizard.py）
# ---------------------------------------------------------------------------

async def test_status_rejects_missing_session_cookie():
    state = GuiSecurityState(port=54404)
    app = build_app(state)
    app.state.site_origin = "https://quant.example"
    app.state.agent_runner = AgentRunner(
        transport=None, buffer=_FakeBuffer(), child=_FakeChild(), mode="sim",
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:54404",
        headers={"Host": "127.0.0.1:54404"},
    ) as client:
        resp = await client.get("/status")
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# 補充：停止 Agent 驗死失敗時如實顯示錯誤，不謊稱已停止
# ---------------------------------------------------------------------------

async def test_stop_agent_reports_error_when_child_fails_to_terminate():
    app, state = _build_app(port=54405, terminate_result=False)
    async with _client_for(app, state) as client:
        resp = await client.post("/status/stop")
    assert resp.status_code == 500
    assert "無法確認終止" in resp.text
    assert app.state.agent_runner._stopping is True  # stop() 仍已呼叫，只是 child 沒驗死成功


# ---------------------------------------------------------------------------
# reviewer Important fix 1：`_exit_soon()` 的 task 引用必須存住（`app.state.shutdown_task`），
# 不能是 codebase 裡唯一一個「建了就丟」的 create_task——用一個假 server 物件驗證
# `should_exit` 真的被排程翻成 True。
# ---------------------------------------------------------------------------

async def test_stop_agent_schedules_uvicorn_should_exit_and_retains_task_reference(monkeypatch):
    import asyncio

    import quanquant.agent.gui.status_routes as status_routes

    monkeypatch.setattr(status_routes, "_SHUTDOWN_DELAY_SECONDS", 0.01)

    class _FakeServer:
        def __init__(self):
            self.should_exit = False

    app, state = _build_app(port=54411)
    app.state.uvicorn_server = _FakeServer()
    async with _client_for(app, state) as client:
        resp = await client.post("/status/stop")
    assert resp.status_code == 200

    task = app.state.shutdown_task
    assert task is not None  # 引用必須被存住，不能是無人持有、可能被 GC 的裸 task
    await asyncio.wait_for(task, timeout=2)
    assert app.state.uvicorn_server.should_exit is True


# ---------------------------------------------------------------------------
# reviewer Important fix 2：停止成功頁不能沿用 1 秒 meta-refresh（會導航到即將關閉的
# server，使用者看到的其實是連線錯誤）——成功頁必須是無 refresh 的終態頁。
# ---------------------------------------------------------------------------

async def test_stop_agent_success_page_has_no_meta_refresh():
    app, state = _build_app(port=54412)
    async with _client_for(app, state) as client:
        resp = await client.post("/status/stop")
    assert resp.status_code == 200
    assert 'http-equiv="refresh"' not in resp.text
    assert "可以關閉這個分頁" in resp.text


async def test_stop_agent_failure_page_still_has_meta_refresh():
    """驗死失敗時 GUI 仍活著（沒有真的停止），狀態頁應維持既有的 1 秒自動更新，
    不應被誤套用終態頁的『無 refresh』行為。"""
    app, state = _build_app(port=54413, terminate_result=False)
    async with _client_for(app, state) as client:
        resp = await client.post("/status/stop")
    assert resp.status_code == 500
    assert 'http-equiv="refresh"' in resp.text


# ---------------------------------------------------------------------------
# 終審收口：clear_credential／delete_profile／reauth 真的接上 Task 11（keyring）／
# Task 12（profile registry），不再是樁接文案。
# ---------------------------------------------------------------------------

async def test_delete_profile_clears_keyring_and_registry_when_buffer_is_empty(fake_keyring_backend):
    app, state = _build_app(port=54406, pending=0)
    profile = app.state.gui_current_profile
    keyring_store.save_token(site_origin=app.state.site_origin, profile_id=profile.profile_id,
                              token="tok", expires_at="2099-01-01T00:00:00", username="u")
    keyring_store.save_broker_credentials(site_origin=app.state.site_origin, profile_id=profile.profile_id,
                                           api_key="k", secret_key="s")
    profile_registry.upsert_profile(site_origin=app.state.site_origin, profile_id=profile.profile_id,
                                     username=profile.username, buffer_path=profile.buffer_path)

    async with _client_for(app, state) as client:
        resp = await client.post("/status/delete-profile")

    assert resp.status_code == 200
    assert "已刪除" in resp.text
    assert keyring_store.load_token(site_origin=app.state.site_origin, profile_id=profile.profile_id) is None
    assert keyring_store.load_broker_credentials(
        site_origin=app.state.site_origin, profile_id=profile.profile_id) is None
    assert profile_registry.find_profile(site_origin=app.state.site_origin, profile_id=profile.profile_id) is None


async def test_delete_profile_rejects_when_profile_unidentifiable():
    app, state = _build_app(port=54417, pending=0, profile=None)
    async with _client_for(app, state) as client:
        resp = await client.post("/status/delete-profile")
    assert resp.status_code == 409
    assert "無法識別" in resp.text


# ---------------------------------------------------------------------------
# 補充：重新授權——「未勾」分支維持既有指引文案；「已勾記住」分支見 module docstring
# 「重新授權」段：InstanceLock 已被目前這個 runner 持有，不能透過整套精靈重跑，改跑獨立
# 一輪 device flow，核准後直接 rotate_token_secret 寫入 keyring。
# ---------------------------------------------------------------------------

async def test_reauth_not_remembering_shows_restart_guidance():
    app, state = _build_app(port=54407)
    async with _client_for(app, state) as client:
        resp = await client.post("/status/reauth")
    assert resp.status_code == 200
    assert "停止 Agent" in resp.text and "重新啟動" in resp.text


def _reauth_device_flow_handler(profile_id: str, username: str = "tester"):
    """approved 回應的 `profile_id` 可調整——供 mismatch 測試指定一個不同於目前 profile
    的值。"""
    def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/agent/device-code":
            return httpx.Response(200, json={
                "device_code": "test-reauth-device-code", "user_code": "REAUTH-CODE",
                "verification_path": AGENT_AUTHORIZE_PATH, "interval": 0, "expires_in": 600,
            })
        return httpx.Response(200, json={
            "state": "approved", "token": "new-rotated-token", "profile_id": profile_id,
            "username": username, "token_expires_at": "2099-01-01T00:00:00",
        })
    return _handler


async def test_reauth_remembering_completes_and_rotates_keyring_token(fake_keyring_backend):
    app, state = _build_app(port=54408)
    profile = app.state.gui_current_profile
    app.state.agent_http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_reauth_device_flow_handler(profile.profile_id)),
        base_url=app.state.site_origin,
    )
    try:
        async with _client_for(app, state) as client:
            resp = await client.post("/status/reauth", data={"remember_device": "on"})
            assert resp.status_code == 200
            await asyncio.wait_for(app.state.reauth_task, timeout=2)
            resp2 = await client.get("/status")
        assert app.state.reauth_success is True
        assert "已重新授權" in resp2.text
        loaded = keyring_store.load_token(site_origin=app.state.site_origin, profile_id=profile.profile_id)
        assert loaded is not None and loaded["token"] == "new-rotated-token"
    finally:
        await app.state.agent_http_client.aclose()


async def test_reauth_remembering_rejects_when_approved_account_differs_from_current_profile(
    fake_keyring_backend,
):
    app, state = _build_app(port=54418)
    profile = app.state.gui_current_profile
    app.state.agent_http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_reauth_device_flow_handler("some-other-profile-id")),
        base_url=app.state.site_origin,
    )
    try:
        async with _client_for(app, state) as client:
            await client.post("/status/reauth", data={"remember_device": "on"})
            await asyncio.wait_for(app.state.reauth_task, timeout=2)
            resp2 = await client.get("/status")
        assert app.state.reauth_error == "ProfileMismatch"
        assert "帳號" in resp2.text and "不同" in resp2.text
        assert keyring_store.load_token(
            site_origin=app.state.site_origin, profile_id=profile.profile_id) is None
    finally:
        await app.state.agent_http_client.aclose()


async def test_reauth_remembering_rejects_when_profile_unidentifiable():
    app, state = _build_app(port=54419, profile=None)
    async with _client_for(app, state) as client:
        resp = await client.post("/status/reauth", data={"remember_device": "on"})
    assert resp.status_code == 409
    assert "無法識別" in resp.text
    assert getattr(app.state, "reauth_task", None) is None  # 沒有 profile 就不啟動任何背景 task


# ---------------------------------------------------------------------------
# 終審收口：清除已存憑證——真的呼叫 keyring_store.clear_secret，token/broker 各自獨立。
# ---------------------------------------------------------------------------

async def test_clear_credential_calls_keyring_clear_secret_for_token_and_broker(fake_keyring_backend):
    app, state = _build_app(port=54409)
    profile = app.state.gui_current_profile
    keyring_store.save_token(site_origin=app.state.site_origin, profile_id=profile.profile_id,
                              token="tok", expires_at="2099-01-01T00:00:00", username="u")
    keyring_store.save_broker_credentials(site_origin=app.state.site_origin, profile_id=profile.profile_id,
                                           api_key="k", secret_key="s")

    async with _client_for(app, state) as client:
        resp_token = await client.post("/status/clear-credential", params={"which": "token"})
        resp_broker = await client.post("/status/clear-credential", params={"which": "broker"})

    assert resp_token.status_code == 200 and "已清除" in resp_token.text
    assert resp_broker.status_code == 200 and "已清除" in resp_broker.text
    assert keyring_store.load_token(site_origin=app.state.site_origin, profile_id=profile.profile_id) is None
    assert keyring_store.load_broker_credentials(
        site_origin=app.state.site_origin, profile_id=profile.profile_id) is None


async def test_clear_credential_rejects_when_profile_unidentifiable():
    app, state = _build_app(port=54420, profile=None)
    async with _client_for(app, state) as client:
        resp = await client.post("/status/clear-credential", params={"which": "token"})
    assert resp.status_code == 409
    assert "無法識別" in resp.text


async def test_clear_credential_rejects_unknown_which():
    app, state = _build_app(port=54410)
    async with _client_for(app, state) as client:
        resp = await client.post("/status/clear-credential", params={"which": "bogus"})
    assert resp.status_code == 400
