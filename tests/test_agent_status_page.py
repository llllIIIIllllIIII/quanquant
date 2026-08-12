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
import httpx
import pytest

from quanquant.agent.gui.coordinator import build_app
from quanquant.agent.gui.security import GUI_SESSION_COOKIE, GuiSecurityState
from quanquant.agent.runner import AgentRunner


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
                terminate_result: bool = True):
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
# 補充：刪除 profile 在 buffer 淨空時不誤判為「拒絕」（Task 11/12 尚未落地，先樁接）
# ---------------------------------------------------------------------------

async def test_delete_profile_allows_when_buffer_is_empty_but_stubs_pending_task_11_12():
    app, state = _build_app(port=54406, pending=0)
    async with _client_for(app, state) as client:
        resp = await client.post("/status/delete-profile")
    assert resp.status_code == 200
    assert "Task 11" in resp.text or "Task 12" in resp.text


# ---------------------------------------------------------------------------
# 補充：重新授權／清除已存憑證——Task 11 尚未落地，回應樁接文案而非裸例外
# ---------------------------------------------------------------------------

async def test_reauth_not_remembering_shows_restart_guidance():
    app, state = _build_app(port=54407)
    async with _client_for(app, state) as client:
        resp = await client.post("/status/reauth")
    assert resp.status_code == 200
    assert "停止 Agent" in resp.text and "重新啟動" in resp.text


async def test_reauth_remembering_shows_keyring_stub_notice():
    app, state = _build_app(port=54408)
    async with _client_for(app, state) as client:
        resp = await client.post("/status/reauth", data={"remember_device": "on"})
    assert resp.status_code == 200
    assert "Task 11" in resp.text


async def test_clear_credential_stubs_for_token_and_broker():
    app, state = _build_app(port=54409)
    async with _client_for(app, state) as client:
        resp_token = await client.post("/status/clear-credential", params={"which": "token"})
        resp_broker = await client.post("/status/clear-credential", params={"which": "broker"})
    assert resp_token.status_code == 200 and "Task 11" in resp_token.text
    assert resp_broker.status_code == 200 and "Task 11" in resp_broker.text


async def test_clear_credential_rejects_unknown_which():
    app, state = _build_app(port=54410)
    async with _client_for(app, state) as client:
        resp = await client.post("/status/clear-credential", params={"which": "bogus"})
    assert resp.status_code == 400
