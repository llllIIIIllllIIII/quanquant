"""Task 8：coordinator.py 的可單元測試部分（build_app 的 /bootstrap 路由、attach_runner、
shutdown_runner）。`run_gui()` 本身會真的 bind socket＋開瀏覽器＋跑 uvicorn.Server，屬於
整合/人工驗收範圍（Task 17 manual test plan），此檔不測它，只測可孤立驗證的邏輯單元。
"""
import asyncio
import inspect
from types import SimpleNamespace

import httpx
import pytest
from fastapi import Depends, FastAPI

from quanquant.agent.gui.coordinator import attach_runner, build_app, launch_direct, shutdown_runner
from quanquant.agent.gui.security import (
    GUI_SESSION_COOKIE,
    GuiSecurityState,
    install_security_headers,
    require_gui_session,
)


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:54321")


async def test_bootstrap_redirects_to_setup_and_carries_session_cookie():
    state = GuiSecurityState(port=54321)
    app = build_app(state)
    async with _client(app) as client:
        resp = await client.get("/bootstrap", params={"secret": state.bootstrap_secret},
                                 headers={"Host": "127.0.0.1:54321"}, follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/setup"
    assert GUI_SESSION_COOKIE in resp.cookies
    assert resp.cookies[GUI_SESSION_COOKIE] == state.session_token
    assert state.bootstrap_consumed is True


async def test_bootstrap_second_attempt_is_404_not_reusable():
    state = GuiSecurityState(port=54321)
    app = build_app(state)
    async with _client(app) as client:
        first = await client.get("/bootstrap", params={"secret": state.bootstrap_secret},
                                  headers={"Host": "127.0.0.1:54321"}, follow_redirects=False)
        second = await client.get("/bootstrap", params={"secret": state.bootstrap_secret},
                                   headers={"Host": "127.0.0.1:54321"}, follow_redirects=False)
    assert first.status_code == 303
    assert second.status_code == 404


def test_bootstrap_route_handler_is_a_coroutine_function():
    """Reviewer Important fix 的結構性保證：`bootstrap()` 必須是 `async def`，FastAPI
    才會直接在 event loop 上跑它、絕不丟進 thread-pool——這是
    `consume_bootstrap()` 的 check-then-set 不需要額外鎖就原子的根本原因。純計時型的
    併發測試（見下面 `test_bootstrap_concurrent_requests_...`）在真實 GIL 排程下不
    保證每次都能重現 sync def 版本的競態（已手動驗證：把路由改回 sync `def` 後，
    純計時測試連續跑 5 次仍全部通過，代表那個測法本身不可靠、不能單獨作為回歸防線）
    ——這裡直接斷言路由函式的型別，是唯一能保證『不會因為排程運氣好而漏測』的
    寫法。"""
    state = GuiSecurityState(port=54321)
    app = build_app(state)
    bootstrap_route = next(r for r in app.routes if getattr(r, "path", None) == "/bootstrap")
    assert inspect.iscoroutinefunction(bootstrap_route.endpoint)


async def test_bootstrap_wrong_secret_is_404():
    state = GuiSecurityState(port=54321)
    app = build_app(state)
    async with _client(app) as client:
        resp = await client.get("/bootstrap", params={"secret": "wrong"},
                                 headers={"Host": "127.0.0.1:54321"}, follow_redirects=False)
    assert resp.status_code == 404


async def test_bootstrap_concurrent_requests_exactly_one_wins_and_cookie_survives_require_gui_session():
    """Reviewer Important fix：`bootstrap()` 若是 sync `def`，FastAPI 會丟進
    thread-pool 執行（真 OS 執行緒併發）——`consume_bootstrap()` 的 check-then-set
    （`bootstrap_consumed` 判斷＋寫入）沒有鎖保護，兩個併發首請求（瀏覽器
    prefetch/雙擊/防毒掃連結）可能都通過檢查、各自發不同 session_token，只有最後
    寫入的存活，另一個拿到的 cookie 永久失效且 secret 已消費——需重啟整個 GUI
    程序才能復原。改成 `async def` 後整段（含 `consume_bootstrap`，內部無任何
    await）在單執行緒 event loop 上原子執行，不需要額外的鎖：這裡用
    `asyncio.gather` 真的同時送出兩個帶正確 secret 的請求，驗證恰一個 303+
    Set-Cookie、另一個 404；並確認勝出的 cookie 真的能通過 `require_gui_session`
    （不是另一個已經失效、被覆寫掉的 token）。"""
    state = GuiSecurityState(port=54321)
    app = build_app(state)
    async with _client(app) as client:
        first, second = await asyncio.gather(
            client.get("/bootstrap", params={"secret": state.bootstrap_secret},
                       headers={"Host": "127.0.0.1:54321"}, follow_redirects=False),
            client.get("/bootstrap", params={"secret": state.bootstrap_secret},
                       headers={"Host": "127.0.0.1:54321"}, follow_redirects=False),
        )
    results = [first, second]
    assert sorted(r.status_code for r in results) == [303, 404]
    winner = first if first.status_code == 303 else second
    winning_cookie = winner.cookies[GUI_SESSION_COOKIE]
    assert winning_cookie == state.session_token   # 勝者拿到的正是目前存活的那個 token

    protected_app = FastAPI()
    install_security_headers(protected_app)

    @protected_app.get("/protected", dependencies=[Depends(require_gui_session)])
    def protected():
        return {"ok": True}

    protected_app.state.gui_security = state
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=protected_app),
                                  base_url="http://127.0.0.1:54321",
                                  cookies={GUI_SESSION_COOKIE: winning_cookie}) as client2:
        resp = await client2.get("/protected", headers={"Host": "127.0.0.1:54321"})
    assert resp.status_code == 200


class _FakeTransport:
    def __init__(self):
        self.close_calls = 0

    async def close(self):
        self.close_calls += 1


class _FakeChild:
    def __init__(self, terminate_result=True):
        self.terminate_calls = 0
        self._terminate_result = terminate_result

    def terminate(self):
        self.terminate_calls += 1
        return self._terminate_result


class _FakeRunner:
    def __init__(self, *, forever=None, child_terminate_result=True):
        self._stopping = False
        self._transport = _FakeTransport()
        self._child = _FakeChild(terminate_result=child_terminate_result)
        self._forever = forever

    def stop(self):
        self._stopping = True

    async def run_forever(self):
        if self._forever is not None:
            await self._forever()
        else:
            await asyncio.Event().wait()   # 一直跑到被 cancel


def _fake_app():
    return SimpleNamespace(state=SimpleNamespace(agent_runner=None, runner_task=None))


async def test_attach_runner_starts_background_task_and_registers_state():
    app = _fake_app()
    runner = _FakeRunner()
    task = attach_runner(app, runner)
    assert app.state.agent_runner is runner
    assert app.state.runner_task is task
    assert not task.done()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_attach_runner_swallows_fatal_error_gui_stays_alive(caplog):
    import logging
    caplog.set_level(logging.ERROR)

    async def _boom():
        raise RuntimeError("agent 帳號不符，拒絕啟動")

    app = _fake_app()
    runner = _FakeRunner(forever=_boom)
    task = attach_runner(app, runner)
    await asyncio.wait_for(task, timeout=2)
    assert task.exception() is None   # 沒有把例外原樣拋出協調器
    assert any("agent runner 發生致命錯誤" in r.message for r in caplog.records)


async def test_shutdown_runner_is_noop_when_no_runner_attached():
    app = _fake_app()
    assert await shutdown_runner(app) is True


async def test_shutdown_runner_stops_runner_closes_transport_and_terminates_child():
    app = _fake_app()
    runner = _FakeRunner()
    attach_runner(app, runner)
    result = await shutdown_runner(app)
    assert result is True
    assert runner._stopping is True
    assert runner._transport.close_calls == 1
    assert runner._child.terminate_calls == 1
    assert app.state.runner_task.done()


async def test_shutdown_runner_returns_false_when_child_fails_to_terminate():
    app = _fake_app()
    runner = _FakeRunner(child_terminate_result=False)
    attach_runner(app, runner)
    result = await shutdown_runner(app)
    assert result is False


async def test_shutdown_runner_is_idempotent_across_repeated_calls():
    app = _fake_app()
    runner = _FakeRunner()
    attach_runner(app, runner)
    first = await shutdown_runner(app)
    second = await shutdown_runner(app)
    assert first is True and second is True
    assert runner._child.terminate_calls == 2   # 兩次呼叫都安全，第二次一樣是 no-op 結果


async def test_shutdown_runner_releases_instance_lock(tmp_path):
    """Task 14 補入項：GUI 關閉（OS signal 經 lifespan shutdown，或『停止 Agent』）收尾時
    釋放 app.state.instance_lock，讓同一個 profile 之後可以重新 launch。"""
    from quanquant.agent import profile_registry as profile_registry_module

    app = _fake_app()
    runner = _FakeRunner()
    attach_runner(app, runner)

    buffer_path = tmp_path / "outbox.db"
    lock = profile_registry_module.InstanceLock(buffer_path)
    lock.acquire()
    app.state.instance_lock = lock

    result = await shutdown_runner(app)
    assert result is True
    assert app.state.instance_lock is None

    probe_lock = profile_registry_module.InstanceLock(buffer_path)
    probe_lock.acquire()   # 若沒釋放，這裡會拋 AgentAlreadyRunningError
    probe_lock.release()


async def test_shutdown_runner_is_noop_for_instance_lock_when_none_attached():
    app = _fake_app()
    assert await shutdown_runner(app) is True
    assert getattr(app.state, "instance_lock", None) is None


# ---------------------------------------------------------------------------
# Task 13 reviewer 必修（Important）：launch_direct() 的 "rejected" 分支——之前零測試
# 覆蓋，正是根因分析裡 race-prone 的整合點。probe_direct_connect() 本身的『立即返回，
# 非撞 timeout』行為已在 test_agent_gui_startup_flow.py 鎖住；這裡鎖 launch_direct()
# 收到 "rejected" 後的收尾：導向 /setup＋重新授權提示，且完全不讀 keyring token 的
# expires_at（不信任 metadata，只信任 probe 觀察到的真實握手結果）。
# ---------------------------------------------------------------------------

class _RejectingDirectRunner:
    """模擬真實 run_forever(stop_on_token_reject=True) 在 TokenRejectedError 分支很快
    讓 connection 落在 "rejected"——probe_direct_connect() 的輪詢迴圈本身已有獨立測試
    鎖住（test_agent_gui_startup_flow.py），這裡不重複驗證那段。"""

    def __init__(self):
        self.connection = "connecting"

    async def snapshot(self):
        return SimpleNamespace(connection=self.connection)

    async def run_forever(self, *, stop_on_token_reject=False):
        assert stop_on_token_reject is True  # GUI direct 路徑唯一允許停用無限重試的地方
        self.connection = "rejected"


class _NoExpiresAtPeekToken(dict):
    """任何讀取 "expires_at" 這個 key 都讓測試直接失敗——鎖住『launch_direct() 不信任
    keyring metadata，只信任 probe_direct_connect() 的真實握手結果』這條規則。"""

    def __getitem__(self, key):
        if key == "expires_at":
            raise AssertionError("launch_direct() 不應讀取 keyring token 的 expires_at")
        return super().__getitem__(key)

    def get(self, key, default=None):
        if key == "expires_at":
            raise AssertionError("launch_direct() 不應讀取 keyring token 的 expires_at")
        return super().get(key, default)


async def test_launch_direct_rejected_branch_redirects_to_setup_and_ignores_expires_at(monkeypatch, tmp_path):
    import quanquant.agent.buffer as buffer_module
    import quanquant.agent.keyring_store as keyring_store_module
    import quanquant.agent.runner as runner_module
    import quanquant.agent.ws_client as ws_client_module

    fake_runner = _RejectingDirectRunner()

    monkeypatch.setattr(keyring_store_module, "load_token", lambda **kw: _NoExpiresAtPeekToken(
        token="tok", expires_at="2099-01-01T00:00:00", username="alice",
    ))
    monkeypatch.setattr(keyring_store_module, "load_broker_credentials",
                         lambda **kw: {"api_key": "K1", "secret_key": "S1"})
    monkeypatch.setattr(buffer_module, "DurableBuffer", lambda path: object())
    monkeypatch.setattr(runner_module, "ChildHandle", lambda **kw: object())
    monkeypatch.setattr(runner_module, "AgentRunner", lambda **kw: fake_runner)
    monkeypatch.setattr(ws_client_module, "WebsocketsTransport", lambda url, *, token: object())

    app = _fake_app()
    # Task 14：launch_direct() 現在會對這個路徑真的取 InstanceLock（見其 docstring），
    # 用 tmp_path 而非佔位字串路徑，避免撞到唯讀檔案系統。
    profile = SimpleNamespace(profile_id="1", buffer_path=str(tmp_path / "outbox.db"))

    target_path, notice = await launch_direct(app, profile=profile, site_origin="https://q.example")

    assert target_path == "/setup"
    assert notice is not None and "重新授權" in notice
    await asyncio.gather(app.state.runner_task, return_exceptions=True)  # 收尾，避免未回收例外警告


# ---------------------------------------------------------------------------
# Reviewer Minor 2：手動 URL 重入防呆——已有一個尚未結束的 runner 時不重新讀
# keyring／建構第二個 AgentRunner（避免第二條 WS 連線＋背景 task 洩漏）。
# ---------------------------------------------------------------------------

async def test_launch_direct_skips_relaunch_when_runner_already_running(monkeypatch):
    import quanquant.agent.keyring_store as keyring_store_module

    def _must_not_be_called(**kwargs):
        raise AssertionError("已有 runner 在跑時 launch_direct() 不該再讀 keyring")

    monkeypatch.setattr(keyring_store_module, "load_token", _must_not_be_called)
    monkeypatch.setattr(keyring_store_module, "load_broker_credentials", _must_not_be_called)

    async def _hang():
        await asyncio.Event().wait()

    app = _fake_app()
    app.state.agent_runner = object()
    app.state.runner_task = asyncio.create_task(_hang())

    profile = SimpleNamespace(profile_id="1", buffer_path="/x1")
    target_path, notice = await launch_direct(app, profile=profile, site_origin="https://q.example")

    assert target_path == "/status" and notice is None

    app.state.runner_task.cancel()
    await asyncio.gather(app.state.runner_task, return_exceptions=True)


# ---------------------------------------------------------------------------
# Task 14 補入項（controller 依 spec §5.3）：InstanceLock 接線——launch_direct() 建構
# runner 前 acquire，同 profile 禁止第二個 agent 程序（驗收情境 10）。
# ---------------------------------------------------------------------------

class _ConnectedDirectRunner:
    """直接落在 "connected"，probe_direct_connect() 立刻返回，不必等 run_forever。"""

    async def snapshot(self):
        return SimpleNamespace(connection="connected")

    async def run_forever(self, *, stop_on_token_reject=False):
        await asyncio.Event().wait()   # 一直跑到被 cancel


async def test_launch_direct_blocked_when_instance_lock_already_held(monkeypatch, tmp_path):
    from quanquant.agent import profile_registry as profile_registry_module
    import quanquant.agent.keyring_store as keyring_store_module

    def _must_not_be_called(**kwargs):
        raise AssertionError("instance lock 衝突時不該繼續讀 keyring／建構 runner")

    monkeypatch.setattr(keyring_store_module, "load_token", lambda **kw: {
        "token": "tok", "expires_at": "2099-01-01T00:00:00", "username": "alice",
    })
    monkeypatch.setattr(keyring_store_module, "load_broker_credentials",
                         lambda **kw: {"api_key": "K1", "secret_key": "S1"})

    buffer_path = tmp_path / "outbox.db"
    app = _fake_app()
    profile = SimpleNamespace(profile_id="1", buffer_path=str(buffer_path))

    external_lock = profile_registry_module.InstanceLock(buffer_path)
    external_lock.acquire()
    try:
        target_path, notice = await launch_direct(app, profile=profile, site_origin="https://q.example")
        assert target_path == "/setup"
        assert notice is not None and "已在執行中" in notice
        assert app.state.agent_runner is None
    finally:
        external_lock.release()


async def test_launch_direct_acquires_and_stores_lock_then_recoverable_after_release(monkeypatch, tmp_path):
    import quanquant.agent.buffer as buffer_module
    from quanquant.agent import profile_registry as profile_registry_module
    import quanquant.agent.keyring_store as keyring_store_module
    import quanquant.agent.runner as runner_module
    import quanquant.agent.ws_client as ws_client_module

    monkeypatch.setattr(keyring_store_module, "load_token", lambda **kw: {
        "token": "tok", "expires_at": "2099-01-01T00:00:00", "username": "alice",
    })
    monkeypatch.setattr(keyring_store_module, "load_broker_credentials",
                         lambda **kw: {"api_key": "K1", "secret_key": "S1"})
    monkeypatch.setattr(buffer_module, "DurableBuffer", lambda path: object())
    monkeypatch.setattr(runner_module, "ChildHandle", lambda **kw: object())
    fake_runner = _ConnectedDirectRunner()
    monkeypatch.setattr(runner_module, "AgentRunner", lambda **kw: fake_runner)
    monkeypatch.setattr(ws_client_module, "WebsocketsTransport", lambda url, *, token: object())

    buffer_path = tmp_path / "outbox.db"
    app = _fake_app()
    profile = SimpleNamespace(profile_id="1", buffer_path=str(buffer_path))

    target_path, notice = await launch_direct(app, profile=profile, site_origin="https://q.example")
    assert target_path == "/status" and notice is None
    assert isinstance(app.state.instance_lock, profile_registry_module.InstanceLock)

    # 尚未 release：另一個 InstanceLock 嘗試 acquire 同一路徑必須被擋
    probe_lock = profile_registry_module.InstanceLock(buffer_path)
    with pytest.raises(profile_registry_module.AgentAlreadyRunningError):
        probe_lock.acquire()

    app.state.instance_lock.release()
    app.state.runner_task.cancel()
    await asyncio.gather(app.state.runner_task, return_exceptions=True)

    # release 後可重取
    probe_lock2 = profile_registry_module.InstanceLock(buffer_path)
    probe_lock2.acquire()
    probe_lock2.release()


async def test_launch_direct_rejected_branch_releases_instance_lock(monkeypatch, tmp_path):
    """"rejected" 分支的 runner 已經真的停止——不釋放 lock 會讓同一個 GUI 程序自己都無法
    在重新授權後再次 launch_direct() 同一個 profile（見 coordinator.py launch_direct
    docstring）。"""
    import quanquant.agent.buffer as buffer_module
    from quanquant.agent import profile_registry as profile_registry_module
    import quanquant.agent.keyring_store as keyring_store_module
    import quanquant.agent.runner as runner_module
    import quanquant.agent.ws_client as ws_client_module

    fake_runner = _RejectingDirectRunner()

    monkeypatch.setattr(keyring_store_module, "load_token", lambda **kw: {
        "token": "tok", "expires_at": "2099-01-01T00:00:00", "username": "alice",
    })
    monkeypatch.setattr(keyring_store_module, "load_broker_credentials",
                         lambda **kw: {"api_key": "K1", "secret_key": "S1"})
    monkeypatch.setattr(buffer_module, "DurableBuffer", lambda path: object())
    monkeypatch.setattr(runner_module, "ChildHandle", lambda **kw: object())
    monkeypatch.setattr(runner_module, "AgentRunner", lambda **kw: fake_runner)
    monkeypatch.setattr(ws_client_module, "WebsocketsTransport", lambda url, *, token: object())

    buffer_path = tmp_path / "outbox.db"
    app = _fake_app()
    profile = SimpleNamespace(profile_id="1", buffer_path=str(buffer_path))

    target_path, notice = await launch_direct(app, profile=profile, site_origin="https://q.example")
    assert target_path == "/setup" and notice is not None

    probe_lock = profile_registry_module.InstanceLock(buffer_path)
    probe_lock.acquire()   # lock 已釋放，這裡不該拋 AgentAlreadyRunningError
    probe_lock.release()


async def test_launch_direct_releases_lock_when_runner_construction_fails(monkeypatch, tmp_path):
    """Critical fix（reviewer 判定）：`lock.acquire()` 後 `AgentRunner`/`DurableBuffer`
    等建構若真的拋例外（例如 `DurableBuffer.__init__` 的 `RefuseStartError`），先前完全
    沒有 try/except 包住，lock fd 會直接洩漏——同一個 profile 之後永遠回報「已在執行
    中」，直到重啟整個 GUI 程序才會因程序結束而連帶釋放。比照
    `test_step3_launch_releases_lock_when_runner_construction_fails`（setup_routes.py
    那一半）鎖住同一條規則。"""
    import quanquant.agent.buffer as buffer_module
    from quanquant.agent import profile_registry as profile_registry_module
    import quanquant.agent.keyring_store as keyring_store_module
    import quanquant.agent.runner as runner_module
    import quanquant.agent.ws_client as ws_client_module

    def _boom(**kwargs):
        raise RuntimeError("construction failed")

    monkeypatch.setattr(keyring_store_module, "load_token", lambda **kw: {
        "token": "tok", "expires_at": "2099-01-01T00:00:00", "username": "alice",
    })
    monkeypatch.setattr(keyring_store_module, "load_broker_credentials",
                         lambda **kw: {"api_key": "K1", "secret_key": "S1"})
    monkeypatch.setattr(buffer_module, "DurableBuffer", lambda path: object())
    monkeypatch.setattr(runner_module, "ChildHandle", lambda **kw: object())
    monkeypatch.setattr(runner_module, "AgentRunner", _boom)
    monkeypatch.setattr(ws_client_module, "WebsocketsTransport", lambda url, *, token: object())

    buffer_path = tmp_path / "outbox.db"
    app = _fake_app()
    profile = SimpleNamespace(profile_id="1", buffer_path=str(buffer_path))

    target_path, notice = await launch_direct(app, profile=profile, site_origin="https://q.example")
    assert target_path == "/setup"
    assert notice is not None
    assert app.state.agent_runner is None

    probe_lock = profile_registry_module.InstanceLock(buffer_path)
    probe_lock.acquire()   # 若沒釋放，這裡會拋 AgentAlreadyRunningError
    probe_lock.release()
