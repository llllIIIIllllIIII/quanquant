"""Task 8：coordinator.py 的可單元測試部分（build_app 的 /bootstrap 路由、attach_runner、
shutdown_runner）。`run_gui()` 本身會真的 bind socket＋開瀏覽器＋跑 uvicorn.Server，屬於
整合/人工驗收範圍（Task 17 manual test plan），此檔不測它，只測可孤立驗證的邏輯單元。
"""
import asyncio
from types import SimpleNamespace

import httpx

from quanquant.agent.gui.coordinator import attach_runner, build_app, shutdown_runner
from quanquant.agent.gui.security import GUI_SESSION_COOKIE, GuiSecurityState


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


async def test_bootstrap_wrong_secret_is_404():
    state = GuiSecurityState(port=54321)
    app = build_app(state)
    async with _client(app) as client:
        resp = await client.get("/bootstrap", params={"secret": "wrong"},
                                 headers={"Host": "127.0.0.1:54321"}, follow_redirects=False)
    assert resp.status_code == 404


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
