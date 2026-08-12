"""Task 8：coordinator.py 的可單元測試部分（build_app 的 /bootstrap 路由、attach_runner、
shutdown_runner）。`run_gui()` 本身會真的 bind socket＋開瀏覽器＋跑 uvicorn.Server，屬於
整合/人工驗收範圍（Task 17 manual test plan），此檔不測它，只測可孤立驗證的邏輯單元。
"""
import asyncio
import inspect
from types import SimpleNamespace

import httpx
from fastapi import Depends, FastAPI

from quanquant.agent.gui.coordinator import attach_runner, build_app, shutdown_runner
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
