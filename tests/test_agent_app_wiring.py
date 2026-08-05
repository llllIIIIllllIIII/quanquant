import asyncio
from types import SimpleNamespace
import pytest
from quanquant.broker.session_state import OrderSessionState
from quanquant.config import Settings
from quanquant.web.app import _start_agent_channel_subsystem


def _settings(**kw):
    base = dict(order_channel="agent", order_mode="sim", agent_ws_token="tok",
                order_owner_user_ids="1", session_secret="s")
    base.update(kw)
    return Settings(**base)


def _app():
    return SimpleNamespace(state=SimpleNamespace(order_events=None))


async def _run(settings, engine, monkeypatch):
    # 防呆：wiring 測試絕不能碰真實 quanquant.db —— get_engine 換成 in-memory
    monkeypatch.setattr("quanquant.web.app.get_engine", lambda: engine)
    app, state, tasks = _app(), OrderSessionState(), []
    await _start_agent_channel_subsystem(app, settings, tasks, state, None)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    return app, state, tasks


async def test_agent_mode_requires_sim(engine, monkeypatch):
    _, state, tasks = await _run(_settings(order_mode="real"), engine, monkeypatch)
    assert not state.ready and not state.disabled          # 真故障 → /healthz 503
    assert "僅支援" in (state.last_error or "") and tasks == []


async def test_agent_mode_without_token_disabled(engine, monkeypatch):
    _, state, tasks = await _run(_settings(agent_ws_token=""), engine, monkeypatch)
    assert state.disabled and tasks == []                   # 刻意停用 → /healthz 200


async def test_agent_mode_without_owner_disabled(engine, monkeypatch):
    _, state, _ = await _run(_settings(order_owner_user_ids=""), engine, monkeypatch)
    assert state.disabled and "order_owner_user_ids" in (state.last_error or "")


async def test_agent_mode_happy_path_wires_state(engine, monkeypatch):
    app, state, tasks = await _run(_settings(), engine, monkeypatch)
    from quanquant.broker.agent_channel import AgentChannel
    from quanquant.broker.shioaji_adapter import ShioajiAdapter
    assert isinstance(app.state.agent_channel, AgentChannel)
    assert isinstance(app.state.order_service, ShioajiAdapter)
    assert app.state.order_risk_guard is not None
    assert callable(app.state.order_session_factory)
    assert state.disabled and "agent 未連線" in (state.last_error or "")
    assert len(tasks) >= 2                                  # inbox worker + agent watchdog
