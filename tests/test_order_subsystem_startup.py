"""`_start_order_subsystem`（Task 8 readiness gate，web/app.py 的 lifespan 輔助函式）：
ORDER_MODE 拼錯/軟性停用只讓下單子系統關閉，app 其餘功能不受影響；`connect()` 成功才
publish `app.state.order_service`（fail closed，不留 detached task 吞例外）。用假
ShioajiAdapter（monkeypatch）+ 測試用 in-memory engine（monkeypatch `get_engine`），
不連真網路、不碰真正的 quanquant.db。"""
import asyncio

from fastapi import FastAPI
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, create_engine

from quanquant.config import Settings
from quanquant.web.app import _start_order_subsystem


def _test_engine():
    eng = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(eng)
    return eng


def _settings(**over):
    base = dict(
        shioaji_trade_api_key="k", shioaji_trade_secret_key="s",
        order_mode="sim", order_owner_user_ids="1", symbol="TXF",
    )
    base.update(over)
    return Settings(**base)


class _FakeAdapterOK:
    def __init__(self, **kw):
        self.mode = kw["mode"]
        self.account = "F1"
        self.connect_called = False
        self._map_deal_report = lambda payload: None
        self._map_order_report = lambda payload: None

    async def connect(self) -> None:
        self.connect_called = True


class _FakeAdapterFails:
    def __init__(self, **kw):
        self._map_deal_report = lambda payload: None
        self._map_order_report = lambda payload: None

    async def connect(self) -> None:
        raise RuntimeError("login 失敗")


def test_order_mode_typo_disables_subsystem_without_crashing():
    app = FastAPI()
    tasks: list = []
    asyncio.run(_start_order_subsystem(app, _settings(order_mode="paper"), tasks))

    assert app.state.order_service is None
    assert app.state.order_session_state.ready is False
    assert "ORDER_MODE" in app.state.order_session_state.last_error
    assert tasks == []  # app 其餘功能不受影響，下單子系統本身也沒有留下背景 task


def test_preflight_soft_disable_reason_reflected_and_no_tasks_started():
    app = FastAPI()
    tasks: list = []
    asyncio.run(_start_order_subsystem(app, _settings(order_owner_user_ids=""), tasks))

    assert app.state.order_service is None
    assert app.state.order_session_state.ready is False
    assert app.state.order_session_state.last_error
    assert tasks == []


def test_connect_failure_is_fail_closed_and_marks_unhealthy(monkeypatch):
    monkeypatch.setattr("quanquant.web.app.get_engine", _test_engine)
    monkeypatch.setattr("quanquant.broker.shioaji_adapter.ShioajiAdapter", _FakeAdapterFails)

    app = FastAPI()
    tasks: list = []
    asyncio.run(_start_order_subsystem(app, _settings(), tasks))

    assert app.state.order_service is None  # readiness gate：connect 未成功不 publish
    assert app.state.order_session_state.ready is False
    assert "connect" in app.state.order_session_state.last_error
    assert tasks == []  # 不留 detached task


def test_connect_success_publishes_service_and_schedules_background_tasks(monkeypatch):
    monkeypatch.setattr("quanquant.web.app.get_engine", _test_engine)
    monkeypatch.setattr("quanquant.broker.shioaji_adapter.ShioajiAdapter", _FakeAdapterOK)

    app = FastAPI()
    tasks: list = []
    asyncio.run(_start_order_subsystem(app, _settings(), tasks))

    assert app.state.order_service is not None
    assert app.state.order_service.connect_called is True
    assert app.state.order_session_state.ready is True
    assert app.state.order_risk_guard is not None
    assert app.state.order_inbox_worker is not None
    # inbox worker run() + watchdog + confirm token 清理，三個背景 task 都掛上去
    assert len(tasks) == 3
