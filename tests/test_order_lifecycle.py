"""broker.lifecycle：shutdown sentinel（round3 #17，逾時回 False + 標 unhealthy、不宣稱
背景工作已真的停止）+ confirm token 定期清理（round3 #16）。用假 order_service/inbox_worker，
不連真網路、不依賴真 FastAPI lifespan。"""
import asyncio
import datetime as dt

from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.broker.lifecycle import run_confirm_token_cleanup, shutdown_order_subsystem
from quanquant.broker.session_state import OrderSessionState
from quanquant.db.models import ConfirmToken


class _FakeOrderService:
    def __init__(self, *, close_delay: float = 0.0, raises: bool = False):
        self.close_delay = close_delay
        self.raises = raises
        self.closed = False

    async def close(self) -> None:
        if self.close_delay:
            await asyncio.sleep(self.close_delay)
        if self.raises:
            raise RuntimeError("logout 失敗")
        self.closed = True


class _FakeInboxWorker:
    def __init__(self, *, drained: bool = True):
        self._drained = drained
        self.stop_and_drain_calls = 0

    async def stop_and_drain(self, timeout: float = 5.0) -> bool:
        self.stop_and_drain_calls += 1
        return self._drained


def test_shutdown_closes_service_then_drains_worker_and_returns_true():
    service = _FakeOrderService()
    worker = _FakeInboxWorker(drained=True)
    state = OrderSessionState()
    state.mark_ready()

    ok = asyncio.run(shutdown_order_subsystem(
        order_service=service, inbox_worker=worker, state=state, timeout=1.0,
    ))

    assert ok is True
    assert service.closed is True
    assert worker.stop_and_drain_calls == 1
    assert state.ready is True  # 成功 shutdown 不代表要動 state（app.py 自行決定何時整體收尾）


def test_shutdown_returns_false_and_marks_unhealthy_when_worker_drain_times_out():
    service = _FakeOrderService()
    worker = _FakeInboxWorker(drained=False)  # 模擬逾時
    state = OrderSessionState()
    state.mark_ready()

    ok = asyncio.run(shutdown_order_subsystem(
        order_service=service, inbox_worker=worker, state=state, timeout=1.0,
    ))

    assert ok is False  # 不宣稱背景工作已經真的停止
    assert state.ready is False
    assert state.last_error is not None


def test_shutdown_returns_false_when_order_service_close_times_out():
    service = _FakeOrderService(close_delay=1.0)  # 比 timeout 慢
    worker = _FakeInboxWorker(drained=True)
    state = OrderSessionState()

    ok = asyncio.run(shutdown_order_subsystem(
        order_service=service, inbox_worker=worker, state=state, timeout=0.05,
    ))

    assert ok is False
    assert worker.stop_and_drain_calls == 1  # 仍然嘗試 drain worker，不因 close 逾時就整段放棄
    assert state.ready is False


def test_shutdown_returns_false_when_order_service_close_raises():
    service = _FakeOrderService(raises=True)
    worker = _FakeInboxWorker(drained=True)
    state = OrderSessionState()

    ok = asyncio.run(shutdown_order_subsystem(
        order_service=service, inbox_worker=worker, state=state, timeout=1.0,
    ))

    assert ok is False


def test_shutdown_is_noop_safe_when_order_subsystem_never_enabled():
    ok = asyncio.run(shutdown_order_subsystem(
        order_service=None, inbox_worker=None, state=None, timeout=1.0,
    ))
    assert ok is True


def test_run_confirm_token_cleanup_removes_expired_rows_periodically(engine):
    with Session(engine) as s:
        brepo.create_confirm_token_row(
            s, jti="EXPIRED", actor_user_id=1, payload_hash="H1",
            expires_at=dt.datetime(2020, 1, 1),  # 遠早於現在，鐵定過期
        )
        s.commit()

    async def scenario():
        task = asyncio.create_task(run_confirm_token_cleanup(lambda: Session(engine), interval=0.02))
        await asyncio.sleep(0.08)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(scenario())

    with Session(engine) as s:
        remaining = list(s.exec(select(ConfirmToken)))
        assert remaining == []


def test_run_confirm_token_cleanup_survives_transient_session_factory_error():
    calls = {"n": 0}

    def _flaky_session_factory():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("DB 暫時不可用")
        raise RuntimeError("仍然失敗，但迴圈不應該死掉")

    async def scenario():
        task = asyncio.create_task(run_confirm_token_cleanup(_flaky_session_factory, interval=0.01))
        await asyncio.sleep(0.05)
        still_running = not task.done()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return still_running

    assert asyncio.run(scenario()) is True  # 單次失敗不中止背景清理迴圈
    assert calls["n"] >= 2
