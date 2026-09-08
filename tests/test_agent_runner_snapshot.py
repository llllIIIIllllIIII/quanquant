import asyncio

import pytest

from quanquant.agent.runner import AgentRunner


class _FakeBuffer:
    def __init__(self, pending=0):
        self._pending = pending
        self.path = "/tmp/fake.db"
    def unsent_count(self):
        return self._pending
    def read_sentinel(self):
        return None
    def get_health_epoch(self):
        return 0


class _FakeChild:
    alive = False
    def start(self):
        return "F1"


@pytest.mark.asyncio
async def test_snapshot_reflects_initial_state():
    runner = AgentRunner(transport=None, buffer=_FakeBuffer(pending=3), child=_FakeChild(), mode="sim")
    snap = await runner.snapshot()
    assert snap.mode == "sim"
    assert snap.buffer_pending == 3
    assert snap.latched is False
    assert snap.connection == "connecting"


@pytest.mark.asyncio
async def test_snapshot_reflects_latched_state_from_persisted_health():
    class _LatchedBuffer(_FakeBuffer):
        def read_sentinel(self):
            return {"epoch": 2, "detail": "buffer 目錄唯讀"}
    runner = AgentRunner(transport=None, buffer=_LatchedBuffer(), child=_FakeChild(), mode="sim")
    snap = await runner.snapshot()
    assert snap.latched is True and snap.latch_detail == "buffer 目錄唯讀" and snap.health_epoch == 2


@pytest.mark.asyncio
async def test_snapshot_is_immutable_and_each_call_is_fresh_instance():
    runner = AgentRunner(transport=None, buffer=_FakeBuffer(pending=1), child=_FakeChild(), mode="sim")
    s1 = await runner.snapshot()
    with pytest.raises(Exception):
        s1.buffer_pending = 99  # frozen dataclass 拒絕賦值
    runner._buffer._pending = 5
    s2 = await runner.snapshot()
    assert s1.buffer_pending == 1 and s2.buffer_pending == 5  # 不是同一份被 mutate


# ---------------------------------------------------------------------------
# Task 13：TokenRejectedError 分類＋單一集中判斷點＋stop_on_token_reject 契約
# ---------------------------------------------------------------------------

def test_connection_state_for_session_exception_maps_token_rejected_to_rejected():
    from quanquant.agent.runner import _connection_state_for_session_exception
    from quanquant.agent.ws_client import TokenRejectedError
    assert _connection_state_for_session_exception(TokenRejectedError("x")) == "rejected"


def test_connection_state_for_session_exception_maps_other_exceptions_to_reconnecting():
    from quanquant.agent.runner import _connection_state_for_session_exception
    assert _connection_state_for_session_exception(RuntimeError("x")) == "reconnecting"
    assert _connection_state_for_session_exception(ConnectionError("x")) == "reconnecting"


@pytest.mark.asyncio
async def test_run_forever_stops_and_latches_rejected_when_flag_set():
    from quanquant.agent.ws_client import TokenRejectedError

    runner = AgentRunner(transport=None, buffer=_FakeBuffer(), child=_FakeChild(), mode="sim")

    async def _boom():
        runner._connection_state = "rejected"  # run_once() 集中點會做的事，這裡直接模擬其副作用
        raise TokenRejectedError("rejected")
    runner.run_once = _boom

    await runner.run_forever(stop_on_token_reject=True)  # 不得往外拋、必須正常 return

    snap = await runner.snapshot()
    assert snap.connection == "rejected"
    assert runner._stopping is True  # 已停止，迴圈不會再重試


@pytest.mark.asyncio
async def test_run_forever_default_flag_retries_normally_on_token_rejected_g5():
    """G5：stop_on_token_reject 預設 False 時，TokenRejectedError 走既有一般例外/backoff
    路徑——不 stop、不提早 return，迴圈照舊繼續重試（用呼叫次數證明有進第二輪）。"""
    from quanquant.agent.ws_client import TokenRejectedError

    runner = AgentRunner(transport=None, buffer=_FakeBuffer(), child=_FakeChild(), mode="sim",
                          backoff_base=0.01, backoff_max=0.01, stable_session_seconds=999)
    calls = []

    async def _boom():
        calls.append(1)
        if len(calls) >= 2:
            runner.stop()
            return
        raise TokenRejectedError("rejected")
    runner.run_once = _boom

    await runner.run_forever()  # stop_on_token_reject 未傳，預設 False

    assert len(calls) == 2  # 第一輪拒絕後仍然重試了第二輪，backoff 行為與既有一般例外一致
