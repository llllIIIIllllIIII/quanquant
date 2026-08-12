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
