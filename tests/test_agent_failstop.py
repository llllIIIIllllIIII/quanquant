"""Inc1 D9/G2（Task 12）：durable fail-stop 全鏈——latch/sentinel/health sender/lease/probe。

spec D9「G2 fail-stop 狀態機」①-⑧（R5-1 重定義後的三條可實現保證）逐條覆蓋，依所在層分區：

  - agent 端（native_runner.py `_wrap_on_raw` 退化寫入 + sentinel + 專用 IPC；buffer.py
    sentinel/health_epoch/probe 原語；runner.py `AgentRunner` 的 latch/recover/health
    sender/native 呼叫前守門）。
  - server 端（agent_channel.py `AgentChannel` 的 epoch 單調性/pending_health；
    agent_registry.py 的 heartbeat lease；agent_ws.py 的 UpHealth/UpCommandRejected
    handler 全鏈，經真 WS 連線驗證）。

測試對照 spec 測試重點清單 S#6/19/24/30/31/35（見
docs/superpowers/plans/2026-08-07-local-broker-agent-inc1-design.md Task 12 段）。
"""
import asyncio
import datetime as dt
import json
import multiprocessing as mp
import sqlite3
import time
from decimal import Decimal

import pytest
from sqlmodel import Session, select
from starlette.testclient import TestClient

import quanquant.agent.native_runner as nr
from quanquant.agent.buffer import DurableBuffer
from quanquant.agent.runner import AgentRunner
from quanquant.auth import service as auth_service
from quanquant.auth.agent_tokens import issue_token
from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.broker import repository as brepo
from quanquant.broker.agent_channel import AgentChannel
from quanquant.broker.agent_registry import AgentRegistry, UserAgentSlot, run_health_lease_watchdog
from quanquant.broker.session_state import OrderSessionState
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.config import get_settings
from quanquant.db.models import AgentCommand, Order, QuotaReservation
from quanquant.web.app import create_app
from quanquant.web.deps import get_session

# ===========================================================================
# 1. buffer.py：sentinel 檔（buffer 之外路徑）＋health_epoch 持久化＋storage probe
# ===========================================================================


def test_sentinel_write_read_clear_roundtrip(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    assert buf.read_sentinel() is None
    assert not buf.has_sentinel()
    buf.write_sentinel(epoch=3, detail="boom")
    assert buf.has_sentinel()
    assert buf.read_sentinel() == {"epoch": 3, "detail": "boom"}
    buf.clear_sentinel()
    assert buf.read_sentinel() is None
    assert not buf.has_sentinel()


# ---- R7-2（HIGH，codex 終審 round7）：sentinel 加 fault_token（唯一 nonce）欄位。原本供
# AgentRunner._recover() 在 terminate 前後比對、偵測 dying-gasp；2026-08-08 G2 恢復降級為
# 啟動時 probe 後 `_recover()` 已移除，欄位保留當 sentinel 的純診斷資訊 ----


def test_write_sentinel_with_fault_token_roundtrips(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    buf.write_sentinel(epoch=1, detail="x", fault_token="tok-1")
    assert buf.read_sentinel() == {"epoch": 1, "detail": "x", "fault_token": "tok-1"}


def test_write_sentinel_without_fault_token_omits_key_backward_compat(tmp_path):
    """不傳 `fault_token`（既有呼叫端——`AgentRunner._latch` 的父程序端覆寫、以及本檔
    其他既有測試）——JSON 內完全不寫這個鍵（不是寫入字面 `null`），維持既有測試對
    `read_sentinel()` 的精確 dict 比對逐位元組相容，`.get("fault_token")` 讀到的自然是
    `None`。"""
    buf = DurableBuffer(tmp_path / "o.db")
    buf.write_sentinel(epoch=1, detail="x")
    assert buf.read_sentinel() == {"epoch": 1, "detail": "x"}
    assert buf.read_sentinel().get("fault_token") is None


# ---- N9-3（MEDIUM，codex 終審 round9）：sentinel 讀取失敗 fail-open——舊版 `read_
# sentinel()` 把 OSError/JSONDecodeError 一律吞掉回 None，跟「檔案真的不存在」混為一談，
# 呼叫端因此可能把「讀不到」誤判成「無 latch」而上報 status="ok"（fail-open）。修法：只有
# FileNotFoundError 代表無 sentinel（回 None）；存在但讀不出來一律 raise
# SentinelUnreadableError，呼叫端必須 fail-closed（見 test_agent_runner.py 的
# `_load_persisted_health` 對照測試）。----


def test_read_sentinel_missing_file_returns_none(tmp_path):
    """回歸：真的沒有 sentinel（檔案不存在）仍必須回 None，不是 raise——這是唯一合法代表
    「無 latch」的情況，N9-3 修法不能連這個都一起變嚴格。"""
    buf = DurableBuffer(tmp_path / "o.db")
    assert buf.read_sentinel() is None


def test_read_sentinel_corrupted_json_raises_unreadable_not_none(tmp_path):
    """sentinel 檔存在但 JSON 損毀（例如寫到一半就崩潰）——必須 raise
    `SentinelUnreadableError`，不是靜靜回 None（那會被誤判成「無 latch」）。"""
    from quanquant.agent.buffer import SentinelUnreadableError

    buf = DurableBuffer(tmp_path / "o.db")
    buf.write_sentinel(epoch=1, detail="x")
    (tmp_path / "o.db.failstop").write_text("{not valid json", encoding="utf-8")

    with pytest.raises(SentinelUnreadableError):
        buf.read_sentinel()


def test_read_sentinel_os_error_raises_unreadable_not_none(tmp_path, monkeypatch):
    """sentinel 檔存在但讀取本身失敗（模擬權限錯誤等非 FileNotFoundError 的 OSError）
    ——同樣必須 raise `SentinelUnreadableError`，不是回 None。"""
    from pathlib import Path

    from quanquant.agent.buffer import SentinelUnreadableError

    buf = DurableBuffer(tmp_path / "o.db")
    buf.write_sentinel(epoch=1, detail="x")

    def _boom(self, *, encoding=None):
        raise PermissionError("模擬權限錯誤")

    monkeypatch.setattr(Path, "read_text", _boom)

    with pytest.raises(SentinelUnreadableError):
        buf.read_sentinel()


def test_sentinel_path_is_outside_sqlite_buffer_file(tmp_path):
    """G2①：sentinel 是 buffer 之外的獨立檔案——不是 SQLite 的一部分，buffer 檔案本身損毀
    也不影響它的存在。"""
    buf = DurableBuffer(tmp_path / "o.db")
    buf.write_sentinel(epoch=1, detail="x")
    assert (tmp_path / "o.db.failstop").exists()
    assert (tmp_path / "o.db").exists()  # 兩個獨立檔案


def test_health_epoch_persists_across_instances(tmp_path):
    path = tmp_path / "o.db"
    buf1 = DurableBuffer(path)
    assert buf1.get_health_epoch() == 0
    buf1.set_health_epoch(4)
    buf2 = DurableBuffer(path)  # 全新物件、同一個檔案
    assert buf2.get_health_epoch() == 4


def test_probe_roundtrip_succeeds_on_healthy_buffer(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    assert buf.probe() is True


def test_probe_fails_when_sqlite_broken(tmp_path, monkeypatch):
    buf = DurableBuffer(tmp_path / "o.db")

    def _boom():
        raise sqlite3.OperationalError("disk full")

    monkeypatch.setattr(buf, "_conn", _boom)
    assert buf.probe() is False  # 不 raise——呼叫端據此判斷不解除 latch


# ===========================================================================
# 2. native_runner.py：child callback 落地失敗（含退化寫入亦失敗）→ sentinel ＋ 專用 IPC
# ===========================================================================


def test_wrap_on_raw_primary_write_success_no_sentinel(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    account_box = nr._AccountBox()
    account_box.value = "F1"
    on_raw = nr._wrap_on_raw(buf, mode="sim", account_box=account_box)
    eid = on_raw("deal_report", {"n": 1})
    assert eid > 0
    assert buf.pending()[0].payload == {"n": 1}
    assert not buf.has_sentinel()


def test_wrap_on_raw_degraded_write_succeeds_when_primary_fails(tmp_path, monkeypatch):
    """C5（HIGH，codex 終審，收緊自 spec 原案「雙寫失敗才 latch」）：主寫入（SQLite）失敗、
    退化寫入（純檔案）成功——**仍然 latch**（degraded 檔僅供人工救援，不是與主寫入等價的
    落地路徑，見 `_wrap_on_raw` docstring），但不 raise（維持既有回傳語意，呼叫端/callback
    thread 不需要另外處理例外）。"""
    buf = DurableBuffer(tmp_path / "o.db")
    monkeypatch.setattr(
        buf, "append", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("sqlite boom"))
    )
    parent_conn, child_conn = mp.Pipe()
    account_box = nr._AccountBox()
    account_box.value = "F1"
    on_raw = nr._wrap_on_raw(buf, mode="sim", account_box=account_box, failstop_conn=child_conn)
    result = on_raw("deal_report", {"n": 1})
    assert result == -1  # 退化寫入沒有 SQLite row id 可回
    degraded_path = nr._degraded_write_path(buf)
    assert degraded_path.exists()
    line = json.loads(degraded_path.read_text(encoding="utf-8").strip().splitlines()[-1])
    assert line == {"kind": "deal_report", "payload": {"n": 1}, "account": "F1", "mode": "sim"}
    assert buf.has_sentinel()  # C5：主寫入失敗即 latch，degraded 檔只是救援副本
    sentinel = buf.read_sentinel()
    assert "sqlite boom" in sentinel["detail"] and str(degraded_path) in sentinel["detail"]
    assert parent_conn.poll(2), "父程序應收到 failstop IPC 通知（degraded 成功也要通知）"
    notice = parent_conn.recv()
    assert notice["type"] == "failstop"


def test_wrap_on_raw_primary_failure_trips_child_latch_even_when_degraded_succeeds(tmp_path, monkeypatch):
    """C4/C5：`latch`（`ChildFailstopLatch`）在主寫入失敗時同步 trip——不論退化寫入是否
    成功，`_dispatch` 才能在 native 呼叫前即時看到（不必等 IPC round-trip）。"""
    buf = DurableBuffer(tmp_path / "o.db")
    monkeypatch.setattr(
        buf, "append", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("sqlite boom"))
    )
    account_box = nr._AccountBox()
    account_box.value = "F1"
    latch = nr.ChildFailstopLatch()
    on_raw = nr._wrap_on_raw(buf, mode="sim", account_box=account_box, latch=latch)
    assert latch.tripped is False
    on_raw("deal_report", {"n": 1})
    assert latch.tripped is True


def test_wrap_on_raw_double_failure_writes_sentinel_and_notifies_parent_then_reraises(
    tmp_path, monkeypatch,
):
    """G2①核心：主寫入 + 退化寫入皆失敗 → 寫 sentinel（durable、buffer 之外）＋經專用 IPC
    channel（不混 RPC pipe，R2-8）通知父程序 → 原樣 re-raise（callback 執行緒仍知道這次
    真的沒有落地）。"""
    buf = DurableBuffer(tmp_path / "o.db")
    monkeypatch.setattr(
        buf, "append", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("primary boom"))
    )
    monkeypatch.setattr(
        nr, "_try_degraded_write",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("degraded boom")),
    )
    account_box = nr._AccountBox()
    account_box.value = "F1"
    parent_conn, child_conn = mp.Pipe()
    on_raw = nr._wrap_on_raw(buf, mode="sim", account_box=account_box, failstop_conn=child_conn)

    with pytest.raises(RuntimeError, match="degraded boom"):
        on_raw("deal_report", {"n": 1})

    assert buf.has_sentinel()
    sentinel = buf.read_sentinel()
    assert "primary boom" in sentinel["detail"] and "degraded boom" in sentinel["detail"]

    assert parent_conn.poll(2), "父程序應收到 failstop IPC 通知"
    notice = parent_conn.recv()
    assert notice["type"] == "failstop"
    assert "primary boom" in notice["detail"]


def test_wrap_on_raw_double_failure_without_failstop_conn_still_writes_sentinel(
    tmp_path, monkeypatch,
):
    """failstop_conn=None（相容尚未接線這條 channel 的呼叫端）：仍寫 sentinel（durable
    latch 標記不依賴 IPC 是否存在），只是沒有即時 IPC 通知。"""
    buf = DurableBuffer(tmp_path / "o.db")
    monkeypatch.setattr(
        buf, "append", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("primary boom"))
    )
    monkeypatch.setattr(
        nr, "_try_degraded_write",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("degraded boom")),
    )
    account_box = nr._AccountBox()
    account_box.value = "F1"
    on_raw = nr._wrap_on_raw(buf, mode="sim", account_box=account_box, failstop_conn=None)

    with pytest.raises(RuntimeError):
        on_raw("deal_report", {"n": 1})
    assert buf.has_sentinel()


# ---- R7-2（HIGH，codex 終審 round7）：_trigger_failstop_latch 每次呼叫都寫入一個全新、
# 獨一無二的 fault_token（不是舊版固定字面 epoch=-1 那種永遠不變的值）----


def test_trigger_failstop_latch_writes_unique_fault_token_each_call(tmp_path):
    """每次呼叫都必須拿到不同的 fault_token——原本是 `AgentRunner._recover()` 用 before/
    after 比對偵測 dying-gasp 的前提，2026-08-08 該比對隨 `_recover()` 移除，這裡繼續驗證
    唯一性純粹是保留診斷資訊的正確性（若每次都寫同一個值，等於重演 R5-b 記載過的
    `epoch=-1` 字面比較舊卡死模式）。"""
    buf = DurableBuffer(tmp_path / "o.db")
    latch = nr.ChildFailstopLatch()

    nr._trigger_failstop_latch(buf, None, latch, "detail1")
    token1 = buf.read_sentinel()["fault_token"]
    assert token1  # 非空字串

    nr._trigger_failstop_latch(buf, None, latch, "detail2")
    token2 = buf.read_sentinel()["fault_token"]

    assert token1 != token2


def test_wrap_on_raw_double_failure_sentinel_carries_fault_token(tmp_path, monkeypatch):
    """G2①/R7-2 核心路徑（雙寫皆失敗）：sentinel 內容必須帶上 fault_token，不是只有
    epoch/detail。"""
    buf = DurableBuffer(tmp_path / "o.db")
    monkeypatch.setattr(
        buf, "append", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("primary boom"))
    )
    monkeypatch.setattr(
        nr, "_try_degraded_write",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("degraded boom")),
    )
    account_box = nr._AccountBox()
    account_box.value = "F1"
    on_raw = nr._wrap_on_raw(buf, mode="sim", account_box=account_box)

    with pytest.raises(RuntimeError):
        on_raw("deal_report", {"n": 1})

    sentinel = buf.read_sentinel()
    assert sentinel["fault_token"]


# ===========================================================================
# 3. AgentChannel：epoch 單調性（G2⑤/R5-1）＋ pending_health（G2⑥）＋ per-session 基準重宣告
# ===========================================================================


def test_note_health_accepts_ok_and_advances_max_epoch():
    ch = AgentChannel()
    ch.mark_logged_in("F1", health_epoch=0)
    assert ch.note_health(status="ok", health_epoch=0) is True
    assert ch.health_status == "ok" and ch.failstop is False
    assert ch.last_ok_heartbeat is not None


def test_note_health_stale_lower_epoch_ok_never_resurrects_after_failstop():
    """S#24/R5-1：見過較大 epoch 後，較舊的 ok（epoch 較小）一律忽略——latch 之後遲到的舊
    ok 不會誤把 failstop 恢復成 ok。"""
    ch = AgentChannel()
    ch.mark_logged_in("F1", health_epoch=0)
    assert ch.note_health(status="ok", health_epoch=0) is True
    assert ch.failstop is False

    assert ch.note_health(status="failstop", health_epoch=1) is True
    assert ch.failstop is True

    # 遲到的舊 ok（epoch=0，latch 之前的）——必須被忽略，不恢復。
    accepted = ch.note_health(status="ok", health_epoch=0)
    assert accepted is False
    assert ch.failstop is True  # 依然是 failstop，沒有被舊訊息誤救回


def test_mark_logged_in_resets_max_epoch_buffer_rebuild_does_not_deadlock():
    """R3-2/R5-1「buffer 重建 epoch 歸零 → 重宣告不死鎖」：本連線見過 epoch=5 之後斷線，
    agent 端 buffer 重建（health_epoch 歸零），新連線 UpLogin 宣告 health_epoch=0——
    server 若記著舊連線的 max=5 會永久拒收，必須靠 mark_logged_in 重設（覆寫而非取
    max）讓新連線的 epoch=0 ok 被正常接受。"""
    ch = AgentChannel()
    ch.mark_logged_in("F1", health_epoch=0)
    assert ch.note_health(status="ok", health_epoch=5) is True  # 舊連線曾見過 epoch=5

    # 斷線重連：新的 UpLogin 宣告 health_epoch=0（buffer 重建歸零）。
    ch.mark_logged_in("F1", health_epoch=0)
    assert ch.health_status == "unknown"  # pending_health：重宣告後健康狀態重置
    accepted = ch.note_health(status="ok", health_epoch=0)
    assert accepted is True  # 不會被舊連線遺留的 max=5 永久拒收


def test_mark_logged_in_resets_pending_health_and_failstop_flag():
    ch = AgentChannel()
    ch.mark_logged_in("F1", health_epoch=0)
    ch.note_health(status="failstop", health_epoch=1)
    assert ch.failstop is True

    ch.mark_logged_in("F1", health_epoch=2)  # 重連（同帳號）
    assert ch.failstop is False and ch.health_status == "unknown"


# ===========================================================================
# 4. agent_registry.py：server heartbeat lease（G2③）
# ===========================================================================


def _bare_slot(user_id: int) -> UserAgentSlot:
    return UserAgentSlot(
        user_id=user_id, channel=AgentChannel(), gateway=None, adapter=None,
        session_state=OrderSessionState(), supervisor=BrokerSupervisor(), tasks=[],
    )


async def test_health_lease_watchdog_marks_not_ready_after_lease_expires():
    registry = AgentRegistry()
    slot = _bare_slot(1)
    slot.session_state.mark_ready()
    slot.channel.last_ok_heartbeat = time.monotonic() - 100  # 遠早於 lease
    registry.add(slot)

    task = asyncio.create_task(run_health_lease_watchdog(registry, lease_seconds=1.0, interval=0.02))
    try:
        end = time.monotonic() + 2.0
        while time.monotonic() < end and slot.session_state.ready:
            await asyncio.sleep(0.02)
        assert slot.session_state.ready is False
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_health_lease_watchdog_leaves_fresh_ok_slot_ready():
    registry = AgentRegistry()
    slot = _bare_slot(1)
    slot.session_state.mark_ready()
    slot.channel.last_ok_heartbeat = time.monotonic()  # 剛剛才收過 ok
    registry.add(slot)

    task = asyncio.create_task(run_health_lease_watchdog(registry, lease_seconds=1.0, interval=0.02))
    try:
        await asyncio.sleep(0.15)
        assert slot.session_state.ready is True  # 未過期，不被誤標
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_health_lease_watchdog_ignores_slot_never_seen_ok():
    """尚未收過任何 ok（pending_health，`last_ok_heartbeat is None`）——`ready` 本就是
    False，lease 判定天然不適用，watchdog 不該對它做任何事（no-op，不炸）。"""
    registry = AgentRegistry()
    slot = _bare_slot(1)
    registry.add(slot)

    task = asyncio.create_task(run_health_lease_watchdog(registry, lease_seconds=1.0, interval=0.02))
    try:
        await asyncio.sleep(0.1)
        assert slot.session_state.ready is False
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


# ===========================================================================
# 5. runner.py AgentRunner：latch/recover/health sender/native 呼叫前守門（agent 端全鏈）
# ===========================================================================


class _FakeTransport:
    def __init__(self):
        self.sent: list[dict] = []
        self.incoming: asyncio.Queue = asyncio.Queue()
        self.send_block: asyncio.Event | None = None
        self.send_blocked_event: asyncio.Event = asyncio.Event()

    async def connect(self):
        pass

    async def send(self, msg):
        if self.send_block is not None:
            self.send_blocked_event.set()
            await self.send_block.wait()
        self.sent.append(msg)

    async def receive(self):
        return await self.incoming.get()

    async def close(self):
        pass

    def healths(self):
        return [m for m in self.sent if m.get("type") == "health"]

    def rejects(self):
        return [m for m in self.sent if m.get("type") == "cmd_rejected"]


class _FakeChild:
    """Round5：不再提供 `.respawn()` 的假體專屬語意——child 不再原地換血，`ensure_child()`
    （唯一 spawner）只需要 `start()`/`terminate()`/`alive`；`ping()` 供 `_child_watchdog`
    凍結偵測（N6 之後改用 `ping_detail()`）；`poll_failstop()`/`push_failstop()` 供
    `_failstop_watchdog` 模擬 child 落地失敗通知。

    `terminate_calls` 記錄每次呼叫附帶的 `expected_generation`，供測試斷言
    `ChildHandle.terminate()` 的 generation fencing（R3-1）有沒有被正確傳入；
    `terminate_exc`/`refuse_to_die` 保留給需要模擬 terminate 失敗/驗不死的測試使用
    （2026-08-08 之後 `_recover()` 已移除，`AgentRunner` 沒有任何路徑會依賴這兩個欄位
    自動驅動任何行為，純粹是測試替身的通用能力）。"""

    def __init__(self):
        self.alive = False
        self.starts = 0
        self.generation = 0
        self.ops: list[dict] = []
        self.ping_ok = True
        self.latched = False           # N6: ping_detail() 模擬 watchdog 偵測 child latch
        self.request_exc = None
        self.terminate_exc = None
        self.refuse_to_die = False
        self.terminate_calls: list[int | None] = []
        self._failstop_queue: list[dict] = []

    def start(self):
        self.starts += 1
        self.generation += 1
        self.alive = True
        return "F1"

    def request(self, op, *, timeout):
        self.ops.append(op)
        if self.request_exc:
            raise self.request_exc
        return {"ok": True, "result": {"ordno": "101AA1", "broker_order_id": "101AA1"}}

    def ping(self, *, timeout):
        return self.ping_ok

    def ping_detail(self, *, timeout):
        return {"ok": self.ping_ok, "latched": self.latched,
                "generation": self.generation, "fault_seq": 1 if self.latched else 0}

    def terminate(self, *, expected_generation: int | None = None) -> bool:
        self.terminate_calls.append(expected_generation)
        if self.terminate_exc is not None:
            raise self.terminate_exc
        if self.refuse_to_die:
            # N9-1: 比照真 ChildHandle.terminate() 的 False 語意——kill+join 後仍驗到
            # 存活，呼叫端必須消費這個回傳值、不得假裝已清乾淨。
            return False
        self.alive = False
        return True

    def poll_failstop(self, timeout: float = 0.0):
        if self._failstop_queue:
            return self._failstop_queue.pop(0)
        return None

    def push_failstop(self, detail: str = "boom", generation: int | None = None) -> None:
        gen = generation if generation is not None else self.generation
        self._failstop_queue.append({"type": "failstop", "detail": detail, "generation": gen})


async def _until(cond, timeout=3.0):
    async def _poll():
        while not cond():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_poll(), timeout)


def _runner(tr, child, buf, **overrides):
    kwargs = dict(
        transport=tr, buffer=buf, child=child, pump_interval=0.02, resend_after=5.0,
        child_command_timeout=0.5, heartbeat_interval=30, child_ping_interval=30,
        child_ping_timeout=5, backoff_base=0.01, backoff_max=0.05,
        failstop_poll_timeout=0.02,
    )
    kwargs.update(overrides)
    return AgentRunner(**kwargs)


def _place_msg(cmd_id: str) -> dict:
    return {"type": "place", "cmd_id": cmd_id, "account": "F1", "mode": "sim",
            "expires_at": "2099-01-01T00:00:00",
            "native": {"action": "Buy", "price": "0", "qty": 1, "price_type": "MKT",
                       "order_type": "IOC", "octype": "Auto"}}


async def test_child_failstop_notice_latches_agent_bumps_epoch_and_reports_status(tmp_path):
    """G2①/⑤：child 經專用 IPC 通知落地失敗 → agent latch＋epoch+=1＋寫 sentinel，並經
    單一序列化 health sender 回報 status="failstop"。

    flaky 修復備忘（fix/flaky-timing-tests）：`AgentRunner._latch()`（runner.py）拿到
    `_recovery_lock` 後先同步設 `self._latched = True`／`self._health_epoch += 1`，
    「之後」才 `await asyncio.to_thread(self._buffer.write_sentinel, ...)`——sentinel
    落地是 offload 到 thread pool 執行的獨立步驟，不是跟 `_latched` 翻轉同一瞬間完成。
    原本只等 `r._latched` 就直接斷言 `buf.has_sentinel()`，CPU 壓力下 thread pool
    排程延後、sentinel 檔還沒寫出，斷言就會偶發撲空（CI 兩次 PR run 各紅過一次）。
    改成連 `buf.has_sentinel()` 一起等，等到才代表 `_latch()` 這個原子轉移真正走完，
    不是放寬條件——最後仍原樣斷言 `_health_epoch`／`has_sentinel()`。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)
    assert tr.healths()[0] == {"type": "health", "status": "ok", "detail": None, "health_epoch": 0}

    child.push_failstop("buffer 落地失敗")
    await _until(lambda: r._latched and buf.has_sentinel())
    assert r._health_epoch == 1
    assert buf.has_sentinel()

    await _until(lambda: any(h["status"] == "failstop" for h in tr.healths()))
    latest = next(h for h in tr.healths() if h["status"] == "failstop")
    assert latest["health_epoch"] == 1 and latest["detail"] == "buffer 落地失敗"

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_latched_agent_rejects_mutating_command_via_upcommandrejected_not_outbox(tmp_path):
    """G2②：latch 中的 mutating 指令一律 volatile `UpCommandRejected` 直送，不進 outbox
    （不是 cmd_ack）、native 完全沒被呼叫。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)

    child.push_failstop("boom")
    await _until(lambda: r._latched)

    tr.incoming.put_nowait(_place_msg("c1"))
    await _until(lambda: len(tr.rejects()) >= 1)
    rej = tr.rejects()[0]
    assert rej["cmd_id"] == "c1" and rej["error_kind"] == "failstop"
    assert child.ops == []           # native 完全沒被呼叫
    assert buf.pending() == []       # 不是 cmd_ack，沒有進 outbox
    assert not any(m.get("type") == "cmd_ack" for m in tr.sent)

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_latch_during_ledger_replay_still_returns_cached_ack_not_rejected(tmp_path):
    """G2②的界線：latch 只擋「尚未真正執行過」的指令——已經真的執行過、ledger 命中的重播
    （from-outbox at-least-once）不該被 latch 攔下，存檔結果才是唯一誠實的答案。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)

    tr.incoming.put_nowait(_place_msg("c1"))
    await _until(lambda: any(m.get("type") == "cmd_ack" for m in tr.sent))
    assert len(child.ops) == 1
    first_ack = next(m for m in tr.sent if m["type"] == "cmd_ack")

    # 先讓 server 確認收到第一筆 ack（不然 outbox 那筆列一直是未送狀態，
    # ensure_cmd_ack_pending 之後找到同一筆未送列就不會新增列，_pump 也要等
    # resend_after 才會重送，拖慢測試——比照既有 test_agent_child.py 既有測試手法）。
    tr.incoming.put_nowait({"type": "report_ack", "event_id": first_ack["event_id"]})
    await _until(lambda: buf.unsent_count() == 0)

    child.push_failstop("boom")
    await _until(lambda: r._latched)

    tr.incoming.put_nowait(_place_msg("c1"))  # 重連補送同一 cmd_id
    await _until(lambda: len([m for m in tr.sent if m.get("type") == "cmd_ack"]) >= 2)
    assert len(child.ops) == 1  # 沒有重打 native
    assert tr.rejects() == []   # 不是拒絕，是重送存檔 ack

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


# ===========================================================================
# R3-3（MEDIUM，codex 終審 round3）：failstop notice 帶 child generation——respawn 換代後
# 才被取出的舊 notice 不得誤 latch 目前健康的新 child。
# ===========================================================================


async def test_stale_generation_failstop_notice_does_not_latch_current_child(tmp_path):
    """模擬「舊 child 臨終前排進 pipe、respawn 完成後才被取出」的過期通知（generation=0，
    早於目前 `ensure_child()` 建立的第 1 代）——`_failstop_watchdog` 必須丟棄，完全不誤
    latch 目前這一代健康的 child。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()   # child.generation 現在是 1
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)

    child.push_failstop("舊 child 的過期通知", generation=0)
    await asyncio.sleep(0.15)
    assert r._latched is False
    assert not buf.has_sentinel()
    assert not any(h["status"] == "failstop" for h in tr.healths())

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_current_generation_failstop_notice_still_latches(tmp_path):
    """反面對照：generation 相符（目前這一代 child 真的送出的通知）仍必須正常 latch——
    R3-3 的修法不能連正常路徑都一起擋掉。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()   # child.generation 現在是 1
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)

    child.push_failstop("目前這一代的真實故障", generation=1)
    await _until(lambda: r._latched)
    assert buf.has_sentinel()

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


# ===========================================================================
# R4（codex 終審 round4）：round3 修復的收窄版 TOCTOU/生命週期殘餘。
#
# Round5 收斂後的清理：`_respawn_child()`／`ChildHandle.respawn()` 已整個移除（recovery
# 不再原地換血 child），round4 專屬針對這兩者的四支測試（respawn 呼叫端取消後背景
# thread 收割、respawn 過期 no-op、respawn 後清 sentinel 前重驗、persist_only 輪次重驗）
# 隨之移除——這些危險情境的前提（一個活著的 asyncio session 內原子替換 child）在新設計
# 下結構性不存在。`ChildHandle` 本身的 `_lock`/`_generation` fencing（R3-1，保護
# `start()`/`terminate()`/`_rpc()`/`poll_failstop()`）不受影響，繼續在
# `test_agent_runner.py` 驗證。`_latch()` 的 generation 二次核對（R4-c）與新故障/latch
# 本身無關，下面這支測試繼續有效、原樣保留。
# ===========================================================================


async def test_watchdog_notice_generation_recheck_inside_latch_lock_after_regenerate(tmp_path):
    """R4-c（MEDIUM）：`_failstop_watchdog` 在鎖外核對過 notice 的 generation 與當下相符
    後才呼叫 `_latch()`——但「核對通過」與「_latch() 真正拿到 `_recovery_lock`」之間仍有
    一段沒有互斥的窗口。這裡先佔住 `_recovery_lock`，推一則 generation 相符的通知（能通過
    鎖外核對），確認 watchdog 已經卡在等鎖之後，才讓 child 換代，再放鎖——`_latch()` 拿到
    鎖後必須重新核對，發現已經不符，no-op：不誤把這則過期通知套用到目前這一代健康的
    child。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()   # child.generation 現在是 1
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)

    await r._recovery_lock.acquire()   # 模擬鎖被另一條路徑（例如另一次 recovery）佔住
    child.push_failstop("boom", generation=1)   # 與目前 generation 相符，通過鎖外初次核對
    await asyncio.sleep(0.1)   # 讓 _failstop_watchdog 跑過鎖外核對，卡在等 _recovery_lock

    child.generation = 2   # 模擬鎖被佔住的這段期間，child 已經換代（例如另一次 respawn 完成）
    r._recovery_lock.release()

    await asyncio.sleep(0.15)
    assert r._latched is False          # no-op：沒有把過期通知套用到目前這一代 child
    assert not buf.has_sentinel()
    assert not any(h["status"] == "failstop" for h in tr.healths())

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_stuck_send_does_not_block_new_latch_and_stale_queued_frame_is_dropped(tmp_path):
    """R3-3/R4-1/R5-1：一個 health frame 卡在 `transport.send()`（模擬網路卡住）時，新的
    failstop 仍能立刻 latch＋epoch++（不被卡住的 send 拖住）；排隊中尚未出隊的舊 epoch
    frame 在出隊時重驗發現已過期，直接丟棄，不會被誤送成一則「舊 epoch 的 ok」。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf, heartbeat_interval=30)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)  # 初始 login 觸發的 health 已送達

    tr.send_block = asyncio.Event()
    r._health_queue.put_nowait(r._health_epoch)   # frame A：sender 即將拿到、卡在 send
    r._health_queue.put_nowait(r._health_epoch)   # frame B：卡在佇列裡，尚未出隊
    await asyncio.wait_for(tr.send_blocked_event.wait(), timeout=2)  # 確認 sender 已卡住

    await asyncio.wait_for(r._latch("stuck-send-race"), timeout=1)  # 不被卡住的 send 拖住
    assert r._latched and r._health_epoch == 1

    tr.send_block.set()  # 放行卡住的 send（frame A，epoch=0 ok——已交付 wire，無法撤回）
    await _until(lambda: any(
        h["health_epoch"] == 1 and h["status"] == "failstop" for h in tr.healths()
    ))

    ok_epoch0_count = len([h for h in tr.healths() if h["health_epoch"] == 0 and h["status"] == "ok"])
    assert ok_epoch0_count == 2  # 初始一筆 + frame A；frame B（排隊中的舊 epoch）被出隊重驗丟棄

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_transient_ready_window_downlink_command_still_rejected_by_agent_latch(tmp_path):
    """G2 重定義的安全鏈（權威安全點在 agent latch）：即使 server 端因為傳播延遲仍樂觀地
    以為 ready（本測試不模擬 server，只證明 agent 側本身的權威守門），latch 之後任何下行
    的 mutating 指令一律先過 latch 檢查再談 native——不存在「latch 生效後還有機會執行
    native」的窗口。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)

    await r._latch("race")  # 模擬 latch 生效的瞬間（child 通知已處理完畢）
    assert r._latched

    # 立刻送一個「假裝 server 還不知道」的下行指令。
    tr.incoming.put_nowait(_place_msg("c-race"))
    await _until(lambda: len(tr.rejects()) >= 1)
    assert child.ops == []  # native 從未被呼叫——latch 是絕對的守門，不是機率性的

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_agent_startup_loads_durable_latch_from_sentinel(tmp_path):
    """G2①：sentinel 存在＝latch（durable，重啟仍在效）——agent 程序重啟（新 AgentRunner
    實例，同一個 buffer 檔）時，建構子必須從 sentinel 恢復 latch 狀態，不會假裝一切正常。"""
    path = tmp_path / "o.db"
    pre = DurableBuffer(path)
    pre.write_sentinel(epoch=7, detail="上次崩潰前留下的 latch")

    tr, child = _FakeTransport(), _FakeChild()
    r = _runner(tr, child, DurableBuffer(path))
    assert r._latched is True
    assert r._health_epoch == 7

    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)
    assert tr.healths()[0]["status"] == "failstop" and tr.healths()[0]["health_epoch"] == 7
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_corrupted_sentinel_fails_closed_keeps_latched_never_reports_ok(tmp_path):
    """N9-3（MEDIUM，codex 終審 round9）：sentinel 檔存在但讀取/解析失敗（模擬檔案損毀）
    ——舊版 `read_sentinel()` 把這種情況跟「真的沒有 sentinel」混為一談都回 `None`，
    `_load_persisted_health` 因此會誤判「無 latch」，讓一個其實還在 failstop 的 agent 上報
    `status="ok"`（fail-open，本 fix 要堵的洞）。修法後 `read_sentinel()` 存在但讀不出來
    會 raise `SentinelUnreadableError`，`_load_persisted_health` 對此 fail-closed：維持
    latch（epoch 退回 buffer meta，因為 sentinel 本身讀不到），不誤判為健康。

    比照 `test_agent_startup_loads_durable_latch_from_sentinel`（sentinel 正常存在的
    對照組）直測 `_load_persisted_health`／`run_once()`，不經 `_startup_recovery_probe`
    （那是另一層，只探測 SQLite 本身是否健康、不讀 sentinel，不在本測試範圍內）。"""
    path = tmp_path / "o.db"
    pre = DurableBuffer(path)
    pre.set_health_epoch(9)
    # sentinel 檔案本身損毀（模擬權限錯誤/內容毀損）：檔案存在，但讀不出正確內容。
    (tmp_path / "o.db.failstop").write_text("{broken", encoding="utf-8")

    tr, child = _FakeTransport(), _FakeChild()
    r = _runner(tr, child, DurableBuffer(path))
    assert r._latched is True     # fail-closed：不是誤判為「無 latch」
    assert r._health_epoch == 9   # epoch 退回 buffer meta（sentinel 本身讀不到）

    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)
    assert tr.healths()[0]["status"] == "failstop"
    assert not any(h["status"] == "ok" for h in tr.healths())   # 全程沒有假 healthy

    tr.incoming.put_nowait(_place_msg("c-corrupted-sentinel"))
    await _until(lambda: len(tr.rejects()) >= 1)
    assert child.ops == []   # native 完全沒被呼叫，agent 全程 failstop

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


# ===========================================================================
# 5b. AgentRunner._startup_recovery_probe（2026-08-08，設計降級，使用者拍板）：G2 恢復
# 降級為「只在 agent 程序啟動時做一次 storage probe」，取代 session 進行中的自動恢復——
# `_recover()`/`_recovery_prober()`/`SessionRestartRequested` 整組移除。8 輪終審逐輪打磨
# 出的 in-session recovery（dying-gasp/terminate 驗死/generation fencing）在這個時間點
# 結構性不需要：`run_forever()` 呼叫 `_startup_recovery_probe()` 時 `ensure_child()` 尚未
# 被呼叫過，沒有 child、沒有並發 sentinel writer。
# ===========================================================================


async def test_startup_probe_fails_keeps_latched_agent_runs_in_failstop_mode(
    tmp_path, monkeypatch,
):
    """probe 失敗（buffer 仍壞）：`run_forever()` 開頭的啟動探測不通過，agent 保持
    latched 以 failstop 模式運行——不重試、不清 sentinel，新指令繼續被拒。"""
    path = tmp_path / "o.db"
    pre = DurableBuffer(path)
    pre.write_sentinel(epoch=3, detail="上次崩潰前留下的 latch")

    buf = DurableBuffer(path)
    monkeypatch.setattr(buf, "probe", lambda: False)
    tr, child = _FakeTransport(), _FakeChild()
    r = _runner(tr, child, buf)
    assert r._latched is True

    task = asyncio.create_task(r.run_forever())
    await _until(lambda: len(tr.healths()) >= 1)
    assert tr.healths()[0]["status"] == "failstop" and tr.healths()[0]["health_epoch"] == 3
    assert r._latched is True
    assert buf.has_sentinel()   # 沒被清掉

    tr.incoming.put_nowait(_place_msg("c-probe-fail"))
    await _until(lambda: len(tr.rejects()) >= 1)
    assert child.ops == []   # native 完全沒被呼叫，agent 全程 failstop

    r.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_startup_probe_passes_clears_sentinel_unlatches_and_reports_ok(tmp_path):
    """probe 通過（buffer 本身完好，只是上次崩潰留下 sentinel）：`run_forever()` 開頭的
    啟動探測必須在第一個 session 開始之前就清掉 sentinel、解除 latch——第一筆 login 後
    的健康訊框應直接是 status="ok"，不會像舊版 session-restart 那樣先送一筆 failstop、
    再等第二個 session 才恢復（這裡只有一筆 login）。"""
    path = tmp_path / "o.db"
    pre = DurableBuffer(path)
    pre.write_sentinel(epoch=5, detail="上次崩潰前留下的 latch")

    tr, child = _FakeTransport(), _FakeChild()
    r = _runner(tr, child, DurableBuffer(path))
    assert r._latched is True

    task = asyncio.create_task(r.run_forever())
    await _until(lambda: len(tr.healths()) >= 1)
    assert r._latched is False
    assert tr.healths()[0]["status"] == "ok" and tr.healths()[0]["health_epoch"] == 5
    assert len([m for m in tr.sent if m["type"] == "login"]) == 1   # 沒有 session-restart

    fresh = DurableBuffer(path)
    assert not fresh.has_sentinel()
    assert fresh.get_health_epoch() == 5

    r.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_latch_never_auto_clears_during_running_session_only_next_process_start(
    tmp_path,
):
    """session 進行中 latch 後永不自動解除（G2 手動重啟恢復的核心保證）：child 經 IPC
    通知父程序 latch 之後，即使 buffer 本身完好（probe 若被呼叫必定會過）、session 一直
    存活、時間經過遠超過舊版 `_recovery_prober` 的探測間隔，agent 也不會自己清除 latch——
    必須等下一次 `AgentRunner`/`run_forever()`（模擬程序重啟）才會重新探測並恢復。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    task = asyncio.create_task(r.run_forever())
    await _until(lambda: len(tr.healths()) >= 1)

    child.push_failstop("boom")
    await _until(lambda: r._latched)
    assert buf.has_sentinel()

    # 存活期間持續跑一段時間（buffer 本身完好，若有任何背景重驗機制會通過）——舊版
    # `_recovery_prober` 預設每 5 秒探一次，這裡多等幾輪確認沒有任何自動恢復發生。
    await asyncio.sleep(0.3)
    assert r._latched is True
    assert buf.has_sentinel()
    assert not any(h["status"] == "ok" and h["health_epoch"] == r._health_epoch
                   for h in tr.healths())
    assert len([m for m in tr.sent if m["type"] == "login"]) == 1   # 同一個 session 沒有重啟

    r.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    # 模擬「使用者手動重啟 agent 程序」：新的 AgentRunner 實例（同一個 buffer 檔）在
    # 建構子讀到仍在效的 sentinel、`run_forever()` 開頭的啟動探測這次會清掉它。
    r2 = _runner(tr, _FakeChild(), buf)
    assert r2._latched is True   # 建構子照舊從 sentinel 恢復 latch 狀態
    task2 = asyncio.create_task(r2.run_forever())
    await _until(lambda: not r2._latched, timeout=3)
    assert not buf.has_sentinel()
    r2.stop()
    task2.cancel()
    await asyncio.gather(task2, return_exceptions=True)


async def test_startup_probe_persist_failure_keeps_latch(tmp_path, monkeypatch):
    """C3 邏輯沿用：probe 通過但持久化步驟（epoch 寫入／sentinel 清除）失敗——不得讓
    in-memory 狀態「假裝恢復」，必須保持 latched，等下一次程序重啟再試。"""
    path = tmp_path / "o.db"
    pre = DurableBuffer(path)
    pre.write_sentinel(epoch=2, detail="上次崩潰前留下的 latch")

    buf = DurableBuffer(path)
    monkeypatch.setattr(
        buf, "set_health_epoch",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("epoch persist boom")),
    )
    tr, child = _FakeTransport(), _FakeChild()
    r = _runner(tr, child, buf)
    assert r._latched is True

    task = asyncio.create_task(r.run_forever())
    await _until(lambda: len(tr.healths()) >= 1)
    assert r._latched is True
    assert tr.healths()[0]["status"] == "failstop"
    assert buf.has_sentinel()

    r.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


# ===========================================================================
# 6. agent_ws.py：G2 全鏈（server 端，經真 WS 連線驗證，S#6/19）
# ===========================================================================


class _FakeHub:
    def __init__(self):
        self.publishes = 0

    def publish(self):
        self.publishes += 1


class _FakeAdapter:
    def __init__(self):
        self.account = ""
        self.reconcile_calls = 0

    async def reconcile(self):
        self.reconcile_calls += 1


class _FakeRiskGuard:
    def __init__(self, owner_ids):
        self._owner_ids = set(owner_ids)

    def is_owner(self, user_id):
        return user_id in self._owner_ids


class _SpyOpsAlerter:
    def __init__(self):
        self.failstop_calls: list[dict] = []
        self.emits: list[dict] = []

    def failstop(self, *, user_id, enabled, detail=""):
        self.failstop_calls.append({"user_id": user_id, "enabled": enabled, "detail": detail})

    def emit(self, key, title, detail="", *, severity="warn", throttle=0.0):
        self.emits.append({"key": key, "title": title, "detail": detail})


def _wait(cond, timeout=2.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.02)
    return False


def _slot(app):
    return app.state.agent_registry.get(app.state.agent_test_owner_id)


@pytest.fixture
def ws_env(engine, monkeypatch):
    get_settings.cache_clear()
    app = create_app()

    def _session_override():
        with Session(engine) as s:
            yield s

    app.dependency_overrides[get_session] = _session_override
    with Session(engine) as s:
        owner = auth_service.create_user(s, "agent-owner", "pw", role="admin")
        owner_id = owner.id
        token = issue_token(s, user_id=owner_id, ttl_days=30)
    slot = UserAgentSlot(
        user_id=owner_id, channel=AgentChannel(), gateway=None, adapter=_FakeAdapter(),
        session_state=OrderSessionState(), supervisor=BrokerSupervisor(), tasks=[],
    )
    registry = AgentRegistry()
    registry.add(slot)
    app.state.agent_registry = registry
    app.state.order_events = _FakeHub()
    app.state.order_session_factory = lambda: Session(engine)
    app.state.order_risk_guard = _FakeRiskGuard({owner_id})
    app.state.ops_alerter = _SpyOpsAlerter()
    app.state.agent_test_token = token
    app.state.agent_test_owner_id = owner_id
    yield app
    get_settings.cache_clear()


def test_upheath_ok_after_login_marks_ready_failstop_before_that_never_ready(ws_env):
    """G2⑥：UpLogin 後未收本連線 ok 前不 ready（pending_health）。"""
    client = TestClient(ws_env)
    with client.websocket_connect(
        "/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}
    ) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2,
                     "health_epoch": 0})
        assert _wait(lambda: _slot(ws_env).adapter.account == "F1")
        assert _slot(ws_env).session_state.ready is False  # 未收 ok 前

        ws.send_json({"type": "health", "status": "ok", "health_epoch": 0})
        assert _wait(lambda: _slot(ws_env).session_state.ready)


def test_g2_full_chain_failstop_notready_alert_reject_recover_ready(ws_env, engine):
    """S#6/19 全鏈：login→ok(ready)→UpHealth(failstop)→server not-ready＋OpsAlerter 告警→
    UpCommandRejected→applier CAS 落 acked_error（place→failed＋release，D4 轉移表）→
    UpHealth(ok，同 epoch)→server 恢復 ready。"""
    owner_id = ws_env.state.agent_test_owner_id
    with Session(engine) as s:
        order = brepo.create_order(
            s, client_order_id="c-fs-1", request_hash="H", user_id=owner_id, mode="sim",
            broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
            price=Decimal("21500"), price_type="LMT", order_type="ROD", octype="Auto",
            trading_day="2026-08-07",
        )
        order.status = "unknown"
        s.add(order)
        brepo.reserve_quota(s, reservation_id="c-fs-1", user_id=owner_id, mode="sim",
                            trading_day="2026-08-07", qty=1, daily_limit=100)
        s.add(AgentCommand(
            cmd_id="cmd-fs-1", user_id=owner_id, kind="place", broker="shioaji",
            account="F1", mode="sim", client_order_id="c-fs-1", reservation_id="c-fs-1",
            payload=json.dumps({"action": "Buy", "price": "21500", "qty": 1, "price_type": "LMT",
                                 "order_type": "ROD", "octype": "Auto"}),
            expires_at=dt.datetime(2099, 1, 1),
        ))
        s.commit()

    client = TestClient(ws_env)
    with client.websocket_connect(
        "/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}
    ) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2,
                     "health_epoch": 0})
        ws.send_json({"type": "health", "status": "ok", "health_epoch": 0})
        assert _wait(lambda: _slot(ws_env).session_state.ready)

        # ① agent latch → 上報 status="failstop"，epoch 單調 +1。
        ws.send_json({"type": "health", "status": "failstop", "health_epoch": 1,
                     "detail": "buffer 落地失敗"})
        assert _wait(lambda: _slot(ws_env).session_state.ready is False)
        assert _wait(lambda: len(ws_env.state.ops_alerter.failstop_calls) == 1)
        alert = ws_env.state.ops_alerter.failstop_calls[0]
        assert alert["enabled"] is True and alert["user_id"] == owner_id

        # ② latch 期間拒新指令 → server applier CAS 落 acked_error，依 D4 轉移表 place
        # 明確拒絕 → failed ＋ release quota。
        ws.send_json({"type": "cmd_rejected", "cmd_id": "cmd-fs-1", "error_kind": "failstop"})

        def _resolved():
            with Session(engine) as s:
                row = s.get(AgentCommand, "cmd-fs-1")
                return row is not None and row.resolved_at is not None
        assert _wait(_resolved)

        with Session(engine) as s:
            cmd_row = s.get(AgentCommand, "cmd-fs-1")
            assert cmd_row.outcome == "error" and cmd_row.resolved_via == "ack"
            o = s.exec(select(Order).where(Order.client_order_id == "c-fs-1")).one()
            assert o.status == "failed"
            reservation = s.exec(
                select(QuotaReservation).where(QuotaReservation.reservation_id == "c-fs-1")
            ).one()
            assert reservation.state == "released"

        # ③ probe 通過 → 恢復（同 epoch，不是新 epoch）→ server ready。
        ws.send_json({"type": "health", "status": "ok", "health_epoch": 1})
        assert _wait(lambda: _slot(ws_env).session_state.ready)
        assert _wait(lambda: len(ws_env.state.ops_alerter.failstop_calls) == 2)
        assert ws_env.state.ops_alerter.failstop_calls[1]["enabled"] is False


def test_stale_lower_epoch_ok_after_failstop_does_not_resurrect_server_ready(ws_env):
    """R5-1：見過較大 epoch（failstop）後，遲到的舊 ok（epoch 較小）一律忽略，server 端
    ready 不被誤救回。"""
    client = TestClient(ws_env)
    with client.websocket_connect(
        "/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}
    ) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2,
                     "health_epoch": 0})
        ws.send_json({"type": "health", "status": "ok", "health_epoch": 0})
        assert _wait(lambda: _slot(ws_env).session_state.ready)

        ws.send_json({"type": "health", "status": "failstop", "health_epoch": 1})
        assert _wait(lambda: _slot(ws_env).session_state.ready is False)

        # 遲到的舊 ok（epoch=0，發生在 failstop 之前）——必須被忽略。
        ws.send_json({"type": "health", "status": "ok", "health_epoch": 0})
        time.sleep(0.2)
        assert _slot(ws_env).session_state.ready is False  # 沒有被誤救回


def test_reconnect_after_buffer_rebuild_lower_epoch_is_accepted_no_deadlock(ws_env):
    """R3-2/R5-1「buffer 重建 epoch 歸零→重宣告不死鎖」：第一條連線見過 epoch=5，斷線後
    第二條連線（新 generation）宣告 health_epoch=0（模擬 agent 端 buffer 重建歸零）——
    server 必須接受這次重宣告的較低 epoch，不會因為記著舊連線的高 epoch 而永久卡
    pending_health。"""
    client = TestClient(ws_env)
    with client.websocket_connect(
        "/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}
    ) as ws1:
        ws1.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2,
                      "health_epoch": 5})
        ws1.send_json({"type": "health", "status": "ok", "health_epoch": 5})
        assert _wait(lambda: _slot(ws_env).session_state.ready)

    with client.websocket_connect(
        "/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}
    ) as ws2:
        ws2.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2,
                      "health_epoch": 0})
        assert _slot(ws_env).session_state.ready is False  # pending_health
        ws2.send_json({"type": "health", "status": "ok", "health_epoch": 0})
        assert _wait(lambda: _slot(ws_env).session_state.ready)  # 沒有死鎖


def test_disconnect_and_failstop_transitions_do_not_spam_connect_disconnect_alerts(ws_env):
    """D9：連線/斷線本身不告警——只有健康語意真的轉換（進入/解除 failstop）才報。"""
    client = TestClient(ws_env)
    with client.websocket_connect(
        "/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}
    ) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2,
                     "health_epoch": 0})
        ws.send_json({"type": "health", "status": "ok", "health_epoch": 0})
        assert _wait(lambda: _slot(ws_env).session_state.ready)
    assert _wait(lambda: _slot(ws_env).session_state.disabled)  # 斷線
    assert ws_env.state.ops_alerter.failstop_calls == []  # 連線/斷線本身不告警


def test_failstop_detail_not_leaked_to_agent_status_badge(ws_env, monkeypatch):
    """Task 12 審查必修（Task 13 落地）：`UpHealth.detail` 是 agent 端本機的原始例外字串
    （可能夾帶英文例外類名/agent 本機檔案路徑），orders 頁 badge 是一般 owner 使用者都看得到
    的 UI（不是維運限定的 /healthz）——不得原樣顯示，只能顯示固定的繁體通用訊息；原始 detail
    只能進 log，不得外洩內部路徑/英文例外字串。"""
    monkeypatch.setenv("ORDER_CHANNEL", "agent")
    get_settings.cache_clear()
    owner_id = ws_env.state.agent_test_owner_id
    client = TestClient(ws_env)
    try:
        with client.websocket_connect(
            "/ws/agent", headers={"x-agent-token": ws_env.state.agent_test_token}
        ) as ws:
            ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2,
                         "health_epoch": 0})
            ws.send_json({"type": "health", "status": "ok", "health_epoch": 0})
            assert _wait(lambda: _slot(ws_env).session_state.ready)

            raw_detail = "Traceback: FileNotFoundError: /Users/henry/.secret/o.db.failstop"
            ws.send_json({"type": "health", "status": "failstop", "health_epoch": 1,
                         "detail": raw_detail})
            assert _wait(lambda: _slot(ws_env).session_state.ready is False)

            client.cookies.set(SESSION_COOKIE, sign_session(owner_id, 0))
            badge = client.get("/orders/agent-status").text
            assert "agent 儲存故障，交易已停止" in badge
            assert raw_detail not in badge
            assert "FileNotFoundError" not in badge
            assert "/Users/henry" not in badge
    finally:
        get_settings.cache_clear()

