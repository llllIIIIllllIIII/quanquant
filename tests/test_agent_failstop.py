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
    def __init__(self):
        self.alive = False
        self.starts = 0
        self.generation = 0
        self.ops: list[dict] = []
        self.ping_ok = True
        # R3-2（codex 終審 round3）：respawn 後確認用的 ping_detail 額外回報「新 child 是
        # 否又立即 latch」——測試預設 False（respawn 乾淨成功），個別測試可設 True 模擬
        # 新 child 在 connect 後、清 sentinel 前又故障一次的假 healthy 縫。
        self.respawn_latched = False
        self.request_exc = None
        # R3-4（codex 終審 round3）：respawn 專屬 backoff／staged recovery 測試用——
        # >0 時 start() 連續丟例外（模擬 respawn 失敗），每呼叫一次遞減。
        self.fail_starts_remaining = 0
        self._failstop_queue: list[dict] = []

    def start(self):
        self.starts += 1
        if self.fail_starts_remaining > 0:
            self.fail_starts_remaining -= 1
            raise RuntimeError("模擬 respawn 失敗")
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
        if not self.ping_ok:
            return {"ok": False, "latched": None}
        return {"ok": True, "latched": self.respawn_latched}

    def terminate(self):
        self.alive = False

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
        recovery_probe_interval=0.03, failstop_poll_timeout=0.02,
        # R3-4：respawn 專屬 backoff 的 class 預設值是 5s/300s（避免正式環境每 5 秒真登入
        # 一次）——測試預設縮小到跟 recovery_probe_interval 同一數量級，讓 recovery 測試
        # 不必依賴「系統開機時間已經超過 5 秒」這種隱性、脆弱的前提（`time.monotonic()`
        # 在部分平台是量測開機時間，不是行程啟動時間）。要測 backoff 本身遞增/重設行為的
        # 測試會用 overrides 明確覆寫回較大的值。
        respawn_backoff_base=0.01, respawn_backoff_max=0.05,
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
    單一序列化 health sender 回報 status="failstop"。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)
    assert tr.healths()[0] == {"type": "health", "status": "ok", "detail": None, "health_epoch": 0}

    child.push_failstop("buffer 落地失敗")
    await _until(lambda: r._latched)
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


async def test_recover_after_probe_passes_restores_ok_clears_sentinel_sets_epoch(tmp_path):
    """G2④/⑦：storage probe 通過（buf 本身完好，只是被手動 latch）→ 解除 latch、清
    sentinel、`health_epoch` 落回 buffer meta、單一序列化 sender 回報最新 status="ok"。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)

    child.push_failstop("boom")
    await _until(lambda: r._latched)

    await _until(lambda: not r._latched, timeout=3)  # 下一輪 probe 自然通過（buf 本身完好）
    assert not buf.has_sentinel()
    assert buf.get_health_epoch() == r._health_epoch == 1

    await _until(lambda: any(
        h["status"] == "ok" and h["health_epoch"] == 1 for h in tr.healths()
    ))

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_recover_respawns_child_and_new_mutating_op_executes_after_recovery(tmp_path):
    """N2（HIGH，codex 終審 round2）：`ChildFailstopLatch`（native_runner.py）在 child 進程內
    無 rearm、永久單向 trip——recovery 若只翻 parent 的 `_latched=False`，child 內部的本地
    latch 依然 tripped，之後任何 mutating RPC 送到 child 仍會被 `_dispatch` 擋下回
    failstop，形成「parent 回報 healthy，交易卻永久失敗」的假 healthy。驗證：probe 通過後
    recovery 必須真的把 child 換掉（`starts` 計數增加＝可觀察的「舊 child 物件被換掉」訊號）
    ，且換掉之後的新 mutating 指令能正常執行、拿到 ok 的 cmd_ack（不是被本地 latch 擋下）。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    starts_before_recovery = child.starts
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)

    child.push_failstop("boom")
    await _until(lambda: r._latched)

    await _until(lambda: not r._latched, timeout=3)  # probe 通過→respawn 成功→ping 確認過
    assert child.starts == starts_before_recovery + 1  # 真的重啟過一次 child（respawn）

    tr.incoming.put_nowait(_place_msg("c-after-recover"))
    await _until(lambda: any(m.get("type") == "cmd_ack" for m in tr.sent))
    ack = next(m for m in tr.sent if m["type"] == "cmd_ack")
    assert ack["ok"] is True  # 新 child（全新本地 latch）正常放行，不是被舊 latch 擋下
    assert any(op.get("op") == "place" for op in child.ops)

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_recover_stays_latched_when_child_respawn_ping_fails(tmp_path):
    """N2：probe 通過，但 respawn 後用來確認新 child 真的可用的 ping 失敗——不能就地翻
    `_latched=False`（新 child 未必真的可用，parent 卻已經對外回報 healthy）。保留 latch，
    交下一輪 `_recovery_prober` 重試，且不送出任何 `status="ok"` 的健康訊框、sentinel 仍在
    （C3 的順序保證延伸：respawn 這一步失敗，等同持久化步驟失敗，整段不生效）。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)

    child.push_failstop("boom")
    await _until(lambda: r._latched)

    child.ping_ok = False  # respawn 本身（terminate/start）仍會成功，但確認 ping 會失敗

    await asyncio.sleep(0.15)  # 讓至少一輪 _recovery_prober 跑過（probe 過、respawn 後 ping 敗）
    assert r._latched is True
    assert not any(h["health_epoch"] == 1 and h["status"] == "ok" for h in tr.healths())
    assert buf.has_sentinel()  # 沒被清掉，下次啟動仍會正確載入 latch

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_recover_stays_latched_when_epoch_persist_fails(tmp_path, monkeypatch):
    """C3（HIGH，codex 終審）：`_recover()` 的兩個持久化步驟（先 epoch、後 sentinel）任一
    失敗都必須保留 latch、不送 ok——舊版先翻 `_latched=False` 才做持久化，部分失敗會讓
    in-memory 狀態「假裝恢復」（G2②的 latch 檢查放行 mutating 指令），但 durable 狀態
    其實沒有真的恢復。這裡讓 `set_health_epoch` 直接 raise，驗證 probe 通過後仍卡在
    latched、沒有任何 `status="ok"` 的健康訊框送出。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)

    child.push_failstop("boom")
    await _until(lambda: r._latched)

    monkeypatch.setattr(
        buf, "set_health_epoch", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("epoch persist boom"))
    )

    await asyncio.sleep(0.15)  # 讓至少一輪 _recovery_prober 跑過（probe 會過，epoch 寫入會炸）
    assert r._latched is True  # 仍 latched
    # 注意：tr.healths() 累積連線存續期間送過的所有健康訊框，含 login 後立刻送出的初始
    # epoch=0 "ok"（latch 之前）——這裡要驗證的是「latch 之後（epoch=1）沒有任何 ok 被送
    # 出」，不是「從來沒有送過 ok」。
    assert not any(h["health_epoch"] == 1 and h["status"] == "ok" for h in tr.healths())
    assert buf.has_sentinel()  # sentinel 仍在（沒被清掉）

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_recover_stays_latched_when_sentinel_clear_fails(tmp_path, monkeypatch):
    """C3：反過來——epoch 寫入成功但 sentinel 清除失敗，一樣要保持 latch、不送 ok。且驗證
    「先 epoch 後 sentinel」的順序意圖：epoch 已經真的持久化，下次啟動即使 sentinel 仍在，
    讀到的 epoch 也是正確的最新值，不會用到落後的舊值。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)

    child.push_failstop("boom")
    await _until(lambda: r._latched)

    monkeypatch.setattr(
        buf, "clear_sentinel", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("sentinel clear boom"))
    )

    await asyncio.sleep(0.15)
    assert r._latched is True
    # 同上一個測試：只驗「latch 之後（epoch=1）沒有 ok」，不是「從來沒有送過 ok」。
    assert not any(h["health_epoch"] == 1 and h["status"] == "ok" for h in tr.healths())
    assert buf.get_health_epoch() == 1  # epoch 已經真的持久化（先 epoch 後 sentinel 的順序）
    assert buf.has_sentinel()  # sentinel 清除失敗，仍在

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


# ===========================================================================
# R3-2（HIGH，codex 終審 round3）：recovery ping 必須查 child latch，不能只看 ok=True——
# 否則新 child 在 connect 後、清 sentinel 前又 latch 時，會被誤判成 healthy。
# ===========================================================================


async def test_recover_stays_latched_when_respawned_child_immediately_relatches(tmp_path):
    """respawn 本身（terminate/start/帳號核對）都成功，但確認用的 `ping_detail()` 回報新
    child 本地 latch 已經又 tripped（模擬新 child 在 connect 後、清 sentinel 前又故障一次）
    ——不能清 sentinel／翻 healthy，必須保留 latch，交下一輪重試，且沒有任何
    `status="ok"`（epoch=1）的健康訊框送出。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)

    child.push_failstop("boom")
    await _until(lambda: r._latched)

    child.respawn_latched = True  # 模擬新 child 連上、確認 ping 前又立即 latch
    await asyncio.sleep(0.15)
    assert r._latched is True
    assert not any(h["health_epoch"] == 1 and h["status"] == "ok" for h in tr.healths())
    assert buf.has_sentinel()

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_recover_succeeds_once_respawned_child_latch_clears(tmp_path):
    """反過來驗證正路：一開始 respawn 後又立即 latch（保留 latch），之後（模擬人工排除
    根因）新一輪 respawn 不再 latch——recovery 應該能正常完成，不會被卡死。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)

    child.push_failstop("boom")
    await _until(lambda: r._latched)
    child.respawn_latched = True

    await _until(lambda: child.starts >= 2, timeout=3)  # 至少 respawn 過一次（仍卡在 latch）
    assert r._latched is True

    child.respawn_latched = False  # 根因排除：下一輪 respawn 就不會又 latch
    await _until(lambda: not r._latched, timeout=3)
    assert buf.get_health_epoch() == r._health_epoch == 1

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
# R3-4（HIGH，codex 終審 round3）：respawn-login-quota——respawn 專屬 backoff＋jitter＋
# 上限；FatalAgentError 不得被吞；staged recovery 不重登入；respawn 後帳號不符即 fatal。
# ===========================================================================


async def test_respawn_backoff_grows_on_consecutive_failures_and_resets_on_success(tmp_path):
    """連續 respawn 失敗（模擬 child.start() 一直炸）——respawn 專屬 backoff 必須遞增
    （封頂），且在系統開機時間之類的雜訊之外，用 `_respawn_backoff_history` 直接觀察序列
    形狀（比照既有 session backoff 測試手法）。respawn 一旦成功，backoff 立刻重設回
    base。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf, recovery_probe_interval=0.02,
                respawn_backoff_base=0.05, respawn_backoff_max=0.3)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)

    child.push_failstop("boom")
    await _until(lambda: r._latched)

    child.fail_starts_remaining = 2   # 前兩次 respawn（start()）失敗
    await _until(lambda: len(r._respawn_backoff_history) >= 2, timeout=5)
    first, second = r._respawn_backoff_history[0], r._respawn_backoff_history[1]
    assert first == pytest.approx(0.05)
    assert second > first             # 確實遞增
    assert r._latched is True         # 還沒恢復（前兩次都失敗）

    await _until(lambda: not r._latched, timeout=5)   # 第三次成功
    assert r._respawn_backoff == pytest.approx(0.05)  # 成功後重設回 base

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_respawn_fatal_agent_error_stops_run_forever_without_retry(tmp_path):
    """respawn 途中遇到帳號不符（`FatalAgentError`）——不得被 `_respawn_child()`/
    `_recover()` 的既有 `except Exception` 吞掉，必須原樣往外拋，一路傳到
    `run_forever()` 既有的 fatal 處置（停止、不重試）。`child.starts` 驗證只嘗試過一次
    respawn 就停止，沒有落入無限重試迴圈。"""
    from quanquant.agent.runner import FatalAgentError

    class _FatalOnRespawnChild(_FakeChild):
        def start(self):
            self.starts += 1
            if self.starts == 1:
                self.generation += 1
                self.alive = True
                return "F1"
            raise FatalAgentError("agent 帳號不符（respawn 途中偵測）")

    tr, buf = _FakeTransport(), DurableBuffer(tmp_path / "o.db")
    child = _FatalOnRespawnChild()
    r = _runner(tr, child, buf, recovery_probe_interval=0.02,
                respawn_backoff_base=0.01, respawn_backoff_max=0.05)

    task = asyncio.create_task(r.run_forever())
    await _until(lambda: len(tr.healths()) >= 1)
    child.push_failstop("boom")
    await _until(lambda: r._latched)

    with pytest.raises(FatalAgentError):
        await asyncio.wait_for(task, timeout=5)
    assert child.starts == 2   # 初始 start() 一次＋respawn 觸發 fatal 那一次，沒有再重試


async def test_respawn_account_mismatch_is_fatal_not_silently_accepted(tmp_path):
    """respawn 後新 child 回報的帳號與 session 目前綁定的帳號不符——不得靜默改
    `self._account` 繼續回報 healthy，必須 fatal 停止（一路傳到 `run_forever`）。"""
    from quanquant.agent.runner import FatalAgentError

    class _AccountSwitchChild(_FakeChild):
        def start(self):
            self.starts += 1
            self.generation += 1
            self.alive = True
            return "F1" if self.starts == 1 else "F2"   # respawn 拿到不同帳號

    tr, buf = _FakeTransport(), DurableBuffer(tmp_path / "o.db")
    child = _AccountSwitchChild()
    r = _runner(tr, child, buf, recovery_probe_interval=0.02,
                respawn_backoff_base=0.01, respawn_backoff_max=0.05)

    task = asyncio.create_task(r.run_forever())
    await _until(lambda: len(tr.healths()) >= 1)
    child.push_failstop("boom")
    await _until(lambda: r._latched)

    with pytest.raises(FatalAgentError):
        await asyncio.wait_for(task, timeout=5)
    assert r._account == "F1"   # 沒有被靜默改成 F2


async def test_staged_recovery_persist_only_does_not_respawn_again(tmp_path, monkeypatch):
    """respawn＋ping_detail 都已經成功，但接下來的 epoch 持久化失敗——記
    `_respawn_stage="persist_only"`，之後多輪 `_recovery_prober` 只重試持久化，
    **不再呼叫 `_respawn_child()`（不再登入）**。持久化恢復正常後，完全恢復也沒有多
    respawn 一次。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf, recovery_probe_interval=0.02,
                respawn_backoff_base=0.01, respawn_backoff_max=0.05)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.healths()) >= 1)

    child.push_failstop("boom")
    await _until(lambda: r._latched)

    monkeypatch.setattr(
        buf, "set_health_epoch",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("epoch persist boom"))
    )
    await _until(lambda: r._respawn_stage == "persist_only", timeout=3)
    starts_after_respawn = child.starts
    assert starts_after_respawn >= 2   # 真的 respawn 過一次（ensure_child 的初始 1 次 + 這次）

    await asyncio.sleep(0.15)   # 讓多輪 _recovery_prober 跑過，持久化持續失敗重試
    assert child.starts == starts_after_respawn   # 沒有再次 respawn／重新登入
    assert r._latched is True

    monkeypatch.undo()   # 持久化恢復正常
    await _until(lambda: not r._latched, timeout=3)
    assert child.starts == starts_after_respawn   # 完全恢復也沒有多 respawn 一次
    assert r._respawn_stage is None

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

