import asyncio
import threading
import time
import pytest
from quanquant.agent.buffer import DurableBuffer
from quanquant.agent.runner import AgentRunner


class _FakeTransport:
    def __init__(self):
        self.sent, self.incoming, self.connects = [], asyncio.Queue(), 0
        self.fail_connects = 0                    # 前 N 次 connect 丟例外（Task 13 用）
    async def connect(self):
        self.connects += 1
        if self.connects <= self.fail_connects:
            raise ConnectionError("連不上")
    async def send(self, msg):
        self.sent.append(msg)
    async def receive(self):
        return await self.incoming.get()
    async def close(self):
        pass
    def reports(self):
        return [m for m in self.sent if m["type"] == "report"]


class _FakeChild:
    def __init__(self):
        self.ops, self.starts, self.alive = [], 0, False
        self.generation = 0
        self.ping_ok = True
        self.latched = False           # N6: ping_detail() 模擬 watchdog 偵測 child latch
        self.request_exc = None
        self.terminate_exc = None      # N6-1: 模擬 recovery 同步 terminate 失敗
        self.refuse_to_die = False     # N6-1: terminate 不炸，但 alive 驗不死
        self.terminate_calls: list[int | None] = []   # 記錄每次呼叫的 expected_generation
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
    def terminate(self, *, expected_generation: int | None = None):
        self.terminate_calls.append(expected_generation)
        if self.terminate_exc is not None:
            raise self.terminate_exc
        if not self.refuse_to_die:
            self.alive = False


async def _until(cond, timeout=3.0):
    async def _poll():
        while not cond():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_poll(), timeout)


def _runner(tr, child, buf, **overrides):
    kwargs = dict(transport=tr, buffer=buf, child=child,
                  pump_interval=0.02, resend_after=0.5,
                  child_command_timeout=0.5, heartbeat_interval=30,
                  child_ping_interval=0.05, child_ping_timeout=0.1,
                  backoff_base=0.01, backoff_max=0.05)
    kwargs.update(overrides)
    return AgentRunner(**kwargs)


async def test_run_once_sends_login_first(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.sent) >= 1)
    assert tr.sent[0]["type"] == "login"
    assert tr.sent[0]["account"] == "F1" and tr.sent[0]["mode"] == "sim"
    # codex round2 fix1：protocol 版本必填化後，UpLogin 不再有 default——runner 必須顯式帶
    # protocol=PROTOCOL_VERSION，否則建構就會 ValidationError。
    # Inc1 D7：硬升 v2，PROTOCOL_VERSION 現為 2。
    assert tr.sent[0]["protocol"] == 2
    assert tr.sent[0]["health_epoch"] == 0
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_pump_sends_pending_and_marks_sent_on_ack(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    ids = [buf.append("deal_report", {"n": i}) for i in range(2)]
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.reports()) >= 2)
    assert [m["event_id"] for m in tr.reports()[:2]] == ids
    for eid in ids:
        tr.incoming.put_nowait({"type": "report_ack", "event_id": eid})
    await _until(lambda: buf.unsent_count() == 0)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_pump_resends_when_no_ack(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    eid = buf.append("deal_report", {"n": 1})
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len([m for m in tr.reports() if m["event_id"] == eid]) >= 2,
                 timeout=5)                       # resend_after=0.5 後重送
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_crash_before_ack_resent_by_next_session(tmp_path):
    """零丟單核心測試：送出未 ack 就崩潰 → 重啟後補送。"""
    tr1, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    eid = buf.append("deal_report", {"n": 1})
    r1 = _runner(tr1, child, buf)
    r1.ensure_child()
    t1 = asyncio.create_task(r1.run_once())
    await _until(lambda: len(tr1.reports()) >= 1)
    t1.cancel()                                   # 模擬 agent 崩潰（未收 ack）
    await asyncio.gather(t1, return_exceptions=True)
    tr2 = _FakeTransport()
    r2 = _runner(tr2, child, DurableBuffer(tmp_path / "o.db"))
    r2.ensure_child()
    t2 = asyncio.create_task(r2.run_once())
    await _until(lambda: any(m["event_id"] == eid for m in tr2.reports()))
    t2.cancel()
    await asyncio.gather(t2, return_exceptions=True)


async def test_downlink_place_dispatched_to_child_and_acked(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    tr.incoming.put_nowait({"type": "place", "cmd_id": "c1", "account": "F1", "mode": "sim",
                            "expires_at": "2099-01-01T00:00:00",
                            "native": {"action": "Buy", "price": "0", "qty": 1,
                                       "price_type": "MKT", "order_type": "IOC",
                                       "octype": "Auto"}})
    await _until(lambda: any(m.get("type") == "cmd_ack" for m in tr.sent))
    ack = next(m for m in tr.sent if m.get("type") == "cmd_ack")
    assert ack["cmd_id"] == "c1" and ack["ok"] and ack["result"]["ordno"] == "101AA1"
    assert child.ops[0]["op"] == "place" and child.ops[0]["price"] == "0"
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_child_timeout_yields_error_ack(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    child.request_exc = TimeoutError()
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    tr.incoming.put_nowait({"type": "cancel", "cmd_id": "c2", "account": "F1", "mode": "sim",
                            "expires_at": "2099-01-01T00:00:00", "ordno": "101AA1"})
    await _until(lambda: any(m.get("type") == "cmd_ack" for m in tr.sent))
    ack = next(m for m in tr.sent if m.get("type") == "cmd_ack")
    assert ack["ok"] is False and ack["error_kind"] == "timeout"
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


# ---- Task 11（G3/D8）：DownQueryQty＋reconcile 改回 volatile UpQueryResult ----


async def test_downlink_query_qty_dispatched_to_child_and_returns_query_result(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    child.request = lambda op, *, timeout: {"ok": True, "result": {"qty": 7}}
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    tr.incoming.put_nowait({"type": "query_qty", "cmd_id": "q1", "ordno": "101AA1", "mode": "sim"})
    await _until(lambda: any(m.get("type") == "query_result" for m in tr.sent))
    reply = next(m for m in tr.sent if m.get("type") == "query_result")
    assert reply["cmd_id"] == "q1" and reply["result"] == {"qty": 7}
    assert "event_id" not in reply  # volatile：不進 outbox，無 event_id（D7）
    assert not any(m.get("type") == "cmd_ack" for m in tr.sent)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_downlink_reconcile_now_returns_query_result_not_cmd_ack(tmp_path):
    """Task 11：reconcile 從 Inc0 的 UpCmdAck 切到 volatile UpQueryResult（D4/D7）。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    child.request = lambda op, *, timeout: {"ok": True, "result": {"payloads": [], "newest": None}}
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    tr.incoming.put_nowait({"type": "reconcile", "cmd_id": "r1", "mode": "sim", "after": None})
    await _until(lambda: any(m.get("type") == "query_result" for m in tr.sent))
    reply = next(m for m in tr.sent if m.get("type") == "query_result")
    assert reply["cmd_id"] == "r1" and reply["result"] == {"payloads": [], "newest": None}
    assert not any(m.get("type") == "cmd_ack" for m in tr.sent)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_readonly_command_failure_sends_no_reply(tmp_path):
    """唯讀冪等：child 執行失敗一律不回覆（server 端自然逾時，下輪重試），不猜測失敗原因
    ——UpQueryResult 刻意沒有錯誤欄位可攜帶（D7 R1-7）。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    child.request = lambda op, *, timeout: {"ok": False, "error_kind": "exception", "message": "boom"}
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    tr.incoming.put_nowait({"type": "query_qty", "cmd_id": "q2", "ordno": "101AA1", "mode": "sim"})
    await asyncio.sleep(0.15)
    assert not any(m.get("cmd_id") == "q2" for m in tr.sent)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_readonly_command_child_timeout_sends_no_reply(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    child.request_exc = TimeoutError()
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    tr.incoming.put_nowait({"type": "query_qty", "cmd_id": "q3", "ordno": "101AA1", "mode": "sim"})
    await asyncio.sleep(0.15)
    assert not any(m.get("cmd_id") == "q3" for m in tr.sent)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def test_real_child_handle_spawn_roundtrip(tmp_path):
    """ChildHandle 對真 spawn 子程序的 smoke（fake native factory）。"""
    from quanquant.agent.runner import ChildHandle
    from quanquant.agent.testing import fake_native_factory
    child = ChildHandle(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                        mode="sim", buffer_path=str(tmp_path / "o.db"),
                        native_factory=fake_native_factory)
    assert child.start() == "F1"
    reply = child.request({"op": "ping"}, timeout=10)
    # codex round1 fix1(a)：reply 現在多帶一個 rpc_id 欄位（request() 自動附加、child_main
    # 原樣帶回，用來擋遲到 reply 誤配下一輪 RPC）——不再用嚴格 dict 相等，改斷言子集。
    assert reply["ok"] is True
    child.terminate()
    assert not child.alive


# ---------- Task 13：重連 backoff + 子程序凍結偵測/respawn ----------

async def test_run_forever_reconnects_after_transport_error(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    tr.fail_connects = 1                          # 第一次 connect 失敗
    r = _runner(tr, child, buf)
    task = asyncio.create_task(r.run_forever())
    await _until(lambda: tr.connects >= 2 and any(m["type"] == "login" for m in tr.sent))
    r.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_child_frozen_triggers_respawn_and_relogin(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    child.ping_ok = False                         # 第一個 session 內就判凍結
    r = _runner(tr, child, buf)
    task = asyncio.create_task(r.run_forever())
    await _until(lambda: child.starts >= 2)       # respawn 過
    child.ping_ok = True
    await _until(lambda: len([m for m in tr.sent if m["type"] == "login"]) >= 2)
    r.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_child_watchdog_uses_ping_detail_and_treats_latched_as_frozen(tmp_path):
    """N6（順帶，codex 終審 round6）：`_child_watchdog` 改用 `ping_detail()`（不再只看
    `ping()` 的 bool）——child 本地 latch 已 tripped（即使 `ok=True`，pipe 本身仍活著、
    仍能正常回應）時，watchdog 仍必須視為不健康，觸發既有 `ChildFrozenError` 處理
    （terminate 目前這個 child + 下一輪 respawn 出全新、未 latch 的 child）。這是
    `ping_detail` 目前唯一的 production caller（解掉先前的 dead code diagnostic）。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    child.latched = True   # pipe 本身仍活著（ok=True），但 child 本地 latch 已 tripped
    r = _runner(tr, child, buf)
    task = asyncio.create_task(r.run_forever())
    await _until(lambda: child.starts >= 2)   # watchdog 經 ping_detail 偵測到 latch → respawn
    child.latched = False                     # 新 child（全新 process）本地未 latch
    await _until(lambda: len([m for m in tr.sent if m["type"] == "login"]) >= 2)
    r.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_ensure_child_respawns_when_alive_but_account_lost(tmp_path):
    """接手他人 child 的邊界：child.alive 為 True 但 runner 尚無 _account →
    視同需要重啟（terminate+start），保證 ensure_child() 返回後 _account 非空。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    child.alive = True                            # 模擬接手他人已在跑的 child
    r = _runner(tr, child, buf)
    assert r._account == ""
    r.ensure_child()
    assert child.starts == 1                      # terminate 後 respawn 一次
    assert r._account == "F1"


# ---------- codex round1 fix1（BLOCKER）：ChildHandle pipe 失同步——timeout 後遲到
# reply 污染下一個 RPC ----------
# 危險情境：place 逾時 → 之後 child 完成送出遲到 reply → 下一個 ping/place 把它讀走 →
# 前一單的 broker id 寫進下一張 Order。兩層防線：(a) rpc_id 比對，不符即丟棄繼續在剩餘
# timeout 預算內等；(b) 真的逾時（或 pipe 本身壞掉）就整條 pipe 判死 + terminate，之後
# request()/ping() 一律直接 fail，不再碰這條不可信的 pipe，交給 ensure_child() respawn。

class _FakeProcess:
    def __init__(self, alive: bool = True, refuse_kill: bool = False):
        self._alive = alive
        self.killed = False
        self.joined = False
        # R7-1（HIGH，codex 終審 round7）：模擬「kill()+join(timeout=5) 後 process 仍然
        # 存活」——`refuse_kill=True` 時 `kill()` 不翻轉 `_alive`，讓 `terminate()` 的
        # `is_alive()` 驗證真的驗到「還活著」，用來測 join-timeout 路徑。預設 False，
        # 既有測試（`kill()` 即視為死亡）行為不變。
        self._refuse_kill = refuse_kill

    def is_alive(self) -> bool:
        return self._alive

    def kill(self) -> None:
        self.killed = True
        if not self._refuse_kill:
            self._alive = False

    def join(self, timeout=None) -> None:
        self.joined = True


class _FakeConn:
    """模擬 multiprocessing.Connection：poll_results/recv_queue 依序被消耗，用來精確控制
    「逾時」「遲到 reply」等時序，不必依賴真實 IPC 延遲。"""

    def __init__(self):
        self.sent: list[dict] = []
        self.poll_results: list[bool] = []
        self.recv_queue: list[dict] = []
        self.recv_error: Exception | None = None
        self.send_error: Exception | None = None
        self.closed = False

    def send(self, obj) -> None:
        if self.send_error is not None:
            raise self.send_error
        self.sent.append(obj)

    def poll(self, timeout=None) -> bool:
        if not self.poll_results:
            return False
        return self.poll_results.pop(0)

    def recv(self) -> dict:
        if self.recv_error is not None:
            raise self.recv_error
        return self.recv_queue.pop(0)

    def close(self) -> None:
        self.closed = True


def _bare_child_handle(tmp_path):
    from quanquant.agent.runner import ChildHandle
    return ChildHandle(credentials={}, symbol="TXF", mode="sim",
                       buffer_path=str(tmp_path / "o.db"))


def test_timeout_poisons_pipe_and_second_request_never_touches_it(tmp_path):
    child = _bare_child_handle(tmp_path)
    process, conn = _FakeProcess(), _FakeConn()
    child._process, child._conn = process, conn

    conn.poll_results = [False]      # 第一個 request：poll 逾時
    with pytest.raises(TimeoutError):
        child.request({"op": "place"}, timeout=0.01)

    assert process.killed and process.joined    # 逾時即 terminate
    assert conn.closed
    assert child.alive is False

    # 第二個 request：poisoned 態必須直接 fail，完全不碰 pipe（不再 send/poll/recv）。
    sent_before = len(conn.sent)
    with pytest.raises(TimeoutError):
        child.request({"op": "ping"}, timeout=1)
    assert len(conn.sent) == sent_before


def test_mismatched_rpc_id_reply_is_discarded_within_timeout_budget(tmp_path):
    child = _bare_child_handle(tmp_path)
    process, conn = _FakeProcess(), _FakeConn()
    child._process, child._conn = process, conn

    # 兩次 poll 都成功；第一次 recv 回來的是遲到的上一輪 reply（rpc_id 不符），
    # 第二次才是這次呼叫真正的 reply（rpc_id==1，第一筆 RPC）。
    conn.poll_results = [True, True]
    conn.recv_queue = [
        {"ok": True, "result": {"stale": True}, "rpc_id": 999},
        {"ok": True, "result": {"pong": 1}, "rpc_id": 1},
    ]
    reply = child.request({"op": "ping"}, timeout=1)
    assert reply["result"] == {"pong": 1}
    assert child.alive is True           # 沒有因為「收到一則不符的 reply」就被判死


def test_recv_error_poisons_pipe(tmp_path):
    child = _bare_child_handle(tmp_path)
    process, conn = _FakeProcess(), _FakeConn()
    child._process, child._conn = process, conn
    conn.poll_results = [True]
    conn.recv_error = EOFError("child 端已關閉")

    with pytest.raises(TimeoutError):
        child.request({"op": "ping"}, timeout=1)
    assert child.alive is False
    assert process.killed and conn.closed


def test_child_handle_respawns_after_being_poisoned(tmp_path):
    """真 spawn 一次驗證端到端：逾時判死 → alive=False → 重新 start() 成功（ensure_child
    在 AgentRunner 那層即是靠 alive 決定要不要 respawn，這裡直測 ChildHandle 本身）。"""
    from quanquant.agent.testing import fake_native_factory
    from quanquant.agent.runner import ChildHandle
    child = ChildHandle(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                        mode="sim", buffer_path=str(tmp_path / "o.db"),
                        native_factory=fake_native_factory)
    assert child.start() == "F1"
    real_process = child._process

    fake_conn = _FakeConn()
    fake_conn.poll_results = [False]     # 換上假管線，確定性地模擬逾時
    child._conn = fake_conn
    with pytest.raises(TimeoutError):
        child.request({"op": "ping"}, timeout=0.01)

    assert child.alive is False
    assert real_process.is_alive() is False   # 真的被 kill 了，不是只改旗標

    assert child.start() == "F1"      # respawn：新的真子程序 + 新 pipe
    assert child.alive is True
    child.terminate()


def test_child_handle_ping_wraps_request(tmp_path):
    """直測 ChildHandle.ping() 包裝方法本身（Task 12 只測過底層 request）：
    request 正常回覆 → True；request 逾時（TimeoutError）→ False。"""
    from quanquant.agent.runner import ChildHandle
    child = ChildHandle(credentials={}, symbol="TXF", mode="sim",
                        buffer_path=str(tmp_path / "o.db"))
    child.request = lambda op, *, timeout: {"ok": True}
    assert child.ping(timeout=1) is True

    def _raise_timeout(op, *, timeout):
        raise TimeoutError("agent 子程序逾時未回應")
    child.request = _raise_timeout
    assert child.ping(timeout=1) is False


# ---------- Task 13 Fix Round 1：backoff 重設判準改為「session 存活時間達門檻」 ----------
# 原本用 `_login_sent`（login 送出即 True）判準：login 在四個 session task 啟動前就送出，
# 幾乎所有失敗模式（子程序凍結、receive/pump 例外、server 收線後立斷）都發生在 login 送出
# 之後，導致 backoff 每輪被重設回 base、指數退避形同虛設。改用 `_backoff_history`（每輪
# sleep 前的 backoff 值）斷言序列形狀，避免 wall-clock 間隔斷言的 flaky 風險。

async def test_backoff_not_reset_on_short_lived_session(tmp_path):
    """stable_session_seconds 設到不可能達標 → 即使 login 早就送出，反覆快速凍結的
    session 也不該讓 backoff 重設；應持續倍增（封頂 backoff_max）。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    child.ping_ok = False                          # 每輪 session 一啟動就被判凍結，session 短命
    r = _runner(tr, child, buf, backoff_base=0.05, backoff_max=1.0,
                stable_session_seconds=999.0)       # 不可能達標 → 永不重設
    task = asyncio.create_task(r.run_forever())
    await _until(lambda: len(r._backoff_history) >= 3, timeout=5)
    r.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    first_three = r._backoff_history[:3]
    assert first_three[0] == pytest.approx(0.05)    # 第一輪即 backoff_base
    assert first_three == sorted(first_three)        # 單調不減
    assert first_three[0] < first_three[-1]           # 確實持續成長，未被重設拉回 base


async def test_backoff_resets_after_stable_session(tmp_path):
    """快速失敗兩輪（backoff 已長大）後，第三輪 session 存活時間跨過
    stable_session_seconds 門檻才結束 → 下一輪的 backoff 應重設回 base 級距。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    tr.fail_connects = 2                            # 前兩輪 connect 直接失敗，session 近乎瞬間結束
    r = _runner(tr, child, buf, backoff_base=0.05, backoff_max=1.0,
                stable_session_seconds=0.05)
    task = asyncio.create_task(r.run_forever())

    await _until(lambda: len(r._backoff_history) >= 2)   # 前兩輪快速失敗已記錄
    assert r._backoff_history[0] == pytest.approx(0.05)
    assert r._backoff_history[1] == pytest.approx(0.1)

    await _until(lambda: tr.connects >= 3)          # 第三輪 connect 成功，session 開始存活
    await asyncio.sleep(0.15)                       # 撐過 stable_session_seconds(0.05) 門檻
    child.ping_ok = False                           # 觸發第三輪 session 結束（凍結偵測）

    await _until(lambda: len(r._backoff_history) >= 3)
    assert r._backoff_history[2] == pytest.approx(0.05)   # 存活夠久 → backoff 重設回 base

    r.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


# ---------- codex round2 fix3：ChildHandle send 側 poison + lock 內重檢 ----------
# (a) conn.send() 本身也可能炸（BrokenPipeError/EOFError/OSError）——原本只有 poll/recv
# 包了 try/except，send 炸掉會讓例外原樣往外洩，_poisoned 也不會被設起來。
# (b) poisoned 檢查原本只在取得 lock「之前」做一次——併發等待者若在前一個 RPC 於鎖內
# poison 之後才拿到鎖，會繼續往下碰一條已經不可信的 conn。改成鎖內再檢一次。

def test_send_error_poisons_pipe_and_second_request_never_touches_conn(tmp_path):
    child = _bare_child_handle(tmp_path)
    process, conn = _FakeProcess(), _FakeConn()
    child._process, child._conn = process, conn
    conn.send_error = BrokenPipeError("child 端已死")

    with pytest.raises(TimeoutError):
        child.request({"op": "place"}, timeout=1)

    assert process.killed and process.joined      # send 炸掉也要 terminate
    assert conn.closed
    assert child.alive is False

    sent_before = len(conn.sent)
    with pytest.raises(TimeoutError):
        child.request({"op": "ping"}, timeout=1)
    assert len(conn.sent) == sent_before           # poisoned 態：完全不再碰 conn


def test_lock_rechecks_poisoned_before_touching_conn(tmp_path):
    """模擬併發：第二個等待者已通過鎖外的 poisoned 檢查（當時尚未 poison）、卡在等
    lock；第一個 RPC 在鎖內完成並把 pipe 判死後放鎖——第二個等待者拿到鎖後必須在動
    conn 之前重新檢查一次 poisoned，直接 fail，不送 conn.send()。"""
    child = _bare_child_handle(tmp_path)
    process, conn = _FakeProcess(), _FakeConn()
    child._process, child._conn = process, conn

    child._lock.acquire()
    results: dict[str, Exception] = {}
    try:
        def _second():
            try:
                child.request({"op": "ping"}, timeout=1)
            except TimeoutError as exc:
                results["exc"] = exc

        t = threading.Thread(target=_second)
        t.start()
        time.sleep(0.1)          # 讓第二個等待者跑過鎖外檢查、卡在 lock.acquire()
        child._poisoned = True   # 模擬第一個 RPC 已在鎖內判死
    finally:
        child._lock.release()

    t.join(timeout=2)
    assert isinstance(results.get("exc"), TimeoutError)
    assert conn.sent == []       # 從未碰過 conn


# ---------- R3-1（HIGH，codex 終審 round3）：lifecycle generation fencing——recovery
# respawn（terminate 舊 child、start 新 child）與併發中仍在等舊 pipe 回覆的 RPC（watchdog
# ping、唯讀指令，皆經 asyncio.to_thread）之間，舊版完全無互斥：respawn 可能被舊 RPC
# 逾時後的 _poison() 誤殺；asyncio.to_thread 取消後遲到的 worker 也可能誤碰新 child 的
# conn。修法：start/terminate/_rpc/poll_failstop 共用同一把 threading.RLock，且每筆 RPC
# 在進入鎖之前先捕捉當下的 generation，拿到鎖之後重新核對，不符即丟棄。----------

def test_generation_mismatch_after_concurrent_respawn_discards_stale_rpc_without_touching_new_child(
    tmp_path,
):
    """barrier/事件精確控制「舊 RPC 遲到 vs 新 child」不互殺：一個 RPC 呼叫在捕捉
    generation=1 之後、真正拿到鎖之前，若併發的 respawn（模擬 terminate 舊 child、start
    新 child）已經搶先換代——這筆呼叫拿到鎖後必須發現過期，直接丟棄，完全不碰新一代的
    process/conn（不誤殺、不誤送），也不會碰舊 conn（它從未真正送出過）。"""
    child = _bare_child_handle(tmp_path)
    old_process, old_conn = _FakeProcess(), _FakeConn()
    child._process, child._conn = old_process, old_conn
    child._generation = 1

    child._lock.acquire()   # 模擬「respawn 正在進行中」，佔住鎖
    results: dict[str, Exception] = {}

    def _stale_caller():
        try:
            child.request({"op": "ping"}, timeout=1)
        except TimeoutError as exc:
            results["exc"] = exc

    t = threading.Thread(target=_stale_caller)
    t.start()
    time.sleep(0.1)   # 讓呼叫端跑過「捕捉 generation=1」，卡在等鎖

    # respawn 在鎖內完成：換上新一代 process/conn（terminate 舊的、start 新的都要拿同一把
    # 鎖，所以這裡直接模擬「respawn 已完成」的最終狀態）。
    new_process, new_conn = _FakeProcess(), _FakeConn()
    child._process, child._conn = new_process, new_conn
    child._generation = 2
    child._lock.release()   # 放行——過期呼叫這才拿得到鎖

    t.join(timeout=2)
    assert isinstance(results.get("exc"), TimeoutError)
    assert new_conn.sent == []            # 完全沒碰新 conn
    assert new_process.killed is False    # 新 child 沒有被誤殺（_poison 也沒被觸發到它）
    assert old_conn.sent == []            # 舊 conn 也沒被碰（呼叫從頭到尾都卡在等鎖）


async def test_cancelled_to_thread_worker_late_arrival_after_respawn_is_discarded(tmp_path):
    """`asyncio.to_thread` 的取消不會真的停止底層執行緒——呼叫端（watchdog）已經放棄
    等待，但那個 worker thread 仍在背景跑，直到它真的走完（可能卡在等respawn 持有的鎖）。
    這支測試證明：即使這個「孤兒」worker 是在 respawn 完成之後才真正拿到鎖執行到
    `_rpc()` 的核對段，generation 比對仍能攔下它，不會把它的（遲到的）執行結果套用到
    新 child 身上——新 conn 沒被送過任何東西、新 process 沒被殺、`child.alive` 仍正常
    反映新 child 存活。"""
    child = _bare_child_handle(tmp_path)
    old_process, old_conn = _FakeProcess(), _FakeConn()
    child._process, child._conn = old_process, old_conn
    child._generation = 1

    child._lock.acquire()   # 模擬 respawn 正在進行、佔住鎖

    async def _orphan_ping():
        return await asyncio.to_thread(child.request, {"op": "ping"}, timeout=5)

    task = asyncio.create_task(_orphan_ping())
    await asyncio.sleep(0.05)   # 讓底層 thread pool worker 真的排進去、卡在 acquire()

    task.cancel()   # 呼叫端取消——底層 thread 不受影響，仍在背景卡著等鎖
    with pytest.raises(asyncio.CancelledError):
        await task

    # respawn 完成：換上新一代 process/conn，放鎖——孤兒 worker 這才拿得到鎖。
    new_process, new_conn = _FakeProcess(), _FakeConn()
    child._process, child._conn = new_process, new_conn
    child._generation = 2
    child._lock.release()

    await asyncio.sleep(0.2)   # 給孤兒 worker 執行緒時間跑完（它會發現 generation 不符、丟棄）

    assert new_conn.sent == []          # 孤兒呼叫完全沒碰到新 conn
    assert new_process.killed is False  # 也沒有被誤殺
    assert child.alive is True          # 新 child 仍正常存活


# ---------- N6-1（HIGH，codex 終審 round6）：`ChildHandle.terminate(expected_generation=)`
# fencing——`AgentRunner._recover()` 同步 terminate 呼叫若被取消，底層 thread pool worker
# 仍可能跑完、很久之後才真正拿到鎖執行到這裡；沒有 fencing 會誤殺已經換代的新 child。----

def test_terminate_generation_mismatch_after_concurrent_respawn_is_noop_for_new_child(
    tmp_path,
):
    """barrier/事件精確控制「舊 terminate 呼叫遲到 vs 新 child」不互殺：`_recover()` 在
    捕捉 generation=1 之後、真正拿到鎖之前，若併發的 respawn（模擬 `ensure_child()` 的
    terminate 舊 child、start 新 child）已經搶先換代——這筆 terminate 呼叫拿到鎖後必須
    發現過期，直接 no-op，完全不碰新一代的 process/conn（不誤殺）。"""
    child = _bare_child_handle(tmp_path)
    old_process, old_conn = _FakeProcess(), _FakeConn()
    child._process, child._conn = old_process, old_conn
    child._generation = 1

    child._lock.acquire()   # 模擬「respawn 正在進行中」，佔住鎖
    results: dict[str, object] = {}

    def _stale_terminate_caller():
        results["returned"] = child.terminate(expected_generation=1)

    t = threading.Thread(target=_stale_terminate_caller)
    t.start()
    time.sleep(0.1)   # 讓呼叫端跑過「捕捉 generation=1」，卡在等鎖

    # respawn 在鎖內完成：換上新一代 process/conn（terminate 舊的、start 新的都要拿同一把
    # 鎖，所以這裡直接模擬「respawn 已完成」的最終狀態）。
    new_process, new_conn = _FakeProcess(), _FakeConn()
    child._process, child._conn = new_process, new_conn
    child._generation = 2
    child._lock.release()   # 放行——過期的 terminate 呼叫這才拿得到鎖

    t.join(timeout=2)
    assert results["returned"] is None    # no-op，正常返回，不 raise
    assert new_process.killed is False    # 新 child 沒有被誤殺
    assert new_conn.closed is False       # 新 conn 也沒被碰
    assert child.alive is True            # 新 child 仍正常存活
    assert old_process.killed is False    # 舊 process 也沒被碰（呼叫從頭到尾都卡在等鎖）


# ---------- R7-1（HIGH，codex 終審 round7）：terminate() 沒有真正 verify-dead——舊版
# kill()+join(timeout=5) 後未檢查 process.is_alive() 就無條件把 self._process 設 None，
# 讓 alive 屬性此後永遠回報 False（即使底層真的還活著），_recover() 靠 alive 做的二次確認
# 因此形同虛設。----------


def test_terminate_returns_false_and_keeps_handle_when_process_refuses_to_die(tmp_path):
    """真 `ChildHandle.terminate()` 的 join-timeout 路徑：kill()+join(timeout=5) 後
    `process.is_alive()` 仍回 True（用 `_FakeProcess(refuse_kill=True)` 精確控制，不依賴
    真的能製造出一個吃 SIGKILL 不死的程序）——terminate() 必須回傳 False、**不清**
    `self._process`（handle 保留），`child.alive` 也必須誠實回報 True，不能因為呼叫過
    terminate() 就被錯誤地永遠判定成「已死」。"""
    child = _bare_child_handle(tmp_path)
    process, conn = _FakeProcess(refuse_kill=True), _FakeConn()
    child._process, child._conn = process, conn

    result = child.terminate()

    assert result is False
    assert process.killed is True        # kill() 確實被呼叫過
    assert process.joined is True        # join() 也確實被呼叫過
    assert child._process is process     # R7-1 核心：handle 未被清掉
    assert child._conn is conn           # conn 同樣未被清掉（不確認死亡就不清任何資源）
    assert child.alive is True           # 誠實反映「其實還活著」，不是誤報 False


def test_terminate_returns_true_and_clears_handle_when_process_actually_dies(tmp_path):
    """回歸：process 正常死亡（`_FakeProcess()` 預設行為）時，terminate() 仍必須回傳
    True 並清掉 handle——R7-1 的修法只在「驗不死」時保留狀態，不影響既有的正常成功路徑。"""
    child = _bare_child_handle(tmp_path)
    process, conn = _FakeProcess(), _FakeConn()
    child._process, child._conn = process, conn

    result = child.terminate()

    assert result is True
    assert child._process is None
    assert child._conn is None
    assert child.alive is False


async def test_recover_with_real_child_handle_keeps_latch_when_process_refuses_to_die(
    tmp_path,
):
    """R7-1 整合驗證：用真 `ChildHandle`（不是抽象 `_FakeChild`）接上 `refuse_kill=True`
    的假 process，證明修好的 `terminate()`（join 後真的檢查 is_alive）與既有 `_recover()`
    的『child.alive 二次確認』邏輯組合起來，確實能在 kill/join 沒有真正生效時保留
    latch/sentinel，不會被舊版「terminate 之後一律視為已死」的誤判放行清除。"""
    child = _bare_child_handle(tmp_path)
    process, conn = _FakeProcess(refuse_kill=True), _FakeConn()
    child._process, child._conn = process, conn
    child._generation = 1

    tr, buf = _FakeTransport(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r._latched = True
    r._latch_detail = "boom"
    r._health_epoch = 1
    buf.write_sentinel(epoch=1, detail="boom")

    await r._recover()   # probe 會過（buf 本身健康）；terminate 會被呼叫，但驗不死

    assert r._latched is True
    assert buf.has_sentinel()
    assert buf.get_health_epoch() == 0   # 完全沒有持久化
    assert child.alive is True           # handle 仍保留，誠實反映還活著


async def test_cancelled_terminate_worker_late_arrival_after_respawn_is_discarded(tmp_path):
    """`asyncio.to_thread` 的取消不會真的停止底層執行緒——`_recover()` 的 terminate 呼叫
    若在等鎖期間被取消（例如整條 session 因為別的例外被 `run_once()` 的 finally 收攏），
    底層 worker thread 仍在背景跑，直到真的走完（可能卡在等 respawn 持有的鎖）。即使這個
    「孤兒」worker 是在 respawn 完成之後才真正拿到鎖執行到 fencing 核對，generation 比對
    仍能攔下它，不會誤殺新 session 的 child。"""
    child = _bare_child_handle(tmp_path)
    old_process, old_conn = _FakeProcess(), _FakeConn()
    child._process, child._conn = old_process, old_conn
    child._generation = 1

    child._lock.acquire()   # 模擬 respawn 正在進行、佔住鎖

    async def _orphan_terminate():
        return await asyncio.to_thread(child.terminate, expected_generation=1)

    task = asyncio.create_task(_orphan_terminate())
    await asyncio.sleep(0.05)   # 讓底層 thread pool worker 真的排進去、卡在 acquire()

    task.cancel()   # 呼叫端（模擬 _recover() 所在的 task）取消——底層 thread 不受影響
    with pytest.raises(asyncio.CancelledError):
        await task

    # respawn 完成：換上新一代 process/conn，放鎖——孤兒 worker 這才拿得到鎖。
    new_process, new_conn = _FakeProcess(), _FakeConn()
    child._process, child._conn = new_process, new_conn
    child._generation = 2
    child._lock.release()

    await asyncio.sleep(0.2)   # 給孤兒 worker 執行緒時間跑完（它會發現 generation 不符、丟棄）

    assert new_process.killed is False  # 沒有被誤殺
    assert new_conn.closed is False
    assert child.alive is True          # 新 child 仍正常存活


# ---------- Round5（codex 終審 round5 收斂）：`ChildHandle.respawn(expected_generation)`
# （R4-a 加的 generation-scoped terminate+start transaction）已隨 `AgentRunner.
# _respawn_child()` 一併移除——recovery 不再原地換血 child，改為結束整個 session、交給
# `run_forever()`→`ensure_child()` 這條既有硬化路徑重新 spawn（見 runner.py
# `AgentRunner._recover`/`SessionRestartRequested` docstring）。上面兩支
# `test_generation_mismatch_after_concurrent_respawn_discards_stale_rpc_without_touching_new_child`
# /`test_cancelled_to_thread_worker_late_arrival_after_respawn_is_discarded` 測的是
# `start()`/`terminate()`/`_rpc()` 共用的 `_lock`＋`_generation` fencing 本身（R3-1，保護
# 「併發 RPC vs. 任何一次 terminate+start 交替」，`ensure_child()` 本來就會呼叫這兩個
# 方法）——這道防線保留不動，繼續有效；只有 `.respawn()` 這個方法本身連同其專屬測試被
# 移除。
# ----------


# ---------- codex round2 fix4：帳號不符 → fatal 停止，不進 run_forever 的無限 backoff
# 重試迴圈（每輪重試都是一次真的券商登入，會燒 Shioaji 每日 1000 次配額）----------

async def test_run_forever_does_not_retry_on_fatal_agent_error(tmp_path):
    from quanquant.agent.runner import FatalAgentError

    tr, buf = _FakeTransport(), DurableBuffer(tmp_path / "o.db")

    class _FatalChild(_FakeChild):
        def start(self):
            self.starts += 1
            raise FatalAgentError(
                "agent 帳號不符（outbox 屬於 F1，此次以 F2 登入），拒絕啟動"
            )

    child = _FatalChild()
    r = _runner(tr, child, buf)
    with pytest.raises(FatalAgentError):
        await r.run_forever()
    assert child.starts == 1     # 不重試


def test_child_handle_start_raises_fatal_agent_error_on_account_mismatch(tmp_path):
    """真 spawn 端到端：outbox 已綁定 F2 且有未送列，fake native 固定回帳號 F1 →
    native_runner 的 connect 分支 assert_account 失敗、reply 帶 error_kind=account_mismatch
    → ChildHandle.start() 必須 terminate 子程序後 raise FatalAgentError（而非泛用
    RuntimeError），讓 run_forever 的例外鏈能攔截、停止重試。"""
    from quanquant.agent.runner import ChildHandle, FatalAgentError
    from quanquant.agent.testing import fake_native_factory

    buffer_path = tmp_path / "o.db"
    pre = DurableBuffer(buffer_path)
    pre.assert_account("F2")
    pre.append("deal_report", {"n": 1})

    child = ChildHandle(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                        mode="sim", buffer_path=str(buffer_path),
                        native_factory=fake_native_factory)
    with pytest.raises(FatalAgentError) as exc_info:
        child.start()
    assert "F1" in str(exc_info.value) and "F2" in str(exc_info.value)
    assert child.alive is False


# ---------- N6-3（MEDIUM，codex 終審 round6）：session 結束原因確定性優先序 ----------
# asyncio.wait(FIRST_EXCEPTION) 的 done set 可能同時收攏多個例外——set 迭代順序不保證，
# 「取第一個」等同碰運氣。改用 _select_session_end_exception() 依確定性優先序（
# FatalAgentError ＞ SessionRestartRequested ＞ 其他）挑一個 raise。


def test_select_session_end_exception_priority_order(tmp_path):
    """直測優先序函式本身（快速、決定性，不受 asyncio 排程時序影響）：
    FatalAgentError ＞ SessionRestartRequested ＞ 其他一般例外，兩兩/三者同框都成立。"""
    from quanquant.agent.runner import (
        FatalAgentError, SessionRestartRequested, _select_session_end_exception,
    )

    fatal = FatalAgentError("fatal")
    restart = SessionRestartRequested("restart")
    other1 = RuntimeError("other1")
    other2 = ConnectionError("other2")

    assert _select_session_end_exception([fatal]) is fatal
    assert _select_session_end_exception([restart, fatal]) is fatal
    assert _select_session_end_exception([fatal, restart]) is fatal
    assert _select_session_end_exception([other1, restart]) is restart
    assert _select_session_end_exception([restart, other1]) is restart
    assert _select_session_end_exception([other1, other2]) is other1
    assert _select_session_end_exception([fatal, restart, other1]) is fatal
    assert _select_session_end_exception([other1, restart, fatal]) is fatal


async def test_run_once_selects_fatal_over_restart_when_both_land_in_done_set(tmp_path):
    """N6-3 整合驗證之一：兩個完全不 await 就立刻 raise 的假 task（保證都會在
    `asyncio.wait` 判斷 `done` 之前就完成——`Task.done()`/`.exception()` 的狀態在
    `__step()` 內同步設定，`asyncio.wait` 對 `done` 的計算發生在更後面的迭代，不看 set
    迭代順序碰運氣）驗證確定性優先序：FatalAgentError（不可重試）必須勝出，不被同時完成
    的 SessionRestartRequested 蓋掉。"""
    from quanquant.agent.runner import FatalAgentError, SessionRestartRequested

    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()

    async def _raise_restart():
        raise SessionRestartRequested("boom-restart")

    async def _raise_fatal():
        raise FatalAgentError("boom-fatal")

    r._recovery_prober = _raise_restart
    r._failstop_watchdog = _raise_fatal

    with pytest.raises(FatalAgentError):
        await r.run_once()


async def test_run_once_selects_restart_over_generic_exception_when_both_land_in_done_set(
    tmp_path,
):
    """N6-3 整合驗證之二（反面）：`SessionRestartRequested` 與一般 transport 例外同時
    完成 → 受控重啟訊號必須勝出，不被一般例外蓋掉——`run_forever()` 才能正確識別
    「recovery 已完成」，backoff 不被誤判成一般失敗而重設/亂套。"""
    from quanquant.agent.runner import SessionRestartRequested

    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()

    async def _raise_restart():
        raise SessionRestartRequested("boom-restart")

    async def _raise_transport_error():
        raise ConnectionError("transport 斷線")

    r._recovery_prober = _raise_restart
    r._pump = _raise_transport_error

    with pytest.raises(SessionRestartRequested):
        await r.run_once()


# ---------- Task 9（D11/G1 agent 側）：buffer v2＋command_ledger 執行去重 ----------
# spec D4 agent 端①-④，順序即正確性：①ledger 命中→不重執行，確保 outbox 有未送 ack
# （無則以存檔 result 補 append）；②scope 核對不符→scope_mismatch；③expiry 過期→
# expired（①先於③，S#4）；④執行 native→record_execution 同交易→泵送。

_FAR_FUTURE = "2099-01-01T00:00:00"
_PAST = "2000-01-01T00:00:00"


def _place_msg(cmd_id: str, *, account: str = "F1", mode: str = "sim",
               expires_at: str = _FAR_FUTURE) -> dict:
    return {"type": "place", "cmd_id": cmd_id, "account": account, "mode": mode,
            "expires_at": expires_at,
            "native": {"action": "Buy", "price": "0", "qty": 1, "price_type": "MKT",
                       "order_type": "IOC", "octype": "Auto"}}


async def test_resend_same_cmd_id_after_ack_confirmed_does_not_reexecute_native(tmp_path):
    """S#3：重連補送指令 → agent ledger 命中不重執行、重回存檔 ack。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    msg = _place_msg("c1")
    tr.incoming.put_nowait(msg)
    await _until(lambda: any(m.get("type") == "cmd_ack" for m in tr.sent))
    first_ack = next(m for m in tr.sent if m["type"] == "cmd_ack")
    assert len(child.ops) == 1

    tr.incoming.put_nowait({"type": "report_ack", "event_id": first_ack["event_id"]})
    await _until(lambda: buf.unsent_count() == 0)  # 第一筆 ack 已被 server 收到

    tr.incoming.put_nowait(msg)  # 重連補送：server 重送同一 cmd_id
    await _until(lambda: len([m for m in tr.sent if m.get("type") == "cmd_ack"]) >= 2)

    assert len(child.ops) == 1  # 沒有再打 native——ledger 命中不重執行
    second_ack = [m for m in tr.sent if m["type"] == "cmd_ack"][-1]
    assert second_ack["cmd_id"] == "c1" and second_ack["ok"] is True
    assert second_ack["result"]["ordno"] == "101AA1"
    assert second_ack["event_id"] != first_ack["event_id"]  # 補的是新一筆 outbox 列
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_scope_mismatch_command_rejected_without_calling_child(tmp_path):
    """R1-2/S#16 agent 側：指令 account/mode 與目前登入 scope 不符 → scope_mismatch，
    不進 native、不落 outbox（best-effort 直送，非 durable——遺失靠重送自然收斂）。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    tr.incoming.put_nowait(_place_msg("c1", account="OTHER"))
    await _until(lambda: any(m.get("type") == "cmd_ack" for m in tr.sent))
    ack = next(m for m in tr.sent if m["type"] == "cmd_ack")
    assert ack["ok"] is False and ack["error_kind"] == "scope_mismatch"
    assert child.ops == []
    assert buf.pending() == []
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_expired_command_rejected_without_calling_child(tmp_path):
    """S#4：過期指令 → agent 拒執行回 expired，不進 native、不落 outbox。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    tr.incoming.put_nowait({"type": "cancel", "cmd_id": "c1", "account": "F1", "mode": "sim",
                            "expires_at": _PAST, "ordno": "101AA1"})
    await _until(lambda: any(m.get("type") == "cmd_ack" for m in tr.sent))
    ack = next(m for m in tr.sent if m["type"] == "cmd_ack")
    assert ack["ok"] is False and ack["error_kind"] == "expired"
    assert child.ops == []
    assert buf.pending() == []
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_expired_but_ledger_hit_still_resends_cached_ack_not_expired(tmp_path):
    """S#4 關鍵順序：①先於③——已經真的執行過的指令，重送時就算附帶的 expires_at 已過期，
    也不能被③攔下改判 expired（那會誤導 server 判 failed+release，但其實已執行過一次，
    存檔 ack 才是唯一誠實的答案）。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    msg = _place_msg("c1")
    tr.incoming.put_nowait(msg)
    await _until(lambda: any(m.get("type") == "cmd_ack" for m in tr.sent))
    first_ack = next(m for m in tr.sent if m["type"] == "cmd_ack")
    tr.incoming.put_nowait({"type": "report_ack", "event_id": first_ack["event_id"]})
    await _until(lambda: buf.unsent_count() == 0)

    expired_resend = dict(msg, expires_at=_PAST)
    tr.incoming.put_nowait(expired_resend)
    await _until(lambda: len([m for m in tr.sent if m.get("type") == "cmd_ack"]) >= 2)

    assert len(child.ops) == 1  # 仍然沒有重打 native
    second_ack = [m for m in tr.sent if m["type"] == "cmd_ack"][-1]
    assert second_ack["ok"] is True and second_ack.get("error_kind") != "expired"
    assert second_ack["result"]["ordno"] == "101AA1"
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_update_scope_mismatch_and_expired_do_not_touch_child(tmp_path):
    """S#21 agent 側：update 指令的 scope_mismatch／expired 分支同樣不碰 native。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    tr.incoming.put_nowait({"type": "update", "cmd_id": "u1", "account": "WRONG",
                            "mode": "sim", "expires_at": _FAR_FUTURE,
                            "ordno": "101AA1", "qty": 2})
    await _until(lambda: any(m.get("type") == "cmd_ack" for m in tr.sent))
    ack1 = next(m for m in tr.sent if m["type"] == "cmd_ack")
    assert ack1["error_kind"] == "scope_mismatch"

    tr.incoming.put_nowait({"type": "update", "cmd_id": "u2", "account": "F1", "mode": "sim",
                            "expires_at": _PAST, "ordno": "101AA1", "qty": 2})
    await _until(lambda: len([m for m in tr.sent if m.get("type") == "cmd_ack"]) >= 2)
    ack2 = [m for m in tr.sent if m["type"] == "cmd_ack"][-1]
    assert ack2["error_kind"] == "expired"
    assert child.ops == []
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_pump_uses_row_stamped_account_over_session_account(tmp_path):
    """I7 來源端蓋章：callback 落 outbox 當下已蓋的 account/mode 優先於 runner 目前
    session 的帳號（同一 buffer 檔本受 assert_account tripwire 保護，這裡只驗證
    _pump 的欄位來源優先序本身）。"""
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    buf.append("deal_report", {"n": 1}, account="STAMPED", mode="sim")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.reports()) >= 1)
    assert tr.reports()[0]["account"] == "STAMPED"
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
