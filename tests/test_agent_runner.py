import asyncio
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
        self.ping_ok = True
        self.request_exc = None
    def start(self):
        self.starts += 1
        self.alive = True
        return "F1"
    def request(self, op, *, timeout):
        self.ops.append(op)
        if self.request_exc:
            raise self.request_exc
        return {"ok": True, "result": {"ordno": "101AA1", "broker_order_id": "101AA1"}}
    def ping(self, *, timeout):
        return self.ping_ok
    def terminate(self):
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
    assert tr.sent[0]["protocol"] == 1
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
    tr.incoming.put_nowait({"type": "place", "cmd_id": "c1", "mode": "sim",
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
    tr.incoming.put_nowait({"type": "cancel", "cmd_id": "c2", "mode": "sim",
                            "ordno": "101AA1"})
    await _until(lambda: any(m.get("type") == "cmd_ack" for m in tr.sent))
    ack = next(m for m in tr.sent if m.get("type") == "cmd_ack")
    assert ack["ok"] is False and ack["error_kind"] == "timeout"
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
    def __init__(self, alive: bool = True):
        self._alive = alive
        self.killed = False
        self.joined = False

    def is_alive(self) -> bool:
        return self._alive

    def kill(self) -> None:
        self.killed = True
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
        self.closed = False

    def send(self, obj) -> None:
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
