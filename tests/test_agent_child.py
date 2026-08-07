"""SDK 子程序迴圈（#203 隔離）：child_main 序列處理 op、callback 同步落地 durable buffer、
例外一律結構化＋redact，迴圈本身不因單一 op 失敗而中斷。用 threading 版跑迴圈邏輯（Step 1
四個測試），另外一條 smoke 用真 multiprocessing spawn 驗 pickle/進入點/管線暢通（Inc0
Task 11，brief 逐字）。"""
import multiprocessing as mp
import threading
from decimal import Decimal

import pytest

import quanquant.agent.native_runner as nr
from quanquant.agent.buffer import DurableBuffer
from quanquant.agent.native_runner import ChildFailstopLatch, _dispatch, child_main
from quanquant.agent.testing import FakeNativeClient, fake_native_factory
from quanquant.broker.shioaji_adapter import ShioajiAdapter
from quanquant.broker.supervisor import BrokerSupervisor


def _start_child_thread(tmp_path):
    """用執行緒跑 child_main（測迴圈邏輯；真 Process spawn 另測一條 smoke）。"""
    parent_conn, child_conn = mp.Pipe()
    t = threading.Thread(
        target=child_main, args=(child_conn,),
        kwargs=dict(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                    mode="sim", buffer_path=str(tmp_path / "o.db"),
                    native_factory=fake_native_factory),
        daemon=True)
    t.start()
    return parent_conn, t


def _rpc(conn, msg, timeout=5):
    conn.send(msg)
    assert conn.poll(timeout), f"子程序 {msg['op']} 無回應"
    return conn.recv()


def test_connect_then_place_acks_and_persists_reports(tmp_path):
    conn, t = _start_child_thread(tmp_path)
    # codex round1 fix1(a)：reply 多帶 rpc_id 欄位（op 未帶 rpc_id 時原樣回 None）——
    # 不再用嚴格 dict 相等，改斷言子集。
    reply0 = _rpc(conn, {"op": "connect"})
    assert reply0["ok"] is True and reply0["account"] == "F1"
    reply = _rpc(conn, {"op": "place", "action": "Buy", "price": "0", "qty": 1,
                        "price_type": "MKT", "order_type": "IOC", "octype": "Auto"})
    assert reply["ok"] and reply["result"]["ordno"] == "101AA1"
    buf = DurableBuffer(tmp_path / "o.db")
    kinds = [r.kind for r in buf.pending()]
    assert kinds == ["order_report", "deal_report"]        # callback 已同步落地
    _rpc(conn, {"op": "shutdown"})
    t.join(timeout=5)


# ---- Task 9（D5/I7）：callback 落 outbox 當下蓋章 account/mode，來源端不可變 ----

def test_reports_stamped_with_account_and_mode_at_callback_time(tmp_path):
    conn, t = _start_child_thread(tmp_path)
    _rpc(conn, {"op": "connect"})
    _rpc(conn, {"op": "place", "action": "Buy", "price": "0", "qty": 1,
                "price_type": "MKT", "order_type": "IOC", "octype": "Auto"})
    buf = DurableBuffer(tmp_path / "o.db")
    rows = buf.pending()
    assert len(rows) == 2
    assert all(r.account == "F1" and r.mode == "sim" for r in rows)
    _rpc(conn, {"op": "shutdown"})
    t.join(timeout=5)


def test_cancel_unknown_maps_exception_like_production(tmp_path):
    # codex round1 fix4(b)：FakeNativeClient.cancel 對未知 ordno 原本 raise
    # TradeNotFoundError，但 production 的 native.cancel()（native.py 227-233）對同樣情況
    # raise 一般 OrderError（TradeNotFoundError 只用在 update()）。改讓 fake 對齊 production
    # 契約——error_kind 變成 "exception"，不再是 "trade_not_found"。
    conn, t = _start_child_thread(tmp_path)
    _rpc(conn, {"op": "connect"})
    reply = _rpc(conn, {"op": "cancel", "ordno": "NOPE"})
    assert reply["ok"] is False and reply["error_kind"] == "exception"
    assert "NOPE" in reply["message"]
    _rpc(conn, {"op": "shutdown"})
    t.join(timeout=5)


def test_exception_reply_is_redacted_and_loop_survives(tmp_path):
    # 用可辨識的憑證值 + FakeNativeClient 的 BOOM 後門（cancel("BOOM") raise 內嵌 api_key）
    parent_conn, child_conn = mp.Pipe()
    t = threading.Thread(
        target=child_main, args=(child_conn,),
        kwargs=dict(credentials={"api_key": "SECRET-KEY-123", "secret_key": "SECRET-VAL-456"},
                    symbol="TXF", mode="sim", buffer_path=str(tmp_path / "o.db"),
                    native_factory=fake_native_factory),
        daemon=True)
    t.start()
    _rpc(parent_conn, {"op": "connect"})
    reply = _rpc(parent_conn, {"op": "cancel", "ordno": "BOOM"})
    assert reply["ok"] is False and reply["error_kind"] == "exception"
    assert "SECRET-KEY-123" not in reply["message"]         # redact_secrets 已遮蔽
    assert "SECRET-VAL-456" not in reply["message"]
    # codex round1 fix1(a)：reply 多帶 rpc_id 欄位——不再用嚴格 dict 相等。
    assert _rpc(parent_conn, {"op": "ping"})["ok"] is True  # 迴圈仍活著
    _rpc(parent_conn, {"op": "shutdown"})
    t.join(timeout=5)


# ---- codex round1 fix3（HIGH）：例外處理原本用 log.exception（帶 exc_info/traceback），
# Shioaji 例外字串可能內嵌憑證——本機 log 檔會外洩。改 log.error 帶已 redact 過的訊息、
# 不附 exc_info。----

def test_exception_log_does_not_leak_credentials(tmp_path, caplog):
    caplog.set_level("ERROR", logger="quanquant.agent.native_runner")
    parent_conn, child_conn = mp.Pipe()
    t = threading.Thread(
        target=child_main, args=(child_conn,),
        kwargs=dict(credentials={"api_key": "SECRET-KEY-999", "secret_key": "SECRET-VAL-999"},
                    symbol="TXF", mode="sim", buffer_path=str(tmp_path / "o.db"),
                    native_factory=fake_native_factory),
        daemon=True)
    t.start()
    _rpc(parent_conn, {"op": "connect"})
    _rpc(parent_conn, {"op": "cancel", "ordno": "BOOM"})
    _rpc(parent_conn, {"op": "shutdown"})
    t.join(timeout=5)

    assert "SECRET-KEY-999" not in caplog.text
    assert "SECRET-VAL-999" not in caplog.text
    assert "Traceback" not in caplog.text   # log.error 不帶 exc_info，不應印出 traceback


def test_child_refuses_non_sim_mode(tmp_path):
    parent_conn, child_conn = mp.Pipe()
    t = threading.Thread(
        target=child_main, args=(child_conn,),
        kwargs=dict(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                    mode="real", buffer_path=str(tmp_path / "o.db"),
                    native_factory=fake_native_factory),
        daemon=True)
    t.start()
    reply = _rpc(parent_conn, {"op": "connect"})
    assert reply["ok"] is False and reply["error_kind"] == "mode_mismatch"
    _rpc(parent_conn, {"op": "shutdown"})
    t.join(timeout=5)


# ---- C4（HIGH，codex 終審）：child 進程內 thread-safe latch——native 呼叫正前方再檢查一次
# ----


def test_dispatch_blocks_native_when_latch_tripped_before_check(tmp_path):
    """模擬「parent 最後一次 `AgentRunner._latched` 檢查已通過、`asyncio.to_thread(child.
    request, ...)` 尚未真正排程執行」這段窗——用 `threading.Barrier` 讓「callback 執行緒
    trip() latch」與「主執行緒即將呼叫 `_dispatch()`」在同一個會合點交錯，`t.join()` 確保
    trip() 保證發生在 `_dispatch()` 的 tripped 檢查之前才放行（而非仰賴機率性的真實競態），
    驗證 native.place 完全不會被呼叫——子程序 callback 落地也完全沒發生，buffer 仍是空的。"""
    buf = DurableBuffer(tmp_path / "o.db")
    latch = ChildFailstopLatch()
    native = fake_native_factory(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                                 mode="sim", on_raw=buf.append)
    native.connect()

    barrier = threading.Barrier(2)

    def _callback_thread():
        barrier.wait(timeout=5)
        latch.trip()  # 模擬 SDK callback 執行緒偵測到雙寫落地失敗，同步 trip

    t = threading.Thread(target=_callback_thread)
    t.start()
    barrier.wait(timeout=5)  # 與 callback 執行緒同時抵達會合點
    t.join(timeout=5)  # 確保 trip() 已完成才做 dispatch 檢查（模擬「trip 發生在檢查前」）

    reply = _dispatch(native, {"op": "place", "action": "Buy", "price": "0", "qty": 1,
                                "price_type": "MKT", "order_type": "IOC", "octype": "Auto"},
                       latch=latch)
    assert reply["ok"] is False and reply["error_kind"] == "failstop"
    assert buf.pending() == []  # native.place 完全沒被呼叫，沒有任何 order/deal 回報落地


def test_dispatch_ignores_latch_for_readonly_ops(tmp_path):
    """C4：latch 只擋 place/cancel/update 三種 mutating op——唯讀 op（ping/query_qty/
    reconcile）不受影響，latch 中仍能正常回應（G2②的既有邊界：latch 不擋唯讀查詢）。"""
    buf = DurableBuffer(tmp_path / "o.db")
    latch = ChildFailstopLatch()
    latch.trip()
    native = fake_native_factory(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                                 mode="sim", on_raw=buf.append)
    native.connect()

    reply = _dispatch(native, {"op": "ping"}, latch=latch)
    assert reply["ok"] is True


# ---- R3-2（HIGH，codex 終審 round3）：ping 回傳帶上 child 本地 latch 狀態——recovery
# 用來確認「respawn 後的新 child 是否又立即 latch」，光是 ok=True 不足以支撐這個判斷 ----


def test_dispatch_ping_reports_latched_true_when_child_latch_tripped(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    latch = ChildFailstopLatch()
    latch.trip()
    native = fake_native_factory(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                                 mode="sim", on_raw=buf.append)
    native.connect()

    reply = _dispatch(native, {"op": "ping"}, latch=latch)
    assert reply["ok"] is True and reply["latched"] is True


def test_dispatch_ping_reports_latched_false_when_child_latch_not_tripped(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    latch = ChildFailstopLatch()
    native = fake_native_factory(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                                 mode="sim", on_raw=buf.append)
    native.connect()

    reply = _dispatch(native, {"op": "ping"}, latch=latch)
    assert reply["ok"] is True and reply["latched"] is False


def test_dispatch_ping_reports_latched_false_when_no_latch_supplied(tmp_path):
    """相容尚未接線 latch 的呼叫端（latch=None，預設值）——不 raise，回 latched=False。"""
    buf = DurableBuffer(tmp_path / "o.db")
    native = fake_native_factory(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                                 mode="sim", on_raw=buf.append)
    native.connect()

    reply = _dispatch(native, {"op": "ping"})
    assert reply["ok"] is True and reply["latched"] is False


# ---- R3-3（MEDIUM，codex 終審 round3）：failstop IPC 通知帶上送出當下的 child
# generation——respawn 換代後，父程序靠這個欄位丟棄舊 child 的過期通知，不誤 latch 目前
# 這一代健康的 child ----


def test_trigger_failstop_latch_notice_carries_generation(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    parent_conn, child_conn = mp.Pipe()
    latch = ChildFailstopLatch()

    nr._trigger_failstop_latch(buf, child_conn, latch, "detail", generation=3)

    assert parent_conn.poll(2)
    notice = parent_conn.recv()
    assert notice == {"type": "failstop", "detail": "detail", "generation": 3}


def test_trigger_failstop_latch_notice_generation_defaults_to_zero(tmp_path):
    """相容尚未傳入 generation 的既有呼叫端——預設 0，不 raise。"""
    buf = DurableBuffer(tmp_path / "o.db")
    parent_conn, child_conn = mp.Pipe()
    latch = ChildFailstopLatch()

    nr._trigger_failstop_latch(buf, child_conn, latch, "detail")

    notice = parent_conn.recv()
    assert notice["generation"] == 0


def test_wrap_on_raw_double_failure_notice_carries_generation(tmp_path, monkeypatch):
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
    on_raw = nr._wrap_on_raw(buf, mode="sim", account_box=account_box, failstop_conn=child_conn,
                              generation=7)

    with pytest.raises(RuntimeError, match="degraded boom"):
        on_raw("deal_report", {"n": 1})

    notice = parent_conn.recv()
    assert notice["generation"] == 7


def test_child_main_ping_carries_generation_from_start_kwarg(tmp_path):
    """`child_main` 的 `generation` kwarg（由 `ChildHandle.start()` 傳入）要能一路帶到
    `_wrap_on_raw`——這裡直接驗證 child_main 有把它原樣接住並轉交（透過真的走一次
    connect+ping，確認迴圈沒有因為多了這個 kwarg 而炸開）。"""
    parent_conn, child_conn = mp.Pipe()
    t = threading.Thread(
        target=child_main, args=(child_conn,),
        kwargs=dict(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                    mode="sim", buffer_path=str(tmp_path / "o.db"),
                    native_factory=fake_native_factory, generation=9),
        daemon=True)
    t.start()
    _rpc(parent_conn, {"op": "connect"})
    reply = _rpc(parent_conn, {"op": "ping"})
    assert reply["ok"] is True and reply["latched"] is False
    _rpc(parent_conn, {"op": "shutdown"})
    t.join(timeout=5)


def test_child_main_rejects_mutating_op_after_dual_write_failure_trips_latch(tmp_path, monkeypatch):
    """C4：`child_main` 主迴圈真的接線 latch——SDK callback（這裡用 `FakeNativeClient`
    的同步呼叫模擬）雙寫落地失敗時，經 `_wrap_on_raw` 觸發的 `latch.trip()` 必須讓緊接著
    的下一筆 mutating RPC 在 `_dispatch` 的 native 呼叫前被擋下。用 monkeypatch
    `DurableBuffer.append`（class 層級——`child_main` 內部自建的 buffer instance 也會受
    影響，測試拿不到那個 instance 的參照）模擬主寫入失敗；退化寫入走真實檔案路徑（正常
    會成功，只 latch 不 raise，見 C5）。驗證：第一筆 place 本身仍完整執行（native.place
    已經被呼叫，callback 只是在事後才發現落地失敗），但**緊接著的下一筆** mutating op
    會被本地 latch 擋下——這正是 C4 要堵的窗：即使父程序的 IPC 通知還沒被
    `_failstop_watchdog` 處理，child 本地已經知道自己壞了。"""
    monkeypatch.setattr(
        DurableBuffer, "append",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("primary boom"))
    )
    conn, t = _start_child_thread(tmp_path)
    _rpc(conn, {"op": "connect"})
    reply1 = _rpc(conn, {"op": "place", "action": "Buy", "price": "0", "qty": 1,
                         "price_type": "MKT", "order_type": "IOC", "octype": "Auto"})
    assert reply1["ok"] is True  # 這筆 native 呼叫本身沒被擋（latch 是「呼叫前」才生效）

    reply2 = _rpc(conn, {"op": "cancel", "ordno": "101AA1"})
    assert reply2["ok"] is False and reply2["error_kind"] == "failstop"

    _rpc(conn, {"op": "shutdown"})
    t.join(timeout=5)


# ---- N1（HIGH，codex 終審 round2）：primary append 失敗後，latch 必須是「第一個動作」，
# 排在退化寫入/sentinel fsync/IPC 這些 I/O 之前——I/O 阻塞期間 child 主迴圈不得放行新的
# mutating native 呼叫。用可控的 threading.Event 讓「callback 執行緒卡在 I/O 中」與
# 「主執行緒斷言 tripped＋跑 _dispatch」精確交錯（不是機率性競態）。----


def test_on_raw_trips_latch_before_degraded_write_io_blocks(tmp_path, monkeypatch):
    """primary `buffer.append` 拋錯後，`_wrap_on_raw._on_raw` 必須先 `latch.trip()` 才去做
    退化寫入——這裡把 `_try_degraded_write` 換成卡住不放的假 I/O，驗證：卡住期間
    `latch.tripped` 已經是 True，且同一時間 `_dispatch` 一個 mutating op 會被擋下、
    native 完全不會被呼叫。舊版（trip 排在退化寫入之後）在這個卡住的窗口內
    `latch.tripped` 仍是 False，這支測試會在舊版程式碼上紅（revert 這次修法即可重現）。"""
    buf = DurableBuffer(tmp_path / "o.db")
    monkeypatch.setattr(
        buf, "append", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("primary boom"))
    )
    entered_io = threading.Event()
    release_io = threading.Event()

    def _blocked_degraded_write(*a, **k):
        entered_io.set()
        assert release_io.wait(timeout=5), "退化寫入卡住逾時，測試設計有誤"

    monkeypatch.setattr(nr, "_try_degraded_write", _blocked_degraded_write)

    account_box = nr._AccountBox()
    account_box.value = "F1"
    latch = nr.ChildFailstopLatch()
    on_raw = nr._wrap_on_raw(buf, mode="sim", account_box=account_box, latch=latch)

    callback_thread = threading.Thread(target=lambda: on_raw("deal_report", {"n": 1}))
    callback_thread.start()
    try:
        assert entered_io.wait(timeout=5), "callback 執行緒應已卡在退化寫入 I/O 內"

        # N1 核心斷言：退化寫入 I/O 仍卡住的當下，latch 必須已經 trip。
        assert latch.tripped is True

        native = fake_native_factory(
            credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF", mode="sim",
            on_raw=lambda *a, **k: None,
        )
        native.connect()
        reply = _dispatch(
            native, {"op": "place", "action": "Buy", "price": "0", "qty": 1,
                     "price_type": "MKT", "order_type": "IOC", "octype": "Auto"},
            latch=latch,
        )
        assert reply["ok"] is False and reply["error_kind"] == "failstop"
    finally:
        release_io.set()
        callback_thread.join(timeout=5)


def test_trigger_failstop_latch_trips_before_sentinel_write_io_blocks(tmp_path, monkeypatch):
    """`_trigger_failstop_latch` 內部也必須把 trip 排在 sentinel fsync／IPC 之前——即使
    未來有呼叫路徑不經 `_wrap_on_raw` 提早 trip，直接呼叫這個函式也要保證同樣的順序。
    用卡住的 `buffer.write_sentinel` 模擬 fsync 阻塞，驗證卡住期間 `_dispatch` 已經看得到
    tripped=True、native 完全不會被呼叫。"""
    buf = DurableBuffer(tmp_path / "o.db")
    entered_io = threading.Event()
    release_io = threading.Event()

    def _blocked_write_sentinel(*a, **k):
        entered_io.set()
        assert release_io.wait(timeout=5), "sentinel 寫入卡住逾時，測試設計有誤"

    monkeypatch.setattr(buf, "write_sentinel", _blocked_write_sentinel)
    latch = nr.ChildFailstopLatch()

    t = threading.Thread(
        target=lambda: nr._trigger_failstop_latch(buf, None, latch, "detail")
    )
    t.start()
    try:
        assert entered_io.wait(timeout=5), "應已卡在 sentinel 寫入 I/O 內"
        assert latch.tripped is True  # I/O 仍卡住時 latch 已經 trip

        native = fake_native_factory(
            credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF", mode="sim",
            on_raw=lambda *a, **k: None,
        )
        native.connect()
        reply = _dispatch(native, {"op": "cancel", "ordno": "X"}, latch=latch)
        assert reply["ok"] is False and reply["error_kind"] == "failstop"
    finally:
        release_io.set()
        t.join(timeout=5)


def test_real_process_spawn_smoke(tmp_path):
    """真 multiprocessing spawn 一次：驗 pickle/進入點/管線暢通。"""
    ctx = mp.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe()
    p = ctx.Process(target=child_main, args=(child_conn,),
                    kwargs=dict(credentials={"api_key": "k", "secret_key": "s"},
                                symbol="TXF", mode="sim",
                                buffer_path=str(tmp_path / "o.db"),
                                native_factory=fake_native_factory))
    p.start()
    assert _rpc(parent_conn, {"op": "connect"}, timeout=30)["ok"]
    assert _rpc(parent_conn, {"op": "ping"})["ok"]
    _rpc(parent_conn, {"op": "shutdown"})
    p.join(timeout=10)
    assert p.exitcode == 0


# ---- 建議追加：FakeNativeClient 產生的 payload 必須能被真 adapter mapper 解析 ----
# （2026-07-28 實測定案的欄位形狀——如果這裡漏了欄位，Task 15 整合測試才會在更難除錯的
# 位置炸開；在這裡先鎖住，讓 mapper 不相容的回歸能在最靠近源頭處被抓到。）

def _adapter_stub_for_mapper():
    return ShioajiAdapter(
        api_key="k", secret_key="s", ca_path=None, ca_passwd=None, person_id=None,
        symbol="TXF", mode="sim", session_factory=lambda: None,
        supervisor=BrokerSupervisor(),
    )


def test_fake_native_client_reports_are_mapper_compatible():
    captured: list[tuple[str, dict]] = []
    client = FakeNativeClient(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                               mode="sim", on_raw=lambda kind, payload: captured.append((kind, payload)))
    assert client.connect() == "F1"
    # 用非零價格（LMT）：deal_report 要能通過 Fill.__post_init__ 的 price>0 驗證，
    # 才能真的驗到 _map_deal_report 全鏈路成功（MKT 的 price="0" 只適合測 ack/持久化路徑）。
    result = client.place(action="Buy", price=Decimal("18500"), qty=1, price_type="LMT",
                           order_type="IOC", octype="Auto")
    assert result == {"ordno": "101AA1", "broker_order_id": "101AA1"}
    assert [kind for kind, _ in captured] == ["order_report", "deal_report"]

    adapter = _adapter_stub_for_mapper()
    order_kind, order_payload = captured[0]
    deal_kind, deal_payload = captured[1]

    order_report = adapter._map_order_report(order_payload)          # 不 raise
    assert order_report.ordno == "101AA1" and order_report.status == "submitted"

    fill = adapter._map_deal_report(deal_payload)                    # 不 raise
    assert fill.ordno == "101AA1" and fill.qty == 1 and fill.action == "Buy"


# ---- Task 11 審查遺留（Task 12 依賴這三個契約）：update/reconcile op 的正式回歸測試 ----


def test_update_op_with_price(tmp_path):
    conn, t = _start_child_thread(tmp_path)
    _rpc(conn, {"op": "connect"})
    _rpc(conn, {"op": "place", "action": "Buy", "price": "0", "qty": 1,
                "price_type": "MKT", "order_type": "IOC", "octype": "Auto"})
    reply = _rpc(conn, {"op": "update", "ordno": "101AA1", "price": "21600", "qty": 2,
                        "price_type": "LMT"})
    # codex round1 fix1(a)：reply 多帶 rpc_id 欄位——不再用嚴格 dict 相等。
    assert reply["ok"] is True and reply["result"] == {}
    _rpc(conn, {"op": "shutdown"})
    t.join(timeout=5)


def test_update_op_price_none(tmp_path):
    # price=None 透傳路徑（例如只改 qty 不改價）：FakeNativeClient.update 對 None 不 raise。
    conn, t = _start_child_thread(tmp_path)
    _rpc(conn, {"op": "connect"})
    _rpc(conn, {"op": "place", "action": "Buy", "price": "0", "qty": 1,
                "price_type": "MKT", "order_type": "IOC", "octype": "Auto"})
    reply = _rpc(conn, {"op": "update", "ordno": "101AA1", "price": None, "qty": 3,
                        "price_type": None})
    # codex round1 fix1(a)：reply 多帶 rpc_id 欄位——不再用嚴格 dict 相等。
    assert reply["ok"] is True and reply["result"] == {}
    _rpc(conn, {"op": "shutdown"})
    t.join(timeout=5)


# ---- codex round1 fix6（MEDIUM）：connect 成功後呼 buffer.assert_account——outbox 有
# 前一帳號未送回報時，換帳號啟動要被擋下，避免回報跨帳號錯配。----

def test_connect_account_switch_with_unsent_rows_fails(tmp_path):
    # 先塞入「前一帳號 F2」的 meta + 未送列，模擬換帳號啟動（FakeNativeClient.connect
    # 固定回 "F1"）。
    pre = DurableBuffer(tmp_path / "o.db")
    pre.assert_account("F2")
    pre.append("deal_report", {"n": 1})

    conn, t = _start_child_thread(tmp_path)
    reply = _rpc(conn, {"op": "connect"})
    assert reply["ok"] is False
    assert "F2" in reply["message"] and "F1" in reply["message"]
    # codex round2 fix4(a)：reply 帶可辨識標記 error_kind="account_mismatch"——runner.py
    # 的 ChildHandle.start() 靠這個欄位判斷要 raise FatalAgentError（停止重試，防止
    # run_forever 的 backoff 迴圈每輪都真的重打一次 Shioaji 登入、燒配額），而不是把它當
    # 一般連線失敗無限重試。
    assert reply["error_kind"] == "account_mismatch"
    _rpc(conn, {"op": "shutdown"})
    t.join(timeout=5)


def test_connect_same_account_restart_ok_even_with_unsent_rows(tmp_path):
    conn, t = _start_child_thread(tmp_path)
    reply = _rpc(conn, {"op": "connect"})
    assert reply["ok"] is True and reply["account"] == "F1"
    _rpc(conn, {"op": "place", "action": "Buy", "price": "0", "qty": 1,
                "price_type": "MKT", "order_type": "IOC", "octype": "Auto"})  # 留未送列
    _rpc(conn, {"op": "shutdown"})
    t.join(timeout=5)

    conn2, t2 = _start_child_thread(tmp_path)   # 模擬同帳號重啟（同一 buffer 檔）
    reply2 = _rpc(conn2, {"op": "connect"})
    assert reply2["ok"] is True and reply2["account"] == "F1"
    _rpc(conn2, {"op": "shutdown"})
    t2.join(timeout=5)


def test_reconcile_op_reply_shape(tmp_path):
    conn, t = _start_child_thread(tmp_path)
    _rpc(conn, {"op": "connect"})
    reply = _rpc(conn, {"op": "reconcile", "after": None})
    # codex round1 fix1(a)：reply 多帶 rpc_id 欄位——不再用嚴格 dict 相等。
    assert reply["ok"] is True and reply["result"] == {"payloads": [], "newest": None}
    # after 帶 ISO 字串：驗 datetime.fromisoformat 解析不炸（FakeNativeClient 本身不檢查值）。
    reply2 = _rpc(conn, {"op": "reconcile", "after": "2026-08-04T09:00:00"})
    assert reply2["ok"] is True and reply2["result"] == {"payloads": [], "newest": None}
    _rpc(conn, {"op": "shutdown"})
    t.join(timeout=5)


# ---- Task 11（G3/D8）：query_qty op，等價 shioaji_adapter._query_order_qty_blocking ----


def test_query_qty_op_returns_current_qty(tmp_path):
    conn, t = _start_child_thread(tmp_path)
    _rpc(conn, {"op": "connect"})
    place_reply = _rpc(conn, {"op": "place", "action": "Buy", "price": "0", "qty": 3,
                              "price_type": "MKT", "order_type": "IOC", "octype": "Auto"})
    ordno = place_reply["result"]["ordno"]
    reply = _rpc(conn, {"op": "query_qty", "ordno": ordno})
    assert reply["ok"] is True and reply["result"] == {"qty": 3}
    _rpc(conn, {"op": "shutdown"})
    t.join(timeout=5)


def test_query_qty_op_returns_none_when_order_not_found(tmp_path):
    """委託已從券商目前清單消失（如已完全結案/從未存在）——回 qty=None，不猜測。"""
    conn, t = _start_child_thread(tmp_path)
    _rpc(conn, {"op": "connect"})
    reply = _rpc(conn, {"op": "query_qty", "ordno": "NOPE"})
    assert reply["ok"] is True and reply["result"] == {"qty": None}
    _rpc(conn, {"op": "shutdown"})
    t.join(timeout=5)
