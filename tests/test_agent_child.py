"""SDK 子程序迴圈（#203 隔離）：child_main 序列處理 op、callback 同步落地 durable buffer、
例外一律結構化＋redact，迴圈本身不因單一 op 失敗而中斷。用 threading 版跑迴圈邏輯（Step 1
四個測試），另外一條 smoke 用真 multiprocessing spawn 驗 pickle/進入點/管線暢通（Inc0
Task 11，brief 逐字）。"""
import multiprocessing as mp
import threading
from decimal import Decimal

from quanquant.agent.buffer import DurableBuffer
from quanquant.agent.native_runner import child_main
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
    assert _rpc(conn, {"op": "connect"}) == {"ok": True, "account": "F1"}
    reply = _rpc(conn, {"op": "place", "action": "Buy", "price": "0", "qty": 1,
                        "price_type": "MKT", "order_type": "IOC", "octype": "Auto"})
    assert reply["ok"] and reply["result"]["ordno"] == "101AA1"
    buf = DurableBuffer(tmp_path / "o.db")
    kinds = [r.kind for r in buf.pending()]
    assert kinds == ["order_report", "deal_report"]        # callback 已同步落地
    _rpc(conn, {"op": "shutdown"}); t.join(timeout=5)


def test_cancel_unknown_maps_trade_not_found(tmp_path):
    conn, t = _start_child_thread(tmp_path)
    _rpc(conn, {"op": "connect"})
    reply = _rpc(conn, {"op": "cancel", "ordno": "NOPE"})
    assert reply == {"ok": False, "error_kind": "trade_not_found",
                     "message": reply["message"], "result": {"ordno": "NOPE"}}
    _rpc(conn, {"op": "shutdown"}); t.join(timeout=5)


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
    assert _rpc(parent_conn, {"op": "ping"}) == {"ok": True}  # 迴圈仍活著
    _rpc(parent_conn, {"op": "shutdown"}); t.join(timeout=5)


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
    _rpc(parent_conn, {"op": "shutdown"}); t.join(timeout=5)


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
    assert reply == {"ok": True, "result": {}}
    _rpc(conn, {"op": "shutdown"}); t.join(timeout=5)


def test_update_op_price_none(tmp_path):
    # price=None 透傳路徑（例如只改 qty 不改價）：FakeNativeClient.update 對 None 不 raise。
    conn, t = _start_child_thread(tmp_path)
    _rpc(conn, {"op": "connect"})
    _rpc(conn, {"op": "place", "action": "Buy", "price": "0", "qty": 1,
                "price_type": "MKT", "order_type": "IOC", "octype": "Auto"})
    reply = _rpc(conn, {"op": "update", "ordno": "101AA1", "price": None, "qty": 3,
                        "price_type": None})
    assert reply == {"ok": True, "result": {}}
    _rpc(conn, {"op": "shutdown"}); t.join(timeout=5)


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
    assert reply == {"ok": True, "result": {"payloads": [], "newest": None}}
    # after 帶 ISO 字串：驗 datetime.fromisoformat 解析不炸（FakeNativeClient 本身不檢查值）。
    reply2 = _rpc(conn, {"op": "reconcile", "after": "2026-08-04T09:00:00"})
    assert reply2 == {"ok": True, "result": {"payloads": [], "newest": None}}
    _rpc(conn, {"op": "shutdown"}); t.join(timeout=5)
