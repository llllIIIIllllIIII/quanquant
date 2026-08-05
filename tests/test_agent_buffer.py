import threading
from quanquant.agent.buffer import DurableBuffer


def test_append_survives_reopen(tmp_path):
    p = tmp_path / "outbox.db"
    eid = DurableBuffer(p).append("deal_report", {"trade_id": "T1", "中文": "好"})
    rows = DurableBuffer(p).pending()          # 全新連線（模擬程序重啟）
    assert rows[0].id == eid and rows[0].payload["trade_id"] == "T1"
    assert rows[0].payload["中文"] == "好"


def test_mark_sent_removes_from_pending(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    eid = buf.append("order_report", {"k": 1})
    buf.mark_sent(eid)
    assert buf.pending() == [] and buf.unsent_count() == 0


def test_pending_orders_by_id_and_respects_limit(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    ids = [buf.append("deal_report", {"n": i}) for i in range(5)]
    got = buf.pending(limit=3)
    assert [r.id for r in got] == ids[:3]


def test_append_from_thread_visible_to_main(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    t = threading.Thread(target=lambda: buf.append("deal_report", {"x": 1}))
    t.start(); t.join()
    assert buf.unsent_count() == 1


def test_cross_instance_visibility_same_file(tmp_path):
    # 模擬「子程序寫、父程序讀」的跨程序共享（同檔不同連線）
    p = tmp_path / "o.db"
    writer, reader = DurableBuffer(p), DurableBuffer(p)
    eid = writer.append("deal_report", {"x": 1})
    assert [r.id for r in reader.pending()] == [eid]
    reader.mark_sent(eid)
    assert writer.unsent_count() == 0
