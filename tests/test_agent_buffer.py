import sqlite3
import threading
import pytest
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
    t.start()
    t.join()
    assert buf.unsent_count() == 1


def test_cross_instance_visibility_same_file(tmp_path):
    # 模擬「子程序寫、父程序讀」的跨程序共享（同檔不同連線）
    p = tmp_path / "o.db"
    writer, reader = DurableBuffer(p), DurableBuffer(p)
    eid = writer.append("deal_report", {"x": 1})
    assert [r.id for r in reader.pending()] == [eid]
    reader.mark_sent(eid)
    assert writer.unsent_count() == 0


# ---- codex round1 fix7（LOW）：outbox 累積無上限，加 prune_sent 定期清已送達列 ----

def test_prune_sent_removes_rows_older_than_retention_keeps_rest(tmp_path):
    p = tmp_path / "o.db"
    buf = DurableBuffer(p)
    old_id = buf.append("deal_report", {"n": "old"})
    recent_id = buf.append("deal_report", {"n": "recent"})
    unsent_id = buf.append("deal_report", {"n": "unsent"})
    buf.mark_sent(old_id)
    buf.mark_sent(recent_id)
    with sqlite3.connect(str(p)) as conn:
        conn.execute("UPDATE outbox SET sent_at = datetime('now', '-10 days') WHERE id = ?",
                     (old_id,))
        conn.execute("UPDATE outbox SET sent_at = datetime('now', '-1 days') WHERE id = ?",
                     (recent_id,))

    deleted = buf.prune_sent(retention_days=7)

    assert deleted == 1
    with sqlite3.connect(str(p)) as conn:
        remaining = {r[0] for r in conn.execute("SELECT id FROM outbox").fetchall()}
    assert old_id not in remaining
    assert recent_id in remaining
    assert unsent_id in remaining
    assert buf.unsent_count() == 1


# ---- codex round1 fix6（MEDIUM）：帳號切換 tripwire——outbox 有前一帳號未送回報時，
# 拒絕以不同帳號啟動，避免跨帳號錯配（券商回報被灌進錯的帳號 session）----

def test_assert_account_first_boot_writes_meta(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    buf.assert_account("F1")  # 空 meta：直接寫入，不 raise
    with sqlite3.connect(str(tmp_path / "o.db")) as conn:
        row = conn.execute("SELECT value FROM meta WHERE key = 'account'").fetchone()
    assert row == ("F1",)


def test_assert_account_same_account_restart_ok(tmp_path):
    p = tmp_path / "o.db"
    buf = DurableBuffer(p)
    buf.assert_account("F1")
    buf.append("deal_report", {"n": 1})  # 留下未送列
    buf2 = DurableBuffer(p)
    buf2.assert_account("F1")  # 同帳號重啟：即使有未送列也 ok


def test_assert_account_switch_with_unsent_rows_raises(tmp_path):
    p = tmp_path / "o.db"
    buf = DurableBuffer(p)
    buf.assert_account("F1")
    buf.append("deal_report", {"n": 1})  # 前一帳號留下未送回報
    buf2 = DurableBuffer(p)
    with pytest.raises(RuntimeError):
        buf2.assert_account("F2")


def test_assert_account_switch_without_unsent_rows_updates_meta(tmp_path):
    p = tmp_path / "o.db"
    buf = DurableBuffer(p)
    buf.assert_account("F1")
    eid = buf.append("deal_report", {"n": 1})
    buf.mark_sent(eid)  # 已送達，無未送列
    buf2 = DurableBuffer(p)
    buf2.assert_account("F2")  # 換帳號但無未送列 → 放行且更新 meta
    with sqlite3.connect(str(p)) as conn:
        row = conn.execute("SELECT value FROM meta WHERE key = 'account'").fetchone()
    assert row == ("F2",)


def test_init_auto_prunes_old_sent_rows_on_reopen(tmp_path):
    p = tmp_path / "o.db"
    buf = DurableBuffer(p)
    old_id = buf.append("deal_report", {"n": "old"})
    buf.mark_sent(old_id)
    with sqlite3.connect(str(p)) as conn:
        conn.execute("UPDATE outbox SET sent_at = datetime('now', '-30 days') WHERE id = ?",
                     (old_id,))

    DurableBuffer(p)  # 模擬 agent 重啟：__init__ 尾端自動跑一次 prune（預設 7 天）

    with sqlite3.connect(str(p)) as conn:
        remaining = {r[0] for r in conn.execute("SELECT id FROM outbox").fetchall()}
    assert old_id not in remaining
