import json
import sqlite3
import threading
import pytest
from quanquant.agent.buffer import DurableBuffer, RefuseStartError


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


# ---- Task 9（D11/G1 agent 側）：buffer schema v2 升級（outbox +account/mode/cmd_id、
# 新表 command_ledger、meta['schema_version']）＋ command_ledger 執行去重 ----

def _v1_schema_at(path) -> None:
    """手工建一個 Inc0 舊版（v1）outbox：無 account/mode/cmd_id 欄、無 command_ledger 表、
    meta 無 schema_version——模擬升級前存量部署，驗證 D11 三種升級情境。"""
    with sqlite3.connect(str(path)) as conn:
        conn.executescript(
            "CREATE TABLE outbox ("
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "  kind TEXT NOT NULL,"
            "  payload TEXT NOT NULL,"
            "  created_at TEXT NOT NULL DEFAULT (datetime('now')),"
            "  sent_at TEXT"
            ");"
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);"
        )


def _table_columns(path, table: str) -> set[str]:
    with sqlite3.connect(str(path)) as conn:
        return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def test_new_file_builds_schema_v2_directly(tmp_path):
    p = tmp_path / "o.db"
    DurableBuffer(p)
    assert {"account", "mode", "cmd_id"} <= _table_columns(p, "outbox")
    assert _table_columns(p, "command_ledger") == {"cmd_id", "kind", "result", "executed_at"}
    with sqlite3.connect(str(p)) as conn:
        row = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    assert row == ("2",)


def test_v1_buffer_with_unsent_rows_refuses_start(tmp_path):
    p = tmp_path / "o.db"
    _v1_schema_at(p)
    with sqlite3.connect(str(p)) as conn:
        conn.execute("INSERT INTO outbox (kind, payload) VALUES ('deal_report', '{}')")
    with pytest.raises(RefuseStartError):
        DurableBuffer(p)
    # 拒啟不能動到既有資料——舊版格式必須原封不動，操作者才能退回舊版 agent 繼續送完。
    assert "cmd_id" not in _table_columns(p, "outbox")
    with sqlite3.connect(str(p)) as conn:
        row = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    assert row is None


def test_v1_buffer_clean_upgrade_rebuilds_schema_and_keeps_other_meta(tmp_path):
    p = tmp_path / "o.db"
    _v1_schema_at(p)
    with sqlite3.connect(str(p)) as conn:
        conn.execute(
            "INSERT INTO outbox (kind, payload, sent_at) "
            "VALUES ('deal_report', '{}', datetime('now'))"
        )  # 已送達，無未送列——「乾淨」升級前提
        conn.execute("INSERT INTO meta (key, value) VALUES ('account', 'F1')")

    DurableBuffer(p)  # 無未送列：乾淨升級，重建 schema（僅限空 outbox，D11）

    assert {"account", "mode", "cmd_id"} <= _table_columns(p, "outbox")
    assert _table_columns(p, "command_ledger") == {"cmd_id", "kind", "result", "executed_at"}
    with sqlite3.connect(str(p)) as conn:
        version = conn.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()
        account = conn.execute("SELECT value FROM meta WHERE key = 'account'").fetchone()
    assert version == ("2",)
    assert account == ("F1",)  # meta 表本身不重建，帳號切換 tripwire 記錄留存


def test_lookup_command_returns_none_when_absent(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    assert buf.lookup_command("c1") is None


def test_record_execution_writes_ledger_and_outbox_ack_in_one_go(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    result = {"ok": True, "result": {"ordno": "101AA1"}, "error_kind": None, "message": None}

    event_id = buf.record_execution("c1", "place", result, account="F1", mode="sim")

    assert buf.lookup_command("c1") == json.dumps(result, ensure_ascii=False)
    pending = buf.pending()
    assert len(pending) == 1
    row = pending[0]
    assert row.id == event_id
    assert row.cmd_id == "c1" and row.kind == "cmd_ack"
    assert row.payload == result
    assert row.account == "F1" and row.mode == "sim"


def test_ensure_cmd_ack_pending_appends_fresh_row_when_previous_already_sent(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    result = {"ok": False, "result": None, "error_kind": "timeout", "message": "逾時"}
    first_id = buf.record_execution("c1", "cancel", result, account="F1", mode="sim")
    buf.mark_sent(first_id)  # 第一筆 ack 已被 server 收到（DownReportAck）

    cached = buf.lookup_command("c1")
    second_id = buf.ensure_cmd_ack_pending("c1", cached, account="F1", mode="sim")

    assert second_id != first_id
    pending_ids = [r.id for r in buf.pending()]
    assert pending_ids == [second_id]  # 只補一筆未送 ack，不影響已送列


def test_ensure_cmd_ack_pending_noop_when_unsent_row_already_pending(tmp_path):
    """R2-9：同一 cmd 至多一筆未送 ack——check-and-insert，不重複 append。"""
    buf = DurableBuffer(tmp_path / "o.db")
    result = {"ok": True, "result": {}, "error_kind": None, "message": None}
    first_id = buf.record_execution("c1", "update", result, account="F1", mode="sim")

    cached = buf.lookup_command("c1")
    second_id = buf.ensure_cmd_ack_pending("c1", cached, account="F1", mode="sim")

    assert second_id == first_id
    assert len(buf.pending()) == 1


def test_outbox_cmd_id_partial_unique_index_blocks_two_unsent_rows_same_cmd(tmp_path):
    """R2-9 最後一道防線：即使繞過 buffer.py 的 check-and-insert 直接寫 SQL，partial
    unique index（cmd_id WHERE sent_at IS NULL）本身也擋得住第二筆未送列。"""
    p = tmp_path / "o.db"
    DurableBuffer(p)  # 先跑過 schema 建置
    with sqlite3.connect(str(p)) as conn:
        conn.execute("INSERT INTO outbox (kind, payload, cmd_id) VALUES ('cmd_ack', '{}', 'c1')")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO outbox (kind, payload, cmd_id) VALUES ('cmd_ack', '{}', 'c1')"
            )


def test_append_stamps_account_mode_cmd_id_visible_in_pending(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    eid = buf.append("deal_report", {"n": 1}, account="F1", mode="sim")
    row = buf.pending()[0]
    assert row.id == eid and row.account == "F1" and row.mode == "sim" and row.cmd_id is None
