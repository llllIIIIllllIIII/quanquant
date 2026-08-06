"""Startup migration: ALTER TABLE adds user_id to pre-account-era tables."""
import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError

from quanquant.db.migrate import ensure_columns


def _old_engine(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'old.db'}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE trades (id INTEGER PRIMARY KEY, symbol TEXT)"))
        conn.execute(text("CREATE TABLE alerts (id INTEGER PRIMARY KEY, symbol TEXT)"))
    return eng


def test_adds_user_id_columns(tmp_path):
    eng = _old_engine(tmp_path)
    ensure_columns(eng)
    insp = inspect(eng)
    assert "user_id" in {c["name"] for c in insp.get_columns("trades")}
    assert "user_id" in {c["name"] for c in insp.get_columns("alerts")}


def test_idempotent(tmp_path):
    eng = _old_engine(tmp_path)
    ensure_columns(eng)
    ensure_columns(eng)  # second run must not raise
    insp = inspect(eng)
    assert "user_id" in {c["name"] for c in insp.get_columns("trades")}


def test_missing_table_skipped(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'empty.db'}")
    ensure_columns(eng)  # no tables at all — must not raise


def test_adds_chart_color_scheme_to_users(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'old_users.db'}")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)"
        ))
    ensure_columns(eng)
    insp = inspect(eng)
    assert "chart_color_scheme" in {c["name"] for c in insp.get_columns("users")}


def test_chart_color_scheme_idempotent(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'old_users2.db'}")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)"
        ))
    ensure_columns(eng)
    ensure_columns(eng)  # 第二次不得 raise
    insp = inspect(eng)
    assert "chart_color_scheme" in {c["name"] for c in insp.get_columns("users")}


def test_adds_theme_to_users(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'old_users3.db'}")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)"
        ))
    ensure_columns(eng)
    insp = inspect(eng)
    assert "theme" in {c["name"] for c in insp.get_columns("users")}


def test_theme_idempotent(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'old_users4.db'}")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)"
        ))
    ensure_columns(eng)
    ensure_columns(eng)  # 第二次不得 raise
    insp = inspect(eng)
    assert "theme" in {c["name"] for c in insp.get_columns("users")}


def test_adds_mode_and_source_to_trades_with_defaults(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'old_mode.db'}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE trades (id INTEGER PRIMARY KEY, symbol TEXT)"))
        conn.execute(text("INSERT INTO trades (id, symbol) VALUES (1, 'TXF')"))
    ensure_columns(eng)
    insp = inspect(eng)
    cols = {c["name"] for c in insp.get_columns("trades")}
    assert "mode" in cols and "source" in cols
    with eng.begin() as conn:
        row = conn.execute(text("SELECT mode, source FROM trades WHERE id = 1")).one()
    assert row[0] == "real" and row[1] == "manual"  # 既有列以 DEFAULT 回填


def test_mode_source_migration_idempotent(tmp_path):
    eng = _old_engine(tmp_path)
    ensure_columns(eng)
    ensure_columns(eng)  # 第二次不得 raise
    insp = inspect(eng)
    cols = {c["name"] for c in insp.get_columns("trades")}
    assert "mode" in cols and "source" in cols


def test_mode_check_constraint_rejects_illegal_value_after_migration(tmp_path):
    """V3-4：mode 於 DB 層加 CHECK，非法值連 pydantic 都不用就被 DB 擋下。"""
    eng = create_engine(f"sqlite:///{tmp_path / 'old_mode_check.db'}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE trades (id INTEGER PRIMARY KEY, symbol TEXT)"))
    ensure_columns(eng)
    with pytest.raises(IntegrityError):
        with eng.begin() as conn:
            conn.execute(text("INSERT INTO trades (id, symbol, mode) VALUES (1, 'TXF', 'paper')"))


def test_source_check_constraint_rejects_illegal_value_after_migration(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'old_source_check.db'}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE trades (id INTEGER PRIMARY KEY, symbol TEXT)"))
    ensure_columns(eng)
    with pytest.raises(IntegrityError):
        with eng.begin() as conn:
            conn.execute(text("INSERT INTO trades (id, symbol, source) VALUES (1, 'TXF', 'auto')"))


def test_mode_check_constraint_rejects_explicit_null_after_migration(tmp_path):
    """C2：nullable ALTER 的 CHECK(mode IN(...)) 對 NULL 為 UNKNOWN 而非 FALSE，
    改成 CHECK(mode IS NOT NULL AND mode IN(...)) 後明確 NULL 也要被擋下。"""
    eng = create_engine(f"sqlite:///{tmp_path / 'old_mode_null.db'}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE trades (id INTEGER PRIMARY KEY, symbol TEXT)"))
    ensure_columns(eng)
    with pytest.raises(IntegrityError):
        with eng.begin() as conn:
            conn.execute(text("INSERT INTO trades (id, symbol, mode) VALUES (1, 'TXF', NULL)"))


def test_source_check_constraint_rejects_explicit_null_after_migration(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'old_source_null.db'}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE trades (id INTEGER PRIMARY KEY, symbol TEXT)"))
    ensure_columns(eng)
    with pytest.raises(IntegrityError):
        with eng.begin() as conn:
            conn.execute(text("INSERT INTO trades (id, symbol, source) VALUES (1, 'TXF', NULL)"))


def _old_raw_inbox_engine(tmp_path):
    """Inc1（D5）之前的 raw_inbox：無 user_id/account/mode/quarantine_reason 四欄。"""
    eng = create_engine(f"sqlite:///{tmp_path / 'old_raw_inbox.db'}")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE raw_inbox (id INTEGER PRIMARY KEY, kind TEXT, broker TEXT, payload TEXT)"
        ))
    return eng


def test_adds_scope_columns_to_raw_inbox(tmp_path):
    """D5：raw_inbox 補 user_id/account/mode（S1/S2/S3 scope 蓋章）＋ quarantine_reason（R2-6 分級）。"""
    eng = _old_raw_inbox_engine(tmp_path)
    ensure_columns(eng)
    insp = inspect(eng)
    cols = {c["name"] for c in insp.get_columns("raw_inbox")}
    assert {"user_id", "account", "mode", "quarantine_reason"} <= cols


def test_raw_inbox_scope_columns_migration_idempotent(tmp_path):
    eng = _old_raw_inbox_engine(tmp_path)
    ensure_columns(eng)
    ensure_columns(eng)  # 第二次不得 raise
    insp = inspect(eng)
    cols = {c["name"] for c in insp.get_columns("raw_inbox")}
    assert {"user_id", "account", "mode", "quarantine_reason"} <= cols
