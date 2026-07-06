"""Startup migration: ALTER TABLE adds user_id to pre-account-era tables."""
from sqlalchemy import create_engine, inspect, text

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
