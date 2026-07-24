"""Startup-time lightweight migrations (the project has no alembic).

`create_all` only creates missing tables — it never adds columns to existing
ones. ensure_columns() inspects each table and issues `ALTER TABLE ... ADD
COLUMN` for columns the ORM model has but the DB lacks. Plain ADD COLUMN of a
nullable column is portable across SQLite and Postgres.
"""
from sqlalchemy import inspect, text

# (table, column, DDL type) — nullable so ALTER works on populated tables.
# mode/source 的 DDL 帶 DEFAULT（既有列自動回填）+ CHECK（DB 層拒絕非法值）。
# CHECK 用 "col IS NOT NULL AND col IN (...)" 而非單純 "col IN (...)"：ALTER 出的
# 欄位仍是 nullable，若只寫 IN(...)，明確 INSERT NULL 時 CHECK 對 NULL 求值是
# UNKNOWN（非 FALSE）會被當作通過，等於防線形同虛設（round3 覆核 C2）。
_MIGRATIONS = [
    ("trades", "user_id", "INTEGER"),
    ("alerts", "user_id", "INTEGER"),
    ("users", "chart_color_scheme", "VARCHAR"),
    ("users", "theme", "VARCHAR"),
    ("trades", "mode", "VARCHAR DEFAULT 'real' CHECK (mode IS NOT NULL AND mode IN ('sim','real'))"),
    ("trades", "source", "VARCHAR DEFAULT 'manual' CHECK (source IS NOT NULL AND source IN ('manual','shioaji'))"),
]


def ensure_columns(engine) -> None:
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    with engine.begin() as conn:
        for table, column, ddl_type in _MIGRATIONS:
            if table not in tables:
                continue
            cols = {c["name"] for c in inspector.get_columns(table)}
            if column not in cols:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type}"))
