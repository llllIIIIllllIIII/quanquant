"""Startup-time lightweight migrations (the project has no alembic).

`create_all` only creates missing tables — it never adds columns to existing
ones. ensure_columns() inspects each table and issues `ALTER TABLE ... ADD
COLUMN` for columns the ORM model has but the DB lacks. Plain ADD COLUMN of a
nullable column is portable across SQLite and Postgres.
"""
from sqlalchemy import inspect, text

# (table, column, DDL type) — nullable so ALTER works on populated tables.
_MIGRATIONS = [
    ("trades", "user_id", "INTEGER"),
    ("alerts", "user_id", "INTEGER"),
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
