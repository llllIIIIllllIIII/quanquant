"""Database engine, session factory, and schema init.

The engine is memoized from `settings.db_url` (SQLite by default). Swapping to
Postgres is a one-line db_url change. WAL is enabled for SQLite so concurrent
reads (multiple browser tabs) don't block the single writer.
"""
from collections.abc import Iterator

from sqlalchemy import event
from sqlmodel import Session, SQLModel, create_engine

from quanquant.config import get_settings

_engine = None


def get_engine():
    global _engine
    if _engine is not None:
        return _engine

    settings = get_settings()
    is_sqlite = settings.db_url.startswith("sqlite")
    connect_args = {"check_same_thread": False} if is_sqlite else {}
    # pool_pre_ping: survive server-side disconnects (e.g. Postgres container restart)
    engine_kwargs = {} if is_sqlite else {"pool_pre_ping": True}
    _engine = create_engine(settings.db_url, connect_args=connect_args, **engine_kwargs)

    if is_sqlite:

        @event.listens_for(_engine, "connect")
        def _set_sqlite_pragmas(dbapi_conn, _record):  # noqa: ANN001
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()

    return _engine


def init_db() -> None:
    """Create all tables. Importing models registers them on SQLModel.metadata."""
    from quanquant.db import models  # noqa: F401

    SQLModel.metadata.create_all(get_engine())


def get_session() -> Iterator[Session]:
    with Session(get_engine()) as session:
        yield session
