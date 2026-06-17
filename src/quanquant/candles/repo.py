"""DB access for candles: dialect-aware upsert, fast tuple reads, quote pruning.

Read paths use raw SQL into FastCandle named tuples (TEXT → float directly),
bypassing ORM hydration and Decimal parsing — the API serves floats anyway, and
this keeps large derived-timeframe aggregations (10k–100k 1m rows) cheap.
Writes stay ORM/Decimal for exactness.
"""
from collections.abc import Sequence
from datetime import datetime
from typing import NamedTuple

from sqlalchemy import delete, text
from sqlmodel import Session, select

from quanquant.db.models import Candle, Quote


class FastCandle(NamedTuple):
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    trading_date: str | None = None

_UPDATE_COLS = ("open", "high", "low", "close", "volume", "source", "session",
                "trading_date", "updated_at")

# SQLite caps bound parameters per statement (999 on older builds). 80 rows ×
# 12 columns = 960 params — safe everywhere; large backfills chunk transparently.
_UPSERT_CHUNK_ROWS = 80


def upsert_candles(session: Session, rows: Sequence[Candle]) -> None:
    """INSERT .. ON CONFLICT(symbol,timeframe,ts) DO UPDATE — idempotent writes.

    Picks the sqlite or postgresql insert construct from the bound dialect, so the
    same code runs on both (the planned Postgres migration changes only db_url).
    """
    if not rows:
        return

    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":  # pragma: no cover - exercised after PG migration
        from sqlalchemy.dialects.postgresql import insert
    else:
        from sqlalchemy.dialects.sqlite import insert

    values = [
        {
            "symbol": r.symbol, "timeframe": r.timeframe, "ts": r.ts,
            "open": r.open, "high": r.high, "low": r.low, "close": r.close,
            "volume": r.volume, "source": r.source, "session": r.session,
            "trading_date": r.trading_date, "updated_at": r.updated_at,
        }
        for r in rows
    ]

    for i in range(0, len(values), _UPSERT_CHUNK_ROWS):
        stmt = insert(Candle.__table__).values(values[i : i + _UPSERT_CHUNK_ROWS])
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol", "timeframe", "ts"],
            set_={col: stmt.excluded[col] for col in _UPDATE_COLS},
        )
        session.exec(stmt)  # type: ignore[call-overload]
    session.commit()


def _fast_rows(result, *, with_trading_date: bool) -> list[FastCandle]:
    if with_trading_date:
        return [
            FastCandle(r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4]), r[5], r[6])
            for r in result
        ]
    return [
        FastCandle(r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4]), r[5])
        for r in result
    ]


def select_1m_desc(
    session: Session, symbol: str, *, before_ms: int | None, limit: int,
    session_filter: str | None = None,
) -> list[FastCandle]:
    """ASCENDING page of the newest 1m candles strictly before `before_ms`.

    session_filter "day"/"night" restricts to that session; None = both.
    """
    sql = (
        "SELECT ts, open, high, low, close, volume FROM candles "
        "WHERE symbol = :symbol AND timeframe = '1m' "
        + ("AND ts < :before " if before_ms is not None else "")
        + ("AND session = :sess " if session_filter else "")
        + "ORDER BY ts DESC LIMIT :limit"
    )
    params: dict = {"symbol": symbol, "limit": limit}
    if before_ms is not None:
        params["before"] = before_ms
    if session_filter:
        params["sess"] = session_filter
    rows = session.connection().execute(text(sql), params).fetchall()
    rows.reverse()
    return _fast_rows(rows, with_trading_date=False)


def select_1m_range(
    session: Session, symbol: str, *, start_ms: int, session_filter: str | None = None
) -> list[FastCandle]:
    """Ascending 1m candles with ts >= start_ms (for /latest re-aggregation)."""
    sql = (
        "SELECT ts, open, high, low, close, volume FROM candles "
        "WHERE symbol = :symbol AND timeframe = '1m' AND ts >= :start "
        + ("AND session = :sess " if session_filter else "")
        + "ORDER BY ts"
    )
    params: dict = {"symbol": symbol, "start": start_ms}
    if session_filter:
        params["sess"] = session_filter
    rows = session.connection().execute(text(sql), params).fetchall()
    return _fast_rows(rows, with_trading_date=False)


def exists_1m_before(
    session: Session, symbol: str, ts_ms: int, session_filter: str | None = None
) -> bool:
    stmt = select(Candle.ts).where(
        Candle.symbol == symbol, Candle.timeframe == "1m", Candle.ts < ts_ms
    )
    if session_filter:
        stmt = stmt.where(Candle.session == session_filter)
    return session.exec(stmt.limit(1)).first() is not None


def select_1d_all(session: Session, symbol: str) -> list[FastCandle]:
    """All stored 1d candles ascending (years of daily data ≈ a few thousand rows)."""
    sql = (
        "SELECT ts, open, high, low, close, volume, trading_date FROM candles "
        "WHERE symbol = :symbol AND timeframe = '1d' ORDER BY ts"
    )
    rows = session.connection().execute(text(sql), {"symbol": symbol}).fetchall()
    return _fast_rows(rows, with_trading_date=True)


def max_1d_trading_date(session: Session, symbol: str) -> str | None:
    stmt = (
        select(Candle.trading_date)
        .where(Candle.symbol == symbol, Candle.timeframe == "1d")
        .order_by(Candle.ts.desc())  # type: ignore[union-attr]
        .limit(1)
    )
    return session.exec(stmt).first()


def distinct_1m_day_dates_after(
    session: Session, symbol: str, after_date: str | None
) -> list[str]:
    """Distinct day-session trading dates present in 1m data, after `after_date`."""
    stmt = (
        select(Candle.trading_date)
        .where(
            Candle.symbol == symbol,
            Candle.timeframe == "1m",
            Candle.session == "day",
            Candle.trading_date.is_not(None),  # type: ignore[union-attr]
        )
        .distinct()
    )
    if after_date is not None:
        stmt = stmt.where(Candle.trading_date > after_date)  # type: ignore[arg-type]
    return sorted(d for d in session.exec(stmt) if d is not None)


def select_1m_for_day_session(
    session: Session, symbol: str, trading_date: str
) -> list[FastCandle]:
    """Ascending day-session 1m candles for one trading date."""
    sql = (
        "SELECT ts, open, high, low, close, volume FROM candles "
        "WHERE symbol = :symbol AND timeframe = '1m' AND session = 'day' "
        "AND trading_date = :td ORDER BY ts"
    )
    rows = session.connection().execute(
        text(sql), {"symbol": symbol, "td": trading_date}
    ).fetchall()
    return _fast_rows(rows, with_trading_date=False)


def prune_quotes(session: Session, cutoff: datetime) -> int:
    result = session.exec(delete(Quote).where(Quote.fetched_at < cutoff))  # type: ignore[call-overload]
    session.commit()
    return result.rowcount or 0
