"""Session-anchored bucket math for TXF candles — pure functions, no I/O.

All CST (Asia/Taipei) calendar math for the candle pipeline lives in this module.
Candle timestamps everywhere else are integer epoch milliseconds UTC.

Bucketing rule (Taiwan charting convention): intraday buckets are anchored at the
session open — day session 08:45, night session 15:00 — so e.g. 10分K runs
08:45–08:55, and night buckets cross midnight without resetting. A tick exactly
at the session close (13:45 / 05:00) clamps into the session's last bucket.
"""
from datetime import date, datetime, time, timedelta

from quanquant.candles.timeframes import TIMEFRAMES
from quanquant.market_hours import CST

_DAY_OPEN = time(8, 45)
_DAY_CLOSE = time(13, 45)
_NIGHT_OPEN = time(15, 0)
_NIGHT_CLOSE = time(5, 0)


def _cst(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, tz=CST)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def session_bounds_cst(ts_cst: datetime) -> tuple[datetime, datetime, str] | None:
    """(session_open, session_close, "day"|"night") containing ts_cst, else None.

    Membership uses minute precision (seconds ignored), mirroring
    market_hours.get_session: 13:45:30 still belongs to the day session.
    """
    d = ts_cst.date()
    t = ts_cst.time().replace(second=0, microsecond=0)

    def combine(day: date, tm: time) -> datetime:
        return datetime.combine(day, tm, tzinfo=CST)

    if _DAY_OPEN <= t <= _DAY_CLOSE:
        return combine(d, _DAY_OPEN), combine(d, _DAY_CLOSE), "day"
    if t >= _NIGHT_OPEN:
        return combine(d, _NIGHT_OPEN), combine(d + timedelta(days=1), _NIGHT_CLOSE), "night"
    if t <= _NIGHT_CLOSE:
        return combine(d - timedelta(days=1), _NIGHT_OPEN), combine(d, _NIGHT_CLOSE), "night"
    return None


def session_of_ms(ts_ms: int) -> str | None:
    """"day" | "night" | None for an epoch-ms timestamp."""
    bounds = session_bounds_cst(_cst(ts_ms))
    return bounds[2] if bounds else None


def bucket_start_ms(ts_ms: int, tf: str) -> int | None:
    """Session-anchored bucket start (epoch ms UTC) for an intraday timeframe.

    Returns None when the timestamp falls outside trading sessions.
    A tick exactly at session close clamps into the last bucket.
    """
    spec = TIMEFRAMES[tf]
    if spec.kind != "intraday":
        raise ValueError(f"bucket_start_ms only handles intraday timeframes, got {tf!r}")

    ts_cst = _cst(ts_ms)
    bounds = session_bounds_cst(ts_cst)
    if bounds is None:
        return None
    open_dt, close_dt, _ = bounds

    elapsed_min = int((ts_cst - open_dt).total_seconds() // 60)
    session_min = int((close_dt - open_dt).total_seconds() // 60)
    if elapsed_min >= session_min:  # close tick → last bucket
        elapsed_min = session_min - 1

    assert spec.minutes is not None
    start = open_dt + timedelta(minutes=(elapsed_min // spec.minutes) * spec.minutes)
    return _ms(start)


def day_session_date(ts_ms: int) -> str | None:
    """CST trading date ("YYYY-MM-DD") if ts is in the DAY session, else None."""
    ts_cst = _cst(ts_ms)
    bounds = session_bounds_cst(ts_cst)
    if bounds is None or bounds[2] != "day":
        return None
    return bounds[0].date().isoformat()


def day_open_ms(trading_date: str) -> int:
    """Epoch ms of 08:45 CST on a trading date — canonical ts for 日K bars."""
    d = date.fromisoformat(trading_date)
    return _ms(datetime.combine(d, _DAY_OPEN, tzinfo=CST))


def third_wednesday(year: int, month: int) -> date:
    """TXF settlement day of a contract month (the third Wednesday)."""
    first = date(year, month, 1)
    first_wednesday = first + timedelta(days=(2 - first.weekday()) % 7)  # Mon=0, Wed=2
    return first_wednesday + timedelta(days=14)
