"""TAIFEX trading-day calendar — which dates/sessions actually trade.

`bucketing.py` classifies sessions by time-of-day only (08:45–13:45 day,
15:00–05:00 night) and has NO concept of trading days, so it returns "day"/
"night" for weekends and holidays too. The 24/7 poller, fed stale quotes on
non-trading days, would then build phantom 1m bars. This module adds the
trading-day gate, applied ONLY on the live-ingest path (CandleBuilder) so the
pure bucketing/backfill/derivation logic is untouched.

Holidays are embedded (not a data file) to avoid wheel/Docker packaging risk.
Refresh yearly from TAIFEX's official 開休市日 schedule; ad-hoc closures
(typhoon days) must be added manually — the day-session data_date staleness net
in CandleBuilder and the re-runnable `clean-nontrading` command cover stragglers.

Session-anchor rule: a session is valid only if its ANCHOR trading date is a
trading day. Day anchor = the date; night anchor = the 15:00 date (so a Friday
night session running to Saturday 05:00 anchors to Friday and stays valid, while
Saturday/Sunday daytime and holiday sessions are excluded).
"""
from datetime import date, datetime, time
from functools import lru_cache

from quanquant.candles.bucketing import session_bounds_cst, third_wednesday
from quanquant.config import get_settings
from quanquant.market_hours import CST

# Market-closed weekday dates (weekends are excluded separately). Source: TAIFEX /
# TWSE official holiday schedules. CNY "settlement-only" weekdays are included
# (no trading). Settlement days (3rd Wednesday) are NOT here — they trade with an
# early 13:30 day close, handled below.
_HOLIDAYS: frozenset[date] = frozenset(
    date.fromisoformat(s)
    for s in (
        # 2024
        "2024-01-01", "2024-02-08", "2024-02-09", "2024-02-12", "2024-02-13",
        "2024-02-14", "2024-02-28", "2024-04-04", "2024-04-05", "2024-05-01",
        "2024-06-10", "2024-09-17", "2024-10-10",
        # 2025
        "2025-01-01", "2025-01-23", "2025-01-24", "2025-01-27", "2025-01-28",
        "2025-01-29", "2025-01-30", "2025-01-31", "2025-02-28", "2025-04-03",
        "2025-04-04", "2025-05-01", "2025-05-30", "2025-10-06", "2025-10-10",
        "2025-12-25",
        # 2026
        "2026-01-01", "2026-02-12", "2026-02-13", "2026-02-16", "2026-02-17",
        "2026-02-18", "2026-02-19", "2026-02-20", "2026-02-27", "2026-04-03",
        "2026-04-06", "2026-05-01", "2026-06-19", "2026-09-25", "2026-09-28",
        "2026-10-09", "2026-10-26", "2026-12-25",
        # 2027 (best-effort; refresh when TAIFEX publishes)
        "2027-01-01", "2027-02-02", "2027-02-03", "2027-02-04", "2027-02-05",
        "2027-02-08", "2027-02-09", "2027-02-10", "2027-03-01", "2027-04-02",
        "2027-04-05", "2027-04-30", "2027-06-09", "2027-09-15", "2027-09-28",
        "2027-10-11", "2027-10-25", "2027-12-24",
    )
)

_SETTLEMENT_DAY_CLOSE = time(13, 30)  # expiring contract's day session ends here


@lru_cache(maxsize=8)
def _parse_holidays(raw: str) -> frozenset[date]:
    """解析 EXTRA_HOLIDAYS（逗號分隔 ISO 日期）；壞值略過（防禦式）。"""
    out: set[date] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(date.fromisoformat(part))
        except ValueError:
            continue
    return frozenset(out)


def is_trading_day(d: date) -> bool:
    """Mon–Fri 且不在 TAIFEX 假日（內建清單 ∪ EXTRA_HOLIDAYS env）。"""
    if d.weekday() >= 5:
        return False
    if d in _HOLIDAYS:
        return False
    return d not in _parse_holidays(get_settings().extra_holidays)


def session_anchor_date(ts_ms: int) -> tuple[date, str] | None:
    """(anchor_trading_date, "day"|"night") by time-of-day, before the holiday
    check. Night anchor is the 15:00 date (Fri-night→Sat anchors to Fri)."""
    ts_cst = datetime.fromtimestamp(ts_ms / 1000, tz=CST)
    bounds = session_bounds_cst(ts_cst)
    if bounds is None:
        return None
    open_dt, _close_dt, sess = bounds
    return open_dt.date(), sess


def is_trading_session(ts_ms: int) -> str | None:
    """"day"/"night"/None — valid only on a real trading session.

    Returns None on weekends, holidays, and (settlement day, day session) after
    13:30 when the expiring contract has stopped trading.
    """
    res = session_anchor_date(ts_ms)
    if res is None:
        return None
    anchor, sess = res
    if not is_trading_day(anchor):
        return None
    if sess == "day" and anchor == third_wednesday(anchor.year, anchor.month):
        t = datetime.fromtimestamp(ts_ms / 1000, tz=CST).time()
        if (t.hour, t.minute) > (_SETTLEMENT_DAY_CLOSE.hour, _SETTLEMENT_DAY_CLOSE.minute):
            return None
    return sess
