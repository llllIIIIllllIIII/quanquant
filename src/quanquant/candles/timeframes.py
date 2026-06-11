"""Timeframe registry.

Canonical storage holds only 1m and 1d candles; every other timeframe is derived
on demand (intraday TFs aggregate from 1m, daily TFs aggregate from 1d).
"""
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Timeframe:
    code: str            # e.g. "5m"
    label: str           # e.g. "5分"
    kind: str            # "intraday" | "daily"
    minutes: int | None  # intraday only
    canonical: bool      # stored in the candles table


TIMEFRAMES: dict[str, Timeframe] = {
    tf.code: tf
    for tf in (
        Timeframe("1m", "1分", "intraday", 1, True),
        Timeframe("5m", "5分", "intraday", 5, False),
        Timeframe("10m", "10分", "intraday", 10, False),
        Timeframe("15m", "15分", "intraday", 15, False),
        Timeframe("20m", "20分", "intraday", 20, False),
        Timeframe("30m", "30分", "intraday", 30, False),
        Timeframe("60m", "60分", "intraday", 60, False),
        Timeframe("4h", "4時", "intraday", 240, False),
        Timeframe("1d", "日", "daily", None, True),
        Timeframe("3d", "3日", "daily", None, False),
        Timeframe("1w", "週", "daily", None, False),
        Timeframe("1M", "月", "daily", None, False),
    )
}
