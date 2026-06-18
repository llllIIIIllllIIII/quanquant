"""Pure aggregation of canonical candles into derived timeframes.

Input rows are Candle ORM objects (or anything with .ts/.open/.high/.low/.close/
.volume/.trading_date) sorted ascending by ts. Output "Bar" dicts are the API/
chart wire format: {"timestamp": ms, "open": float, ...} — this is the Decimal→
float edge (TXF prices are integral ticks, so no precision risk).
"""
from collections.abc import Sequence
from datetime import date

from quanquant.candles.bucketing import bucket_start_ms
from quanquant.candles.market_calendar import is_trading_session
from quanquant.candles.timeframes import TIMEFRAMES

Bar = dict


def to_bars(rows: Sequence) -> list[Bar]:
    """1:1 conversion of candle rows to wire bars (used for tf == canonical)."""
    return [
        {
            "timestamp": r.ts,
            "open": float(r.open),
            "high": float(r.high),
            "low": float(r.low),
            "close": float(r.close),
            "volume": r.volume,
        }
        for r in rows
    ]


def aggregate_intraday(rows: Sequence, tf: str) -> list[Bar]:
    """Group ascending 1m candles into session-anchored tf buckets.

    Bar timestamp is the canonical bucket start (e.g. 08:45) even when the
    bucket's first 1m row is missing (e.g. data starts at 08:46).
    """
    out: list[Bar] = []
    cur_key: int | None = None
    cur: Bar | None = None
    for r in rows:
        key = bucket_start_ms(r.ts, tf)
        if key is None:  # outside trading sessions (time-of-day) — skip defensively
            continue
        if key != cur_key:
            # Calendar gate: bucket_start_ms knows only time-of-day, so it accepts
            # weekend/holiday timestamps. Drop buckets that aren't a real trading
            # session — defends the chart against phantom bars sitting in the DB
            # (e.g. built by a pre-fix poller over a weekend) that would otherwise
            # render as a flat empty block. Checked once per bucket (cheap).
            if is_trading_session(r.ts) is None:
                continue
            if cur is not None:
                out.append(cur)
            cur_key = key
            cur = {
                "timestamp": key,
                "open": float(r.open),
                "high": float(r.high),
                "low": float(r.low),
                "close": float(r.close),
                "volume": r.volume,
            }
        else:
            assert cur is not None
            cur["high"] = max(cur["high"], float(r.high))
            cur["low"] = min(cur["low"], float(r.low))
            cur["close"] = float(r.close)
            cur["volume"] += r.volume
    if cur is not None:
        out.append(cur)
    return out


def _daily_group_key(row, tf: str, index: int):
    d = date.fromisoformat(row.trading_date)
    if tf == "1w":
        iso = d.isocalendar()
        return (iso[0], iso[1])
    if tf == "1M":
        return (d.year, d.month)
    if tf == "3d":  # positional groups of 3 trading days from the earliest bar
        return index // 3
    raise ValueError(f"not a derived daily timeframe: {tf!r}")


def aggregate_daily(rows: Sequence, tf: str) -> list[Bar]:
    """Group ascending 1d candles into 3d / 1w / 1M bars.

    Bar timestamp = ts of the group's first trading day. Note: extending the
    backfill start date later shifts 3d groups (positional grouping) — accepted
    and documented; bars are recomputed per request, nothing stored.
    """
    if TIMEFRAMES[tf].kind != "daily":
        raise ValueError(f"aggregate_daily only handles daily timeframes, got {tf!r}")

    out: list[Bar] = []
    cur_key = None
    cur: Bar | None = None
    for i, r in enumerate(rows):
        key = _daily_group_key(r, tf, i)
        if key != cur_key:
            if cur is not None:
                out.append(cur)
            cur_key = key
            cur = {
                "timestamp": r.ts,
                "open": float(r.open),
                "high": float(r.high),
                "low": float(r.low),
                "close": float(r.close),
                "volume": r.volume,
            }
        else:
            assert cur is not None
            cur["high"] = max(cur["high"], float(r.high))
            cur["low"] = min(cur["low"], float(r.low))
            cur["close"] = float(r.close)
            cur["volume"] += r.volume
    if cur is not None:
        out.append(cur)
    return out
