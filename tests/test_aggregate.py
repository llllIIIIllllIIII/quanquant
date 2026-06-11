from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

from quanquant.candles.aggregate import aggregate_daily, aggregate_intraday, to_bars
from quanquant.market_hours import CST


def ms(y, m, d, hh, mm) -> int:
    return int(datetime(y, m, d, hh, mm, tzinfo=CST).timestamp() * 1000)


def c1m(ts, o, h, lo, cl, v):
    return SimpleNamespace(
        ts=ts, open=Decimal(o), high=Decimal(h), low=Decimal(lo), close=Decimal(cl),
        volume=v, trading_date=None,
    )


def c1d(date_str, o, h, lo, cl, v):
    y, m, d = (int(x) for x in date_str.split("-"))
    return SimpleNamespace(
        ts=ms(y, m, d, 8, 45), open=Decimal(o), high=Decimal(h), low=Decimal(lo),
        close=Decimal(cl), volume=v, trading_date=date_str,
    )


def test_to_bars():
    bars = to_bars([c1m(ms(2026, 6, 10, 9, 0), "100", "110", "90", "105", 7)])
    assert bars == [
        {"timestamp": ms(2026, 6, 10, 9, 0), "open": 100.0, "high": 110.0,
         "low": 90.0, "close": 105.0, "volume": 7}
    ]


def test_aggregate_1m_to_5m_ohlcv():
    rows = [
        c1m(ms(2026, 6, 10, 9, 0), "100", "101", "99", "100", 10),
        c1m(ms(2026, 6, 10, 9, 1), "100", "105", "100", "104", 20),
        c1m(ms(2026, 6, 10, 9, 4), "104", "104", "95", "96", 5),
        c1m(ms(2026, 6, 10, 9, 5), "96", "97", "96", "97", 8),  # next bucket
    ]
    bars = aggregate_intraday(rows, "5m")
    assert len(bars) == 2
    b0 = bars[0]
    assert b0["timestamp"] == ms(2026, 6, 10, 9, 0)
    assert (b0["open"], b0["high"], b0["low"], b0["close"], b0["volume"]) == (
        100.0, 105.0, 95.0, 96.0, 35
    )
    assert bars[1]["timestamp"] == ms(2026, 6, 10, 9, 5)


def test_bucket_timestamp_is_canonical_even_if_first_minute_missing():
    # data starts at 08:46 — the bucket still reports 08:45 as its start
    rows = [c1m(ms(2026, 6, 10, 8, 46), "100", "101", "99", "100", 10)]
    bars = aggregate_intraday(rows, "10m")
    assert bars[0]["timestamp"] == ms(2026, 6, 10, 8, 45)


def test_aggregate_3d_positional_groups():
    days = ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04",
            "2026-06-05", "2026-06-08", "2026-06-09"]
    rows = [c1d(d, "100", "110", "90", "105", 1) for d in days]
    bars = aggregate_daily(rows, "3d")
    assert len(bars) == 3  # 3 + 3 + 1
    assert bars[0]["timestamp"] == rows[0].ts
    assert bars[1]["timestamp"] == rows[3].ts
    assert bars[2]["timestamp"] == rows[6].ts
    assert bars[0]["volume"] == 3


def test_aggregate_week_iso_across_year_boundary():
    # 2025-12-29 (Mon) … 2026-01-02 (Fri) are all ISO week 2026-W01
    days = ["2025-12-29", "2025-12-30", "2025-12-31", "2026-01-02", "2026-01-05"]
    rows = [c1d(d, "100", "110", "90", "105", 1) for d in days]
    bars = aggregate_daily(rows, "1w")
    assert len(bars) == 2  # W01 (4 days) + W02
    assert bars[0]["volume"] == 4
    assert bars[0]["timestamp"] == rows[0].ts


def test_aggregate_month_calendar():
    days = ["2026-05-28", "2026-05-29", "2026-06-01", "2026-06-02"]
    rows = [
        c1d(days[0], "100", "110", "95", "108", 1),
        c1d(days[1], "108", "112", "100", "101", 2),
        c1d(days[2], "101", "120", "101", "119", 3),
        c1d(days[3], "119", "121", "110", "111", 4),
    ]
    bars = aggregate_daily(rows, "1M")
    assert len(bars) == 2
    may, june = bars
    assert (may["open"], may["high"], may["low"], may["close"], may["volume"]) == (
        100.0, 112.0, 95.0, 101.0, 3
    )
    assert (june["open"], june["close"], june["volume"]) == (101.0, 111.0, 7)
