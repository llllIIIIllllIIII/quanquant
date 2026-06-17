from datetime import datetime

from quanquant.alerts.engine import newest_closed
from quanquant.market_hours import CST


def ms(hh, mm, d=16) -> int:
    return int(datetime(2026, 6, d, hh, mm, tzinfo=CST).timestamp() * 1000)


def bar(ts) -> dict:
    return {"timestamp": ts, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}


def test_newest_closed_last_bar_in_progress():
    bars = [bar(ms(9, 0)), bar(ms(9, 5)), bar(ms(9, 10))]
    ts, series = newest_closed(bars, "5m", ms(9, 12))  # 9:10 bucket not closed yet
    assert ts == ms(9, 5)
    assert series[-1]["timestamp"] == ms(9, 5)


def test_newest_closed_final_bar_via_wallclock():
    bars = [bar(ms(9, 0)), bar(ms(9, 5))]
    ts, series = newest_closed(bars, "5m", ms(9, 20))  # past 9:05 bucket close (9:10)
    assert ts == ms(9, 5)
    assert len(series) == 2 and series[-1]["timestamp"] == ms(9, 5)


def test_newest_closed_only_in_progress_bar():
    assert newest_closed([bar(ms(9, 0))], "5m", ms(9, 2)) == (None, [])
    assert newest_closed([], "5m", ms(9, 2)) == (None, [])


def test_newest_closed_daily_uses_second_to_last():
    bars = [bar(ms(8, 45, d=15)), bar(ms(8, 45, d=16))]  # yesterday + today (in-progress)
    ts, series = newest_closed(bars, "1d", ms(10, 0))
    assert ts == ms(8, 45, d=15)
    assert series == bars[:-1]
