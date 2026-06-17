from datetime import date, datetime, timezone
from decimal import Decimal

from quanquant.candles.builder import CandleBuilder
from quanquant.candles.market_calendar import (
    is_trading_day,
    is_trading_session,
    session_anchor_date,
)
from quanquant.market_hours import CST
from quanquant.models import FuturesSnapshot


def ms(y, m, d, hh, mm, ss=0) -> int:
    return int(datetime(y, m, d, hh, mm, ss, tzinfo=CST).timestamp() * 1000)


def snap(ts_ms: int, price="18000", data_date="") -> FuturesSnapshot:
    utc = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    p = Decimal(price)
    return FuturesSnapshot(
        symbol="TXF", price=p, change=Decimal(0), change_pct=0.0, volume=1000,
        open_price=p, high_price=p, low_price=p, fetched_at=utc,
        data_date=data_date, contract_month="TXFF6",
    )


# --- is_trading_day ---


def test_is_trading_day():
    assert is_trading_day(date(2026, 6, 16)) is True       # Tuesday
    assert is_trading_day(date(2026, 6, 13)) is False       # Saturday
    assert is_trading_day(date(2026, 6, 14)) is False       # Sunday
    assert is_trading_day(date(2026, 6, 19)) is False       # Dragon Boat holiday
    assert is_trading_day(date(2026, 1, 1)) is False        # New Year
    assert is_trading_day(date(2026, 2, 16)) is False       # CNY


# --- is_trading_session: weekends / holidays excluded ---


def test_weekend_daytime_is_not_trading():
    assert is_trading_session(ms(2026, 6, 13, 10, 0)) is None   # Sat day
    assert is_trading_session(ms(2026, 6, 14, 10, 0)) is None   # Sun day
    assert is_trading_session(ms(2026, 6, 14, 16, 0)) is None   # Sun night-open time


def test_friday_night_into_saturday_morning_is_trading():
    assert is_trading_session(ms(2026, 6, 12, 16, 0)) == "night"   # Fri 16:00
    assert is_trading_session(ms(2026, 6, 13, 3, 0)) == "night"    # Sat 03:00, anchor Fri


def test_holiday_is_not_trading():
    assert is_trading_session(ms(2026, 6, 19, 10, 0)) is None   # Dragon Boat (Fri), day
    assert is_trading_session(ms(2026, 6, 19, 16, 0)) is None   # holiday night anchor=holiday


def test_normal_weekday_sessions():
    assert is_trading_session(ms(2026, 6, 16, 10, 0)) == "day"
    assert is_trading_session(ms(2026, 6, 16, 16, 0)) == "night"


# --- settlement day (3rd Wednesday) early 13:30 day close ---


def test_settlement_day_early_close():
    # 2026-06-17 is the third Wednesday of June 2026
    assert is_trading_session(ms(2026, 6, 17, 13, 0)) == "day"     # before 13:30
    assert is_trading_session(ms(2026, 6, 17, 13, 30)) == "day"    # the 13:30 minute
    assert is_trading_session(ms(2026, 6, 17, 13, 40)) is None     # after 13:30
    assert is_trading_session(ms(2026, 6, 17, 16, 0)) == "night"   # night still trades


def test_session_anchor_date():
    assert session_anchor_date(ms(2026, 6, 13, 3, 0)) == (date(2026, 6, 12), "night")
    assert session_anchor_date(ms(2026, 6, 16, 10, 0)) == (date(2026, 6, 16), "day")
    assert session_anchor_date(ms(2026, 6, 16, 2, 0)) == (date(2026, 6, 15), "night")


# --- builder gate ---


def test_builder_skips_sunday():
    b = CandleBuilder("TXF")
    assert b.on_snapshot(snap(ms(2026, 6, 14, 10, 0))) == []


def test_builder_builds_friday_night():
    b = CandleBuilder("TXF")
    rows = b.on_snapshot(snap(ms(2026, 6, 12, 16, 0)))
    assert len(rows) == 1 and rows[0].session == "night"


def test_builder_staleness_net_blocks_stale_day_quote():
    b = CandleBuilder("TXF")
    # day session on a real trading day, but the quote's CDate is an earlier date
    rows = b.on_snapshot(snap(ms(2026, 6, 16, 10, 0), data_date="2026/06/12"))
    assert rows == []


def test_builder_staleness_net_allows_fresh_and_empty():
    b = CandleBuilder("TXF")
    assert b.on_snapshot(snap(ms(2026, 6, 16, 10, 0), data_date="2026/06/16"))  # fresh
    b2 = CandleBuilder("TXF")
    assert b2.on_snapshot(snap(ms(2026, 6, 16, 10, 1), data_date=""))  # empty → not blocked
