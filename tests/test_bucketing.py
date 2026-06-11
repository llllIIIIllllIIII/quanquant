from datetime import date, datetime

import pytest

from quanquant.candles.bucketing import (
    bucket_start_ms,
    day_open_ms,
    day_session_date,
    session_of_ms,
    third_wednesday,
)
from quanquant.market_hours import CST


def ms(y, m, d, hh, mm, ss=0) -> int:
    return int(datetime(y, m, d, hh, mm, ss, tzinfo=CST).timestamp() * 1000)


# --- session membership ---


def test_session_of_ms():
    assert session_of_ms(ms(2026, 6, 10, 9, 0)) == "day"
    assert session_of_ms(ms(2026, 6, 10, 8, 44)) is None  # before open
    assert session_of_ms(ms(2026, 6, 10, 14, 30)) is None  # between sessions
    assert session_of_ms(ms(2026, 6, 10, 15, 0)) == "night"
    assert session_of_ms(ms(2026, 6, 11, 0, 30)) == "night"  # after midnight
    assert session_of_ms(ms(2026, 6, 11, 5, 0)) == "night"  # night close tick
    assert session_of_ms(ms(2026, 6, 11, 5, 1)) is None


# --- day-session anchored buckets (08:45) ---


def test_10m_day_buckets_anchor_0845():
    b = ms(2026, 6, 10, 8, 45)
    assert bucket_start_ms(ms(2026, 6, 10, 8, 45), "10m") == b
    assert bucket_start_ms(ms(2026, 6, 10, 8, 46), "10m") == b
    assert bucket_start_ms(ms(2026, 6, 10, 8, 54, 59), "10m") == b
    assert bucket_start_ms(ms(2026, 6, 10, 8, 55), "10m") == ms(2026, 6, 10, 8, 55)


def test_day_close_tick_clamps_into_last_bucket():
    # 13:45 close tick belongs to the 13:35 10m bucket, not a new one
    assert bucket_start_ms(ms(2026, 6, 10, 13, 45), "10m") == ms(2026, 6, 10, 13, 35)
    assert bucket_start_ms(ms(2026, 6, 10, 13, 44), "10m") == ms(2026, 6, 10, 13, 35)


def test_closed_period_returns_none():
    assert bucket_start_ms(ms(2026, 6, 10, 8, 44), "10m") is None
    assert bucket_start_ms(ms(2026, 6, 10, 14, 0), "5m") is None


def test_5m_and_15m_alignment():
    assert bucket_start_ms(ms(2026, 6, 10, 9, 2), "5m") == ms(2026, 6, 10, 9, 0)
    assert bucket_start_ms(ms(2026, 6, 10, 8, 59), "15m") == ms(2026, 6, 10, 8, 45)
    assert bucket_start_ms(ms(2026, 6, 10, 9, 0), "15m") == ms(2026, 6, 10, 9, 0)


# --- night session: anchored 15:00, crossing midnight ---


def test_night_buckets_cross_midnight():
    assert bucket_start_ms(ms(2026, 6, 10, 23, 55), "10m") == ms(2026, 6, 10, 23, 50)
    assert bucket_start_ms(ms(2026, 6, 11, 0, 5), "10m") == ms(2026, 6, 11, 0, 0)
    # 00:30's session anchor is 15:00 of the PREVIOUS day
    assert bucket_start_ms(ms(2026, 6, 11, 0, 30), "60m") == ms(2026, 6, 11, 0, 0)
    assert bucket_start_ms(ms(2026, 6, 10, 15, 59), "60m") == ms(2026, 6, 10, 15, 0)


# --- 4h session-anchored buckets ---


def test_4h_day_buckets():
    assert bucket_start_ms(ms(2026, 6, 10, 9, 30), "4h") == ms(2026, 6, 10, 8, 45)
    assert bucket_start_ms(ms(2026, 6, 10, 13, 30), "4h") == ms(2026, 6, 10, 12, 45)


def test_4h_night_buckets():
    assert bucket_start_ms(ms(2026, 6, 10, 18, 0), "4h") == ms(2026, 6, 10, 15, 0)
    assert bucket_start_ms(ms(2026, 6, 10, 22, 0), "4h") == ms(2026, 6, 10, 19, 0)
    assert bucket_start_ms(ms(2026, 6, 11, 2, 0), "4h") == ms(2026, 6, 10, 23, 0)
    assert bucket_start_ms(ms(2026, 6, 11, 4, 0), "4h") == ms(2026, 6, 11, 3, 0)
    # 05:00 close tick clamps into the 03:00 bucket
    assert bucket_start_ms(ms(2026, 6, 11, 5, 0), "4h") == ms(2026, 6, 11, 3, 0)


def test_daily_tf_rejected():
    with pytest.raises(ValueError):
        bucket_start_ms(ms(2026, 6, 10, 9, 0), "1d")


# --- trading date helpers ---


def test_day_session_date():
    assert day_session_date(ms(2026, 6, 10, 9, 0)) == "2026-06-10"
    assert day_session_date(ms(2026, 6, 10, 16, 0)) is None  # night
    assert day_session_date(ms(2026, 6, 10, 14, 0)) is None  # closed


def test_day_open_ms_round_trip():
    assert day_open_ms("2026-06-10") == ms(2026, 6, 10, 8, 45)


# --- settlement day ---


def test_third_wednesday():
    assert third_wednesday(2026, 6) == date(2026, 6, 17)
    assert third_wednesday(2026, 7) == date(2026, 7, 15)
    assert third_wednesday(2025, 1) == date(2025, 1, 15)
    assert third_wednesday(2026, 4) == date(2026, 4, 15)
