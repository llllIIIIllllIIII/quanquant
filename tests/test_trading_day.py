"""008/010：trading_day 定義（含夜盤跨日）——純函式，no DB。"""
import datetime as dt

import pytest

from quanquant.journal.trading_day import (
    next_trading_day,
    previous_trading_day,
    today_trading_day,
    trading_day_bounds,
    trading_day_of,
)


# ---- trading_day_of ----

def test_day_session_time_is_same_calendar_date():
    assert trading_day_of(dt.datetime(2026, 9, 2, 9, 31)) == dt.date(2026, 9, 2)


def test_early_morning_continuation_is_same_calendar_date():
    """凌晨延續（前一晚夜盤的延伸）本身的日曆日期已經是「今天」，不需再位移。"""
    assert trading_day_of(dt.datetime(2026, 9, 2, 2, 14)) == dt.date(2026, 9, 2)


def test_night_session_open_rolls_to_next_trading_day():
    """①核心案例：昨晚 23:06 的夜盤單算「今日」（2026-09-02 是週三，前一天 09-01 週二）。"""
    assert trading_day_of(dt.datetime(2026, 9, 1, 23, 6, 40)) == dt.date(2026, 9, 2)


def test_night_session_exactly_at_open_boundary():
    assert trading_day_of(dt.datetime(2026, 9, 1, 15, 0, 0)) == dt.date(2026, 9, 2)


def test_night_session_just_before_open_boundary_stays_same_day():
    assert trading_day_of(dt.datetime(2026, 9, 1, 14, 59, 59)) == dt.date(2026, 9, 1)


def test_friday_night_session_rolls_to_monday_skipping_weekend():
    # 2026-09-04 是週五；週五夜盤（15:00 起）算下一個交易日 = 週一 09-07（跳過週末）。
    assert trading_day_of(dt.datetime(2026, 9, 4, 20, 0)) == dt.date(2026, 9, 7)


# ---- next/previous trading day ----

def test_next_trading_day_skips_weekend():
    assert next_trading_day(dt.date(2026, 9, 4)) == dt.date(2026, 9, 7)  # Fri -> Mon


def test_previous_trading_day_skips_weekend():
    assert previous_trading_day(dt.date(2026, 9, 7)) == dt.date(2026, 9, 4)  # Mon -> Fri


# ---- trading_day_bounds ----

def test_single_day_bounds_include_previous_night_exclude_own_night():
    lower, upper = trading_day_bounds(dt.date(2026, 9, 2), dt.date(2026, 9, 2))
    assert lower == dt.datetime(2026, 9, 1, 15, 0, 0)  # 前一交易日夜盤開盤
    # 上界剛好在「今天自己的夜盤開盤（15:00）」之前一微秒——今天自己的夜盤屬於下一個交易日
    assert upper == dt.datetime(2026, 9, 2, 14, 59, 59, 999999)


def test_single_day_bounds_membership():
    lower, upper = trading_day_bounds(dt.date(2026, 9, 2), dt.date(2026, 9, 2))
    # 昨晚 23:06 的夜盤單落在邊界內
    assert lower <= dt.datetime(2026, 9, 1, 23, 6, 40) <= upper
    # 今天日盤 09:31 也在邊界內
    assert lower <= dt.datetime(2026, 9, 2, 9, 31, 0) <= upper
    # 今天自己的夜盤（15:00 之後）不在邊界內——屬於下一個交易日
    assert not (lower <= dt.datetime(2026, 9, 2, 20, 0, 0) <= upper)


def test_range_bounds_span_multiple_days():
    lower, upper = trading_day_bounds(dt.date(2026, 8, 31), dt.date(2026, 9, 2))
    assert lower == dt.datetime(2026, 8, 28, 15, 0, 0)  # 08-31(一) 的前一交易日 = 08-28(五)
    assert upper == dt.datetime(2026, 9, 2, 14, 59, 59, 999999)


# ---- today_trading_day ----

def test_today_trading_day_uses_injected_now_day_session():
    assert today_trading_day(dt.datetime(2026, 9, 2, 10, 0)) == dt.date(2026, 9, 2)


def test_today_trading_day_uses_injected_now_night_session():
    assert today_trading_day(dt.datetime(2026, 9, 1, 23, 0)) == dt.date(2026, 9, 2)


# ---- HIGH-1（終審 2026-09-04）：<15:00 分支必須也檢查 is_trading_day，否則週末的
# 「早盤延續」判定會與 trading_day_bounds 自相矛盾（同一段真實夜盤 session 被切成兩個
# 不同 trading_day，週六那截成孤兒）。2026-09-04 是週五、09-05 週六、09-06 週日、
# 09-07 是週一（跨週末，前一交易日＝週五 09-04）。----

def test_saturday_early_morning_continuation_belongs_to_following_monday():
    """週五夜盤跨午夜延續到週六凌晨的真實成交時段——必須算週一，不是週六。"""
    assert trading_day_of(dt.datetime(2026, 9, 5, 1, 0)) == dt.date(2026, 9, 7)


def test_saturday_daytime_belongs_to_following_monday():
    assert trading_day_of(dt.datetime(2026, 9, 5, 10, 0)) == dt.date(2026, 9, 7)


def test_sunday_daytime_belongs_to_following_monday():
    assert trading_day_of(dt.datetime(2026, 9, 6, 10, 0)) == dt.date(2026, 9, 7)


# ---- 性質測試（終審點名）：對任一交易日 D（含跨週末的週一），bounds(D,D) 區間內
# 「所有」時刻的 trading_day_of 都必須等於 D——這是 trading_day_of 與
# trading_day_bounds 兩支函式不能自相矛盾的核心不變量，用細粒度取樣覆蓋整個區間
# （而不只是邊界點），並確認緊鄰區間外的兩個時刻不等於 D。----

_SAMPLE_TRADING_DAYS = [
    dt.date(2026, 9, 2),  # 週三，前一天週二（一般平日）
    dt.date(2026, 9, 4),  # 週五
    dt.date(2026, 9, 7),  # 週一，跨週末（前一交易日＝前週五）
]


@pytest.mark.parametrize("day", _SAMPLE_TRADING_DAYS)
def test_trading_day_of_matches_bounds_for_every_sampled_moment(day):
    lower, upper = trading_day_bounds(day, day)
    step = dt.timedelta(minutes=37)  # 與 24h/夜盤跨日邊界不對齊，刻意錯開取樣避免只踩到整點
    t = lower
    while t <= upper:
        assert trading_day_of(t) == day, f"{t} 落在 {day} 的 bounds 內，trading_day_of 卻回 {trading_day_of(t)}"
        t += step
    assert trading_day_of(lower) == day
    assert trading_day_of(upper) == day


@pytest.mark.parametrize("day", _SAMPLE_TRADING_DAYS)
def test_trading_day_of_just_outside_bounds_is_not_this_day(day):
    lower, upper = trading_day_bounds(day, day)
    assert trading_day_of(lower - dt.timedelta(microseconds=1)) != day
    assert trading_day_of(upper + dt.timedelta(microseconds=1)) != day
