"""交易日 (trading_day) 定義——供 008 交易績效頁／010 每日復盤共用。

TAIFEX 夜盤跨日慣例：夜盤開盤（15:00）之後成交的委託算「次一交易日」（跳過週末／假日），
早盤延續（00:00–05:00）與日盤本身（08:45–13:45）算當日曆日。純函式，輸入為 naive CST
本地時間（如 `Trade.entry_time`/`exit_time`，見 `broker/position_tracker.py::_utc_dt_to_cst`
——那些欄位本來就存 CST，不是 UTC）。

刻意獨立於 `candles/market_calendar.py`（該模組明訂「僅套用在即時進 K 的 live-ingest
path」）——這裡是另一個消費情境（績效頁的日期篩選／每日復盤），只重用假日曆
（`is_trading_day`），不擴大該模組原本的適用範圍註記。
"""
from datetime import date, datetime, time, timedelta, timezone

from quanquant.candles.market_calendar import is_trading_day

_NIGHT_OPEN = time(15, 0)
_CST = timezone(timedelta(hours=8))


def next_trading_day(d: date) -> date:
    """下一個交易日（跳過週末與假日）。"""
    nxt = d + timedelta(days=1)
    while not is_trading_day(nxt):
        nxt += timedelta(days=1)
    return nxt


def previous_trading_day(d: date) -> date:
    """上一個交易日（跳過週末與假日）。"""
    prev = d - timedelta(days=1)
    while not is_trading_day(prev):
        prev -= timedelta(days=1)
    return prev


def trading_day_of(dt: datetime) -> date:
    """naive CST 本地時間所屬的交易日。

    夜盤開盤（>=15:00）之後 → 算次一交易日；其餘時段（含 00:00–05:00 的早盤延續）
    → 算當日曆日，**但當日曆日本身不是交易日時**（週末／假日——真實情境是前一個交易日
    夜盤跨午夜延續到週六凌晨，或單純週末白天的雜訊時刻）一樣要位移到次一交易日。

    終審 HIGH-1（2026-09-04）：舊版此分支不檢查 `is_trading_day`，導致週六 01:00
    （週五夜盤跨午夜的真實成交時段）回「週六」而非「週一」，與 `trading_day_bounds`
    自相矛盾——bounds(週一) 涵蓋週六凌晨這段時間，但該時刻算出來的 trading_day_of 卻
    不等於週一，同一段夜盤 session 因此會被切成兩個不同的 trading_day（週六成孤兒）。
    """
    if dt.time() >= _NIGHT_OPEN:
        return next_trading_day(dt.date())
    d = dt.date()
    return d if is_trading_day(d) else next_trading_day(d)


def trading_day_bounds(date_from: date, date_to: date) -> tuple[datetime, datetime]:
    """交易日區間 [date_from, date_to]（含）對應的 entry_time/exit_time 查詢邊界
    （naive CST，含 date_from 的前一夜盤，不含 date_to 當天開始的夜盤——那屬於下一個
    交易日）。"""
    lower = datetime.combine(previous_trading_day(date_from), _NIGHT_OPEN)
    upper = datetime.combine(date_to, _NIGHT_OPEN) - timedelta(microseconds=1)
    return lower, upper


def today_trading_day(now: datetime | None = None) -> date:
    """目前所屬的交易日。`now` 供測試注入固定時刻；預設用真正的當下（CST）。"""
    if now is None:
        now = datetime.now(_CST).replace(tzinfo=None)
    return trading_day_of(now)
