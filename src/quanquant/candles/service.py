"""Candle query service — derive-on-demand over the canonical 1m/1d store.

Performance notes:
- Reads come back as FastCandle tuples (no ORM/Decimal overhead).
- History pages (`before` set) are immutable under live appends → cached 10 min.
  First pages (no `before`) contain the live bar → cached 2 s (poll smoothing).
- The stored 1d series is re-read at most every 30 s; the synthetic "today" bar
  is always computed fresh so it ticks live.
- A daily backfill run from another process becomes visible within the TTLs.
"""
import time
from dataclasses import dataclass

from sqlmodel import Session

from quanquant.candles import repo
from quanquant.candles.aggregate import Bar, aggregate_daily, aggregate_intraday, to_bars
from quanquant.candles.bucketing import day_open_ms
from quanquant.candles.repo import FastCandle
from quanquant.candles.timeframes import TIMEFRAMES

_MAX_1M_FETCH = 200_000  # hard cap on 1m rows pulled for one derived-TF page

_PAGE_TTL_HISTORY = 600.0  # pages with `before` (closed history)
_PAGE_TTL_LIVE = 2.0       # first page (contains the live bar)
_D1_TTL = 30.0             # stored 1d series
_PAGE_CACHE_MAX = 256


@dataclass(frozen=True, slots=True)
class CandlePage:
    bars: list[Bar]  # ascending
    has_more: bool


_page_cache: dict[tuple, tuple[float, CandlePage]] = {}
_d1_cache: dict[str, tuple[float, list[FastCandle]]] = {}


def clear_caches() -> None:
    """For tests and post-backfill freshness."""
    _page_cache.clear()
    _d1_cache.clear()


def _cache_get(key: tuple) -> CandlePage | None:
    hit = _page_cache.get(key)
    if hit is None:
        return None
    expires, page = hit
    if time.monotonic() > expires:
        _page_cache.pop(key, None)
        return None
    return page


def _cache_put(key: tuple, page: CandlePage, ttl: float) -> None:
    if len(_page_cache) >= _PAGE_CACHE_MAX:
        now = time.monotonic()
        for k in [k for k, (exp, _) in _page_cache.items() if exp < now]:
            _page_cache.pop(k, None)
        while len(_page_cache) >= _PAGE_CACHE_MAX:  # still full → drop oldest inserts
            _page_cache.pop(next(iter(_page_cache)), None)
    _page_cache[key] = (time.monotonic() + ttl, page)


def get_candles(
    session: Session, symbol: str, tf: str, *, before: int | None = None, limit: int = 500
) -> CandlePage:
    spec = TIMEFRAMES[tf]  # KeyError → router turns into 422

    key = (symbol, tf, before, limit)
    cached = _cache_get(key)
    if cached is not None:
        return cached

    if spec.kind == "intraday":
        if tf == "1m":
            page = _page_1m(session, symbol, before, limit)
        else:
            page = _page_derived_intraday(session, symbol, tf, before, limit)
    else:
        page = _page_daily(session, symbol, tf, before, limit)

    _cache_put(key, page, _PAGE_TTL_HISTORY if before is not None else _PAGE_TTL_LIVE)
    return page


def get_latest(session: Session, symbol: str, tf: str, *, since: int) -> list[Bar]:
    """Bars with timestamp >= since, freshly re-aggregated (usually 1–2 bars)."""
    spec = TIMEFRAMES[tf]
    if spec.kind == "intraday":
        rows = repo.select_1m_range(session, symbol, start_ms=since)
        if tf == "1m":
            return to_bars(rows)
        return [b for b in aggregate_intraday(rows, tf) if b["timestamp"] >= since]
    series = _full_1d_series(session, symbol)
    bars = to_bars(series) if tf == "1d" else aggregate_daily(series, tf)
    return [b for b in bars if b["timestamp"] >= since]


# --- pages ---


def _page_1m(session: Session, symbol: str, before: int | None, limit: int) -> CandlePage:
    rows = repo.select_1m_desc(session, symbol, before_ms=before, limit=limit)  # ascending
    if not rows:
        return CandlePage(bars=[], has_more=False)
    return CandlePage(
        bars=to_bars(rows),
        has_more=repo.exists_1m_before(session, symbol, rows[0].ts),
    )


def _page_derived_intraday(
    session: Session, symbol: str, tf: str, before: int | None, limit: int
) -> CandlePage:
    minutes = TIMEFRAMES[tf].minutes
    assert minutes is not None
    fetch = min(limit * minutes + minutes, _MAX_1M_FETCH)
    rows = repo.select_1m_desc(session, symbol, before_ms=before, limit=fetch)  # ascending
    if not rows:
        return CandlePage(bars=[], has_more=False)

    truncated = len(rows) == fetch  # more 1m rows may exist before our window
    bars = aggregate_intraday(rows, tf)
    if truncated and len(bars) > 1:
        bars = bars[1:]  # oldest bucket may be missing leading 1m rows — drop it
    bars = bars[-limit:]
    if not bars:
        return CandlePage(bars=[], has_more=False)

    has_more = repo.exists_1m_before(session, symbol, bars[0]["timestamp"])
    return CandlePage(bars=bars, has_more=has_more)


def _page_daily(
    session: Session, symbol: str, tf: str, before: int | None, limit: int
) -> CandlePage:
    series = _full_1d_series(session, symbol)
    bars = to_bars(series) if tf == "1d" else aggregate_daily(series, tf)
    if before is not None:
        bars = [b for b in bars if b["timestamp"] < before]
    page = bars[-limit:]
    return CandlePage(bars=page, has_more=len(bars) > len(page))


# --- 1d series = stored backfill + derived tail from day-session 1m data ---


def _stored_1d(session: Session, symbol: str) -> list[FastCandle]:
    hit = _d1_cache.get(symbol)
    if hit is not None and time.monotonic() <= hit[0]:
        return hit[1]
    rows = repo.select_1d_all(session, symbol)
    _d1_cache[symbol] = (time.monotonic() + _D1_TTL, rows)
    return rows


def _full_1d_series(session: Session, symbol: str) -> list[FastCandle]:
    stored = _stored_1d(session, symbol)
    last_date = stored[-1].trading_date if stored else None
    tail_dates = repo.distinct_1m_day_dates_after(session, symbol, last_date)
    tail = [
        bar
        for d in tail_dates
        if (bar := _synthesize_1d(session, symbol, d)) is not None
    ]
    return stored + tail


def _synthesize_1d(session: Session, symbol: str, trading_date: str) -> FastCandle | None:
    """Aggregate one date's day-session 1m candles into a synthetic 日K row.

    Never stored — the official backfill row replaces it on the next run.
    """
    rows = repo.select_1m_for_day_session(session, symbol, trading_date)
    if not rows:
        return None
    return FastCandle(
        ts=day_open_ms(trading_date),
        open=rows[0].open,
        high=max(r.high for r in rows),
        low=min(r.low for r in rows),
        close=rows[-1].close,
        volume=sum(r.volume for r in rows),
        trading_date=trading_date,
    )
