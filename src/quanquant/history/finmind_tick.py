"""1-minute 台指期 candles aggregated from FinMind's TaiwanFuturesTick dataset.

Requires the FinMind sponsor (贊助) tier. Tick history goes back to 2011-01-03;
one request returns one calendar day of ticks (~200k rows / ~19 MB for TX), so
fetches are chunked per day and aggregated to 1m bars immediately.

Front-month (近月) selection per tick:
- ticks before 15:00 reference their own calendar date D — a contract is front
  through its settlement day (third Wednesday) inclusive;
- ticks from 15:00 on belong to the night session, which already trades the
  NEXT front month on settlement day — they reference D+1.
Tick volumes are summed per minute (true per-minute volume, better than the
cumulative diff used by the live builder).
"""
import asyncio
import logging
import re
from datetime import date, datetime, timedelta

from decimal import Decimal

import httpx

from quanquant.candles.bucketing import (
    bucket_start_ms,
    day_session_date,
    session_of_ms,
    third_wednesday,
)
from quanquant.config import get_settings
from quanquant.history.base import HistoricalCandle, HistoryProvider
from quanquant.market_hours import CST

logger = logging.getLogger(__name__)

_API_URL = "https://api.finmindtrade.com/api/v4/data"
_DATASET = "TaiwanFuturesTick"
_SYMBOL_MAP = {"TXF": "TX", "MXF": "MTX"}
_CONTRACT_RE = re.compile(r"^\d{6}$")
_NIGHT_OPEN_HOUR = 15


class FinMindTickProvider(HistoryProvider):
    timeframe = "1m"

    def __init__(self, token: str | None = None, timeout: float = 120.0) -> None:
        self._token = token if token is not None else get_settings().finmind_token
        self._client = httpx.AsyncClient(timeout=timeout)

    async def fetch_candles(self, symbol: str, start: date, end: date) -> list[HistoricalCandle]:
        if not self._token:
            raise ValueError(
                "FinMind token missing — TaiwanFuturesTick needs a sponsor-tier token "
                "(set FINMIND_TOKEN in .env)"
            )
        data_id = _SYMBOL_MAP.get(symbol, symbol)
        out: list[HistoricalCandle] = []
        day = start
        while day <= end:
            rows = await self._fetch_day(data_id, day)
            out.extend(self._aggregate_day(day, rows))
            day += timedelta(days=1)
            if day <= end:
                await asyncio.sleep(0.25)  # polite pacing, far under rate limits
        return out

    async def _fetch_day(self, data_id: str, day: date) -> list[dict]:
        response = await self._client.get(
            _API_URL,
            params={
                "dataset": _DATASET,
                "data_id": data_id,
                "start_date": day.isoformat(),
                "token": self._token,
            },
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") not in (200, None):
            raise ValueError(f"FinMind API error: {payload.get('msg')}")
        return payload.get("data", [])

    def _aggregate_day(self, day: date, rows: list[dict]) -> list[HistoricalCandle]:
        if not rows:
            return []

        front_day = self._front_month(rows, day)
        front_night = self._front_month(rows, day + timedelta(days=1))

        # tick -> 1m OHLCV, keyed by session-anchored minute bucket
        bars: dict[int, list] = {}  # ts -> [o, h, l, c, v]
        for row in rows:
            contract = str(row.get("contract_date", ""))
            if not _CONTRACT_RE.match(contract):
                continue  # spreads like "202606/202607"
            try:
                dt = datetime.fromisoformat(row["date"]).replace(tzinfo=CST)
            except (KeyError, ValueError):
                continue
            front = front_night if dt.hour >= _NIGHT_OPEN_HOUR else front_day
            if contract != front:
                continue
            ts_ms = int(dt.timestamp() * 1000)
            bucket = bucket_start_ms(ts_ms, "1m")
            if bucket is None:
                continue  # outside trading sessions
            price = float(row["price"])
            vol = int(row.get("volume") or 0)
            bar = bars.get(bucket)
            if bar is None:
                bars[bucket] = [price, price, price, price, vol]
            else:
                if price > bar[1]:
                    bar[1] = price
                if price < bar[2]:
                    bar[2] = price
                bar[3] = price
                bar[4] += vol

        out = []
        for ts in sorted(bars):
            o, h, lo, c, v = bars[ts]
            out.append(
                HistoricalCandle(
                    ts=ts,
                    open=Decimal(str(o)),
                    high=Decimal(str(h)),
                    low=Decimal(str(lo)),
                    close=Decimal(str(c)),
                    volume=v,
                    trading_date=day_session_date(ts),  # None for night bars
                    session=session_of_ms(ts) or "day",
                    source="finmind_tick",
                )
            )
        return out

    @staticmethod
    def _front_month(rows: list[dict], reference: date) -> str:
        contracts = sorted(
            {
                str(r.get("contract_date", ""))
                for r in rows
                if _CONTRACT_RE.match(str(r.get("contract_date", "")))
            }
        )
        eligible = [
            c for c in contracts if third_wednesday(int(c[:4]), int(c[4:6])) >= reference
        ]
        if eligible:
            return eligible[0]
        return contracts[0] if contracts else ""

    async def close(self) -> None:
        await self._client.aclose()
