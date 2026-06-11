"""Daily 台指期 candles from FinMind's TaiwanFuturesDaily dataset.

FinMind returns one row per (date, contract_date, trading_session). To build a
continuous front-month (近月) daily series we:
  1. keep regular-session rows only (trading_session == "position", 日盤),
  2. drop calendar-spread rows (contract_date like "202606/202607"),
  3. per date, pick the nearest unexpired contract: a contract is the front
     month through its settlement day (third Wednesday) inclusive; the series
     rolls to the next month the following trading day.

The roll gap between contracts is left as-is (unadjusted continuous series).
"""
import logging
import re
from datetime import date
from decimal import Decimal

import httpx

from quanquant.candles.bucketing import day_open_ms, third_wednesday
from quanquant.config import get_settings
from quanquant.history.base import HistoricalCandle, HistoryProvider

logger = logging.getLogger(__name__)

_API_URL = "https://api.finmindtrade.com/api/v4/data"
_DATASET = "TaiwanFuturesDaily"
_SYMBOL_MAP = {"TXF": "TX", "MXF": "MTX"}  # our symbol -> FinMind futures_id
_CONTRACT_RE = re.compile(r"^\d{6}$")


class FinMindDailyProvider(HistoryProvider):
    timeframe = "1d"

    def __init__(self, token: str | None = None, timeout: float = 30.0) -> None:
        self._token = token if token is not None else get_settings().finmind_token
        self._client = httpx.AsyncClient(timeout=timeout)

    async def fetch_candles(self, symbol: str, start: date, end: date) -> list[HistoricalCandle]:
        if not self._token:
            raise ValueError(
                "FinMind token missing — set FINMIND_TOKEN in .env "
                "(free registration at finmindtrade.com)"
            )
        data_id = _SYMBOL_MAP.get(symbol, symbol)

        rows: list[dict] = []
        # chunk by calendar year to keep responses small (well under rate limits)
        for year in range(start.year, end.year + 1):
            chunk_start = max(start, date(year, 1, 1))
            chunk_end = min(end, date(year, 12, 31))
            rows.extend(await self._fetch_chunk(data_id, chunk_start, chunk_end))

        return self._to_series(rows)

    async def _fetch_chunk(self, data_id: str, start: date, end: date) -> list[dict]:
        response = await self._client.get(
            _API_URL,
            params={
                "dataset": _DATASET,
                "data_id": data_id,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "token": self._token,
            },
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") not in (200, None):
            raise ValueError(f"FinMind API error: {payload.get('msg')}")
        return payload.get("data", [])

    def _to_series(self, rows: list[dict]) -> list[HistoricalCandle]:
        # group regular-session, non-spread rows by date
        by_date: dict[str, list[dict]] = {}
        for row in rows:
            if row.get("trading_session") != "position":  # 日盤 only
                continue
            contract = str(row.get("contract_date", ""))
            if not _CONTRACT_RE.match(contract):  # drop spreads like "202606/202607"
                continue
            by_date.setdefault(row["date"], []).append(row)

        out: list[HistoricalCandle] = []
        for day_str in sorted(by_date):
            row = self._pick_front_month(day_str, by_date[day_str])
            if row is None:
                continue
            candle = self._parse_row(day_str, row)
            if candle is not None:
                out.append(candle)
        return out

    @staticmethod
    def _pick_front_month(day_str: str, rows: list[dict]) -> dict | None:
        """Nearest contract whose settlement day (3rd Wednesday) >= the date."""
        row_date = date.fromisoformat(day_str)

        def settlement(row: dict) -> date:
            c = str(row["contract_date"])
            return third_wednesday(int(c[:4]), int(c[4:6]))

        eligible = [r for r in rows if settlement(r) >= row_date]
        if not eligible:
            logger.warning("FinMind: no unexpired contract on %s — using earliest", day_str)
            eligible = rows
        if not eligible:
            return None
        return min(eligible, key=lambda r: str(r["contract_date"]))

    @staticmethod
    def _parse_row(day_str: str, row: dict) -> HistoricalCandle | None:
        try:
            o, h, lo, c = (Decimal(str(row[k])) for k in ("open", "max", "min", "close"))
        except (KeyError, ArithmeticError):
            logger.warning("FinMind: unparsable row on %s: %r", day_str, row)
            return None
        if o <= 0 or h <= 0:  # holidays sometimes appear as zero rows
            return None
        return HistoricalCandle(
            ts=day_open_ms(day_str),
            open=o, high=h, low=lo, close=c,
            volume=int(row.get("volume") or 0),
            trading_date=day_str,
            session="day",
            source="finmind",
        )

    async def close(self) -> None:
        await self._client.aclose()
