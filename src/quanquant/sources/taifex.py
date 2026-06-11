from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import httpx

from quanquant.market_hours import get_session
from quanquant.models import FuturesSnapshot
from quanquant.sources.base import DataSource

_MIS_URL = "https://mis.taifex.com.tw/futures/api/getQuoteList"

# TAIFEX month codes: A=Jan, B=Feb, C=Mar, D=Apr, E=May, F=Jun,
#                    G=Jul, H=Aug, I=Sep, J=Oct, K=Nov, L=Dec
_MONTH_CODES = {c: i + 1 for i, c in enumerate("ABCDEFGHIJKL")}


def _d(value: str, fallback: str = "0") -> Decimal:
    try:
        return Decimal(value) if value else Decimal(fallback)
    except InvalidOperation:
        return Decimal(fallback)


class TaifexSource(DataSource):
    """
    Fetches TXF real-time quotes from TAIFEX Market Information System.
    Endpoint: POST https://mis.taifex.com.tw/futures/api/getQuoteList
    No authentication required. Returns live quotes updated every few seconds.
    """

    def __init__(self, timeout: float = 10.0) -> None:
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Referer": "https://mis.taifex.com.tw/",
                "Content-Type": "application/json",
            },
        )

    async def fetch_snapshot(self, symbol: str = "TXF") -> FuturesSnapshot:
        response = await self._client.post(
            _MIS_URL,
            json={"MarketCode": "0", "CommodityID": symbol},
        )
        response.raise_for_status()
        payload = response.json()

        if payload.get("RtCode") != "0":
            raise ValueError(f"TAIFEX API error: {payload.get('RtMsg')}")

        records = payload.get("RtData", {}).get("QuoteList", [])
        if not records:
            raise ValueError(f"No quote data returned for {symbol}")

        row = self._select_nearest_contract(records, symbol)
        return self._parse_row(row, symbol)

    def _select_nearest_contract(self, records: list[dict], symbol: str) -> dict:
        # Contracts in the list alternate: TXFF6-F, TXFF6-M, TXFG6-F, TXFG6-M, ...
        # -F = day session (日盤 08:45-13:45), -M = night session (夜盤 15:00-05:00)
        session = get_session()
        primary_suffix = "-M" if session == "night" else "-F"
        fallback_suffix = "-F" if session == "night" else "-M"

        def _filter(suffix: str, require_price: bool) -> list[dict]:
            return [
                r for r in records
                if r["SymbolID"].startswith(symbol)
                and r["SymbolID"].endswith(suffix)
                and (not require_price or r["CLastPrice"])
            ]

        # Records are ordered by expiry — first hit = nearest month
        for suffix in (primary_suffix, fallback_suffix):
            candidates = _filter(suffix, require_price=True)
            if candidates:
                return candidates[0]

        # Market closed with no last prices — return nearest contract of primary session
        candidates = _filter(primary_suffix, require_price=False)
        if candidates:
            return candidates[0]

        raise ValueError(f"No futures contract found for {symbol}")

    def _parse_row(self, row: dict, symbol: str) -> FuturesSnapshot:
        # Use settlement price as fallback when last price is unavailable
        price_str = row["CLastPrice"] or row.get("SettlementPrice") or row["CRefPrice"]
        ref = row["CRefPrice"]

        price = _d(price_str)
        change = price - _d(ref) if ref else _d(row.get("CDiff", "0"))
        change_pct = float(row.get("CDiffRate") or 0)

        return FuturesSnapshot(
            symbol=symbol,
            price=price,
            change=change,
            change_pct=change_pct,
            volume=int(row.get("CTotalVolume") or 0),
            open_price=_d(row.get("COpenPrice", "0")),
            high_price=_d(row.get("CHighPrice", "0")),
            low_price=_d(row.get("CLowPrice", "0")),
            fetched_at=datetime.now(timezone.utc),
            data_date=row.get("CDate", ""),
            contract_month=row.get("SymbolID", ""),
        )

    async def close(self) -> None:
        await self._client.aclose()
