from datetime import datetime, timezone
from decimal import Decimal

import httpx

from quanquant.models import FuturesSnapshot
from quanquant.sources.base import DataSource

_BASE_URL = "https://api.finmindtrade.com/api/v4"


class FinMindSource(DataSource):
    """
    Fetches TXF snapshots from the FinMind public API.
    Endpoint: GET /taiwan_futures_snapshot?data_id=TXF
    Rate limit: ~300 req/hour unauthenticated, ~600 with free token.
    """

    def __init__(self, token: str | None = None, timeout: float = 10.0) -> None:
        self._token = token
        self._client = httpx.AsyncClient(
            base_url=_BASE_URL,
            timeout=timeout,
            headers={"User-Agent": "quanquant/0.1"},
        )

    async def fetch_snapshot(self, symbol: str = "TXF") -> FuturesSnapshot:
        params: dict[str, str] = {"data_id": symbol}
        if self._token:
            params["token"] = self._token

        response = await self._client.get("/taiwan_futures_snapshot", params=params)
        response.raise_for_status()
        payload = response.json()

        records = payload.get("data", [])
        if not records:
            raise ValueError(f"No snapshot data returned for {symbol}")

        # Use the first record (nearest active contract)
        return self._parse_row(records[0], symbol)

    def _parse_row(self, row: dict, symbol: str) -> FuturesSnapshot:
        return FuturesSnapshot(
            symbol=symbol,
            price=Decimal(str(row["close"])),
            change=Decimal(str(row["change"])),
            change_pct=float(row["change_percent"]),
            volume=int(row["volume"]),
            open_price=Decimal(str(row["open"])),
            high_price=Decimal(str(row["max"])),
            low_price=Decimal(str(row["min"])),
            fetched_at=datetime.now(timezone.utc),
            data_date=str(row.get("date", "")),
            contract_month=str(row.get("contract_date", "")),
        )

    async def close(self) -> None:
        await self._client.aclose()
