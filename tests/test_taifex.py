from decimal import Decimal

import pytest
import respx
from httpx import Response

from quanquant.sources.taifex import TaifexSource

_SAMPLE = {
    "RtCode": "0",
    "RtMsg": "OK",
    "RtData": {
        "QuoteList": [
            {
                "SymbolID": "TXFF6-F", "CLastPrice": "18000", "CRefPrice": "17950",
                "CDiff": "50", "CDiffRate": "0.28", "CTotalVolume": "12345",
                "COpenPrice": "17960", "CHighPrice": "18050", "CLowPrice": "17940", "CDate": "2026-06-01",
            },
            {"SymbolID": "TXFF6-M", "CLastPrice": "", "CRefPrice": "17950"},
            {
                "SymbolID": "TXFG6-F", "CLastPrice": "18005", "CRefPrice": "17955",
                "CTotalVolume": "10", "COpenPrice": "17960", "CHighPrice": "18060", "CLowPrice": "17950",
                "CDate": "2026-06-01",
            },
        ]
    },
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_snapshot_parses_nearest_contract_with_price():
    respx.post("https://mis.taifex.com.tw/futures/api/getQuoteList").mock(
        return_value=Response(200, json=_SAMPLE)
    )
    async with TaifexSource() as src:
        snap = await src.fetch_snapshot("TXF")

    # Regardless of current session, the nearest contract carrying a price is TXFF6-F.
    assert snap.price == Decimal("18000")
    assert snap.high_price == Decimal("18050")
    assert snap.low_price == Decimal("17940")
    assert snap.volume == 12345
    assert snap.contract_month == "TXFF6-F"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_snapshot_raises_on_api_error():
    respx.post("https://mis.taifex.com.tw/futures/api/getQuoteList").mock(
        return_value=Response(200, json={"RtCode": "1", "RtMsg": "bad"})
    )
    with pytest.raises(ValueError):
        async with TaifexSource() as src:
            await src.fetch_snapshot("TXF")
