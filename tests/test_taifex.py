from datetime import datetime
from decimal import Decimal

import pytest
import respx
from httpx import Response

from quanquant.market_hours import CST
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


_SAMPLE_CTIME = {
    "RtCode": "0",
    "RtMsg": "OK",
    "RtData": {
        "QuoteList": [
            {
                "SymbolID": "TXFF6-F", "CLastPrice": "18000", "CRefPrice": "17950",
                "CDiff": "50", "CDiffRate": "0.28", "CTotalVolume": "12345",
                "COpenPrice": "17960", "CHighPrice": "18050", "CLowPrice": "17940",
                "CDate": "2026-06-01", "CTime": "10:30:45",
            },
        ]
    },
}


@pytest.mark.asyncio
@respx.mock
async def test_parse_row_reads_ctime_as_trade_time():
    respx.post("https://mis.taifex.com.tw/futures/api/getQuoteList").mock(
        return_value=Response(200, json=_SAMPLE_CTIME)
    )
    async with TaifexSource() as src:
        snap = await src.fetch_snapshot("TXF")
    assert snap.trade_time == datetime(2026, 6, 1, 10, 30, 45, tzinfo=CST)


@pytest.mark.asyncio
@respx.mock
async def test_missing_ctime_gives_none_trade_time():
    respx.post("https://mis.taifex.com.tw/futures/api/getQuoteList").mock(
        return_value=Response(200, json=_SAMPLE)  # 既有 sample 無 CTime
    )
    async with TaifexSource() as src:
        snap = await src.fetch_snapshot("TXF")
    assert snap.trade_time is None


# 同時提供日盤(-F)與夜盤(-M)結算價列：_select_nearest_contract 的 settlement
# fallback（require_price=False）只試當下 session 的 primary_suffix，故兩者皆備，
# 測試才不受執行時段（日/夜盤）影響；兩列 CLastPrice 皆空 → is_fresh 恆 False。
_SAMPLE_CLOSED = {
    "RtCode": "0", "RtMsg": "OK",
    "RtData": {"QuoteList": [
        {"SymbolID": "TXFF6-F", "CLastPrice": "", "CRefPrice": "17950",
         "SettlementPrice": "17950", "CTotalVolume": "12345", "CDate": "2026-06-01"},
        {"SymbolID": "TXFF6-M", "CLastPrice": "", "CRefPrice": "17950",
         "SettlementPrice": "17950", "CTotalVolume": "12345", "CDate": "2026-06-01"},
    ]},
}


@pytest.mark.asyncio
@respx.mock
async def test_settlement_fallback_marked_not_fresh():
    respx.post("https://mis.taifex.com.tw/futures/api/getQuoteList").mock(
        return_value=Response(200, json=_SAMPLE_CLOSED)
    )
    async with TaifexSource() as src:
        snap = await src.fetch_snapshot("TXF")
    assert snap.is_fresh is False   # 結算價 fallback，非新成交
