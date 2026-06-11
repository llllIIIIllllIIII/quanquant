from datetime import date, datetime
from decimal import Decimal

import pytest
import respx
from httpx import Response

from quanquant.history.finmind_tick import FinMindTickProvider
from quanquant.market_hours import CST


def ms(y, m, d, hh, mm, ss=0) -> int:
    return int(datetime(y, m, d, hh, mm, ss, tzinfo=CST).timestamp() * 1000)


def tick(dt_str, contract, price, vol):
    return {"date": dt_str, "futures_id": "TX", "contract_date": contract,
            "price": price, "volume": vol}


# 2026-06-17 is the June settlement day (third Wednesday).
_FIXTURE = {
    "msg": "success", "status": 200,
    "data": [
        # day session on settlement day: 202606 still front
        tick("2026-06-17 09:00:01", "202606", 43000.0, 2),
        tick("2026-06-17 09:00:30", "202606", 43050.0, 3),
        tick("2026-06-17 09:00:59", "202606", 42990.0, 1),
        tick("2026-06-17 09:01:10", "202606", 43010.0, 4),
        # next-month ticks during day session must be IGNORED (not front yet)
        tick("2026-06-17 09:00:15", "202607", 42900.0, 99),
        # spread rows always ignored
        tick("2026-06-17 09:00:20", "202606/202607", -50.0, 7),
        # between sessions — outside trading hours, dropped
        tick("2026-06-17 14:30:00", "202607", 43100.0, 5),
        # night session on settlement day: front month has ROLLED to 202607
        tick("2026-06-17 15:00:05", "202607", 43120.0, 6),
        tick("2026-06-17 15:00:40", "202607", 43150.0, 2),
        # old contract printing at night must be IGNORED (expired)
        tick("2026-06-17 15:00:10", "202606", 43000.0, 88),
    ],
}


@pytest.mark.asyncio
@respx.mock
async def test_tick_aggregation_and_settlement_roll():
    respx.get("https://api.finmindtrade.com/api/v4/data").mock(
        return_value=Response(200, json=_FIXTURE)
    )
    async with FinMindTickProvider(token="t") as provider:
        candles = await provider.fetch_candles("TXF", date(2026, 6, 17), date(2026, 6, 17))

    assert [c.ts for c in candles] == [
        ms(2026, 6, 17, 9, 0), ms(2026, 6, 17, 9, 1), ms(2026, 6, 17, 15, 0),
    ]

    bar_0900 = candles[0]
    assert bar_0900.open == Decimal("43000.0")
    assert bar_0900.high == Decimal("43050.0")
    assert bar_0900.low == Decimal("42990.0")
    assert bar_0900.close == Decimal("42990.0")
    assert bar_0900.volume == 6  # 2+3+1; the 202607/spread ticks excluded
    assert bar_0900.session == "day" and bar_0900.trading_date == "2026-06-17"

    night = candles[2]
    assert night.open == Decimal("43120.0") and night.close == Decimal("43150.0")
    assert night.volume == 8  # 6+2; expired 202606's 88 lots excluded
    assert night.session == "night" and night.trading_date is None
    assert all(c.source == "finmind_tick" for c in candles)


@pytest.mark.asyncio
@respx.mock
async def test_tick_requires_token():
    async with FinMindTickProvider(token="") as provider:
        with pytest.raises(ValueError, match="sponsor"):
            await provider.fetch_candles("TXF", date(2026, 6, 17), date(2026, 6, 17))


@pytest.mark.asyncio
@respx.mock
async def test_tick_empty_day():
    respx.get("https://api.finmindtrade.com/api/v4/data").mock(
        return_value=Response(200, json={"msg": "success", "status": 200, "data": []})
    )
    async with FinMindTickProvider(token="t") as provider:
        candles = await provider.fetch_candles("TXF", date(2026, 6, 14), date(2026, 6, 14))
    assert candles == []
