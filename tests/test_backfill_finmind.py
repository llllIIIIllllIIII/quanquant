from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
import respx
from httpx import Response
from sqlmodel import Session, select

from quanquant.backfill_cli import run_rebuild_1m
from quanquant.db.models import Candle, Quote
from quanquant.history.finmind_daily import FinMindDailyProvider
from quanquant.market_hours import CST

# 2026-06 settlement (third Wednesday) = 2026-06-17.
# Fixture: 16th (前結算日), 17th (結算日), 18th (轉倉日) + spreads + after-market rows.
_FIXTURE = {
    "msg": "success",
    "status": 200,
    "data": [
        # 06-16: front month 202606 still eligible
        {"date": "2026-06-16", "future_id": "TX", "contract_date": "202606",
         "open": 22000, "max": 22100, "min": 21900, "close": 22050, "volume": 100000,
         "trading_session": "position"},
        {"date": "2026-06-16", "future_id": "TX", "contract_date": "202607",
         "open": 21950, "max": 22050, "min": 21850, "close": 22000, "volume": 20000,
         "trading_session": "position"},
        {"date": "2026-06-16", "future_id": "TX", "contract_date": "202606/202607",
         "open": -50, "max": -40, "min": -60, "close": -45, "volume": 500,
         "trading_session": "position"},  # spread row must be dropped
        {"date": "2026-06-16", "future_id": "TX", "contract_date": "202606",
         "open": 22010, "max": 22060, "min": 21960, "close": 22020, "volume": 30000,
         "trading_session": "after_market"},  # 盤後 must be dropped
        # 06-17: settlement day — 202606 settles today, still front month
        {"date": "2026-06-17", "future_id": "TX", "contract_date": "202606",
         "open": 22050, "max": 22150, "min": 22000, "close": 22100, "volume": 80000,
         "trading_session": "position"},
        {"date": "2026-06-17", "future_id": "TX", "contract_date": "202607",
         "open": 22000, "max": 22100, "min": 21950, "close": 22060, "volume": 60000,
         "trading_session": "position"},
        # 06-18: day after settlement — rolls to 202607
        {"date": "2026-06-18", "future_id": "TX", "contract_date": "202607",
         "open": 22070, "max": 22200, "min": 22050, "close": 22180, "volume": 90000,
         "trading_session": "position"},
    ],
}


@pytest.mark.asyncio
@respx.mock
async def test_finmind_near_month_series():
    respx.get("https://api.finmindtrade.com/api/v4/data").mock(
        return_value=Response(200, json=_FIXTURE)
    )
    async with FinMindDailyProvider(token="t") as provider:
        candles = await provider.fetch_candles("TXF", date(2026, 6, 16), date(2026, 6, 18))

    assert [c.trading_date for c in candles] == ["2026-06-16", "2026-06-17", "2026-06-18"]
    # 16th & 17th use 202606 (front through settlement day inclusive)
    assert candles[0].close == Decimal("22050")
    assert candles[1].close == Decimal("22100")
    # 18th rolled to 202607
    assert candles[2].close == Decimal("22180")
    assert all(c.session == "day" and c.source == "finmind" for c in candles)


@pytest.mark.asyncio
@respx.mock
async def test_finmind_requires_token():
    async with FinMindDailyProvider(token="") as provider:
        with pytest.raises(ValueError, match="token"):
            await provider.fetch_candles("TXF", date(2026, 1, 1), date(2026, 1, 2))


@pytest.mark.asyncio
@respx.mock
async def test_finmind_fallback_when_no_eligible_contract():
    fixture = {
        "msg": "success", "status": 200,
        "data": [
            {"date": "2026-06-18", "future_id": "TX", "contract_date": "202605",
             "open": 22000, "max": 22100, "min": 21900, "close": 22050, "volume": 10,
             "trading_session": "position"},  # already expired — fallback path
        ],
    }
    respx.get("https://api.finmindtrade.com/api/v4/data").mock(
        return_value=Response(200, json=fixture)
    )
    async with FinMindDailyProvider(token="t") as provider:
        candles = await provider.fetch_candles("TXF", date(2026, 6, 18), date(2026, 6, 18))
    assert len(candles) == 1  # warned, not crashed


def test_rebuild_1m_from_quotes(engine, monkeypatch):
    import quanquant.backfill_cli as cli

    monkeypatch.setattr(cli, "get_engine", lambda: engine)

    def cst_utc(hh, mm, ss):
        return (
            datetime(2026, 6, 10, hh, mm, ss, tzinfo=CST)
            .astimezone(timezone.utc)
            .replace(tzinfo=None)
        )

    with Session(engine) as s:
        for t, price, vol in (
            (cst_utc(9, 0, 0), "18000", 1000),
            (cst_utc(9, 0, 30), "18020", 1040),
            (cst_utc(9, 1, 10), "18010", 1100),
        ):
            s.add(Quote(symbol="TXF", price=Decimal(price), volume=vol, fetched_at=t))
        s.commit()

    count = run_rebuild_1m("TXF", days=None)
    assert count == 2  # two 1m bars

    with Session(engine) as s:
        rows = list(
            s.exec(
                select(Candle)
                .where(Candle.timeframe == "1m")
                .order_by(Candle.ts)  # type: ignore[arg-type]
            )
        )
    assert len(rows) == 2
    assert rows[0].high == Decimal("18020") and rows[0].volume == 40
    assert rows[1].volume == 60
    assert all(r.source == "rebuild" for r in rows)
