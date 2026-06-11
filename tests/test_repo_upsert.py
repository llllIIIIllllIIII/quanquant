from datetime import datetime
from decimal import Decimal

from sqlmodel import Session, select

from quanquant.candles.repo import upsert_candles
from quanquant.db.models import Candle
from quanquant.market_hours import CST


def _rows(n: int) -> list[Candle]:
    base = int(datetime(2026, 6, 10, 8, 45, tzinfo=CST).timestamp() * 1000)
    return [
        Candle(
            symbol="TXF", timeframe="1m", ts=base + i * 60_000,
            open=Decimal(18000 + i), high=Decimal(18005 + i), low=Decimal(17995 + i),
            close=Decimal(18001 + i), volume=i, source="rebuild",
            session="day", trading_date="2026-06-10",
        )
        for i in range(n)
    ]


def test_upsert_chunks_past_sqlite_param_limit(engine):
    """200 rows × 12 cols would exceed old SQLite's 999-param cap in one stmt."""
    with Session(engine) as s:
        upsert_candles(s, _rows(200))
        count = len(list(s.exec(select(Candle).where(Candle.timeframe == "1m"))))
    assert count == 200


def test_upsert_rerun_is_idempotent_and_updates(engine):
    with Session(engine) as s:
        upsert_candles(s, _rows(100))
        updated = _rows(100)
        for r in updated:
            r.close = Decimal("99999")
        upsert_candles(s, updated)

        rows = list(s.exec(select(Candle).where(Candle.timeframe == "1m")))
    assert len(rows) == 100  # no duplicates
    assert all(r.close == Decimal("99999") for r in rows)  # conflict path updated
