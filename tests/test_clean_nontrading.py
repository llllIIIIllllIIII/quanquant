from datetime import datetime
from decimal import Decimal

from sqlmodel import Session, select

import quanquant.backfill_cli as cli
from quanquant.candles.repo import upsert_candles
from quanquant.db.models import Candle
from quanquant.market_hours import CST


def ms(y, m, d, hh, mm) -> int:
    return int(datetime(y, m, d, hh, mm, tzinfo=CST).timestamp() * 1000)


def candle(ts, source, session, td=None):
    p = Decimal("18000")
    return Candle(
        symbol="TXF", timeframe="1m", ts=ts, open=p, high=p, low=p, close=p,
        volume=1, source=source, session=session, trading_date=td,
    )


def test_clean_nontrading_removes_phantoms_keeps_valid(engine, monkeypatch):
    monkeypatch.setattr(cli, "get_engine", lambda: engine)
    with Session(engine) as s:
        upsert_candles(s, [
            candle(ms(2026, 6, 13, 10, 0), "live", "day", "2026-06-13"),   # Sat day — phantom
            candle(ms(2026, 6, 14, 16, 0), "live", "night"),               # Sun night — phantom
            candle(ms(2026, 6, 19, 10, 0), "live", "day", "2026-06-19"),   # holiday — phantom
            candle(ms(2026, 6, 17, 13, 40), "live", "day", "2026-06-17"),  # settlement >13:30 — phantom
            candle(ms(2026, 6, 12, 16, 0), "live", "night"),               # Fri night — VALID
            candle(ms(2026, 6, 16, 10, 0), "live", "day", "2026-06-16"),   # weekday day — VALID
            candle(ms(2026, 6, 13, 3, 0), "finmind_tick", "night"),        # backfill — never touched
        ])

    assert cli.run_clean_nontrading("TXF", dry_run=True) == 4

    deleted = cli.run_clean_nontrading("TXF", dry_run=False)
    assert deleted == 4

    with Session(engine) as s:
        remaining = {(c.ts, c.source) for c in s.exec(select(Candle))}
    assert (ms(2026, 6, 12, 16, 0), "live") in remaining        # Fri night kept
    assert (ms(2026, 6, 16, 10, 0), "live") in remaining        # weekday kept
    assert (ms(2026, 6, 13, 3, 0), "finmind_tick") in remaining  # backfill kept
    assert all(s != "live" or ts not in {
        ms(2026, 6, 13, 10, 0), ms(2026, 6, 14, 16, 0),
        ms(2026, 6, 19, 10, 0), ms(2026, 6, 17, 13, 40),
    } for ts, s in remaining)
    assert len(remaining) == 3
