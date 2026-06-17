from datetime import datetime
from decimal import Decimal

import pytest
from sqlmodel import Session

from quanquant.candles.repo import upsert_candles
from quanquant.db.models import Candle
from quanquant.market_hours import CST


def ms(y, m, d, hh, mm) -> int:
    return int(datetime(y, m, d, hh, mm, tzinfo=CST).timestamp() * 1000)


def c1m(ts, price, session, td=None):
    p = Decimal(price)
    return Candle(
        symbol="TXF", timeframe="1m", ts=ts, open=p, high=p + 5, low=p - 5, close=p + 1,
        volume=10, source="finmind_tick", session=session, trading_date=td,
    )


@pytest.fixture
def seeded(engine):
    """A trading day (2026-06-16): 3 day-session 1m bars (09:00-09:02) +
    3 night-session bars (15:00-15:02)."""
    with Session(engine) as s:
        rows = [c1m(ms(2026, 6, 16, 9, i), "18000", "day", "2026-06-16") for i in range(3)]
        rows += [c1m(ms(2026, 6, 16, 15, i), "18100", "night") for i in range(3)]
        upsert_candles(s, rows)
    return engine


def test_api_session_all(client, seeded):
    bars = client.get("/api/candles", params={"tf": "1m", "limit": 100}).json()["bars"]
    assert len(bars) == 6


def test_api_session_day_only(client, seeded):
    bars = client.get(
        "/api/candles", params={"tf": "1m", "limit": 100, "session": "day"}
    ).json()["bars"]
    assert len(bars) == 3
    assert all(b["timestamp"] < ms(2026, 6, 16, 15, 0) for b in bars)


def test_api_session_night_only(client, seeded):
    bars = client.get(
        "/api/candles", params={"tf": "1m", "limit": 100, "session": "night"}
    ).json()["bars"]
    assert len(bars) == 3
    assert all(b["timestamp"] >= ms(2026, 6, 16, 15, 0) for b in bars)


def test_api_derived_5m_session_day(client, seeded):
    # 3 day bars 09:00-09:02 → one 5m bucket; night excluded
    bars = client.get(
        "/api/candles", params={"tf": "5m", "limit": 100, "session": "day"}
    ).json()["bars"]
    assert len(bars) == 1
    assert bars[0]["timestamp"] == ms(2026, 6, 16, 9, 0)
    assert bars[0]["volume"] == 30


def test_api_daily_ignores_session(client, seeded):
    # daily synthesizes from day-session 1m regardless of the session param
    all_d = client.get("/api/candles", params={"tf": "1d", "session": "all"}).json()["bars"]
    night_d = client.get("/api/candles", params={"tf": "1d", "session": "night"}).json()["bars"]
    assert all_d == night_d
    assert len(all_d) == 1  # the 2026-06-16 synthetic day-K


def test_api_invalid_session_422(client):
    assert client.get("/api/candles", params={"tf": "1m", "session": "bogus"}).status_code == 422
