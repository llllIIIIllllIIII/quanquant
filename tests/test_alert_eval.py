from datetime import datetime
from decimal import Decimal

from sqlmodel import Session, select

import quanquant.alerts.engine as eng
from quanquant.alerts.engine import evaluate_alert
from quanquant.candles.repo import upsert_candles
from quanquant.db.models import Alert, AlertEvent, Candle
from quanquant.market_hours import CST


def ms(hh, mm) -> int:
    return int(datetime(2026, 6, 16, hh, mm, tzinfo=CST).timestamp() * 1000)


def series(*closes) -> list[dict]:
    return [
        {"timestamp": ms(9, i), "open": c, "high": c, "low": c, "close": c, "volume": 1}
        for i, c in enumerate(closes)
    ]


def price_alert(op, value, **kw) -> Alert:
    return Alert(
        symbol="TXF", timeframe="1m", left_kind="price", op=op,
        right_kind="const", right_value=Decimal(str(value)), **kw,
    )


# --- gte / lte with re-arm ---


def test_gte_fires_once_then_rearms():
    a = price_alert("gte", 100)
    assert evaluate_alert(a, series(105), 1).fired is True
    assert a.armed is False
    assert evaluate_alert(a, series(106), 2).fired is False  # still above, disarmed
    assert evaluate_alert(a, series(95), 3).fired is False   # left condition → re-arm
    assert a.armed is True
    assert evaluate_alert(a, series(101), 4).fired is True   # re-entered


def test_lte_fires():
    a = price_alert("lte", 100)
    assert evaluate_alert(a, series(95), 1).fired is True
    assert evaluate_alert(a, series(90), 2).fired is False
    assert evaluate_alert(a, series(105), 3).fired is False and a.armed is True


def test_fire_once_disables():
    a = price_alert("gte", 100, fire_once=True)
    assert evaluate_alert(a, series(105), 1).fired is True
    assert a.enabled is False


# --- crosses ---


def test_cross_up():
    a = price_alert("cross_up", 100)
    assert evaluate_alert(a, series(99, 101), 2).fired is True       # 99→101 crosses up
    assert evaluate_alert(a, series(101, 102), 3).fired is False     # already above
    assert evaluate_alert(a, series(98, 99), 4).fired is False       # below


def test_cross_down():
    a = price_alert("cross_down", 100)
    assert evaluate_alert(a, series(101, 99), 2).fired is True
    assert evaluate_alert(a, series(99, 98), 3).fired is False


def test_cross_no_double_fire_same_bar():
    a = price_alert("cross_up", 100)
    s = series(99, 101)
    assert evaluate_alert(a, s, 5).fired is True
    assert evaluate_alert(a, s, 5).fired is False  # same closed_ts → no re-fire


# --- indicator operand ---


def test_indicator_left_ma():
    a = Alert(
        symbol="TXF", timeframe="1m", left_kind="indicator", left_name="ma", left_period=3,
        op="gte", right_kind="const", right_value=Decimal("100"),
    )
    # sma(last 3 of [90,100,120]) = 103.33 >= 100 → fire
    assert evaluate_alert(a, series(90, 100, 120), 1).fired is True


def test_insufficient_data_no_fire():
    a = Alert(
        symbol="TXF", timeframe="1m", left_kind="indicator", left_name="ma", left_period=10,
        op="gte", right_kind="const", right_value=Decimal("1"),
    )
    assert evaluate_alert(a, series(1, 2, 3), 1).fired is False


# --- integration: _evaluate_sync against a seeded DB ---


def test_evaluate_sync_fires_and_logs(engine, monkeypatch):
    monkeypatch.setattr(eng, "get_engine", lambda: engine)
    # seed on a past trading day (2025-06-16, Mon) so every bar is closed by wall-clock
    def pms(hh, mm):
        return int(datetime(2025, 6, 16, hh, mm, tzinfo=CST).timestamp() * 1000)

    with Session(engine) as s:
        rows = [
            Candle(
                symbol="TXF", timeframe="1m", ts=pms(9, i),
                open=Decimal(18000 + i), high=Decimal(18000 + i),
                low=Decimal(18000 + i), close=Decimal(18000 + i),
                volume=1, source="finmind_tick", session="day", trading_date="2025-06-16",
            )
            for i in range(5)
        ]
        upsert_candles(s, rows)
        s.add(price_alert("gte", 18002))
        s.commit()

    fired = eng._evaluate_sync("TXF", {})
    assert len(fired) == 1
    assert "≥" in fired[0].body

    with Session(engine) as s:
        events = s.exec(select(AlertEvent)).all()
        assert len(events) == 1
        alert = s.exec(select(Alert)).first()
        assert alert.armed is False  # fired → disarmed
        assert alert.last_triggered_bar_ts == pms(9, 4)  # last closed 1m bar
