from datetime import datetime, timezone
from decimal import Decimal

from quanquant.candles.builder import CandleBuilder
from quanquant.market_hours import CST
from quanquant.models import FuturesSnapshot


def snap(hh, mm, ss, price, cum_vol, y=2026, mo=6, d=10, contract="TXFF6"):
    cst = datetime(y, mo, d, hh, mm, ss, tzinfo=CST)
    return FuturesSnapshot(
        symbol="TXF", price=Decimal(price), change=Decimal(0), change_pct=0.0,
        volume=cum_vol, open_price=Decimal(price), high_price=Decimal(price),
        low_price=Decimal(price), fetched_at=cst.astimezone(timezone.utc),
        data_date="2026-06-10", contract_month=contract,
    )


def ms(hh, mm, y=2026, mo=6, d=10):
    return int(datetime(y, mo, d, hh, mm, tzinfo=CST).timestamp() * 1000)


def test_first_snapshot_volume_baseline_zero():
    b = CandleBuilder("TXF")
    rows = b.on_snapshot(snap(9, 0, 0, "18000", 1000))
    assert len(rows) == 1
    bar = rows[0]
    assert bar.ts == ms(9, 0)
    assert bar.volume == 0  # unknown baseline → 0
    assert bar.open == bar.high == bar.low == bar.close == Decimal("18000")
    assert bar.session == "day" and bar.trading_date == "2026-06-10"


def test_same_minute_merges_ohlcv():
    b = CandleBuilder("TXF")
    b.on_snapshot(snap(9, 0, 0, "18000", 1000))
    b.on_snapshot(snap(9, 0, 20, "18050", 1030))
    rows = b.on_snapshot(snap(9, 0, 40, "17990", 1050))
    assert len(rows) == 1
    bar = rows[0]
    assert bar.high == Decimal("18050") and bar.low == Decimal("17990")
    assert bar.close == Decimal("17990")
    assert bar.volume == 0 + 30 + 20


def test_minute_rollover_returns_old_and_new():
    b = CandleBuilder("TXF")
    b.on_snapshot(snap(9, 0, 55, "18000", 1000))
    rows = b.on_snapshot(snap(9, 1, 5, "18010", 1040))
    assert len(rows) == 2
    old, new = rows
    assert old.ts == ms(9, 0) and new.ts == ms(9, 1)
    assert new.open == Decimal("18010") and new.volume == 40


def test_cumulative_volume_reset_uses_raw_value():
    b = CandleBuilder("TXF")
    b.on_snapshot(snap(13, 44, 0, "18000", 50000))
    rows = b.on_snapshot(snap(15, 0, 5, "18005", 120))  # night session, counter reset
    bar = rows[-1]
    assert bar.session == "night"
    assert bar.volume == 120
    assert bar.trading_date is None  # night rows carry no day-session date


def test_closed_session_emits_nothing_and_resets_baseline():
    b = CandleBuilder("TXF")
    b.on_snapshot(snap(13, 44, 0, "18000", 50000))
    assert b.on_snapshot(snap(14, 30, 0, "18000", 50000)) == []  # closed
    rows = b.on_snapshot(snap(15, 0, 5, "18005", 120))
    assert rows[-1].volume == 0  # baseline was reset during the closed window


def test_source_switch_does_not_diff_across_cumulative_baselines():
    """A MIS-fallback tick mixed into a Shioaji stream must not diff its separate
    cumulative counter against Shioaji's — that produced a phantom ~40k 1m bar."""
    b = CandleBuilder("TXF")
    # Shioaji stream ticks (contract "TXFG6"), small night-session cumulative.
    b.on_snapshot(snap(4, 50, 0, "18000", 300, contract="TXFG6"))
    b.on_snapshot(snap(4, 50, 30, "18005", 320, contract="TXFG6"))
    # MIS fallback fires once during a stream gap; its CTotalVolume (~40k) is a
    # SEPARATE counter tagged with the MIS symbol — must NOT diff against Shioaji.
    rows = b.on_snapshot(snap(4, 51, 0, "18010", 40878, contract="TXFG6-M"))
    assert rows[-1].ts == ms(4, 51)
    assert rows[-1].volume == 0  # identity changed → re-baseline, no 40k phantom
    # Stream resumes: first tick re-baselines (0), then diffs within the same source.
    assert b.on_snapshot(snap(4, 51, 30, "18012", 360, contract="TXFG6"))[-1].volume == 0
    assert b.on_snapshot(snap(4, 51, 50, "18015", 380, contract="TXFG6"))[-1].volume == 20
