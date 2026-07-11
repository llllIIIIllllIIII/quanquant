from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from quanquant.freshness import FreshnessTracker
from quanquant.models import FuturesSnapshot
from quanquant.poller import QuoteEvent, QuotePoller

_AT = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)


def snap(vol, contract="TXFF6-F", trade_time=None, at=_AT, price="18000"):
    p = Decimal(price)
    return FuturesSnapshot(
        symbol="TXF", price=p, change=Decimal(0), change_pct=0.0, volume=vol,
        open_price=p, high_price=p, low_price=p, fetched_at=at,
        data_date="2026-06-16", contract_month=contract, trade_time=trade_time,
    )


def test_first_with_volume_is_fresh():
    t = FreshnessTracker()
    assert t.evaluate(snap(1000)).is_fresh is True
    assert t.last_advance_at == _AT


def test_first_zero_volume_not_fresh():
    t = FreshnessTracker()
    assert t.evaluate(snap(0)).is_fresh is False
    assert t.last_advance_at is None


def test_volume_advance_is_fresh():
    t = FreshnessTracker()
    t.evaluate(snap(1000))
    assert t.evaluate(snap(1010)).is_fresh is True


def test_volume_stall_not_fresh():
    t = FreshnessTracker()
    t.evaluate(snap(1000, at=_AT))
    later = datetime(2026, 6, 16, 10, 1, tzinfo=timezone.utc)
    out = t.evaluate(snap(1000, at=later))
    assert out.is_fresh is False
    assert t.last_advance_at == _AT  # 停滯不推進


def test_ctime_advance_rescues_frozen_volume():
    t = FreshnessTracker()
    t0 = datetime(2026, 6, 16, 10, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 6, 16, 10, 0, 5, tzinfo=timezone.utc)
    t.evaluate(snap(1000, trade_time=t0))
    assert t.evaluate(snap(1000, trade_time=t1)).is_fresh is True  # 量凍結但成交時間前進


def test_cumulative_drop_resets_baseline():
    t = FreshnessTracker()
    t.evaluate(snap(5000))
    assert t.evaluate(snap(3)).is_fresh is True  # 跨盤重數 → 重置，3>0 視為 fresh


def test_identity_change_resets_baseline():
    t = FreshnessTracker()
    t.evaluate(snap(1000, contract="TXFF6-F"))
    assert t.evaluate(snap(1000, contract="TXFG6")).is_fresh is True  # 身分改變 → 重置


def test_source_marked_not_fresh_is_respected():
    t = FreshnessTracker()
    s = replace(snap(1000), is_fresh=False)  # 來源（結算價 fallback）已標記 not-fresh
    assert t.evaluate(s).is_fresh is False   # tracker 不上升級


def _event(vol, at=_AT, contract="TXFF6-F"):
    return QuoteEvent(snapshot=snap(vol, contract=contract, at=at), error=None, at=at)


def test_poller_publish_annotates_is_fresh_and_advance():
    p = QuotePoller(source=None, symbol="TXF", interval=5.0)
    p.publish(_event(1000, at=_AT))
    assert p.last.is_fresh is True
    assert p.freshness.last_advance_at == _AT

    later = _AT + timedelta(minutes=1)
    p.publish(_event(1000, at=later))  # 停滯
    assert p.last.is_fresh is False
    assert p.freshness.last_advance_at == _AT  # 未推進


def test_poller_publish_ignores_error_events():
    p = QuotePoller(source=None, symbol="TXF", interval=5.0)
    p.publish(QuoteEvent(snapshot=None, error="boom", at=_AT))
    assert p.last is None
    assert p.freshness.last_advance_at is None
