import asyncio
import datetime as dt
from decimal import Decimal

import pytest

from quanquant.models import FuturesSnapshot
from quanquant.poller import QuotePoller
from quanquant.sources.base import DataSource


def _snap(price: str) -> FuturesSnapshot:
    p = Decimal(price)
    return FuturesSnapshot(
        symbol="TXF", price=p, change=Decimal("0"), change_pct=0.0, volume=1,
        open_price=p, high_price=p, low_price=p,
        fetched_at=dt.datetime(2026, 6, 1, 1, 0), data_date="2026-06-01", contract_month="TXFF6",
    )


class FakeSource(DataSource):
    def __init__(self, prices, fail_at=None):
        self._prices = list(prices)
        self._i = 0
        self._fail_at = fail_at

    async def fetch_snapshot(self, symbol: str = "TXF") -> FuturesSnapshot:
        i = self._i
        self._i += 1
        if self._fail_at is not None and i == self._fail_at:
            raise RuntimeError("boom")
        return _snap(self._prices[min(i, len(self._prices) - 1)])

    async def close(self) -> None:
        pass


@pytest.mark.asyncio
async def test_broadcasts_to_two_subscribers_and_caches_last():
    poller = QuotePoller(FakeSource(["18000", "18010"]), "TXF", 0.01)
    q1 = poller.subscribe()
    q2 = poller.subscribe()
    task = asyncio.create_task(poller.run())
    try:
        e1 = await asyncio.wait_for(q1.get(), 1)
        e2 = await asyncio.wait_for(q2.get(), 1)
        assert e1.snapshot.price == Decimal("18000")
        assert e2.snapshot.price == Decimal("18000")
        assert poller.last is not None and poller.last.price == Decimal("18000")
    finally:
        task.cancel()


@pytest.mark.asyncio
async def test_fetch_error_does_not_kill_loop():
    poller = QuotePoller(FakeSource(["18000", "18010", "18020"], fail_at=0), "TXF", 0.01)
    q = poller.subscribe()
    task = asyncio.create_task(poller.run())
    try:
        first = await asyncio.wait_for(q.get(), 1)
        assert first.snapshot is None and first.error  # error event broadcast
        second = await asyncio.wait_for(q.get(), 1)
        assert second.snapshot is not None  # loop kept polling after the error
    finally:
        task.cancel()


# --- hub API used by the Shioaji streamer + MIS fallback ---


def test_is_stale_true_before_any_snapshot():
    poller = QuotePoller(FakeSource(["1"]), "TXF", 0.01)
    assert poller.seconds_since_snapshot() == float("inf")
    assert poller.is_stale(0.0) is True


def test_publish_records_last_and_clears_staleness():
    from quanquant.poller import QuoteEvent

    poller = QuotePoller(FakeSource(["1"]), "TXF", 0.01)
    snap = _snap("18500")
    poller.publish(QuoteEvent(snapshot=snap, error=None, at=snap.fetched_at))
    # publish() now runs the snapshot through FreshnessTracker.evaluate(), which
    # returns a `dataclasses.replace`d copy (is_fresh set) rather than the same
    # object — so compare the fields that identify "this is the same quote"
    # instead of object identity.
    assert poller.last is not None
    assert poller.last.price == snap.price
    assert poller.last.volume == snap.volume
    assert poller.seconds_since_snapshot() < 1.0
    assert poller.is_stale(5.0) is False


def test_publish_error_event_leaves_last_and_staleness_untouched():
    from quanquant.poller import QuoteEvent

    poller = QuotePoller(FakeSource(["1"]), "TXF", 0.01)
    poller.publish(QuoteEvent(snapshot=None, error="boom", at=_snap("1").fetched_at))
    assert poller.last is None
    assert poller.is_stale(0.0) is True  # no snapshot ever → still stale


@pytest.mark.asyncio
async def test_publish_fans_out_to_subscribers():
    from quanquant.poller import QuoteEvent

    poller = QuotePoller(FakeSource(["1"]), "TXF", 0.01)
    q = poller.subscribe()
    snap = _snap("18000")
    poller.publish(QuoteEvent(snapshot=snap, error=None, at=snap.fetched_at))
    event = await asyncio.wait_for(q.get(), 1)
    assert event.snapshot.price == Decimal("18000")
