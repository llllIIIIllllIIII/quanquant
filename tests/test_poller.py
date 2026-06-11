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
