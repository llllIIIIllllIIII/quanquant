"""The MIS fallback poll must defer to a healthy stream and take over when it
goes silent — verified offline against a fake source (no network)."""
import asyncio
from decimal import Decimal

import pytest

from quanquant.poller import QuoteEvent, QuotePoller
from quanquant.web.app import _fallback_poll_loop
from tests.test_poller import FakeSource, _snap


@pytest.mark.asyncio
async def test_fallback_stays_silent_while_stream_is_fresh():
    src = FakeSource(["18000"])
    poller = QuotePoller(src, "TXF", 0.01)
    # Simulate a Shioaji tick just arriving → hub is fresh.
    poller.publish(QuoteEvent(snapshot=_snap("99999"), error=None, at=_snap("99999").fetched_at))

    task = asyncio.create_task(_fallback_poll_loop(poller, src, "TXF", 0.01, stale_threshold=5.0))
    await asyncio.sleep(0.05)  # many loop iterations
    task.cancel()

    assert src._i == 0  # fallback never fetched — stream owned the bus
    assert poller.last.price == Decimal("99999")


@pytest.mark.asyncio
async def test_fallback_takes_over_when_stream_never_arrives():
    src = FakeSource(["18000"])
    poller = QuotePoller(src, "TXF", 0.01)  # no snapshot ever → stale from the start

    task = asyncio.create_task(_fallback_poll_loop(poller, src, "TXF", 0.01, stale_threshold=5.0))
    try:
        for _ in range(200):
            if poller.last is not None:
                break
            await asyncio.sleep(0.01)
    finally:
        task.cancel()

    assert poller.last is not None and poller.last.price == Decimal("18000")
