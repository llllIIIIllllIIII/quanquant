"""Shared quote poller with pub/sub fan-out.

A single QuotePoller.run() task fetches snapshots on an interval and broadcasts
each result to every subscriber's queue. Both the CLI and the web server attach
as subscribers, so one upstream poll serves all consumers (and all browser tabs).
"""
import asyncio
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone

from quanquant.freshness import FreshnessTracker
from quanquant.models import FuturesSnapshot
from quanquant.sources.base import DataSource

_QUEUE_MAXSIZE = 100


@dataclass(frozen=True, slots=True)
class QuoteEvent:
    """One poll result: a snapshot on success, or an error message on failure."""

    snapshot: FuturesSnapshot | None
    error: str | None
    at: datetime  # UTC


class QuotePoller:
    """Polls a DataSource on an interval and fans results out to subscribers.

    Does NOT own the source lifecycle — the caller is responsible for opening and
    closing it (e.g. `async with source:`), so the same source can be shared.
    """

    def __init__(self, source: DataSource, symbol: str, interval: float) -> None:
        self._source = source
        self._symbol = symbol
        self._interval = interval
        self._subscribers: set[asyncio.Queue[QuoteEvent]] = set()
        self._last: FuturesSnapshot | None = None
        self._last_snapshot_at: float | None = None  # time.monotonic() of last snapshot
        self._freshness = FreshnessTracker()

    @property
    def symbol(self) -> str:
        """The single commodity this poller tracks (002：/quote 的 symbol-scoped 驗證
        依此判斷所選商品是否有報價來源)。"""
        return self._symbol

    @property
    def last(self) -> FuturesSnapshot | None:
        """The most recent successful snapshot, for late-joining subscribers."""
        return self._last

    @property
    def freshness(self) -> FreshnessTracker:
        """共用的新鮮度判斷器（供報價路由讀 last_advance_at）。"""
        return self._freshness

    def publish(self, event: QuoteEvent) -> None:
        """Record + fan out one event. The single entry point for every producer
        (this poller's own loop, the MIS fallback, and the Shioaji streamer)."""
        if event.snapshot is not None:
            snap = self._freshness.evaluate(event.snapshot)
            event = replace(event, snapshot=snap)
            self._last = snap
            self._last_snapshot_at = time.monotonic()
        self._broadcast(event)

    def seconds_since_snapshot(self) -> float:
        """Seconds since the last successful snapshot (inf if none yet)."""
        if self._last_snapshot_at is None:
            return float("inf")
        return time.monotonic() - self._last_snapshot_at

    def is_stale(self, threshold: float) -> bool:
        """True when no fresh snapshot has arrived within `threshold` seconds —
        used by the fallback poll to decide when to take over from the stream."""
        return self.seconds_since_snapshot() >= threshold

    def subscribe(self) -> asyncio.Queue[QuoteEvent]:
        queue: asyncio.Queue[QuoteEvent] = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[QuoteEvent]) -> None:
        self._subscribers.discard(queue)

    async def run(self) -> None:
        """Poll forever: fetch -> cache last -> broadcast -> sleep.

        Never stops on a fetch error (盤中不可停止更新); it broadcasts an error
        event and keeps polling. Cancellation propagates normally.
        """
        while True:
            try:
                snap = await self._source.fetch_snapshot(self._symbol)
                self.publish(QuoteEvent(snapshot=snap, error=None, at=snap.fetched_at))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # keep the loop alive on any fetch failure
                self.publish(
                    QuoteEvent(snapshot=None, error=str(exc), at=datetime.now(timezone.utc))
                )
            await asyncio.sleep(self._interval)

    def _broadcast(self, event: QuoteEvent) -> None:
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # Slow consumer: drop its oldest event so it always gets the latest.
                try:
                    queue.get_nowait()
                    queue.put_nowait(event)
                except (asyncio.QueueEmpty, asyncio.QueueFull):
                    pass
