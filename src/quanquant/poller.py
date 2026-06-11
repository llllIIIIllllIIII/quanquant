"""Shared quote poller with pub/sub fan-out.

A single QuotePoller.run() task fetches snapshots on an interval and broadcasts
each result to every subscriber's queue. Both the CLI and the web server attach
as subscribers, so one upstream poll serves all consumers (and all browser tabs).
"""
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

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

    @property
    def last(self) -> FuturesSnapshot | None:
        """The most recent successful snapshot, for late-joining subscribers."""
        return self._last

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
                self._last = snap
                self._broadcast(QuoteEvent(snapshot=snap, error=None, at=snap.fetched_at))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # keep the loop alive on any fetch failure
                self._broadcast(
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
