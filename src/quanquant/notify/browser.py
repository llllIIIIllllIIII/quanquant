"""In-memory pub/sub for browser SSE alert toasts (same shape as QuotePoller)."""
import asyncio
import json

from quanquant.notify.base import Notification

_QUEUE_MAXSIZE = 50


class BrowserNotifier:
    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[str]] = set()

    def subscribe(self) -> asyncio.Queue[str]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[str]) -> None:
        self._subscribers.discard(queue)

    async def send(self, n: Notification) -> None:
        payload = json.dumps({
            "body": n.body, "condition": n.condition, "symbol": n.symbol,
            "tf": n.timeframe, "alertId": n.alert_id, "barTs": n.bar_ts,
        })
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:  # slow consumer: drop oldest
                try:
                    queue.get_nowait()
                    queue.put_nowait(payload)
                except (asyncio.QueueEmpty, asyncio.QueueFull):
                    pass
