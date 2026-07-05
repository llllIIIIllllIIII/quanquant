"""In-memory pub/sub for browser SSE alert toasts (same shape as QuotePoller)."""
import asyncio
import json

from quanquant.notify.base import Notification

_QUEUE_MAXSIZE = 50


class BrowserNotifier:
    def __init__(self) -> None:
        self._subscribers: dict[asyncio.Queue[str], int | None] = {}

    def subscribe(self, user_id: int | None = None) -> asyncio.Queue[str]:
        """user_id=None subscribes to everything (tests/system streams); a real
        id only receives that user's notifications plus ownerless broadcasts."""
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
        self._subscribers[queue] = user_id
        return queue

    def unsubscribe(self, queue: asyncio.Queue[str]) -> None:
        self._subscribers.pop(queue, None)

    async def send(self, n: Notification) -> None:
        payload = json.dumps({
            "body": n.body, "condition": n.condition, "symbol": n.symbol,
            "tf": n.timeframe, "alertId": n.alert_id, "barTs": n.bar_ts,
        })
        for queue, uid in list(self._subscribers.items()):
            if n.user_id is not None and uid is not None and uid != n.user_id:
                continue  # someone else's alert
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:  # slow consumer: drop oldest
                try:
                    queue.get_nowait()
                    queue.put_nowait(payload)
                except (asyncio.QueueEmpty, asyncio.QueueFull):
                    pass
