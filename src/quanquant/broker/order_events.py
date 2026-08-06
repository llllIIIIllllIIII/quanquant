"""OrderEventHub：委託/成交/部位「有變動」的記憶體內 pub/sub ping（形狀比照
`quanquant.notify.browser.BrowserNotifier` 與 `poller._broadcast`）。

用途：取代下單頁每 2s 的盲輪詢。RawInboxWorker 把非同步成交/委託回報落地後 publish()
一個**不帶資料的訊號**；`/orders/stream` SSE 端點收到就 yield 一個 `orders-changed` 事件，
瀏覽器據此重抓自己 user-scoped 的委託/部位（`/orders/list`、`/orders/positions` 本就以
`user_id` 過濾並驗所有權）。

刻意設計成「無資料廣播 ping」：ping 不帶任何 per-user 內容，故單一 hub 廣播給所有連線的
瀏覽器也**不會跨用戶洩漏**——每個瀏覽器只會抓到自己的資料。這與既有的 `refreshorders`
HX-Trigger（同樣是無資料的「有東西變了」訊號）語意一致。
"""
import asyncio

_QUEUE_MAXSIZE = 8


class OrderEventHub:
    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[str]] = set()

    def subscribe(self) -> asyncio.Queue[str]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[str]) -> None:
        self._subscribers.discard(queue)

    def publish(self) -> None:
        """Ping 每個訂閱者。同步、非阻塞——可從 RawInboxWorker 的 loop 端協程（`asyncio.
        to_thread` 回來後、已在 event loop 執行緒上）直接呼叫，不需 await、不需
        `call_soon_threadsafe`。QueueFull 代表已有 ping 待處理，coalesce 丟棄無妨
        （裸訊號，瀏覽器只要收到「至少一次」就會重抓最新狀態）。"""
        for queue in list(self._subscribers):
            try:
                queue.put_nowait("1")
            except asyncio.QueueFull:
                pass
