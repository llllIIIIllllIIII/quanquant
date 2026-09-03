"""OrderEventHub：委託/成交/部位「有變動」的記憶體內 pub/sub ping（形狀比照
`quanquant.notify.browser.BrowserNotifier` 與 `poller._broadcast`）。

用途：取代下單頁每 2s 的盲輪詢。RawInboxWorker 把非同步成交/委託回報落地後 publish()
一個**不帶資料的訊號**；`/orders/stream` SSE 端點收到就 yield 一個 `orders-changed` 事件，
瀏覽器據此重抓自己 user-scoped 的委託/部位（`/orders/list`、`/orders/positions` 本就以
`user_id` 過濾並驗所有權）。

刻意設計成「無資料廣播 ping」：ping 不帶任何 per-user 內容，故單一 hub 廣播給所有連線的
瀏覽器也**不會跨用戶洩漏**——每個瀏覽器只會抓到自己的資料。這與既有的 `refreshorders`
HX-Trigger（同樣是無資料的「有東西變了」訊號）語意一致。`subscribe()`/`unsubscribe()`/
`publish()` 三者的既有行為（無參數訂閱、廣播給所有人、item 恆為字串 "1"）**逐位元保留**
——見 `tests/test_order_events.py` 開頭三個既有測試。

007（批次 B-1，2026-09）：新增 `publish_deal`/`publish_order_report` 兩個 **user-scoped**
帶 payload 事件——委託反饋橫幅需要看得懂內容（商品/方向/口數/價格/狀態），不能只靠一個裸
ping 要瀏覽器自己猜。這兩個方法只送給 `subscribe(user_id=...)` 時登記的那個 user（不廣播，
不能讓別的使用者收到別人的下單內容），呼叫端是 RawInboxWorker 落地 Deal／委託回報時
（見 broker/inbox_worker.py `_process_deal`/`_process_order_report`，經
`_flush_pending_events()` 只在 event loop 執行緒上呼叫本模組——**本模組所有方法都只能
在 event loop 執行緒上呼叫**，`asyncio.Queue` 不是 thread-safe，見 CRITICAL-1 修復
記錄）。同一顆 queue 兩種 item 並存：無資料 ping 是裸字串 "1"（既有語意不變）；scoped
事件是 `{"event": <str>, "payload": <dict>}`——`/orders/stream`（web/routers/orders.py
`orders_stream`）依 item 型別分派成對應的 SSE `event:` 名稱。

MEDIUM-4（fresh-context opus 終審修復，2026-09）：ping 與 scoped 事件共用同一顆
`maxsize=8` 的 queue——若原本「queue 滿了就丟掉這次要放的新項」（drop-newest）套用在
ping 上，一次多口市價單分批成交（連續多筆 `publish_deal`）灌爆 queue 後，接在後面的
`publish()` ping 會被排擠到永遠送不進去，委託/成交/部位三個靠 ping 刷新的區塊會整批卡住
（不只是少一條 banner）。改成 drop-oldest（`_put_evicting_oldest`）：queue 滿了先丟最舊
的一項騰位，新項一定放得進去——ping 與 scoped 事件都適用這個策略（ping 本來就是
coalesce 語意「收到至少一次即可」；scoped 事件優先保留較新的內容，對「已經過期的舊
成交提示」價值也較低）。"""
import asyncio

_QUEUE_MAXSIZE = 8


class OrderEventHub:
    def __init__(self) -> None:
        # queue -> owner user_id；None＝未指定 owner（既有 `subscribe()` 無參數呼叫端/
        # 測試的相容值）——這種訂閱者只收得到既有無資料廣播 ping，收不到任何 scoped 事件
        # （`_publish_scoped` 用 `!=` 比對，None 永遠不等於任何真實 user_id）。
        self._subscribers: dict[asyncio.Queue, int | None] = {}

    def subscribe(self, user_id: int | None = None) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
        self._subscribers[queue] = user_id
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self._subscribers.pop(queue, None)

    def publish(self) -> None:
        """Ping 每個訂閱者（廣播，含未指定 user_id 的）。只能在 event loop 執行緒上呼叫
        （見本檔頂部 docstring 的 CRITICAL-1 說明）——`RawInboxWorker._flush_pending_
        events()`/`run()` 保證這一點，`asyncio.to_thread` 回來後、已在 event loop
        執行緒上才呼叫本方法，不需 await、不需 `call_soon_threadsafe`。

        MEDIUM-4：queue 滿了改成丟最舊的一項騰位（見 `_put_evicting_oldest`），不是單純
        丟棄這次的 ping——否則 ping 會被同一顆 queue 上灌爆的 scoped 事件排擠到永遠送不
        出去（見本檔頂部 docstring）。"""
        for queue in list(self._subscribers):
            self._put_evicting_oldest(queue, "1")

    def publish_deal(self, *, user_id: int, payload: dict) -> None:
        """007：逐筆成交回報事件——呼叫端一筆 `Deal` 落地一次呼叫（滑價一價一橫幅天然
        成立，不聚合），只送給這筆成交的 owner。只能在 event loop 執行緒上呼叫（同
        `publish()`）。"""
        self._publish_scoped(user_id=user_id, event="deal", payload=payload)

    def publish_order_report(self, *, user_id: int, payload: dict) -> None:
        """007：委託回報（狀態變化）事件——只送給這張委託的 owner，不廣播。只能在
        event loop 執行緒上呼叫（同 `publish()`）。"""
        self._publish_scoped(user_id=user_id, event="order-report", payload=payload)

    def _publish_scoped(self, *, user_id: int, event: str, payload: dict) -> None:
        """user-scoped：只送給 `subscribe(user_id=user_id)` 登記的那些 queue（同一 user
        可能開多個分頁/連線，全部都要收到）。同 `publish()`，non-blocking，queue 滿了丟
        最舊一項騰位（MEDIUM-4），絕不因為某個訂閱者 queue 滿了就反噬整批發布。"""
        for queue, owner in list(self._subscribers.items()):
            if owner != user_id:
                continue
            self._put_evicting_oldest(queue, {"event": event, "payload": payload})

    @staticmethod
    def _put_evicting_oldest(queue: asyncio.Queue, item) -> None:
        """MEDIUM-4：queue 滿了不是靜默丟掉這次要放的新項（那個舊策略對 ping 是災難，見
        本檔頂部 docstring），改成先丟最舊的一項（`get_nowait`）騰位，新項幾乎必定放得
        進去。只能在 event loop 執行緒上呼叫——單一協作式執行緒下，`get_nowait()` 與
        `put_nowait()` 之間沒有任何 `await`，不會被其他協程插隊搶先消費/放入，不需要
        額外的鎖。三層 try/except 皆是防禦性：`QueueFull`（第一次嘗試放不進去，正常會
        發生的路徑）、`QueueEmpty`（理論上不會發生：滿的 queue 不可能同時是空的，除非有
        並發消費者搶跑，違反上述單執行緒假設）、最後一次 `QueueFull`（理論上不會發生，
        剛騰出一格）——後兩者防禦性放棄、不得反噬呼叫端，不算功能缺陷。"""
        try:
            queue.put_nowait(item)
            return
        except asyncio.QueueFull:
            pass
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull:
            pass
