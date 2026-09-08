"""OrderEventHub 廣播 + RawInboxWorker 成交落地後發布 SSE ping（取代盲輪詢）。

hub 方法皆同步，但 asyncio.Queue 在 3.11 建構時會抓 event loop——故所有測試包在
asyncio.run() 內（有 running loop），不依賴 pytest-asyncio 設定。

007（批次 B-1）：`publish_deal`/`publish_order_report` 是新增的 **user-scoped** 帶
payload 事件，只送給 `subscribe(user_id=...)` 時指定的那個 user；既有 `subscribe()`
（無參數）＋`publish()`（無資料 ping，item 恆為字串 "1"）語意完全不變——下面前三個既有
測試（`test_publish_pings_all_subscribers` 等）逐字保留，作為「既有訂閱行為不得改變」
的迴歸證據（`test_publish_coalesces_when_queue_full_without_raising` 的 `qsize()==8`
斷言在 MEDIUM-4 的 drop-oldest 新策略下依然成立，不需要改寫）。

MEDIUM-4（fresh-context opus 終審修復，2026-09）：queue 滿了的處理策略從「丟棄這次要放
的新項」改成「丟最舊的一項騰位」——否則 ping 會被同一顆 queue 上灌爆的 scoped 事件排擠到
永遠送不出去。"""
import asyncio

from quanquant.broker.inbox_worker import RawInboxWorker
from quanquant.broker.order_events import OrderEventHub
from quanquant.broker.supervisor import BrokerSupervisor


def test_publish_pings_all_subscribers():
    async def _run():
        hub = OrderEventHub()
        q1 = hub.subscribe()
        q2 = hub.subscribe()
        hub.publish()
        assert q1.get_nowait() == "1"
        assert q2.get_nowait() == "1"

    asyncio.run(_run())


def test_unsubscribe_stops_pings():
    async def _run():
        hub = OrderEventHub()
        q = hub.subscribe()
        hub.unsubscribe(q)
        hub.publish()
        assert q.empty()

    asyncio.run(_run())


def test_publish_coalesces_when_queue_full_without_raising():
    async def _run():
        hub = OrderEventHub()
        q = hub.subscribe()
        for _ in range(50):  # 遠超過 maxsize=8
            hub.publish()  # 不得拋 QueueFull
        assert q.qsize() == 8  # 填滿後多餘 ping 被 coalesce 丟棄

    asyncio.run(_run())


def _worker_with_stubbed_batch(*, order_events, returns):
    """建一個最小 worker，把 process_batch_once 換成回傳固定值並在跑完一輪後 set stop
    的 stub（run() 的迴圈只跑一次），用來單測「handled>0 才 publish」這條發布接線。"""
    worker = RawInboxWorker(
        session_factory=lambda: None, supervisor=BrokerSupervisor(),
        deal_mapper=lambda p: None, order_report_mapper=lambda p: None,
        order_events=order_events,
    )

    def _fake_batch():
        worker._stop.set()  # 跑完這一輪就停（Event.set 執行緒安全，在 to_thread 內呼叫 OK）
        return returns

    worker.process_batch_once = _fake_batch
    return worker


def test_worker_publishes_when_batch_handled():
    async def _run():
        published = []
        hub = type("_Hub", (), {"publish": lambda self: published.append(1)})()
        worker = _worker_with_stubbed_batch(order_events=hub, returns=1)  # 有處理到
        await worker.run()
        assert published == [1]

    asyncio.run(_run())


def test_worker_does_not_publish_when_nothing_handled():
    async def _run():
        published = []
        hub = type("_Hub", (), {"publish": lambda self: published.append(1)})()
        worker = _worker_with_stubbed_batch(order_events=hub, returns=0)  # 沒處理到
        await worker.run()
        assert published == []

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# 007（批次 B-1）：user-scoped 帶 payload 事件——委託回報／逐筆成交回報，只送給 owner。
# ---------------------------------------------------------------------------

def test_subscribe_still_defaults_to_no_owner_and_only_gets_broadcast_pings():
    """`subscribe()` 不帶參數＝沒有 owner（既有呼叫端/測試相容值），只收得到既有無資料
    廣播 ping，收不到任何 user-scoped 事件——新增的 scoped 事件不會意外洩漏給它。"""
    async def _run():
        hub = OrderEventHub()
        broadcast_q = hub.subscribe()
        hub.publish()
        hub.publish_deal(user_id=1, payload={"symbol": "TXF"})
        assert broadcast_q.get_nowait() == "1"
        assert broadcast_q.empty()  # scoped 事件沒有進來

    asyncio.run(_run())


def test_publish_deal_reaches_only_the_matching_owner_subscriber():
    async def _run():
        hub = OrderEventHub()
        owner_q = hub.subscribe(user_id=1)
        other_q = hub.subscribe(user_id=2)
        hub.publish_deal(user_id=1, payload={"symbol": "TXF", "action": "Buy", "qty": 1, "price": "18000"})
        item = owner_q.get_nowait()
        assert item == {
            "event": "deal",
            "payload": {"symbol": "TXF", "action": "Buy", "qty": 1, "price": "18000"},
        }
        assert other_q.empty()  # 非 owner 收不到（③）

    asyncio.run(_run())


def test_publish_order_report_reaches_only_the_matching_owner_subscriber():
    async def _run():
        hub = OrderEventHub()
        owner_q = hub.subscribe(user_id=7)
        other_q = hub.subscribe(user_id=8)
        hub.publish_order_report(user_id=7, payload={"symbol": "TXF", "status": "cancelled"})
        item = owner_q.get_nowait()
        assert item == {"event": "order-report", "payload": {"symbol": "TXF", "status": "cancelled"}}
        assert other_q.empty()  # 非 owner 收不到（③）

    asyncio.run(_run())


def test_scoped_publish_evicts_oldest_when_queue_full_instead_of_dropping_newest():
    """MEDIUM-4（fresh-context opus 終審修復，2026-09）：queue 滿了不再是「丟棄這次要放的
    新項」（那個舊策略會讓後面接著送的 `publish()` ping 被同一顆 queue 上的 scoped 事件
    排擠到永遠送不進去，見 `test_ping_still_delivered_after_scoped_event_burst_fills_
    queue`）——改成丟最舊的一項騰位，較新的事件永遠放得進去。一次多口市價單滑價分批成交
    （如 50 個 fill）不會讓 queue 卡死在最早的 8 筆，保留的是最新 8 筆。"""
    async def _run():
        hub = OrderEventHub()
        q = hub.subscribe(user_id=1)
        for i in range(50):  # 遠超過 maxsize=8
            hub.publish_deal(user_id=1, payload={"i": i})  # 不得拋 QueueFull
        assert q.qsize() == 8
        remaining = [q.get_nowait()["payload"]["i"] for _ in range(8)]
        assert remaining == list(range(42, 50))  # 保留最新 8 筆（42..49），不是最舊的 0..7

    asyncio.run(_run())


def test_ping_still_delivered_after_scoped_event_burst_fills_queue():
    """MEDIUM-4：一口多筆市價單滑價分批成交（連續多筆 `publish_deal`）灌爆 8 格 queue
    後，既有 `orders-changed` 廣播 ping（`publish()`）仍然送得到，不會被排擠到永遠進不
    去——否則委託/成交/部位三個靠 ping 刷新的區塊會整批卡住不動（比丟一兩條 banner嚴重
    得多）。這正是 e952ada 修過的「多口市價單 partfilled」情境會踩到的量級。"""
    async def _run():
        hub = OrderEventHub()
        q = hub.subscribe(user_id=1)
        for i in range(20):  # 一口多筆市價單滑價分批成交的典型情境
            hub.publish_deal(user_id=1, payload={"i": i})
        assert q.qsize() == 8  # queue 已灌滿

        hub.publish()  # ping 必須擠得進去，不能被既有 8 筆 scoped 事件排擠掉

        items = []
        while not q.empty():
            items.append(q.get_nowait())
        assert "1" in items  # ping 確實送達（不是被丟棄）

    asyncio.run(_run())


def test_ping_published_before_burst_gets_evicted():
    """反向測試，釘死 inbox_worker.run() 的「先 flush scoped、後發 ping」順序是 load-bearing：
    drop-oldest 之下，ping 若「先」入列、再被 >8 筆 scoped 洪峰蓋過，就會被驅逐——
    這正是把 run() 裡 `_flush_pending_events()` 與 `publish()` 兩行對調後的實際後果。
    此測試綠＝驅逐語意如預期；若未來有人改動入列順序或驅逐策略，配合
    test_ping_still_delivered_after_scoped_event_burst_fills_queue 兩相對照即可定位。"""
    async def _run():
        hub = OrderEventHub()
        q = hub.subscribe(user_id=1)
        hub.publish()  # ping 先入列（模擬順序被對調的錯誤世界）
        for i in range(20):
            hub.publish_deal(user_id=1, payload={"i": i})

        items = []
        while not q.empty():
            items.append(q.get_nowait())
        assert "1" not in items  # ping 被 drop-oldest 驅逐——所以 run() 必須讓 ping 最後入列

    asyncio.run(_run())
