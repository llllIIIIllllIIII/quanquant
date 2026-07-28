"""OrderEventHub 廣播 + RawInboxWorker 成交落地後發布 SSE ping（取代盲輪詢）。

hub 方法皆同步，但 asyncio.Queue 在 3.11 建構時會抓 event loop——故所有測試包在
asyncio.run() 內（有 running loop），不依賴 pytest-asyncio 設定。
"""
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
