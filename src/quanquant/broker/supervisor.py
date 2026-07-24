"""單一序列化通道（V3-2，修 BLOCKER#11/#14）。

任何會讀寫 BrokerPosition/QuotaReservation/native shioaji API 的路徑
（place/update/cancel/positions/close/connect/reconnect/RawInboxWorker 處理批次/watchdog
reconcile）必須先 `async with supervisor.lock:` 才能動作。這是全計畫唯一的序列化保證來源
——取代「單一 uvicorn worker」這個在背景執行緒/watchdog 協程下不成立的假設。

刻意極簡：只是一個共用的 asyncio.Lock，紀律（誰都要先拿鎖）靠 Global Constraints 與
code review 把關，不是靠這個類別本身的複雜度。Task 5 只消費它（RawInboxWorker.run），
Task 6 的 ShioajiAdapter native 呼叫與 Task 8 的 watchdog reconcile 之後共用同一個 instance。
"""
import asyncio


class BrokerSupervisor:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
