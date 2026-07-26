"""單一序列化通道（V3-2，修 BLOCKER#11/#14；round3 #11 補 command executor）。

任何會讀寫 BrokerPosition/QuotaReservation/native shioaji API 的路徑
（place/update/cancel/positions/close/connect/reconnect/RawInboxWorker 處理批次/watchdog
reconcile）必須先 `async with supervisor.lock:` 才能動作。這是全計畫唯一的序列化保證來源
——取代「單一 uvicorn worker」這個在背景執行緒/watchdog 協程下不成立的假設。

round3 #11：ShioajiAdapter 的 place/update/cancel/positions/connect/close 一律經
`run()` 這個 command executor 呼叫，不得各自 `async with supervisor.lock:`——否則
watchdog reconnect 與 place 可能交錯替換 `_api`（round3 覆核抓到的 BLOCKER）。`run()`
內部就是 `async with self.lock:`，與 Task 5 `RawInboxWorker` 既有的裸鎖用法共用同一顆
`asyncio.Lock`，兩種呼叫方式彼此互斥、不衝突——不破壞 Task 5 既有行為。

刻意極簡：只是一個共用的 asyncio.Lock（+ 一層薄薄的 run() 包裝），紀律（誰都要先拿鎖，
且一律走 run()）靠 Global Constraints 與 code review 把關，不是靠這個類別本身的複雜度。
"""
import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


class BrokerSupervisor:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()

    async def run(self, fn: Callable[[], "T | Awaitable[T]"]) -> "T":
        """command executor（round3 #11）：所有 native shioaji API 呼叫
        （connect/reconnect/close/place/update/cancel/positions）都必須經這裡呼叫，
        取代呼叫端各自 `async with supervisor.lock:`——確保 watchdog reconnect 不會與
        送單交錯替換 `_api`。

        `fn` 是零參數 callable：可以是同步 callable（例如 `lambda: self._positions_blocking(...)`，
        直接在鎖內同步執行，適合輕量 SQLite 讀寫，比照本專案 routes 用 sync def 的既有慣例）、
        或回傳 awaitable 的 callable（例如 `lambda: asyncio.to_thread(self._connect_blocking)`，
        或一個 async closure，用來包住「await send_gate 後再呼叫 native API」這種需要在鎖內
        依序執行多個 await 的邏輯）。兩種形式 `run()` 都會在釋放鎖之前把結果/例外處理完。
        """
        async with self.lock:
            result = fn()
            if inspect.isawaitable(result):
                result = await result
            return result
