"""Per-key（IP）token bucket 限流（spec §4.3）——供 `routers/agent_device.py` 的 device-code
發起端擋暴衝請求。純粹的請求頻率節流，不是持久上限；device-code 的持久上限（同 IP／全域
同時 pending 幾筆）由 `auth/device_flow.py` 的 `count_active_pending_for_ip`/
`count_active_pending_global` 查 DB 負責，兩者互補、不重疊。"""
import time


class TokenBucket:
    """每 key（IP）一個 bucket；純記憶體，重啟歸零可接受（頻率限制而非持久計數，持久
    上限由 DB active pending 計數負責，見 device_flow.py）。"""

    def __init__(self, *, rate_per_minute: int, burst: int) -> None:
        self._rate = rate_per_minute
        self._burst = burst
        self._state: dict[str, tuple[float, float]] = {}  # key -> (tokens, last_refill_ts)

    def allow(self, key: str) -> bool:
        """True＝放行並消耗一個 token；False＝超限。取用前先按經過時間（monotonic）補充
        tokens（封頂 burst）。無鎖：FastAPI async 路由在同一 event loop 內循序執行到
        `await` 點前不會交錯，本方法內沒有 `await`，天然原子。"""
        now = time.monotonic()
        tokens, last_ts = self._state.get(key, (float(self._burst), now))
        elapsed = now - last_ts
        tokens = min(self._burst, tokens + elapsed * self._rate / 60.0)
        if tokens >= 1:
            self._state[key] = (tokens - 1, now)
            return True
        self._state[key] = (tokens, now)
        return False
