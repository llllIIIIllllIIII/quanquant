"""下單子系統目前狀態（Task 8）；/healthz 與 watchdog 共用同一個 instance
（app.state.order_session_state），供 readiness gate 判斷 service 是否可用、供維運看
reconnect_attempts/last_error 了解目前狀況。"""
from datetime import datetime, timezone


class OrderSessionState:
    def __init__(self) -> None:
        self.ready = False
        self.last_error: str | None = None
        self.last_connected_at: datetime | None = None
        self.reconnect_attempts = 0

    def mark_ready(self) -> None:
        self.ready = True
        self.last_error = None
        self.last_connected_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self.reconnect_attempts = 0

    def mark_unhealthy(self, error: str) -> None:
        self.ready = False
        self.last_error = error
