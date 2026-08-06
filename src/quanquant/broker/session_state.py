"""下單子系統目前狀態（Task 8）；/healthz 與 watchdog 共用同一個 instance
（app.state.order_session_state），供 readiness gate 判斷 service 是否可用、供維運看
reconnect_attempts/last_error 了解目前狀況。"""
from datetime import datetime, timezone


class OrderSessionState:
    def __init__(self) -> None:
        self.ready = False
        # disabled=True 代表「刻意不啟用下單子系統」（缺金鑰/owner/CA 的本機或唯讀部署），
        # 與 disabled=False 的 unhealthy（connect 失敗/設定錯，屬真故障）語意不同：/healthz
        # 只對後者回非 200，前者仍算健康（見 web/routers/health.py）——避免把「刻意當純看盤
        # 站跑、沒配下單」的正常部署誤判成故障、在無 failover 下被 Caddy 拉掉。
        self.disabled = False
        self.last_error: str | None = None
        self.last_connected_at: datetime | None = None
        self.reconnect_attempts = 0

    def mark_ready(self) -> None:
        self.ready = True
        self.disabled = False
        self.last_error = None
        self.last_connected_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self.reconnect_attempts = 0

    def mark_unhealthy(self, error: str) -> None:
        self.ready = False
        self.disabled = False
        self.last_error = error

    def mark_disabled(self, reason: str) -> None:
        """刻意停用（非故障）：/healthz 視為健康（200）。reason 存進 last_error 供回顯。"""
        self.ready = False
        self.disabled = True
        self.last_error = reason
