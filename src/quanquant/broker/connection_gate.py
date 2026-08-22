"""手動斷線（self-service）的 in-memory 連線封鎖集合（2026-08-22）。

下單頁「斷開 Agent」按鈕（D9）＝自願暫停：`block()` 該 user → agent WS 端點拒絕其重連
（`web/routers/agent_ws.py` 的連線 gate），使用者在下單頁按「允許 Agent 重連」→ `allow()`。
純 in-memory、重啟即清——手動斷線是自願暫停、非鎖定，重啟本就代表使用者已重新掌控，語意相容。

**與冷靜期分離**：冷靜期（真正的自我禁制）的封鎖是 DB 判定（`repository.active_cooldown`，
持久化、admin-only 解除），不走這裡。WS 連線 gate＝`is_blocked(user)`（本集合）OR
active cooldown（DB）；兩者任一命中即拒絕連線。跨 user 無共享狀態，單一 event loop 內
存取，不需鎖。
"""


class AgentConnectionGate:
    def __init__(self) -> None:
        self._blocked: set[int] = set()

    def block(self, user_id: int) -> None:
        """封鎖該 user 的 agent 連線（手動斷線）。冪等。"""
        self._blocked.add(user_id)

    def allow(self, user_id: int) -> None:
        """解除封鎖（下單頁「允許 Agent 重連」）。冪等（未封鎖時 no-op）。"""
        self._blocked.discard(user_id)

    def is_blocked(self, user_id: int) -> bool:
        return user_id in self._blocked
