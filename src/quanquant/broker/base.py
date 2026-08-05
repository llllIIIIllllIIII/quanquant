"""Broker 無關的下單服務介面與例外。"""
from collections.abc import Callable
from typing import Protocol, runtime_checkable

from quanquant.broker.types import Fill, Mode, OrderAck, OrderRequest, Position


class OrderError(Exception):
    """下單/改單/刪單失敗（券商拒單、連線異常、狀態不明等）。"""


class TradeNotFoundError(OrderError):
    """cancel/update 時在券商 list_trades 找不到對應委託（可能已終結或不存在）。"""

    def __init__(self, ordno: str | None = None) -> None:
        super().__init__(
            f"找不到券商對應委託（ordno={ordno!r}），可能已成交/已刪除/跨日，"
            "拒絕在無法確認對應委託的情況下送出改單"
        )
        self.ordno = ordno


class RiskError(Exception):
    """風控攔截（超限、非白名單、kill switch、缺/錯確認 token 等）。

    needs_confirm：True 時代表唯一的攔截原因是「real 缺/錯兩階段確認 token」——
    Task 9 UI 用這個旗標決定要彈確認框還是顯示一般錯誤（其餘原因一律 False）。
    """

    def __init__(self, message: str, *, needs_confirm: bool = False) -> None:
        super().__init__(message)
        self.needs_confirm = needs_confirm


class AuthorizationError(Exception):
    """owner allowlist / 委託所有權驗證失敗（router 對應 403）。"""


class AgentUnavailableError(OrderError):
    """指令送出前 agent 即不在線——保證未送達券商，可安全判 failed。"""


class AgentCommandTimeoutError(OrderError):
    """指令可能已送達 agent/券商但未收到 ack——必須保守判 unknown。"""


@runtime_checkable
class OrderService(Protocol):
    mode: Mode  # server-side 真實 session mode；place/update 產生的 Order/Fill/Trade 一律蓋此值

    async def place(
        self, req: OrderRequest, *, actor_user_id: int, confirm_token: str | None = None
    ) -> OrderAck: ...

    async def cancel(self, broker_order_id: str, *, actor_user_id: int) -> OrderAck: ...

    async def update(
        self,
        broker_order_id: str,
        *,
        actor_user_id: int,
        price=None,
        qty=None,
        confirm_token: str | None = None,
    ) -> OrderAck: ...

    async def positions(self, *, actor_user_id: int) -> list[Position]: ...

    def positions_snapshot(self, *, actor_user_id: int) -> list[Position]:
        """輪詢/唯讀路徑用的無鎖同步部位快照：只讀 DB（committed rows，WAL 下一致快照），
        不搶 broker 序列化鎖、可在 threadpool（同步 def 路由）跑，完全離開 event loop。
        所有權檢查與 `positions()` 相同。"""
        ...

    def on_fill(self, handler: Callable[[Fill], None]) -> None: ...
