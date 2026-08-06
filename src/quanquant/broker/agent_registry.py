"""Inc1 D1/D9 骨架：per-user 隔離單位。

`UserAgentSlot` 打包一個 owner 專屬的 agent 通道 runtime（channel/gateway/adapter/
session_state/supervisor/背景 task）；`AgentRegistry` 是 `user_id -> UserAgentSlot` 的
查詢介面。lifespan（`web/app.py::_start_agent_channel_subsystem`）在啟動時對 owner 白名單
逐一 eager 建好全部 slot（D1：owner 集合小且固定，改名單本就要重啟，不做 lazy 建置/熱載）；
建置完成後 routes／WS handler／watchdog 只呼叫 `get()`/`slots()` 唯讀查詢。

跨 user 完全無共享可變 runtime 狀態（除全域 `RiskGuard`/`OrderEventHub`/DB，見 spec §3
架構總覽與 D3）：任一 user 的 offline/quarantine/慢操作不影響其他 user（I8）——每個 slot
各自一份 `BrokerSupervisor` 鎖，A 的 reconcile 卡住只會佔住 A 自己的鎖，不影響 B 的 place。
"""
import asyncio
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from quanquant.broker.agent_channel import AgentChannel, AgentNativeGateway
    from quanquant.broker.session_state import OrderSessionState
    from quanquant.broker.shioaji_adapter import ShioajiAdapter
    from quanquant.broker.supervisor import BrokerSupervisor


@dataclass
class UserAgentSlot:
    """一個 owner 的完整 agent 通道 runtime。`tasks` 目前只掛 per-slot RawInboxWorker／
    agent watchdog 兩個背景 task（Task 7 骨架）；watchdog 任務本體的 G3 unknown 收斂邏輯
    留給 Task 11/12 填內容，這裡只提供掛載點。"""

    user_id: int
    channel: "AgentChannel"
    gateway: "AgentNativeGateway"
    adapter: "ShioajiAdapter"
    session_state: "OrderSessionState"
    supervisor: "BrokerSupervisor"
    tasks: list[asyncio.Task] = field(default_factory=list)


class AgentRegistry:
    """建置期（lifespan）呼叫 `add()` 逐一塞入 eager 建好的 slot；執行期（routes/WS
    handler/watchdog）只呼叫 `get()`/`slots()` 唯讀查詢，不支援執行期新增/移除
    （owner 名單改動本就需要重啟，見 spec D1 理由，Inc1 不做熱載）。"""

    def __init__(self) -> None:
        self._slots: dict[int, UserAgentSlot] = {}

    def add(self, slot: UserAgentSlot) -> None:
        self._slots[slot.user_id] = slot

    def get(self, user_id: int) -> UserAgentSlot | None:
        return self._slots.get(user_id)

    def slots(self) -> Iterable[UserAgentSlot]:
        return self._slots.values()
