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
import logging
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

log = logging.getLogger(__name__)

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


async def run_health_lease_watchdog(
    registry: AgentRegistry, *, lease_seconds: float, interval: float = 5.0,
) -> None:
    """Inc1 D9/G2③（Task 12）：server 端 heartbeat lease——WS 連線存活不等於健康。這是
    per-registry 全站唯一一份的背景 task（不是 per-slot），每輪掃過全部 slot；斷線本身已由
    `agent_ws.py` 的 `finally` 立即 `mark_disabled`（更即時），這裡只補「連線仍開著、但本連線
    的健康回報（`status="ok"`）已經斷流超過 `lease_seconds`」這一種情境——只在該 slot **目前
    是 ready** 時才出手（`session_state.ready` 本就是 place/cancel/update route 的擋新單
    依據，見 D9 admission gate），避免對本來就還在 pending_health／已離線的 slot 重複標記或
    覆寫更明確的既有錯誤訊息。`channel.last_ok_heartbeat` 只在 `AgentChannel.note_health`
    接受一則 `status="ok"` 時才更新，未曾收過任何 ok（如剛登入、尚在 pending_health）時為
    `None`，此時 lease 判定天然不適用（`ready` 本就還是 False，不需要這裡插手）。"""
    while True:
        await asyncio.sleep(interval)
        now = time.monotonic()
        for slot in registry.slots():
            channel = slot.channel
            last_ok = getattr(channel, "last_ok_heartbeat", None)
            if last_ok is None:
                continue
            if slot.session_state.ready and (now - last_ok) > lease_seconds:
                log.warning(
                    "agent health lease 過期，user_id=%s 標 not-ready（超過 %.0f 秒未收到 "
                    "UpHealth(ok)）", slot.user_id, lease_seconds,
                )
                slot.session_state.mark_unhealthy("健康回報逾時（lease 過期）")
                # C2（HIGH，codex 終審）：`session_state`（UI/badge 用）過去是唯一被更新的
                # readiness 訊號——`channel.admission_ready`（`AgentNativeGateway.
                # admission_ready` 委派讀這個，是 place/update admission 檢查的唯一依據）
                # 並不受影響，導致 lease 過期後 HTTP 下單路徑仍會誤判健康、照常建
                # Order/reservation/ledger。這裡同步 invalidate channel 的健康旗標，讓兩個
                # readiness 訊號一致收斂（`channel.ready`——連線存活＋已登入的寬鬆定義，供
                # reconcile/query_qty 用——刻意不受影響，lease 過期不等於斷線）。
                mark_lease_expired = getattr(channel, "mark_lease_expired", None)
                if mark_lease_expired is not None:
                    mark_lease_expired()
