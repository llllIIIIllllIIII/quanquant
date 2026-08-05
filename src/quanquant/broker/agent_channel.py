"""server 端 agent 連線態與指令多工。

不碰 WS 框架——由 web/routers/agent_ws.py 注入 send_json callable，
故可用純 asyncio 單元測試。上行解析後由端點呼叫 resolve_ack / mark_logged_in。
"""
import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime

from quanquant.broker.agent_protocol import (
    DownCancel, DownPlace, DownReconcile, DownUpdate, PlaceNative, UpCmdAck,
)
from quanquant.broker.base import (
    AgentCommandTimeoutError, AgentUnavailableError, OrderError, TradeNotFoundError,
)
from quanquant.broker.types import OrderRequest


class AgentChannel:
    def __init__(self) -> None:
        self._send: Callable[[dict], Awaitable[None]] | None = None
        self._pending: dict[str, asyncio.Future] = {}
        self.logged_in = False
        self.account = ""
        self.last_heartbeat: float | None = None

    @property
    def connected(self) -> bool:
        return self._send is not None

    @property
    def ready(self) -> bool:
        return self._send is not None and self.logged_in

    def attach(self, send_json: Callable[[dict], Awaitable[None]]) -> None:
        self._send = send_json

    def detach(self) -> None:
        self._send = None
        self.logged_in = False
        pending, self._pending = self._pending, {}
        for fut in pending.values():
            if not fut.done():
                fut.set_exception(AgentCommandTimeoutError("agent 連線中斷，指令結果未知"))

    def mark_logged_in(self, account: str) -> None:
        self.logged_in = True
        self.account = account

    def note_heartbeat(self) -> None:
        self.last_heartbeat = time.monotonic()

    def resolve_ack(self, ack: UpCmdAck) -> None:
        fut = self._pending.pop(ack.cmd_id, None)
        if fut is not None and not fut.done():
            fut.set_result(ack)

    async def request(self, cmd: dict, *, cmd_id: str, timeout: float) -> UpCmdAck:
        if not self.ready:
            raise AgentUnavailableError("agent 未連線或未登入")
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[cmd_id] = fut
        try:
            try:
                await self._send(cmd)
            except Exception as exc:  # socket 已壞：可能已部分送出 → 保守 unknown
                raise AgentCommandTimeoutError(f"下行送出失敗: {exc}") from exc
            try:
                return await asyncio.wait_for(fut, timeout)
            except TimeoutError as exc:
                raise AgentCommandTimeoutError(f"等待 cmd_ack 逾時（{timeout}s）") from exc
        finally:
            self._pending.pop(cmd_id, None)


class AgentNativeGateway:
    """把 adapter 的 native 需求翻成下行指令，把 cmd_ack 翻回結果或既有語意的例外。"""

    def __init__(self, channel: AgentChannel, *, timeout_seconds: float) -> None:
        self._channel = channel
        self._timeout = timeout_seconds

    @property
    def ready(self) -> bool:
        return self._channel.ready

    async def place(self, req: OrderRequest) -> dict:
        cmd = DownPlace(
            cmd_id=uuid.uuid4().hex, mode="sim",
            native=PlaceNative(action=req.action, price=str(req.price), qty=req.qty,
                               price_type=req.price_type, order_type=req.order_type,
                               octype=req.octype),
        )
        ack = await self._channel.request(cmd.model_dump(), cmd_id=cmd.cmd_id,
                                          timeout=self._timeout)
        result = self._unwrap(ack)
        return {"ordno": result.get("ordno"), "broker_order_id": result.get("broker_order_id")}

    async def cancel(self, ordno: str) -> None:
        cmd = DownCancel(cmd_id=uuid.uuid4().hex, mode="sim", ordno=ordno)
        self._unwrap(await self._channel.request(cmd.model_dump(), cmd_id=cmd.cmd_id,
                                                 timeout=self._timeout))

    async def update(self, ordno: str, *, price, qty: int, price_type: str | None = None) -> None:
        cmd = DownUpdate(cmd_id=uuid.uuid4().hex, mode="sim", ordno=ordno,
                         price=(str(price) if price is not None else None),
                         qty=qty, price_type=price_type)
        self._unwrap(await self._channel.request(cmd.model_dump(), cmd_id=cmd.cmd_id,
                                                 timeout=self._timeout))

    async def trades_snapshot(self, after: "datetime | None") -> tuple[list[dict], "datetime | None"]:
        cmd = DownReconcile(cmd_id=uuid.uuid4().hex, mode="sim",
                            after=(after.isoformat() if after is not None else None))
        result = self._unwrap(await self._channel.request(cmd.model_dump(), cmd_id=cmd.cmd_id,
                                                          timeout=self._timeout))
        newest = result.get("newest")
        return result.get("payloads", []), (datetime.fromisoformat(newest) if newest else None)

    @staticmethod
    def _unwrap(ack: UpCmdAck) -> dict:
        if ack.ok:
            return ack.result or {}
        if ack.error_kind == "trade_not_found":
            raise TradeNotFoundError((ack.result or {}).get("ordno"))
        if ack.error_kind == "timeout":
            # codex round1 fix4(a)：型別化——讓 _classify_place_failure 走既有 isinstance
            # 分支穩定判 "unknown"（結果不明、保留配額），不必依賴訊息字串巧合。
            raise AgentCommandTimeoutError(ack.message or "agent 子程序無回應")
        # message 內含券商原始錯誤字串（agent 端已 redact），
        # `code: 4xx` 交給既有 _classify_place_failure 判 failed。
        raise OrderError(ack.message or f"agent 指令失敗（{ack.error_kind}）")
