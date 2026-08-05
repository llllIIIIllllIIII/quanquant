"""agent 端 runner（Inc0 Task 12）：一條 WS session、上行泵（durable buffer → server）、
下行 dispatch（server 指令 → SDK 子程序）。

`ChildHandle` 是 SDK 子程序（`native_runner.child_main`，#203 隔離）的擁有者：spawn、
序列 RPC（`threading.Lock` 保護 pipe，因為 `AgentRunner._receive_loop` 用
`asyncio.to_thread` 呼叫 `request()`，理論上可能有多執行緒同時碰 pipe——這是零丟單/
命令不串話設計的第二層序列化；第一層是 `_receive_loop` 本身逐則 inline 處理下行訊息，
不會同時有兩個 `_execute_command` 在跑）、ping、terminate。

`AgentRunner.run_once()` 是一次完整的 WS session 生命週期：connect → 先送 `UpLogin` →
啟動 `_pump`/`_receive_loop`/`_heartbeat` 三個 task → 任一 task 例外就全部收攏、關閉
transport、例外原樣上拋（給呼叫者，未來 Task 13 的 `run_forever` 決定要不要重試/backoff）。

零丟單設計：`_pump` 只讀 `buffer.pending()`（尚未 `mark_sent` 的列），送出後記一筆
`_inflight` 時間戳（純粹防同一 session 內短時間內重複洗頻，不是持久化機制）；只有收到
server 的 `report_ack` 才 `buffer.mark_sent()`。若這個 process 在 ack 抵達前崩潰，
`_inflight` 隨記憶體消失，但 buffer 裡那筆事件仍是「未送達」狀態——下一個 session（新
`AgentRunner` 實例，同一個 buffer 檔）的 `_pump` 會把它當成 pending 重新送出，天然做到
「送出未 ack 就重啟 → 補送同一 event_id」，不需要額外的崩潰偵測邏輯。
"""
import asyncio
import logging
import multiprocessing as mp
import threading
import time
from typing import Any

from pydantic import ValidationError

from quanquant.broker.agent_protocol import (
    DownCancel, DownHealth, DownPlace, DownReconcile, DownReportAck, DownUpdate,
    UpCmdAck, UpHealth, UpLogin, UpReport, parse_downlink,
)
from quanquant.agent.native_runner import child_main

log = logging.getLogger(__name__)

_CHILD_CONNECT_TIMEOUT = 30.0   # 子程序 spawn + connect 的啟動逾時（非逐次 RPC 逾時）


class ChildFrozenError(RuntimeError):
    """SDK 子程序無回應（疑似 issue #203 凍結），需 respawn。"""


class ChildHandle:
    """SDK 子程序的擁有者：spawn(spawn ctx)、序列 RPC（threading.Lock）、ping、terminate。"""

    def __init__(self, *, credentials: dict, symbol: str, mode: str, buffer_path: str,
                 native_factory=None) -> None:
        self._credentials = credentials
        self._symbol = symbol
        self._mode = mode
        self._buffer_path = buffer_path
        self._native_factory = native_factory
        self._lock = threading.Lock()
        self._process: mp.process.BaseProcess | None = None
        self._conn = None

    def start(self) -> str:
        ctx = mp.get_context("spawn")
        parent_conn, child_conn = ctx.Pipe()
        process = ctx.Process(
            target=child_main, args=(child_conn,),
            kwargs=dict(credentials=self._credentials, symbol=self._symbol, mode=self._mode,
                        buffer_path=self._buffer_path, native_factory=self._native_factory),
        )
        process.start()
        self._process = process
        self._conn = parent_conn
        try:
            reply = self._rpc(self._conn, {"op": "connect"}, timeout=_CHILD_CONNECT_TIMEOUT)
        except Exception as exc:
            self._process.kill()
            self._process.join(timeout=5)
            raise RuntimeError(f"agent 子程序啟動失敗: {exc}") from exc
        if not reply.get("ok"):
            self._process.kill()
            self._process.join(timeout=5)
            raise RuntimeError(f"agent 子程序 connect 失敗: {reply}")
        return reply["account"]

    def request(self, op: dict, *, timeout: float) -> dict:
        return self._rpc(self._conn, op, timeout=timeout)

    def _rpc(self, conn, op: dict, *, timeout: float) -> dict:
        with self._lock:
            conn.send(op)
            if not conn.poll(timeout):
                raise TimeoutError(f"agent 子程序逾時未回應（op={op.get('op')}）")
            return conn.recv()

    def ping(self, *, timeout: float) -> bool:
        try:
            reply = self.request({"op": "ping"}, timeout=timeout)
        except TimeoutError:
            return False
        return bool(reply.get("ok"))

    def terminate(self) -> None:
        if self._process is not None:
            self._process.kill()
            self._process.join(timeout=5)
            self._process = None
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @property
    def alive(self) -> bool:
        return self._process is not None and self._process.is_alive()


def _to_op(msg: Any) -> dict:
    if isinstance(msg, DownPlace):
        n = msg.native
        return {"op": "place", "action": n.action, "price": n.price, "qty": n.qty,
                "price_type": n.price_type, "order_type": n.order_type, "octype": n.octype}
    if isinstance(msg, DownCancel):
        return {"op": "cancel", "ordno": msg.ordno}
    if isinstance(msg, DownUpdate):
        return {"op": "update", "ordno": msg.ordno, "price": msg.price, "qty": msg.qty,
                "price_type": msg.price_type}
    if isinstance(msg, DownReconcile):
        return {"op": "reconcile", "after": msg.after}
    raise ValueError(f"未知下行指令: {msg!r}")


class AgentRunner:
    def __init__(self, *, transport, buffer, child, mode: str = "sim",
                 pump_interval: float = 0.5, resend_after: float = 5.0,
                 child_command_timeout: float = 8.0, child_ping_interval: float = 10.0,
                 child_ping_timeout: float = 20.0, heartbeat_interval: float = 15.0,
                 backoff_base: float = 1.0, backoff_max: float = 60.0) -> None:
        self._transport = transport
        self._buffer = buffer
        self._child = child
        self._mode = mode
        self._pump_interval = pump_interval
        self._resend_after = resend_after
        self._child_command_timeout = child_command_timeout
        self._child_ping_interval = child_ping_interval
        self._child_ping_timeout = child_ping_timeout
        self._heartbeat_interval = heartbeat_interval
        self._backoff_base = backoff_base
        self._backoff_max = backoff_max
        self._account = ""
        self._inflight: dict[int, float] = {}
        self._stopping = False

    def ensure_child(self) -> None:
        """child 未活則 (re)start，記 self._account。"""
        if not self._child.alive:
            self._account = self._child.start()

    async def run_once(self) -> None:
        """單一 WS session：connect→login→pump/receive/heartbeat 直到斷線/例外。"""
        self.ensure_child()
        await self._transport.connect()
        tasks: list[asyncio.Task] = []
        try:
            await self._transport.send(
                UpLogin(account=self._account, mode=self._mode).model_dump()
            )
            tasks = [
                asyncio.create_task(self._pump()),
                asyncio.create_task(self._receive_loop()),
                asyncio.create_task(self._heartbeat()),
            ]
            done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for t in done:
                exc = t.exception()
                if exc is not None:
                    raise exc
        finally:
            for t in tasks:
                t.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await self._transport.close()

    async def run_forever(self) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        self._stopping = True

    async def _pump(self) -> None:
        while True:
            await asyncio.sleep(self._pump_interval)
            now = time.monotonic()
            for row in self._buffer.pending(50):
                sent_at = self._inflight.get(row.id)
                if sent_at is not None and (now - sent_at) < self._resend_after:
                    continue
                await self._transport.send(
                    UpReport(event_id=row.id, kind=row.kind, payload=row.payload).model_dump()
                )
                self._inflight[row.id] = now

    async def _receive_loop(self) -> None:
        while True:
            raw = await self._transport.receive()
            try:
                msg = parse_downlink(raw)
            except ValidationError:
                log.warning("agent 收到不合法下行訊息，忽略：%s", str(raw)[:200])
                continue
            if isinstance(msg, DownReportAck):
                self._buffer.mark_sent(msg.event_id)
                self._inflight.pop(msg.event_id, None)
            elif isinstance(msg, DownHealth):
                await self._transport.send(UpHealth().model_dump())
            else:
                # place/cancel/update/reconcile：inline 序列執行＝agent 端 native 序列化
                # 第一層（同一時間只有一則下行指令在跑），child pipe lock 為第二層。
                await self._execute_command(msg)

    async def _execute_command(self, msg: Any) -> None:
        op = _to_op(msg)
        try:
            reply = await asyncio.to_thread(
                self._child.request, op, timeout=self._child_command_timeout
            )
        except TimeoutError:
            ack = UpCmdAck(cmd_id=msg.cmd_id, ok=False, error_kind="timeout",
                           message="agent 子程序無回應")
        else:
            ack = UpCmdAck(cmd_id=msg.cmd_id, ok=bool(reply.get("ok")),
                           result=reply.get("result"), error_kind=reply.get("error_kind"),
                           message=reply.get("message"))
        await self._transport.send(ack.model_dump())

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            await self._transport.send(UpHealth().model_dump())
