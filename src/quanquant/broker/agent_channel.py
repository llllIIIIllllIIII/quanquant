"""server 端 agent 連線態與指令多工。

不碰 WS 框架——由 web/routers/agent_ws.py 注入 send_json callable，
故可用純 asyncio 單元測試。上行解析後由端點呼叫 resolve_ack / mark_logged_in。
"""
import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone

from quanquant.broker.agent_protocol import (
    DownCancel, DownPlace, DownQueryQty, DownReconcile, DownUpdate, PlaceNative, UpCmdAck,
    UpQueryResult,
)
from quanquant.broker.base import (
    AgentCommandTimeoutError, AgentUnavailableError, OrderError, TradeNotFoundError,
)
from quanquant.broker.types import OrderRequest

# Inc1 D4/D7 §7：設計預設 agent_command_expiry_seconds=120（新設定，尚未接線到
# Settings——command ledger／過期收斂是後續 task 的 runtime 範圍）。這裡先用同一預設值
# 算出 DownPlace/DownCancel/DownUpdate 必填的 expires_at（訊息合法的最小欄位傳遞）。
_DEFAULT_COMMAND_EXPIRY_SECONDS = 120


def _default_expires_at() -> str:
    deadline = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(
        seconds=_DEFAULT_COMMAND_EXPIRY_SECONDS
    )
    return deadline.isoformat()


class AgentChannel:
    def __init__(self) -> None:
        self._send: Callable[[dict], Awaitable[None]] | None = None
        self._pending: dict[str, asyncio.Future] = {}
        self.logged_in = False
        self.account = ""
        self.last_heartbeat: float | None = None
        # Inc1 D9/G2（Task 12）：health 狀態機——`_health_max_epoch` 是本連線（per-session，
        # 隨 mark_logged_in 重宣告而重設，R3-2/R5-1）已見過的最大 health_epoch；只有
        # `health_epoch >= _health_max_epoch` 的 UpHealth 才會被接受（見 note_health）。
        # `last_ok_heartbeat`：只在接受一則 status="ok" 時更新（G2③ lease 判定專用——
        # `last_heartbeat` 是既有的「任何上行訊息都算存活」欄位，WS 連線存活不等於健康，
        # 兩者刻意分開）。`failstop`/`health_status`：目前已知的健康語意，供 UI/告警參考。
        self._health_max_epoch = 0
        self.last_ok_heartbeat: float | None = None
        self.health_status = "unknown"
        self.failstop = False
        # codex round1 fix2：雙連線 generation——新連線取代舊連線後，舊 WS handler 較晚才
        # 跑到自己的 finally 時，若無條件 detach()，會把新連線也拆掉、誤標 offline。每次
        # attach() 遞增這個計數，detach(generation) 只在呼叫者手上的 generation 仍是目前值
        # 時才真的生效，否則視為「舊連線的遲到清理」no-op。
        self._generation = 0
        # codex round3 fix（TOCTOU）：舊連線的 report commit 正在 to_thread 飛行中時，
        # 新連線的「未處理 RawInbox count 查詢 + 換帳號決策 + mark_logged_in + adapter.account
        # 設定」必須被序列化在 commit 完成之後才判定，否則 count 查詢會看到舊 commit 尚未
        # 落地的 0，誤放行換帳號，之後舊 report 才 commit 完成、落在新帳號狀態下。只包 DB
        # commit 與狀態轉換這兩段，不包任何等待 cmd_ack 的路徑，不引入死鎖面。
        self.inbox_lock = asyncio.Lock()

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def connected(self) -> bool:
        return self._send is not None

    @property
    def ready(self) -> bool:
        return self._send is not None and self.logged_in

    def attach(self, send_json: Callable[[dict], Awaitable[None]]) -> int:
        self._generation += 1
        self._send = send_json
        return self._generation

    def detach(self, generation: int | None = None) -> None:
        if generation is not None and generation != self._generation:
            return  # 舊連線的遲到 detach：已被新連線取代，不動目前狀態。
        self._send = None
        self.logged_in = False
        pending, self._pending = self._pending, {}
        for fut in pending.values():
            if not fut.done():
                fut.set_exception(AgentCommandTimeoutError("agent 連線中斷，指令結果未知"))

    def mark_logged_in(self, account: str, *, health_epoch: int = 0) -> None:
        """`health_epoch`：Inc1 D9/G2⑤/R3-2——UpLogin 宣告的本 session 健康狀態基準，**直接
        覆寫**（非取 max）本連線的已見最大 epoch，讓 buffer 重建後較低的 epoch 也能在新連線
        被正確接受，不被舊連線遺留的較高 max 永久拒收（見 R5-1「buffer 重建 epoch 歸零→
        重宣告不死鎖」）。login 後健康狀態重置為 pending（未收過本連線任何健康回報）。"""
        self.logged_in = True
        self.account = account
        self._health_max_epoch = health_epoch
        self.health_status = "unknown"
        self.failstop = False

    def note_heartbeat(self) -> None:
        self.last_heartbeat = time.monotonic()

    def note_health(self, *, status: str, health_epoch: int) -> bool:
        """G2 R2-3/R5-1：epoch 單調性——只接受 `health_epoch >= 已見最大`；較舊的一律忽略
        （回傳 False，呼叫端不應據此改變 ready 狀態，讓「見過較大 epoch 後舊 ok 永不恢復」
        成立）。任何被接受的 UpHealth 都算存活訊號（`note_heartbeat`）；只有被接受且
        `status=="ok"` 才更新 `last_ok_heartbeat`（G2③ lease 判定專用）。"""
        self.note_heartbeat()
        if health_epoch < self._health_max_epoch:
            return False
        self._health_max_epoch = health_epoch
        self.health_status = status
        self.failstop = status == "failstop"
        if status == "ok":
            self.last_ok_heartbeat = time.monotonic()
        return True

    def resolve_ack(self, ack: UpCmdAck) -> None:
        fut = self._pending.pop(ack.cmd_id, None)
        if fut is not None and not fut.done():
            fut.set_result(ack)

    def resolve_query_result(self, msg: UpQueryResult) -> None:
        """Task 11（D7 R1-7）：volatile UpQueryResult 專用——reconcile 快照與 query_qty
        結果共用同一個 `_pending` 等待表（`request()` 本就與訊息型別無關，只認 cmd_id）。
        沒有對應 pending future 的 cmd_id（舊連線殘留重送/agent 端 bug）是安全 no-op，同
        `resolve_ack` 既有慣例。"""
        fut = self._pending.pop(msg.cmd_id, None)
        if fut is not None and not fut.done():
            fut.set_result(msg)

    async def request(self, cmd: dict, *, cmd_id: str, timeout: float) -> UpCmdAck | UpQueryResult:
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

    async def place(self, req: OrderRequest, *, cmd_id: str) -> dict:
        # Inc1 D4/Task 8：`cmd_id` 由呼叫端（ShioajiAdapter 決策段）提供——必須與該指令已經
        # 送前持久化的 `agent_commands` 列同一個 cmd_id，UpCmdAck 的兩維 CAS applier
        # （`agent_commands.apply_command_ack`）才能用 cmd_id 命中同一列。不再自行
        # `uuid.uuid4()`（Inc0 舊行為：ledger 尚不存在時 gateway 自己決定 id 即可，Task 8
        # 之後 id 的權威來源改成 ledger）。
        cmd = DownPlace(
            cmd_id=cmd_id, account=self._channel.account, mode="sim",
            expires_at=_default_expires_at(),
            native=PlaceNative(action=req.action, price=str(req.price), qty=req.qty,
                               price_type=req.price_type, order_type=req.order_type,
                               octype=req.octype),
        )
        ack = await self._channel.request(cmd.model_dump(), cmd_id=cmd.cmd_id,
                                          timeout=self._timeout)
        result = self._unwrap(ack)
        return {"ordno": result.get("ordno"), "broker_order_id": result.get("broker_order_id")}

    async def cancel(self, ordno: str, *, cmd_id: str) -> None:
        cmd = DownCancel(cmd_id=cmd_id, account=self._channel.account, mode="sim",
                         expires_at=_default_expires_at(), ordno=ordno)
        self._unwrap(await self._channel.request(cmd.model_dump(), cmd_id=cmd.cmd_id,
                                                 timeout=self._timeout))

    async def update(
        self, ordno: str, *, price, qty: int, price_type: str | None = None, cmd_id: str
    ) -> None:
        cmd = DownUpdate(cmd_id=cmd_id, account=self._channel.account, mode="sim",
                         expires_at=_default_expires_at(), ordno=ordno,
                         price=(str(price) if price is not None else None),
                         qty=qty, price_type=price_type)
        self._unwrap(await self._channel.request(cmd.model_dump(), cmd_id=cmd.cmd_id,
                                                 timeout=self._timeout))

    async def trades_snapshot(self, after: "datetime | None") -> tuple[list[dict], "datetime | None"]:
        # Task 11（D7）：reconcile 是唯讀指令，改走 volatile UpQueryResult（不再經 UpCmdAck）
        # ——`channel.request()` 對訊息型別無關，逾時一樣拋 AgentCommandTimeoutError（既有
        # 呼叫端 `_reconcile_inner` 沒有 try/except，例外原樣往上拋，行為與改動前一致）。
        cmd = DownReconcile(cmd_id=uuid.uuid4().hex, mode="sim",
                            after=(after.isoformat() if after is not None else None))
        reply = await self._channel.request(cmd.model_dump(), cmd_id=cmd.cmd_id, timeout=self._timeout)
        result = reply.result
        newest = result.get("newest")
        return result.get("payloads", []), (datetime.fromisoformat(newest) if newest else None)

    async def query_qty(self, ordno: str) -> int | None:
        """G3/D8：per-slot watchdog 用來比對改前/改後口數收斂 unknown（`DownQueryQty` 下行，
        `UpQueryResult` 回覆，唯讀不入 ledger，同 `trades_snapshot`）。查無（委託已結案、
        不在 agent 端 `list_trades()` 目前清單）回 None，呼叫端（`agent_commands` resolver）
        視為無法判斷、不猜測——與 in-process `_query_order_qty_blocking` 語意等價。"""
        cmd = DownQueryQty(cmd_id=uuid.uuid4().hex, ordno=ordno, mode="sim")
        reply = await self._channel.request(cmd.model_dump(), cmd_id=cmd.cmd_id, timeout=self._timeout)
        return reply.result.get("qty")

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
