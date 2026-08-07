"""agent 端 runner（Inc0 Task 12/13）：一條 WS session、上行泵（durable buffer →
server）、下行 dispatch（server 指令 → SDK 子程序）、重連 backoff、子程序凍結偵測/
respawn。

`ChildHandle` 是 SDK 子程序（`native_runner.child_main`，#203 隔離）的擁有者：spawn、
序列 RPC（`threading.Lock` 保護 pipe，因為 `AgentRunner._receive_loop` 用
`asyncio.to_thread` 呼叫 `request()`，理論上可能有多執行緒同時碰 pipe——這是零丟單/
命令不串話設計的第二層序列化；第一層是 `_receive_loop` 本身逐則 inline 處理下行訊息，
不會同時有兩個 `_execute_readonly_command`/`_execute_mutating_command` 在跑）、ping、
terminate。

`AgentRunner.run_once()` 是一次完整的 WS session 生命週期：connect → 先送 `UpLogin` →
啟動 `_pump`/`_receive_loop`/`_heartbeat`/`_child_watchdog` 四個 task → 任一 task 例外
就全部收攏、關閉 transport、例外原樣上拋給 `run_forever`。`_child_watchdog` 定期 ping
子程序，False/例外一律視為凍結（issue #203），raise `ChildFrozenError` 讓 session 崩出。

`run_forever()` 不斷跑 `run_once()`：`ChildFrozenError` 時 terminate 子程序，下一輪
`ensure_child()` respawn；任何例外都依 backoff（倍增封頂 `backoff_max`）延遲重試；只有
本輪 session **存活時間達 `stable_session_seconds` 門檻**，backoff 才重設為
`backoff_base`（Task 13 修復：原本用「login 是否已送出」判準，但 login 在四個
session task 啟動前就送出，幾乎所有失敗模式——子程序凍結、receive/pump 例外、server
收線後立斷——都發生在 login 送出之後，導致 backoff 形同虛設；凍結 respawn 迴圈若無退避，
會在 Shioaji 每日 1000 次登入上限下快速燒配額）。server 端視角即 WS 斷線→重連→重新
login，觸發既有 login handler 的 reconcile（T0.2 斷線 reconcile 免費保存，憑證全程留在
父程序記憶體，不落地）。`stop()` 讓迴圈結束。

零丟單設計：`_pump` 只讀 `buffer.pending()`（尚未 `mark_sent` 的列，經 `to_thread` 離開
event loop），送出後記一筆 `_inflight` 時間戳（純粹防同一 session 內短時間內重複洗頻，
不是持久化機制）；只有收到 server 的 `report_ack` 才 `buffer.mark_sent()`（同樣經
`to_thread`）。若這個 process 在 ack 抵達前崩潰，`_inflight` 隨記憶體消失，但 buffer 裡
那筆事件仍是「未送達」狀態——下一個 session（新 `AgentRunner` 實例，同一個 buffer 檔）
的 `_pump` 會把它當成 pending 重新送出，天然做到「送出未 ack 就重啟 → 補送同一
event_id」，不需要額外的崩潰偵測邏輯。
"""
import asyncio
import logging
import multiprocessing as mp
import threading
import time
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from quanquant.broker.agent_protocol import (
    PROTOCOL_VERSION, DownCancel, DownHealth, DownPlace, DownQueryQty, DownReconcile,
    DownReportAck, DownUpdate, UpCmdAck, UpCommandRejected, UpHealth, UpLogin, UpQueryResult,
    UpReport, parse_downlink,
)
from quanquant.agent.native_runner import child_main

log = logging.getLogger(__name__)

_CHILD_CONNECT_TIMEOUT = 30.0   # 子程序 spawn + connect 的啟動逾時（非逐次 RPC 逾時）


def _is_expired(expires_at: str) -> bool:
    """`expires_at` 為 naive-UTC ISO 字串（D7）；與目前 naive-UTC 時間比較，一致換算，
    避免 aware/naive 混用炸 TypeError。"""
    deadline = datetime.fromisoformat(expires_at)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return now >= deadline


class ChildFrozenError(RuntimeError):
    """SDK 子程序無回應（疑似 issue #203 凍結），需 respawn。"""


class FatalAgentError(RuntimeError):
    """不可重試的致命錯誤（codex round2 fix4）：帳號與 outbox 綁定的帳號不符。

    `assert_account` 原本要等 broker login **成功**才失敗，若把它當一般連線失敗處理，
    `run_forever` 會照 backoff 無限重試——每一輪都是一次真的 Shioaji 登入，會燒掉
    person_id 每日 1000 次登入配額。這個例外讓 `run_forever` 的例外鏈識別出「重試也沒用，
    需要人工介入」，直接停止（不 respawn、不 backoff），把例外原樣往外拋給 main。"""


class ChildHandle:
    """SDK 子程序的擁有者：spawn(spawn ctx)、序列 RPC（threading.Lock）、ping、terminate。

    pipe 失同步防線（codex round1 fix1，BLOCKER）：危險情境是 place 逾時後，之後 child
    才姍姍來遲把 reply 送出——若 pipe 被下一個 ping/place 重用，會把這筆遲到 reply 讀走，
    誤把上一單的 broker id 當成這一單的結果。兩層防線：

    (a) 每筆 RPC 帶遞增 `_rpc_seq` 當 rpc_id，child_main 原樣帶回；收到 reply 後比對
        rpc_id，不符（上一輪遲到的 reply）就丟棄，在剩餘 timeout 預算內繼續等真正的回覆。
    (b) 一旦真的逾時、或 pipe 本身壞掉（EOFError/BrokenPipeError/OSError），直接把整條
        pipe 判死（`_poisoned=True`）並 terminate 子程序——之後任何 request()/ping() 都
        不再嘗試碰這條已經不可信的 pipe，直接 fail；`alive` 隨之回 False，交給
        `AgentRunner.ensure_child()` respawn 一條全新的子程序 + pipe。
    """

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
        self._rpc_seq = 0
        self._poisoned = False
        # Inc1 D9/G2①（Task 12）：獨立於 RPC pipe 之外的專用單向 IPC channel（R2-8）——
        # child 只用來送 callback 落地雙寫失敗的通知，父程序（AgentRunner._failstop_watchdog）
        # 是唯一消費端。`ctx.Pipe(duplex=False)` 回傳 (recv-only, send-only) 兩端。
        self._failstop_parent_conn = None
        self._failstop_child_conn = None

    def start(self) -> str:
        ctx = mp.get_context("spawn")
        parent_conn, child_conn = ctx.Pipe()
        failstop_parent_conn, failstop_child_conn = ctx.Pipe(duplex=False)
        process = ctx.Process(
            target=child_main, args=(child_conn,),
            kwargs=dict(credentials=self._credentials, symbol=self._symbol, mode=self._mode,
                        buffer_path=self._buffer_path, native_factory=self._native_factory,
                        failstop_conn=failstop_child_conn),
        )
        process.start()
        self._process = process
        self._conn = parent_conn
        self._failstop_parent_conn = failstop_parent_conn
        self._failstop_child_conn = failstop_child_conn
        self._poisoned = False   # 全新 spawn 的子程序 + pipe：重置前一輪可能留下的中毒態。
        try:
            reply = self._rpc({"op": "connect"}, timeout=_CHILD_CONNECT_TIMEOUT)
        except Exception as exc:
            # _rpc 逾時/pipe 異常時可能已經 poison→terminate 過（self._process 已是
            # None）；用 None 檢查讓這裡的清理對兩種狀態都安全，不重複 kill 一個 None。
            if self._process is not None:
                self._process.kill()
                self._process.join(timeout=5)
                self._process = None
            if self._failstop_parent_conn is not None:
                self._failstop_parent_conn.close()
                self._failstop_parent_conn = None
            if self._failstop_child_conn is not None:
                self._failstop_child_conn.close()
                self._failstop_child_conn = None
            raise RuntimeError(f"agent 子程序啟動失敗: {exc}") from exc
        if not reply.get("ok"):
            self.terminate()
            if reply.get("error_kind") == "account_mismatch":
                # codex round2 fix4：帳號與 outbox 綁定的帳號不符——這不是暫時性連線問題，
                # 重試也沒用。raise FatalAgentError（而非 RuntimeError）讓 run_forever 的
                # 例外鏈識別出「不可重試」，直接停止，不落入 backoff 無限重連（每輪都真的
                # 燒一次 Shioaji 登入配額）。訊息帶處置指引，交給操作者人工介入。
                raise FatalAgentError(
                    f"agent 帳號不符，拒絕啟動（{reply.get('message')}）——請清空這個 outbox"
                    f"（{self._buffer_path}）改用原帳號重啟，或改用原帳號登入；若確認要放棄"
                    "舊帳號未送達的回報，需人工確認後手動刪除 buffer 檔再重啟。"
                )
            raise RuntimeError(f"agent 子程序 connect 失敗: {reply}")
        return reply["account"]

    def request(self, op: dict, *, timeout: float) -> dict:
        return self._rpc(op, timeout=timeout)

    def _rpc(self, op: dict, *, timeout: float) -> dict:
        if self._poisoned:
            raise TimeoutError(
                f"agent 子程序 pipe 已判死（前次逾時遺留遲到回覆風險），拒絕重用"
                f"（op={op.get('op')}）"
            )
        with self._lock:
            # codex round2 fix3(b)：鎖外剛才通過的 poisoned 檢查可能已經過期——若這則
            # request 卡在等鎖的期間，前一個持鎖的 RPC 在鎖內把 pipe 判死了，這裡拿到鎖後
            # 必須重新檢查一次，才能在真的碰 conn（send/poll/recv）之前攔下，不讓併發等待者
            # 誤用一條已經不可信的 pipe。
            if self._poisoned:
                raise TimeoutError(
                    f"agent 子程序 pipe 已判死（前次逾時遺留遲到回覆風險），拒絕重用"
                    f"（op={op.get('op')}）"
                )
            conn = self._conn
            self._rpc_seq += 1
            rpc_id = self._rpc_seq
            try:
                conn.send({**op, "rpc_id": rpc_id})
            except (BrokenPipeError, EOFError, OSError) as exc:
                # codex round2 fix3(a)：send() 本身也可能炸——原本只有 poll/recv 包了
                # try/except，send 失敗會讓例外原樣往外洩、_poisoned 卻沒被設起來，之後的
                # request 還會繼續嘗試碰這條已經壞掉的 pipe。
                self._poison()
                raise TimeoutError(
                    f"agent 子程序 pipe 異常（op={op.get('op')}）: {exc}"
                ) from exc
            deadline = time.monotonic() + timeout
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._poison()
                    raise TimeoutError(f"agent 子程序逾時未回應（op={op.get('op')}）")
                try:
                    ready = conn.poll(remaining)
                except (EOFError, OSError) as exc:
                    self._poison()
                    raise TimeoutError(
                        f"agent 子程序 pipe 異常（op={op.get('op')}）: {exc}"
                    ) from exc
                if not ready:
                    self._poison()
                    raise TimeoutError(f"agent 子程序逾時未回應（op={op.get('op')}）")
                try:
                    reply = conn.recv()
                except (EOFError, OSError) as exc:
                    self._poison()
                    raise TimeoutError(
                        f"agent 子程序 pipe 異常（op={op.get('op')}）: {exc}"
                    ) from exc
                if reply.get("rpc_id") != rpc_id:
                    # 上一輪逾時後才遲到的 reply：丟棄，在剩餘 timeout 預算內繼續等
                    # 這次呼叫真正的回覆——不會被誤配給呼叫端。
                    continue
                return reply

    def _poison(self) -> None:
        self._poisoned = True
        self.terminate()

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
        if self._failstop_parent_conn is not None:
            self._failstop_parent_conn.close()
            self._failstop_parent_conn = None
        if self._failstop_child_conn is not None:
            self._failstop_child_conn.close()
            self._failstop_child_conn = None

    def poll_failstop(self, timeout: float = 0.0) -> dict | None:
        """G2①：（阻塞式，呼叫端須經 `asyncio.to_thread`）輪詢 child 的 failstop 專用
        channel——與 RPC pipe（`_conn`）完全獨立，不受 rpc_id 比對/poison 邏輯影響。回傳
        None 代表這次逾時內沒有新通知；channel 已關閉/不存在時同樣回 None（不 raise，呼叫端
        `AgentRunner._failstop_watchdog` 是個無窮迴圈，不該被單次 poll 的例外打斷）。"""
        conn = self._failstop_parent_conn
        if conn is None:
            return None
        try:
            if conn.poll(timeout):
                return conn.recv()
        except (EOFError, OSError):
            return None
        return None

    @property
    def alive(self) -> bool:
        return (not self._poisoned) and self._process is not None and self._process.is_alive()


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
    if isinstance(msg, DownQueryQty):
        # G3/D8（Task 2 審查發現的缺口，本 task 補上）：DownQueryQty 是唯讀指令，
        # 與 DownReconcile 走同一條 _execute_readonly_command 路徑（回 UpQueryResult）。
        return {"op": "query_qty", "ordno": msg.ordno}
    raise ValueError(f"未知下行指令: {msg!r}")


class AgentRunner:
    def __init__(self, *, transport, buffer, child, mode: str = "sim",
                 pump_interval: float = 0.5, resend_after: float = 5.0,
                 child_command_timeout: float = 8.0, child_ping_interval: float = 10.0,
                 child_ping_timeout: float = 20.0, heartbeat_interval: float = 15.0,
                 backoff_base: float = 1.0, backoff_max: float = 60.0,
                 stable_session_seconds: float = 30.0,
                 recovery_probe_interval: float = 5.0,
                 failstop_poll_timeout: float = 1.0) -> None:
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
        # session 存活時間達此門檻才視為「連線恢復健康」，backoff 重設回 backoff_base
        # （Task 13 修復：不再用「login 是否已送出」判準，理由見 module docstring）。
        self._stable_session_seconds = stable_session_seconds
        self._account = ""
        self._inflight: dict[int, float] = {}
        self._stopping = False
        # 每輪 sleep 前的 backoff 值，僅供測試觀察退避序列形狀，不影響邏輯。
        self._backoff_history: list[float] = []

        # Inc1 D9/G2（Task 12）：fail-stop 狀態機。`_recovery_lock` 只包本機原子轉移
        # （latch/epoch++/sentinel 寫入；probe→清 latch/sentinel→snapshot），絕不 await
        # 任何網路 I/O（R3-3）——WS send 一律搬到 lock 外，見 `_health_sender`。
        self._recovery_probe_interval = recovery_probe_interval
        self._failstop_poll_timeout = failstop_poll_timeout
        self._recovery_lock = asyncio.Lock()
        self._latched = False
        self._latch_detail: str | None = None
        self._health_epoch = 0
        self._health_queue: asyncio.Queue = asyncio.Queue()
        self._load_persisted_health()

    def _load_persisted_health(self) -> None:
        """啟動時（同步、建構子內）讀取 durable 狀態：sentinel 檔存在＝latch 仍在效（buffer
        之外路徑，見 buffer.py write_sentinel docstring）——epoch 取 sentinel 記的值與 buffer
        meta 存的值兩者較大者（sentinel 可能記著父程序來不及覆寫的佔位值 -1，這種情況下改信
        buffer meta；buffer meta 若因為同一次故障也寫不進去，則沿用 sentinel 的值）。沒有
        sentinel 時單純讀 buffer meta（預設 0，全新 buffer 或從未 latch 過）。"""
        sentinel = self._buffer.read_sentinel()
        if sentinel is not None:
            self._latched = True
            self._latch_detail = sentinel.get("detail")
            self._health_epoch = max(int(sentinel.get("epoch", 0)), self._buffer.get_health_epoch())
        else:
            self._health_epoch = self._buffer.get_health_epoch()

    def ensure_child(self) -> None:
        """child 未活則 (re)start；child alive 但 self._account 遺失（接手他人已在跑的
        child 的邊界情況）視同需要重啟。回傳後 self._account 必為非空。"""
        if self._child.alive and self._account:
            return
        if self._child.alive:
            self._child.terminate()
        self._account = self._child.start()

    async def _latch(self, detail: str) -> None:
        """G2①/⑤/⑦：本機原子轉移（latch=True、epoch+=1、sentinel 寫入），只受
        `_recovery_lock` 保護——**絕不 await 任何網路 I/O**（R3-3）：即使 `_health_sender`
        當下卡在一個緩慢/卡住的 `transport.send()`，也不會拖住這裡，因為 sender 只在
        「重驗」那一小段（純記憶體讀取）才持有這把鎖，實際送出永遠在鎖外（見
        `_health_sender`）。latch 完成後把新 epoch 推進健康佇列，交給單一序列化 sender
        擇機送出（不在這裡直接送）。"""
        async with self._recovery_lock:
            self._latched = True
            self._latch_detail = detail
            self._health_epoch += 1
            epoch = self._health_epoch
            await asyncio.to_thread(self._buffer.write_sentinel, epoch=epoch, detail=detail)
        self._health_queue.put_nowait(epoch)

    async def _recover(self) -> None:
        """G2④/⑤/⑦：解除條件＝storage probe（對同一 buffer 寫入→commit→讀回）通過。
        `_recovery_lock` 內完成「probe→清 latch/sentinel→取 (epoch,status) snapshot」的
        本機原子轉移——鎖本身的互斥已保證探針通過的當下不會有新的 `_latch()` 正在進行中
        （沒有『探針期間又壞了但沒被發現』的競態：新故障必須等到這把鎖釋放才能真正 latch，
        屆時 epoch 會再 +1，語意上等價於『先恢復又立即重新故障』，不違反任何不變量）。
        `await transport.send` 永遠在鎖釋放之後才發生（經 `_health_sender`，不在這裡）。

        C3（HIGH，codex 終審）修復：`_latched` 翻 False 必須排在兩個 durable 持久化步驟
        （寫 epoch、清 sentinel）**之後**、且兩者皆成功才翻——舊版先翻 `_latched=False` 再做
        持久化，若中途任一步失敗（I/O 例外），in-memory 狀態已經「假裝恢復」（G2②的 latch
        檢查會放行 mutating 指令），但 durable 狀態其實還沒真的恢復（sentinel 仍在／epoch
        沒真的持久化），造成「部分失敗卻誤判 healthy」。持久化順序（epoch 先、sentinel
        後）刻意選成：即使 epoch 寫成功但清 sentinel 失敗，下次啟動時 sentinel 仍存在會
        正確重新載入 latch，且 `_load_persisted_health` 取 `max(sentinel.epoch, buffer
        meta)` 保證不會用到落後的舊 epoch；反過來若先清 sentinel 才寫 epoch、epoch 寫失敗，
        下次啟動會誤判「未 latch」且 epoch 讀到過舊的值。任一步失敗：保留 latch、記錯，
        不動 `_health_epoch`、不推進健康佇列，留給下一輪 `_recovery_prober` 重試。"""
        async with self._recovery_lock:
            if not self._latched:
                return
            ok = await asyncio.to_thread(self._buffer.probe)
            if not ok:
                return
            epoch = self._health_epoch
            try:
                await asyncio.to_thread(self._buffer.set_health_epoch, epoch)
                await asyncio.to_thread(self._buffer.clear_sentinel)
            except Exception:
                log.exception(
                    "G2④ recover 持久化步驟失敗（epoch 寫入／sentinel 清除），保持 latch，"
                    "留給下一輪 _recovery_prober 重試"
                )
                return
            self._latched = False
            self._latch_detail = None
        self._health_queue.put_nowait(epoch)

    async def _reject_failstop(self, cmd_id: str) -> None:
        """G2②：latch 期間拒絕 mutating 指令——volatile `UpCommandRejected` **直送 WS，
        不經 outbox**（buffer 已壞時仍能拒絕）。送不出去（transport 已斷）就讓例外原樣往外
        拋，交給 `run_once` 的例外收攏斷線，之後由 server 端的 lease 判定 not-ready
        （見 spec D9②：「送不出就斷線交 lease」）。"""
        await self._transport.send(
            UpCommandRejected(cmd_id=cmd_id, error_kind="failstop").model_dump()
        )

    async def run_once(self) -> None:
        """單一 WS session：connect→login→pump/receive/heartbeat/watchdog 直到斷線/例外
        /子程序凍結。session 存活時間由 run_forever 用 time.monotonic() 量測，決定是否
        重設重連 backoff（見 module docstring）。"""
        self.ensure_child()
        await self._transport.connect()
        tasks: list[asyncio.Task] = []
        try:
            await self._transport.send(
                UpLogin(protocol=PROTOCOL_VERSION, account=self._account,
                        mode=self._mode,
                        # Inc1 D7/R3-2/G2⑤：health_epoch 是本 session 的健康狀態基準宣告——
                        # 讀自建構子/`_load_persisted_health` 載入的目前值（latch 中則是
                        # latch 當時的 epoch），buffer 重建歸零由這次宣告吸收（server 端據此
                        # 重設本連線已見最大 epoch，見 agent_ws.py AgentChannel.mark_logged_in）。
                        health_epoch=self._health_epoch).model_dump()
            )
            # D9⑥：login 後立刻排一次健康回報（不等 heartbeat_interval）——server 端的
            # pending_health 才能盡快收斂，不必乾等第一次 heartbeat（生產環境預設 15s，
            # 太慢；latch 中一樣送、status 會如實回報 failstop）。
            self._health_queue.put_nowait(self._health_epoch)
            tasks = [
                asyncio.create_task(self._pump()),
                asyncio.create_task(self._receive_loop()),
                asyncio.create_task(self._heartbeat()),
                asyncio.create_task(self._child_watchdog()),
                asyncio.create_task(self._health_sender()),
                asyncio.create_task(self._failstop_watchdog()),
                asyncio.create_task(self._recovery_prober()),
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
        """不斷跑 run_once()：連線/子程序凍結例外時依 backoff 重試（backoff×2 封頂
        backoff_max）；ChildFrozenError 則 terminate 子程序，下一輪 ensure_child()
        respawn＋（run_once 內）重新登入——server 端視角即 WS 斷線→重連→重新 login，
        觸發既有 login handler 的 reconcile（T0.2）。backoff 重設判準是本輪 session
        「存活時間達 stable_session_seconds」，而非「是否送出 login」（Task 13 修復：
        login 在四個 session task 啟動前就送出，幾乎所有失敗模式都發生在 login 送出之後，
        用它當判準會讓 backoff 每輪重設、指數退避失效——凍結 respawn 迴圈會無節制地重打
        Shioaji 登入，燒每日 1000 次額度）。stop() 後迴圈結束。"""
        backoff = self._backoff_base
        while not self._stopping:
            session_start = time.monotonic()
            try:
                await self.run_once()
            except FatalAgentError:
                # codex round2 fix4：帳號不符等不可重試錯誤——絕不能落入下面的 backoff
                # 重連迴圈（每輪都是一次真的 Shioaji 登入，會燒每日 1000 次配額）。停止
                # 迴圈，例外原樣往外拋給呼叫端（main.py）處理。
                log.error("agent 遇到不可重試的致命錯誤，停止（不重試）")
                self.stop()
                raise
            except ChildFrozenError:
                log.warning("agent 子程序疑似凍結（issue #203），terminate 後下一輪 respawn")
                self._child.terminate()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("agent WS session 異常結束，準備依 backoff 重連")

            elapsed = time.monotonic() - session_start
            if elapsed >= self._stable_session_seconds:
                backoff = self._backoff_base
            self._backoff_history.append(backoff)

            if self._stopping:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._backoff_max)

    def stop(self) -> None:
        self._stopping = True

    async def _pump(self) -> None:
        while True:
            await asyncio.sleep(self._pump_interval)
            now = time.monotonic()
            pending = await asyncio.to_thread(self._buffer.pending, 50)
            for row in pending:
                sent_at = self._inflight.get(row.id)
                if sent_at is not None and (now - sent_at) < self._resend_after:
                    continue
                await self._transport.send(self._to_uplink(row).model_dump())
                self._inflight[row.id] = now

    def _to_uplink(self, row: Any) -> UpReport | UpCmdAck:
        """outbox 列有兩種：`cmd_id` 非空＝D4④執行 native 後存的 cmd_ack（record_execution/
        ensure_cmd_ack_pending 寫入，payload＝{"ok","result","error_kind","message"}）；
        否則是原本的 report。account/mode 優先取列上蓋章值（D5/I7 來源端蓋章、不可變）；
        None 表示這筆是升級前/測試直呼 append() 未帶欄位的舊列，退回 runner 當下 session
        account/mode（同一 buffer 檔受 assert_account tripwire 保護，同一 session 內不會
        跨帳號，退回值語意等價 Inc0）。"""
        account = row.account if row.account is not None else self._account
        mode = row.mode if row.mode is not None else self._mode
        if row.cmd_id is not None:
            payload = row.payload
            return UpCmdAck(cmd_id=row.cmd_id, event_id=row.id,
                            ok=bool(payload.get("ok")), result=payload.get("result"),
                            error_kind=payload.get("error_kind"),
                            message=payload.get("message"))
        return UpReport(event_id=row.id, kind=row.kind, account=account, mode=mode,
                        payload=row.payload)

    async def _receive_loop(self) -> None:
        while True:
            raw = await self._transport.receive()
            try:
                msg = parse_downlink(raw)
            except ValidationError:
                log.warning("agent 收到不合法下行訊息，忽略：%s", str(raw)[:200])
                continue
            if isinstance(msg, DownReportAck):
                await asyncio.to_thread(self._buffer.mark_sent, msg.event_id)
                self._inflight.pop(msg.event_id, None)
            elif isinstance(msg, DownHealth):
                # G2⑧：不直接送——排進健康佇列，交單一序列化 sender 處理（出隊前重驗
                # (epoch,status,latch)，見 `_health_sender`）。
                self._health_queue.put_nowait(self._health_epoch)
            elif isinstance(msg, DownPlace | DownCancel | DownUpdate):
                # Inc1 D4 agent 端①-④（順序即正確性）：inline 序列執行＝agent 端 native
                # 序列化第一層（同一時間只有一則下行指令在跑），child pipe lock 為第二層。
                await self._execute_mutating_command(msg)
            else:
                # reconcile／query_qty（唯讀，D4：不入 ledger，只記三種 mutating op）——
                # Task 11（D7/D8）：改回 volatile UpQueryResult（無 event_id、不進 outbox、
                # 不觸發 DownReportAck），reconcile 快照與 query_qty 結果共用同一條路徑。
                await self._execute_readonly_command(msg)

    async def _execute_readonly_command(self, msg: Any) -> None:
        """reconcile／query_qty 共用：唯讀冪等，失敗（子程序逾時或執行例外）一律不回覆，
        讓 server 端的 `channel.request()` 自然逾時（`AgentCommandTimeoutError`）——下一輪
        watchdog/reconcile 重試即可，不猜測失敗原因、不需要 UpQueryResult 攜帶錯誤欄位
        （D7：UpQueryResult 只有 `result` 一個欄位，就是刻意的最小介面）。"""
        op = _to_op(msg)
        try:
            reply = await asyncio.to_thread(
                self._child.request, op, timeout=self._child_command_timeout
            )
        except TimeoutError:
            log.warning("agent 唯讀指令 %s 逾時，不回覆（server 端自然逾時，下輪重試）", msg.type)
            return
        if not reply.get("ok"):
            log.warning("agent 唯讀指令 %s 執行失敗，不回覆（server 端自然逾時，下輪重試）：%s",
                        msg.type, reply.get("message"))
            return
        await self._transport.send(
            UpQueryResult(cmd_id=msg.cmd_id, result=reply.get("result") or {}).model_dump()
        )

    async def _execute_mutating_command(self, msg: Any) -> None:
        """place/cancel/update：spec D4 agent 端①-④＋G2②latch 檢查，順序即正確性。"""
        cmd_id = msg.cmd_id

        # ① ledger 命中 → 不重執行，確保 outbox 有該 cmd 未送 ack（無則以存檔 result 補
        # append）。刻意排在②③之前（S#4）：已經真的執行過，就算 scope/expiry 這次看起來
        # 不符，也不能改口——存檔結果才是唯一誠實的答案，重執行風險遠高於誤判。ledger 命中
        # 的重播從不呼叫 native，latch 中也照常放行（G2 只擋「尚未真正執行過」的指令）。
        cached = await asyncio.to_thread(self._buffer.lookup_command, cmd_id)
        if cached is not None:
            await asyncio.to_thread(
                self._buffer.ensure_cmd_ack_pending, cmd_id, cached,
                account=self._account, mode=self._mode,
            )
            return  # ack 交給 _pump 走 outbox at-least-once 送出，不在此直送。

        # G2②：latch 中拒絕一切尚未真正執行過的 mutating 指令——volatile 直送，不進
        # outbox（buffer 已壞時仍能拒絕）。排在①之後、②③之前：不做無謂的 scope/expiry
        # 判斷，latch 期間一律先拒。
        if self._latched:
            await self._reject_failstop(cmd_id)
            return

        # ② scope 核對（R1-2）：指令 account/mode 與目前登入 scope 不符 → scope_mismatch，
        # 不執行。best-effort 直送（不進 outbox）——未執行 native，遺失靠重送自然收斂
        # （resend 時 ledger 仍未命中，會重新走到這裡再判一次，結論不變）。
        if msg.account != self._account or msg.mode != self._mode:
            await self._transport.send(UpCmdAck(
                cmd_id=cmd_id, event_id=0, ok=False, error_kind="scope_mismatch",
                message=(f"指令 scope（account={msg.account}, mode={msg.mode}）與目前登入"
                         f"（account={self._account}, mode={self._mode}）不符"),
            ).model_dump())
            return

        # ③ expiry：過期 → expired，不執行。同樣 best-effort 直送（理由同②）。
        if _is_expired(msg.expires_at):
            await self._transport.send(UpCmdAck(
                cmd_id=cmd_id, event_id=0, ok=False, error_kind="expired",
                message="指令已過期，agent 拒絕執行",
            ).model_dump())
            return

        # G2②（再驗一次）：spec 逐字要求「每次 native 呼叫前檢查」——上面②③本身雖無 await
        # 邊界，仍在真正打 native 之前再確認一次，避免未來②③加上 I/O 後留下時間窗、也讓這裡
        # 成為真正權威、緊貼 native 呼叫前的守門點。
        if self._latched:
            await self._reject_failstop(cmd_id)
            return

        # ④ 執行 native → 同一 SQLite 交易寫 command_ledger＋append ack 進 outbox → 泵送。
        # 不論成功/明確失敗/子程序逾時，只要嘗試呼叫過 native 就一律記錄（I6：每筆
        # mutating 指令恰好收斂一次——逾時代表「執行結果未知」，不是「未執行」，重播
        # 風險遠高於漏 ack，同一原則見 child.request timeout 後 pipe 即被判死）。
        op = _to_op(msg)
        try:
            reply = await asyncio.to_thread(
                self._child.request, op, timeout=self._child_command_timeout
            )
        except TimeoutError:
            result = {"ok": False, "result": None, "error_kind": "timeout",
                      "message": "agent 子程序無回應"}
        else:
            result = {"ok": bool(reply.get("ok")), "result": reply.get("result"),
                      "error_kind": reply.get("error_kind"), "message": reply.get("message")}
        await asyncio.to_thread(
            self._buffer.record_execution, cmd_id, msg.type, result,
            account=self._account, mode=self._mode,
        )

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            self._health_queue.put_nowait(self._health_epoch)

    async def _health_sender(self) -> None:
        """G2⑧：唯一序列化送出 health frame 的地方（heartbeat／DownHealth 詢問／
        latch／recover 都只 `put_nowait` 進佇列，不直接 send）。每個 frame 出隊後在
        `_recovery_lock` 下重驗：`frame_epoch` 與目前 `_health_epoch` 不符 → 代表這個 frame
        是在更新的 latch/recover 事件之前排進來的，已經過期，直接丟棄不送（已交付 wire 的
        frame 無法撤回，但排隊中尚未送出的可以）。status/detail 一律讀「送出當下」的目前
        值（不是排入當下的舊值），確保吻合的 epoch 一定對應目前正確的 status。實際
        `transport.send` 在鎖釋放之後才發生（R3-3：不讓網路 I/O 卡住 lock，`_latch` 才能
        永遠不被卡住的 send 拖住）。"""
        while True:
            frame_epoch = await self._health_queue.get()
            async with self._recovery_lock:
                if frame_epoch != self._health_epoch:
                    continue
                status = "failstop" if self._latched else "ok"
                detail = self._latch_detail if self._latched else None
                epoch = self._health_epoch
            await self._transport.send(
                UpHealth(status=status, detail=detail, health_epoch=epoch).model_dump()
            )

    async def _failstop_watchdog(self) -> None:
        """G2①：child 落地失敗經獨立 IPC channel（R2-8，不混 RPC pipe）通知父程序——這裡是
        唯一消費端，收到就立即呼叫 `_latch`（該呼叫本身不受任何網路 I/O 阻塞，見其
        docstring）。`ChildHandle.poll_failstop` 是阻塞呼叫，搬到 thread 執行，逾時內沒有
        通知就回 None、迴圈繼續。`getattr` 容錯（比照 `broker/watchdog.py::_probe_healthy`
        既有慣例）：不支援這個介面的 child（測試替身／未來精簡實作）直接讓這個 task 正常
        結束，不拋例外把整個 `run_once` 拖垮——等價於「這個 child 永遠不會回報 failstop」。"""
        poll = getattr(self._child, "poll_failstop", None)
        if poll is None:
            return
        while True:
            notice = await asyncio.to_thread(poll, self._failstop_poll_timeout)
            if notice is not None:
                await self._latch(notice.get("detail") or "child 回報 buffer 落地失敗")

    async def _recovery_prober(self) -> None:
        """G2④：latch 期間週期性嘗試 storage probe，通過就呼叫 `_recover()` 解除 latch。
        未 latch 時直接跳過（no-op），避免對健康的 buffer 做無謂的探測寫入。"""
        while True:
            await asyncio.sleep(self._recovery_probe_interval)
            if self._latched:
                await self._recover()

    async def _child_watchdog(self) -> None:
        """定期 ping SDK 子程序（#203 凍結偵測）：False 或例外（含逾時）一律視為凍結，
        raise ChildFrozenError 讓 run_once 的 asyncio.wait(FIRST_EXCEPTION) 崩出——
        run_forever 接手 terminate+respawn+重新登入。"""
        while True:
            await asyncio.sleep(self._child_ping_interval)
            try:
                ok = await asyncio.to_thread(
                    self._child.ping, timeout=self._child_ping_timeout
                )
            except Exception as exc:
                raise ChildFrozenError(
                    f"agent 子程序 ping 例外，疑似凍結（issue #203）: {exc}"
                ) from exc
            if not ok:
                raise ChildFrozenError("agent 子程序 ping 逾時/失敗，疑似凍結（issue #203）")
