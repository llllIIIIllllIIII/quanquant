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
    DownReportAck, DownUpdate, UpCmdAck, UpHealth, UpLogin, UpQueryResult, UpReport,
    parse_downlink,
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
                 stable_session_seconds: float = 30.0) -> None:
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

    def ensure_child(self) -> None:
        """child 未活則 (re)start；child alive 但 self._account 遺失（接手他人已在跑的
        child 的邊界情況）視同需要重啟。回傳後 self._account 必為非空。"""
        if self._child.alive and self._account:
            return
        if self._child.alive:
            self._child.terminate()
        self._account = self._child.start()

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
                        # Inc1 D7/R3-2：health_epoch 是本 session 的健康狀態基準宣告。
                        # G2 failstop latch/epoch 追蹤是後續 task 的 runtime 接線，這裡先給
                        # 0（訊息合法的最小欄位傳遞），實際單調遞增計數由之後的 task 補上。
                        health_epoch=0).model_dump()
            )
            tasks = [
                asyncio.create_task(self._pump()),
                asyncio.create_task(self._receive_loop()),
                asyncio.create_task(self._heartbeat()),
                asyncio.create_task(self._child_watchdog()),
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
                await self._transport.send(self._make_health().model_dump())
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
        """place/cancel/update：spec D4 agent 端①-④，順序即正確性。"""
        cmd_id = msg.cmd_id

        # ① ledger 命中 → 不重執行，確保 outbox 有該 cmd 未送 ack（無則以存檔 result 補
        # append）。刻意排在②③之前（S#4）：已經真的執行過，就算 scope/expiry 這次看起來
        # 不符，也不能改口——存檔結果才是唯一誠實的答案，重執行風險遠高於誤判。
        cached = await asyncio.to_thread(self._buffer.lookup_command, cmd_id)
        if cached is not None:
            await asyncio.to_thread(
                self._buffer.ensure_cmd_ack_pending, cmd_id, cached,
                account=self._account, mode=self._mode,
            )
            return  # ack 交給 _pump 走 outbox at-least-once 送出，不在此直送。

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
            await self._transport.send(self._make_health().model_dump())

    def _make_health(self) -> UpHealth:
        # Inc1 D9/G2：status/health_epoch 是 fail-stop latch 狀態機的 wire 表現——那條狀態
        # 機（child 落地失敗→latch→拒新指令→lease→probe 恢復）是後續 task 的 runtime 接線。
        # 這裡先固定回報 status="ok"、health_epoch=0（訊息合法的最小欄位傳遞），與 Inc0
        # 既有行為等價（agent 目前尚不會偵測/latch failstop）。
        return UpHealth(status="ok", detail=None, health_epoch=0)

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
