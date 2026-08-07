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
import random
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
    """SDK 子程序的擁有者：spawn(spawn ctx)、序列 RPC（threading.RLock）、ping、terminate。

    pipe 失同步防線（codex round1 fix1，BLOCKER）：危險情境是 place 逾時後，之後 child
    才姍姍來遲把 reply 送出——若 pipe 被下一個 ping/place 重用，會把這筆遲到 reply 讀走，
    誤把上一單的 broker id 當成這一單的結果。兩層防線：

    (a) 每筆 RPC 帶遞增 `_rpc_seq` 當 rpc_id，child_main 原樣帶回；收到 reply 後比對
        rpc_id，不符（上一輪遲到的 reply）就丟棄，在剩餘 timeout 預算內繼續等真正的回覆。
    (b) 一旦真的逾時、或 pipe 本身壞掉（EOFError/BrokenPipeError/OSError），直接把整條
        pipe 判死（`_poisoned=True`）並 terminate 子程序——之後任何 request()/ping() 都
        不再嘗試碰這條已經不可信的 pipe，直接 fail；`alive` 隨之回 False，交給
        `AgentRunner.ensure_child()` respawn 一條全新的子程序 + pipe。

    R3-1（HIGH，codex 終審 round3）：lifecycle generation fencing——舊版 `start()`/
    `terminate()` 在鎖外直接替換 `self._process`/`self._conn`，與併發中的 RPC（watchdog
    ping、唯讀指令，皆經 `asyncio.to_thread`）完全無互斥。危險情境：recovery 觸發的
    respawn（`terminate()`+`start()`）與一個仍在等待舊 pipe 回覆的 RPC 併發時——舊 RPC
    逾時後呼叫 `_poison()`→`terminate()`，若這時 respawn 已經把 process/conn 換成新
    child，會把新 child 錯殺；`asyncio.to_thread` 的呼叫端就算被取消，底層 thread pool
    worker 仍會跑完，遲到的執行緒若在 respawn 之後才真正碰到 conn，可能誤送/誤讀新 child
    的 pipe。修法：

    - 所有會碰 `_process`/`_conn`/`_poisoned`（含 failstop 專用 conn）的操作
      （`start`/`terminate`/`_rpc`/`poll_failstop`）都經同一把 `threading.RLock`
      （可重入——`start()` 內部會呼叫 `_rpc()` 做 connect，`_poison()` 也會呼叫
      `terminate()`）序列化：respawn 全程與任何 RPC 互斥，兩者不會半途交錯。
    - `_generation`（int，每次 `start()` 成功替換 process/conn 前 +1）：呼叫端
      （`request()`/`poll_failstop()`）在**進入鎖之前**捕捉當下的 generation，代表「這次
      呼叫意圖操作的是哪一代 child」；真正碰 conn 前（`_rpc()` 拿到鎖之後）重新核對，不符
      （代表這段等鎖期間 respawn 已經換代）就直接視為過期丟棄（`TimeoutError`），完全不
      觸碰新一代的 process/conn，也不會誤 poison 新 child。
    """

    def __init__(self, *, credentials: dict, symbol: str, mode: str, buffer_path: str,
                 native_factory=None) -> None:
        self._credentials = credentials
        self._symbol = symbol
        self._mode = mode
        self._buffer_path = buffer_path
        self._native_factory = native_factory
        self._lock = threading.RLock()
        self._process: mp.process.BaseProcess | None = None
        self._conn = None
        self._rpc_seq = 0
        self._poisoned = False
        self._generation = 0
        # Inc1 D9/G2①（Task 12）：獨立於 RPC pipe 之外的專用單向 IPC channel（R2-8）——
        # child 只用來送 callback 落地雙寫失敗的通知，父程序（AgentRunner._failstop_watchdog）
        # 是唯一消費端。`ctx.Pipe(duplex=False)` 回傳 (recv-only, send-only) 兩端。
        self._failstop_parent_conn = None
        self._failstop_child_conn = None

    @property
    def generation(self) -> int:
        """R3-1/R3-3：目前 child 世代——每次 `start()` 成功替換 process/conn 前 +1。
        `AgentRunner._failstop_watchdog` 用它核對 failstop 通知（notice 本身也帶著送出當下
        的 generation，見 native_runner.py）是不是屬於「目前這一代」，過期的（舊 child
        respawn 前排進 pipe、之後才被取出）一律丟棄，不誤 latch 目前健康的新 child。"""
        return self._generation

    def start(self) -> str:
        with self._lock:
            self._generation += 1
            generation = self._generation
            ctx = mp.get_context("spawn")
            parent_conn, child_conn = ctx.Pipe()
            failstop_parent_conn, failstop_child_conn = ctx.Pipe(duplex=False)
            process = ctx.Process(
                target=child_main, args=(child_conn,),
                kwargs=dict(credentials=self._credentials, symbol=self._symbol, mode=self._mode,
                            buffer_path=self._buffer_path, native_factory=self._native_factory,
                            failstop_conn=failstop_child_conn, generation=generation),
            )
            process.start()
            self._process = process
            self._conn = parent_conn
            self._failstop_parent_conn = failstop_parent_conn
            self._failstop_child_conn = failstop_child_conn
            self._poisoned = False   # 全新 spawn 的子程序 + pipe：重置前一輪可能留下的中毒態。
            try:
                reply = self._rpc({"op": "connect"}, timeout=_CHILD_CONNECT_TIMEOUT,
                                   generation=generation)
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

    def respawn(self, expected_generation: int) -> str | None:
        """R4-a（HIGH，codex 終審 round4）：把 recovery 用的 terminate+start 合成單一
        generation-scoped transaction，取代舊版 `_respawn_child()` 拆成兩個獨立、各自可被
        取消的 `asyncio.to_thread` 呼叫（`terminate` 一個、`start` 一個）。危險情境：
        `run_once()` 結束會 cancel 呼叫端（recovery 相關 task），但 `asyncio.to_thread`
        底層真正在跑的 OS thread 無法被真的中止——若 terminate 的 thread 已完成、start 的
        thread 卻在下一個 session 的 `ensure_child()` 已經合法 spawn 出新 child **之後**才
        姍姍來遲執行，舊版會無條件覆寫 `self._process`/`self._conn`，把新 session 剛啟動
        的健康 child 直接洩漏掉（process 沒人 terminate、Shioaji login 也沒登出）。

        修法：terminate→start 全程在同一次 `with self._lock` 持有內完成，且動手前先核對
        `expected_generation` 是否仍是目前 generation——呼叫端在真正發動這次 respawn
        「意圖」的當下（呼叫本方法之前）捕捉這個快照；若這段期間（因為呼叫端被取消、
        thread 排程延遲等原因）目前 generation 已經被別的呼叫（最常見是新 session 的
        `ensure_child()`，它自己的 `start()` 也會讓 generation 前進）換過，代表這次呼叫
        已經過期——no-op，完全不 terminate、不 start，回傳 `None`；呼叫端據此得知「這次
        respawn 沒有生效」，交由下一輪重新讀取目前 generation/alive 狀態對帳，不會誤殺
        任何後續已經換上的新 child，也不會在它之上再疊一個沒人管的重複 child。

        generation 沒變則正常執行：terminate 目前 child（就算已經不 alive 也是 no-op）→
        start 一個新的（內部已含一次 connect RPC、`_generation += 1`）→ 回傳新 child 回報
        的 account（成功時保證非 None，呼叫端可放心用 `is None` 判斷是否過期）。`start()`
        可能拋出的例外（連線失敗、帳號不符 `FatalAgentError`）原樣往外傳，語意不變。"""
        with self._lock:
            if expected_generation != self._generation:
                return None
            self.terminate()
            return self.start()

    def request(self, op: dict, *, timeout: float) -> dict:
        # R3-1：在進入鎖（可能因為併發 respawn 而卡住）之前，先捕捉「這次呼叫意圖操作的
        # generation」——`_rpc()` 拿到鎖之後會重新核對，若這段等待期間 respawn 已經換代，
        # 就地丟棄，不誤觸新一代的 conn。
        return self._rpc(op, timeout=timeout, generation=self._generation)

    def _rpc(self, op: dict, *, timeout: float, generation: int | None = None) -> dict:
        if generation is None:
            generation = self._generation
        if self._poisoned:
            raise TimeoutError(
                f"agent 子程序 pipe 已判死（前次逾時遺留遲到回覆風險），拒絕重用"
                f"（op={op.get('op')}）"
            )
        with self._lock:
            # R3-1：鎖內第一件事就是重新核對 generation——若呼叫端捕捉 generation 之後、
            # 拿到這把鎖之前，respawn 已經換代（terminate 舊 child、start 新 child 都要拿
            # 同一把鎖，只有在這裡放行後才可能發生），這筆呼叫就是「過期意圖」：直接丟棄，
            # 完全不碰 self._process/self._conn（那已經是新一代的），也不會誤把新 conn
            # poison 掉。
            if generation != self._generation:
                raise TimeoutError(
                    f"agent 子程序已被 respawn（generation {generation} 已被取代為 "
                    f"{self._generation}），丟棄過期 RPC（op={op.get('op')}）"
                )
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
            if conn is None:
                # R3-1：respawn（terminate()+start()）若被拆成兩次獨立呼叫，中間有極短的
                # 「目前無 child」窗口（generation 尚未 bump，仍與這筆呼叫捕捉的相同）——
                # 沒有 conn 可用，等同暫時不可用，走既有 TimeoutError 語意，不讓
                # `None.send()` 炸出未經處理的 AttributeError。
                raise TimeoutError(
                    f"agent 子程序目前不可用（respawn 進行中），丟棄（op={op.get('op')}）"
                )
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
        # 呼叫時機必然在 `_rpc()` 自己持有的鎖內（同執行緒、RLock 可重入）——這段期間
        # generation 不可能被其他執行緒改變（想改也要搶同一把鎖），因此這裡毒化/terminate
        # 的必然是呼叫端一開始核對過、目前仍然 current 的那一代 process/conn，不會誤殺
        # 併發中已經換上的新 child（R3-1）。
        self._poisoned = True
        self.terminate()

    def ping(self, *, timeout: float) -> bool:
        try:
            reply = self.request({"op": "ping"}, timeout=timeout)
        except TimeoutError:
            return False
        return bool(reply.get("ok"))

    def ping_detail(self, *, timeout: float) -> dict:
        """R3-2（HIGH，codex 終審 round3）：`ping()` 只回 bool，不足以支撐 recovery 判斷
        「新 child 是否又立即 latch」。回傳完整資訊：`ok`（存活/有回覆）、`latched`
        （native_runner 端 `ChildFailstopLatch.tripped`，見 `_dispatch` 的 ping 分支）；
        逾時/pipe 異常時 `ok=False, latched=None`（None 代表拿不到，呼叫端一律當成不安全
        處理，不得視為「未 latch」而放行）。

        R4-b（HIGH，codex 終審 round4）：額外帶上 `generation`/`fault_seq`（child 自報，見
        native_runner.py `_dispatch` ping 分支）——`AgentRunner._recover()` 在「清 sentinel
        前原子重驗」時用得到；逾時/異常時同樣回 `None`（拿不到，呼叫端不得假設未變）。"""
        try:
            reply = self.request({"op": "ping"}, timeout=timeout)
        except TimeoutError:
            return {"ok": False, "latched": None, "generation": None, "fault_seq": None}
        return {"ok": bool(reply.get("ok")), "latched": bool(reply.get("latched", False)),
                "generation": reply.get("generation"), "fault_seq": reply.get("fault_seq")}

    def terminate(self) -> None:
        with self._lock:
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
        `AgentRunner._failstop_watchdog` 是個無窮迴圈，不該被單次 poll 的例外打斷）。

        R3-1：只用鎖短暫保護「捕捉 (generation, conn) 這對快照」——不是整段阻塞的
        `conn.poll(timeout)` 都持鎖（那樣會跟 place/cancel/update 的 RPC 搶鎖，拖慢下單
        延遲）。阻塞等待結束、真的收到通知後，再核對一次 generation：若這段等待期間
        respawn 已經換代，代表剛剛讀到的是舊 conn 遺留的通知，丟棄不回傳（R3-3 在 notice
        payload 內另外也帶 generation 做第二層防線，見 `AgentRunner._failstop_watchdog`）。
        """
        with self._lock:
            generation = self._generation
            conn = self._failstop_parent_conn
        if conn is None:
            return None
        try:
            if not conn.poll(timeout):
                return None
            notice = conn.recv()
        except (EOFError, OSError):
            return None
        if generation != self._generation:
            return None
        return notice

    @property
    def alive(self) -> bool:
        with self._lock:
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
                 failstop_poll_timeout: float = 1.0,
                 respawn_backoff_base: float = 5.0,
                 respawn_backoff_max: float = 300.0) -> None:
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
        # R3-4（HIGH，codex 終審 round3）：respawn（＝一次真的 Shioaji 登入）專屬的獨立
        # backoff——與 storage probe 呼叫頻率（`recovery_probe_interval`）、session 重連
        # backoff（`_backoff_base`/`_backoff_max`）都無關，避免「每次便宜的 probe 通過就真
        # 登入一次」在 recovery_probe_interval 這麼短的週期下燒光每日登入配額。
        self._respawn_backoff_base = respawn_backoff_base
        self._respawn_backoff_max = respawn_backoff_max
        self._respawn_backoff = respawn_backoff_base
        self._last_respawn_attempt = 0.0
        self._respawn_backoff_history: list[float] = []
        self._respawn_stage: str | None = None  # None｜"persist_only"（respawn 已成功，
        # 只剩 epoch/sentinel 持久化待完成——下一輪只重試持久化，不再 respawn/登入）
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

    async def _latch(self, detail: str, *, expected_generation: int | None = None) -> None:
        """G2①/⑤/⑦：本機原子轉移（latch=True、epoch+=1、sentinel 寫入），只受
        `_recovery_lock` 保護——**絕不 await 任何網路 I/O**（R3-3）：即使 `_health_sender`
        當下卡在一個緩慢/卡住的 `transport.send()`，也不會拖住這裡，因為 sender 只在
        「重驗」那一小段（純記憶體讀取）才持有這把鎖，實際送出永遠在鎖外（見
        `_health_sender`）。latch 完成後把新 epoch 推進健康佇列，交給單一序列化 sender
        擇機送出（不在這裡直接送）。

        R3-4：新一輪故障一律重置 `_respawn_stage`——若沿用上一輪（可能是另一個 child 世代）
        留下的 `"persist_only"` 標記，下一次 `_recover()` 會誤以為「respawn 已經做過了，
        這次只需要重試持久化」而跳過真正需要的 respawn。

        R4-c（MEDIUM，codex 終審 round4）：`expected_generation`——呼叫端（`_failstop_
        watchdog`）在鎖外核對過 notice 的 generation 與當下 `self._child.generation` 相符
        後才呼叫這裡；但「核對通過」與「真正拿到 `_recovery_lock`」之間仍有一段沒有互斥
        的窗口（`_recovery_lock` 可能正被另一個 `_recover()`／`_latch()` 呼叫佔住），這段
        期間 child 若換代（例如 recovery 剛好 respawn 成功），鎖外核對過的 generation 就
        過期了。這裡在拿到鎖之後、真正改動任何狀態之前，用同一個快照重新核對一次——不符
        即 no-op（完全不動 `_latched`/`_health_epoch`/sentinel），避免把一則過期通知套用
        到目前這一代健康的新 child 身上。呼叫端不傳（`expected_generation=None`，既有的
        直接呼叫路徑／測試）視為「呼叫端自己已經確保沒有這個競態」，照舊無條件 latch。"""
        async with self._recovery_lock:
            if expected_generation is not None:
                current_generation = getattr(self._child, "generation", None)
                if current_generation is not None and expected_generation != current_generation:
                    log.warning(
                        "R4-c: agent latch 呼叫過期（notice generation=%r，取得 "
                        "_recovery_lock 後目前 generation=%r 已不同），no-op：等鎖期間 "
                        "child 已換代，不誤 latch 目前這一代健康的 child",
                        expected_generation, current_generation,
                    )
                    return
            self._latched = True
            self._latch_detail = detail
            self._health_epoch += 1
            epoch = self._health_epoch
            self._respawn_stage = None
            await asyncio.to_thread(self._buffer.write_sentinel, epoch=epoch, detail=detail)
        self._health_queue.put_nowait(epoch)

    async def _respawn_child(self) -> bool:
        """N2（HIGH，codex 終審 round2）：`_recover()` 的必要步驟——`ChildFailstopLatch`
        （native_runner.py）在 child 進程內無 rearm、永久單向 trip，唯一能讓它重置的方式是
        把整個 child process 換掉（新 process＝新的 Python 物件圖＝全新 latch）。若 recovery
        只解除 parent 這邊的 `_latched`，child 內部的本地 latch 依然 tripped，`_dispatch`
        會永久對 mutating op 回 failstop——parent 卻已經回報 healthy，形成假 healthy。

        R4-a（HIGH，codex 終審 round4）：舊版把 terminate/start 拆成兩個獨立、各自可被
        `asyncio.to_thread` 取消的呼叫——`run_once()` 結束會 cancel 這裡（連帶
        `_recovery_prober`），但底層 OS thread 無法被真的中止，可能在下一個 session 的
        `ensure_child()` 已經合法 spawn 出新 child 之後才姍姍來遲執行，把新 child 覆寫
        掉（洩漏 process/login）。改用 `ChildHandle.respawn(expected_generation)`——terminate
        +start 收成單一 generation-scoped transaction（見其 docstring），這裡只需要單一
        `asyncio.to_thread(self._child.respawn, expected_generation)` 呼叫。

        另外用 `asyncio.shield()` 包住這個呼叫、明確收割背景執行緒的最終結果
        （`_reap_late_respawn` done callback）：`shield()` 不能真的阻止底層 OS thread 被
        取消（thread 一旦開始執行就是這樣），但可以避免「取消發生時直接放生這個 Future，
        結果被靜默丟棄、甚至觸發 asyncio 的『Task exception was never retrieved』警告」——
        被取消後，背景執行緒最終的結果（成功/失敗/generation 過期的 no-op）改用 log 記錄，
        不假裝這裡曾經觀察到它，下一輪 `_recover()` 一律重新讀 `self._child.generation`/
        `alive` 對帳。

        R3-4④（HIGH，codex 終審 round3）：respawn 拿回的帳號若與這個 session 目前綁定的
        `self._account` 不符——不是「暫時性連線問題」，是需要人工介入的異常（例如憑證/
        帳號設定被動過手腳）；絕不能靜默改 `self._account` 繼續回報 healthy，直接
        `FatalAgentError`（下方 `except Exception` 只吞非 fatal 例外，不會誤攔）。

        R4-d（MEDIUM，codex 終審 round4）：帳號不符時，新 child 其實已經成功啟動、連線
        （`respawn()` 內部的 `start()` 已經跑完）——舊版直接 raise，沒有 terminate，這個
        帳號不符的新 child 會繼續活著（process 沒殺、Shioaji session 沒登出），造成資源/
        登入配額洩漏。raise 前先 terminate 掉它，不留殘留 child。

        respawn（單一 generation-scoped transaction）→ 帳號核對（不符即 terminate+fatal，
        R4-d）→ R3-2：額外用 `ping_detail()`（而非 `ping()`）做「新 child 真的可用、而且
        沒有立刻又 latch」的獨立確認——`ping()` 只回 bool，不足以擋住「新 child 在 connect
        後、清 sentinel 前又故障一次」的假 healthy 縫（`ok=True` 但 `latched=True` 時，一樣
        視為不可用）。respawn（含過期 no-op）/帳號核對(fatal 除外)/ping_detail 任一步失敗：
        記錯、回 False，呼叫端（`_recover`）據此保留 latch、不持久化任何狀態，交下一輪
        `_recovery_prober` 依 respawn 專屬 backoff（R3-4①②）重試——不留下「parent 以為
        恢復了、child 其實沒換成功／又立刻壞掉」的中間態。成功才更新 `self._account`。

        `_recover()` 在真正清 sentinel 前還會再做一次獨立的 ping 重驗（R4-b），這裡的
        `ping_detail()` 只是「respawn 剛完成那一刻」的快速確認，不是最終權威。"""
        expected_generation = self._child.generation
        respawn_task = asyncio.ensure_future(
            asyncio.to_thread(self._child.respawn, expected_generation)
        )

        def _reap_late_respawn(task: "asyncio.Task") -> None:
            # R4-a：呼叫端（這個協程）已經被取消，但底層 to_thread 開的 OS thread 一旦
            # 開始執行就無法真的被中止，仍會在背景跑完——這個 done callback 收割它最終的
            # 結果，不讓它靜默消失（也避免 asyncio 印出「Task exception was never
            # retrieved」）。`ChildHandle.respawn()` 本身的 generation 檢查已經保證：即使
            # 它真的跑完並換上了新 child，也只發生在它捕捉的 `expected_generation` 在它
            # 拿到鎖的當下仍然 current 的情況——不會誤殺任何後續 session 已經換上的新
            # child；下一輪 `_recover()`/`ensure_child()` 一律重新讀取目前 generation/
            # alive 對帳，不依賴這個已經被取消呼叫端的回傳值。
            if task.cancelled():
                return
            exc = task.exception()
            if exc is not None:
                log.warning(
                    "R4-a: respawn（原意圖 generation=%d）在呼叫端取消後才於背景結束，"
                    "且失敗，下一輪 _recover() 會重新對帳：%s", expected_generation, exc,
                )
                return
            result = task.result()
            if result is None:
                log.info(
                    "R4-a: respawn（原意圖 generation=%d）在呼叫端取消後才於背景結束，"
                    "已過期（generation 已換代），no-op", expected_generation,
                )
            else:
                log.warning(
                    "R4-a: respawn（原意圖 generation=%d）在呼叫端取消後才於背景完成"
                    "（account=%s），下一輪 _recover() 會重新讀取目前 generation/alive 對帳",
                    expected_generation, result,
                )

        try:
            account = await asyncio.shield(respawn_task)
        except FatalAgentError:
            # R3-4①：帳號不符等不可重試錯誤——絕不能被下面的 `except Exception` 吞掉、
            # 落入 respawn backoff 的無限重試迴圈（每輪都是一次真的 Shioaji 登入）。原樣
            # 往外拋，交給 `_recover()`/`_recovery_prober` 一路傳到 `run_forever` 的既有
            # FatalAgentError 處置（停止、不重試）。
            raise
        except asyncio.CancelledError:
            # R4-a：呼叫端被取消——`respawn_task` 本身沒有被取消（`shield()` 保護），仍在
            # 背景跑；掛上 done callback 收割它，然後照 asyncio 慣例原樣往外拋，讓取消
            # 正確傳播（`_recover()`/`_recovery_prober` 的 `async with`/task 生命週期不受
            # 影響）。
            respawn_task.add_done_callback(_reap_late_respawn)
            raise
        except Exception:
            log.exception("G2④ recovery：child respawn 失敗，保持 latch，留給下一輪重試")
            return False
        if account is None:
            # R4-a：respawn 呼叫過期（目標 generation 在它拿到鎖之前就已經被其他呼叫換
            # 代）——no-op，完全沒有動到任何 child。當成失敗處理（回 False），交下一輪
            # 重新讀取目前 generation 對帳後再試，不假設過期呼叫等價於成功。
            log.warning(
                "R4-a recovery：respawn 呼叫過期（目標 generation=%d 已被其他呼叫換代），"
                "no-op，保持 latch，留給下一輪重新讀取 generation 對帳後重試",
                expected_generation,
            )
            return False
        if self._account and account != self._account:
            # R4-d：新 child 已經成功啟動、連線——raise 前先 terminate 掉它，不留殘留
            # process/登入。
            await asyncio.to_thread(self._child.terminate)
            raise FatalAgentError(
                f"agent respawn 後新 child 回報的帳號（{account}）與目前 session 綁定帳號"
                f"（{self._account}）不符——拒絕靜默切換帳號繼續回報 healthy，需人工介入"
                "（新 child 已 terminate，不留下殘留 process）"
            )
        try:
            detail = await asyncio.to_thread(
                self._child.ping_detail, timeout=self._child_ping_timeout
            )
        except Exception:
            log.exception("G2④ recovery：respawn 後 ping 例外，保持 latch，留給下一輪重試")
            return False
        if not detail.get("ok"):
            log.error("G2④ recovery：respawn 後 ping 失敗，保持 latch，留給下一輪重試")
            return False
        if detail.get("latched"):
            # R3-2（HIGH，codex 終審 round3）：新 child 在 connect 後、清 sentinel 前又
            # latch——若這裡只看 ok=True 就放行，會清掉 sentinel、回報 healthy，但新 child
            # 其實已經又故障一次（假 healthy）。
            log.error(
                "G2④ recovery：respawn 後新 child 立即又 latch，保持 latch，留給下一輪重試"
            )
            return False
        self._account = account
        return True

    async def _recover(self) -> None:
        """G2④/⑤/⑦：解除條件＝storage probe（對同一 buffer 寫入→commit→讀回）通過，
        respawn（含新 child 沒有立刻又 latch 的確認）成功，且 epoch/sentinel 持久化成功。
        `_recovery_lock` 內完成「probe→respawn child→清 latch/sentinel→取 (epoch,status)
        snapshot」的本機原子轉移——鎖本身的互斥已保證探針通過的當下不會有新的 `_latch()`
        正在進行中（沒有『探針期間又壞了但沒被發現』的競態：新故障必須等到這把鎖釋放才能
        真正 latch，屆時 epoch 會再 +1，語意上等價於『先恢復又立即重新故障』，不違反任何
        不變量）。`await transport.send` 永遠在鎖釋放之後才發生（經 `_health_sender`，不在
        這裡）。

        R3-4（HIGH，codex 終審 round3）：respawn 本身是一次真的 Shioaji 登入——`_recover()`
        被 `_recovery_prober` 以固定 `recovery_probe_interval`（預設 5s，只用來做便宜的
        storage probe）反覆呼叫；若每次 probe 通過就無條件 respawn，形同每 5 秒真登入
        一次，最快 83 分鐘燒光每日 1000 次額度。拆成三段修法：
          ① respawn 專屬 backoff（`_respawn_backoff`）——獨立於 storage probe 的呼叫頻率、
             也獨立於 session 重連用的 `backoff`（`run_forever`）：只有距上次「真的嘗試
             respawn」超過 `_respawn_backoff` 秒才會再打一次；失敗則指數倍增＋jitter
             （封頂 `_respawn_backoff_max`），respawn 成功即重設回 `_respawn_backoff_base`。
             respawn 全程仍在鎖內完成，理由同舊版（避免 respawn 期間又一次 `_latch()`
             與這裡交錯出錯誤 snapshot）。
          ② staged recovery：respawn＋ping_detail 已經成功，但接下來的 epoch/sentinel
             持久化失敗——記 `_respawn_stage = "persist_only"`，下一輪（即使還在
             latched）**只重試持久化，不再呼叫 `_respawn_child()`**（不再登入）。
          ③ `_respawn_child()` 可能原樣拋出 `FatalAgentError`（帳號不符等）——這裡刻意不
             攔截，讓它一路往外拋給 `run_forever` 的既有 fatal 處置（停止、不重試）；
             `async with` 仍會正確釋放鎖。

        C3（HIGH，codex 終審）修復：`_latched` 翻 False 必須排在兩個 durable 持久化步驟
        （寫 epoch、清 sentinel）**之後**、且兩者皆成功才翻——舊版先翻 `_latched=False` 再做
        持久化，若中途任一步失敗（I/O 例外），in-memory 狀態已經「假裝恢復」（G2②的 latch
        檢查會放行 mutating 指令），但 durable 狀態其實還沒真的恢復（sentinel 仍在／epoch
        沒真的持久化），造成「部分失敗卻誤判 healthy」。持久化順序（epoch 先、sentinel
        後）刻意選成：即使 epoch 寫成功但清 sentinel 失敗，下次啟動時 sentinel 仍存在會
        正確重新載入 latch，且 `_load_persisted_health` 取 `max(sentinel.epoch, buffer
        meta)` 保證不會用到落後的舊 epoch；反過來若先清 sentinel 才寫 epoch、epoch 寫失敗，
        下次啟動會誤判「未 latch」且 epoch 讀到過舊的值。任一步失敗：保留 latch、記錯，
        不動 `_health_epoch`，`_respawn_stage` 維持 `"persist_only"`，留給下一輪
        `_recovery_prober` 只重試持久化（不再 respawn/登入）。

        R4-b（HIGH，codex 終審 round4）：舊版只在 `_respawn_child()` 內做過**一次**
        point-in-time ping，之後就直接寫 epoch/清 sentinel/解 latch——respawn 成功、
        ping 通過的那一刻與真正清 sentinel 之間仍有一段窗口（哪怕只是幾個 `await
        asyncio.to_thread` 的排程延遲），child 若在這段窗口內又 trip 一次（新故障，
        不論 IPC 通知有沒有送達——通知本身也可能失敗），完全沒被偵測到，會被誤判成
        healthy。且 `_respawn_stage == "persist_only"` 的輪次舊版完全跳過任何 child
        重驗，直接嘗試持久化。修法：**每一輪**（不論剛 respawn 成功、還是先前輪已進入
        persist_only）在寫 epoch/清 sentinel 前都重新 `ping_detail()` 一次，只有
        `ok=True` 且 `latched=False` 才繼續；不符則保留 latch、把 `_respawn_stage` 重設
        回 `None`（child 本地 latch 一旦 trip 就不會 rearm，繼續在 persist_only 原地重試
        毫無意義，必須讓下一輪重新走一次完整 respawn）。

        sentinel 清除改採 compare-and-clear 語意：清除前重讀一次 sentinel，確認其
        `epoch` 欄位仍與 `self._health_epoch`（本輪要清的那份）相符才真的 unlink——
        `epoch` 是既有 sentinel schema 就有的欄位、也是系統裡現成的「每次故障事件遞增
        一次」版本號，不需要另外擴充 buffer.py 的 sentinel 格式。理由：child 端
        `_trigger_failstop_latch`（native_runner.py）寫 sentinel 完全繞過這把
        `_recovery_lock`（不同進程，鎖不到）——若在我們讀完 sentinel 到真正 unlink 之間，
        child 剛好又落地一次新故障、直接覆寫了一份新的 sentinel（帶新故障的 epoch=-1
        佔位值，或未來被父程序覆寫後的新 epoch），這裡如果不比對就清，會把「代表新故障」
        的 sentinel 憑空刪掉——之後若 agent 進程崩潰，新故障的 durable latch 標記就這樣
        丟失了。sentinel 內容已不存在（`None`）視為「沒東西可清」，照常放行（`clear_
        sentinel()` 本身也是 `unlink(missing_ok=True)`，冪等）。"""
        async with self._recovery_lock:
            if not self._latched:
                return
            ok = await asyncio.to_thread(self._buffer.probe)
            if not ok:
                return
            if self._respawn_stage != "persist_only":
                now = time.monotonic()
                if now - self._last_respawn_attempt < self._respawn_backoff:
                    return  # 還沒到下一次允許 respawn（真登入）的時間點，留給下一輪重試
                self._last_respawn_attempt = now
                self._respawn_backoff_history.append(self._respawn_backoff)
                respawned = await self._respawn_child()
                if not respawned:
                    self._respawn_backoff = min(
                        self._respawn_backoff * 2
                        + random.uniform(0, self._respawn_backoff_base),
                        self._respawn_backoff_max,
                    )
                    return
                self._respawn_backoff = self._respawn_backoff_base
                self._respawn_stage = "persist_only"

            # R4-b：清 sentinel 前的最終原子重驗——不論這輪是剛 respawn 成功、還是先前輪
            # 已經進入 persist_only，一律重新 ping 一次確認 child 現在仍然健康。
            try:
                recheck = await asyncio.to_thread(
                    self._child.ping_detail, timeout=self._child_ping_timeout
                )
            except Exception:
                log.exception(
                    "G2④ recover：清 sentinel 前重驗 child 例外，保持 latch，"
                    "留給下一輪重新 respawn"
                )
                self._respawn_stage = None
                return
            if not recheck.get("ok") or recheck.get("latched"):
                log.error(
                    "G2④ recover：清 sentinel 前重驗 child 發現又故障（ok=%r, latched=%r），"
                    "保持 latch，留給下一輪重新 respawn（child 本地 latch 無 rearm，"
                    "原地重試 persist_only 沒有意義）",
                    recheck.get("ok"), recheck.get("latched"),
                )
                self._respawn_stage = None
                return

            epoch = self._health_epoch
            try:
                current_sentinel = await asyncio.to_thread(self._buffer.read_sentinel)
                if current_sentinel is not None and current_sentinel.get("epoch") != epoch:
                    # compare-and-clear：sentinel 內容已經不是本輪要清的那份（child 在
                    # 這之間又直接落地了一份新故障的 sentinel）——保留現狀，不清除，讓
                    # 下一輪 `_latch()`/`_recover()` 依 sentinel 目前真正的內容重新處理。
                    log.error(
                        "G2④ recover：sentinel 內容在清除前已被改寫（現在 epoch=%r，"
                        "預期 %r），疑似 child 又落地了新故障，保留現狀不清除",
                        current_sentinel.get("epoch"), epoch,
                    )
                    return
                await asyncio.to_thread(self._buffer.set_health_epoch, epoch)
                await asyncio.to_thread(self._buffer.clear_sentinel)
            except Exception:
                log.exception(
                    "G2④ recover 持久化步驟失敗（epoch 寫入／sentinel 清除），保持 latch，"
                    "留給下一輪 _recovery_prober 只重試持久化（respawn 階段已完成，不再登入）"
                )
                return
            self._latched = False
            self._latch_detail = None
            self._respawn_stage = None
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
        結束，不拋例外把整個 `run_once` 拖垮——等價於「這個 child 永遠不會回報 failstop」。

        R3-3（MEDIUM，codex 終審 round3）：notice 帶著送出當下的 child generation（見
        native_runner.py `_trigger_failstop_latch`）——respawn 換代之後，若這裡才取出一則
        屬於「舊 generation」的通知（例如舊 child 臨終前排進 failstop pipe，respawn 過程中
        `ChildHandle.poll_failstop` 卡在鎖外等待，直到 respawn 完成才拿到鎖讀出；R3-1 的
        generation 核對已經先擋掉這條路徑的大多數情況，這裡是第二層防線，防禦任何其他管道
        漏進來的過期通知），一律丟棄，不誤 latch 目前這一代健康的 child（額外的
        respawn/relogin 循環）。"""
        poll = getattr(self._child, "poll_failstop", None)
        if poll is None:
            return
        while True:
            notice = await asyncio.to_thread(poll, self._failstop_poll_timeout)
            if notice is None:
                continue
            current_generation = getattr(self._child, "generation", None)
            notice_generation = notice.get("generation")
            if current_generation is not None and notice_generation != current_generation:
                log.warning(
                    "agent 收到過期 generation 的 failstop 通知（notice_generation=%r，"
                    "目前 generation=%r），丟棄，避免誤 latch 目前這一代 child",
                    notice_generation, current_generation,
                )
                continue
            # R4-c：把這裡核對過的 notice_generation 原樣往下傳——`_latch()` 拿到
            # `_recovery_lock` 之後會用同一個快照再核對一次，堵住「核對通過→等鎖→child
            # 換代」這段窗口（見其 docstring）。
            await self._latch(notice.get("detail") or "child 回報 buffer 落地失敗",
                              expected_generation=notice_generation)

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
