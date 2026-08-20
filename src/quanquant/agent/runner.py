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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from quanquant.broker.agent_protocol import (
    PROTOCOL_VERSION, DownCancel, DownHealth, DownPlace, DownQueryQty, DownReconcile,
    DownReportAck, DownUpdate, UpCmdAck, UpCommandRejected, UpHealth, UpLogin, UpQueryResult,
    UpReport, parse_downlink,
)
from quanquant.agent.buffer import SentinelUnreadableError
from quanquant.agent.native_runner import child_main
from quanquant.agent.ws_client import TokenRejectedError

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


@dataclass(frozen=True)
class AgentSnapshot:
    """整份替換的不可變快照，由主 event loop 單一擁有（GUI 與 runner 同 loop，讀取
    不需 lock）。connection 的合法值為 "connecting"/"connected"/"reconnecting"/
    "offline"/"rejected"（Task 13：WS 握手被拒，token 無效/停用/非 owner）——寫入這個
    欄位永遠只能經過下面 _connection_state_for_session_exception() 這一個集中判斷點
    （run_once() 例外收攏處），或 stop()／run_forever() 的 TokenRejectedError latch
    分支（見其 docstring，刻意不呼叫 stop() 以免蓋掉 "rejected"），不得在
    _pump/_receive_loop/_heartbeat/_child_watchdog 等個別 task 各自 setattr（多處同時寫
    同一欄位、例外同步收攏無 await 讓出點，會有後寫覆蓋先寫的競態，這是 fresh read-back
    覆核抓到的教訓，Task 13 段落有完整根因分析）。"""
    connection: str          # "connecting" | "connected" | "reconnecting" | "offline" | "rejected"（Task 13 起）
    account: str
    mode: str
    latched: bool
    latch_detail: str | None
    health_epoch: int
    buffer_pending: int
    updated_at: datetime


def _connection_state_for_session_exception(exc: BaseException) -> str:
    """run_once() 收攏本輪 session 結束例外後、re-raise 前的唯一狀態判斷點——刻意抽成
    模組層級純函式，不塞進例外處理的行內邏輯：往後任何『某類例外該對應哪個連線態』的
    規則都只改這一個函式，杜絕分散設定造成的競態。Task 13：`TokenRejectedError`（WS
    握手被拒，見 ws_client.py）→ "rejected"；其餘所有例外沿用預設 "reconnecting"。"""
    if isinstance(exc, TokenRejectedError):
        return "rejected"
    return "reconnecting"


def _select_session_end_exception(exceptions: list[BaseException]) -> BaseException:
    """N6-3（MEDIUM，codex 終審 round6）：`AgentRunner.run_once()` 的 `asyncio.wait(
    FIRST_EXCEPTION)` 回傳的 `done` 是個 `set`，可能同時收攏多個例外——迭代順序不保證。
    這裡收集到的 `exceptions` 依確定性優先序挑一個 raise：`FatalAgentError`（最嚴重，
    不可重試）＞其他一般例外（任一個皆可，語意上等價，`run_forever` 一律照 backoff 重試
    處理）。

    2026-08-08（設計降級，使用者拍板）：原本這裡還有第二層優先序
    `SessionRestartRequested`（in-session recovery 的受控重啟訊號）——隨 G2 恢復降級為
    「只在 agent 程序啟動時做一次 storage probe」（見 `AgentRunner._startup_recovery_
    probe`），`_recover()`/`_recovery_prober()` 整組移除，`SessionRestartRequested` 已無
    任何生產路徑會 raise，一併刪除（以簡為準）。

    理由：若 `FatalAgentError` 被別的例外蓋掉，不可重試的致命錯誤可能被當一般錯誤重試，
    持續燒 Shioaji 每日登入配額。`exceptions` 保證非空（呼叫端只在有例外時才呼叫）。"""
    for exc in exceptions:
        if isinstance(exc, FatalAgentError):
            return exc
    return exceptions[0]


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
                # N9-1（HIGH，codex 終審 round9）：改走 `terminate()`（verify-dead，見其
                # docstring）而非舊版直接 kill()+join()+無條件把 self._process 設 None——
                # `_rpc()` 逾時/pipe 異常時可能已經 poison→terminate 過（`terminate()` 對
                # `self._process is None` 是安全 no-op，不重複 kill）；若這裡才第一次嘗試
                # kill 且驗死失敗（kill+join(5s) 後仍存活，極端情況），`terminate()` 會
                # 保留 handle、回傳 False——必須消費這個回傳值：驗死失敗時不能假裝已清乾淨，
                # 否則呼叫端（`ensure_child()`）下一輪可能誤以為可以安全 start() 出第二個
                # child，變成雙 child 併發碰同一 buffer/broker 帳號。
                died = self.terminate()
                if died is False:
                    raise RuntimeError(
                        f"agent 子程序啟動失敗，且 terminate 未能確認其死亡（kill+join(5s) "
                        f"後仍驗到存活，保留 handle 供人工處理，絕不可再次 start）: {exc}"
                    ) from exc
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

    # Round5（codex 終審 round5 收斂）：`respawn(expected_generation)`（R4-a 加的
    # generation-scoped terminate+start transaction）已隨 `AgentRunner._respawn_child()`
    # 一併移除——child 不會被原地換血，respawn 一律走結束整個 session、交給
    # `run_forever()`→`ensure_child()` 這條既有硬化路徑重新 spawn。`start()`/`terminate()`/
    # `_rpc()`/`poll_failstop()` 共用的 `_lock`＋`_generation` fencing（R3-1）本身保留
    # 不動——那是保護「併發 RPC vs. 任何一次 terminate+start 交替」的通用機制，`ensure_
    # child()` 本來就會呼叫 `terminate()`/`start()`，這道防線仍然需要（2026-08-08：G2 恢復
    # 降級為啟動時 probe 後，`ensure_child()` 仍是這道 fencing 唯一的生產呼叫端——respawn
    # 只發生在 `ChildFrozenError`/一般連線失敗後的下一輪，不再有 recovery 觸發的路徑）。

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
        """R3-2（HIGH，codex 終審 round3）：`ping()` 只回 bool，資訊不足。回傳完整資訊：
        `ok`（存活/有回覆）、`latched`（native_runner 端 `ChildFailstopLatch.tripped`，見
        `_dispatch` 的 ping 分支）；逾時/pipe 異常時 `ok=False, latched=None`（None 代表
        拿不到，呼叫端一律當成不安全處理，不得視為「未 latch」而放行）。

        R4-b（HIGH，codex 終審 round4）：額外帶上 `generation`/`fault_seq`（child 自報，見
        native_runner.py `_dispatch` ping 分支），供呼叫端核對是否與先前觀察到的一致；
        逾時/異常時同樣回 `None`（拿不到，呼叫端不得假設未變）。

        N6-4（codex 終審 round6）：Round5 把 recovery 收斂為 session-restart 後，這個方法
        一度失去唯一呼叫端——原本 `AgentRunner._recover()`（2026-08-08 已隨 G2 恢復降級
        移除，見 `AgentRunner._startup_recovery_probe` docstring）在原地 respawn 後用它
        重驗新 child，隨 `_respawn_child()` 一併移除，留下 dead code。現在 `AgentRunner.
        _child_watchdog()` 改用這個方法取代 `ping()`（見其 docstring），是目前唯一的
        production caller：child 本地 latch 已 tripped 時，即使 pipe 本身仍活著、
        `ok=True`，watchdog 也視為不健康——是 failstop 專用 IPC 通知（`native_runner.py
        _trigger_failstop_latch` 的 `failstop_conn.send()`）萬一失敗時的安全網。"""
        try:
            reply = self.request({"op": "ping"}, timeout=timeout)
        except TimeoutError:
            return {"ok": False, "latched": None, "generation": None, "fault_seq": None}
        return {"ok": bool(reply.get("ok")), "latched": bool(reply.get("latched", False)),
                "generation": reply.get("generation"), "fault_seq": reply.get("fault_seq")}

    def terminate(self, *, expected_generation: int | None = None) -> bool | None:
        """N6-1（HIGH，codex 終審 round6）：`expected_generation`——原本主要供
        `AgentRunner._recover()`（2026-08-08 已隨 G2 恢復降級移除，見
        `AgentRunner._startup_recovery_probe` docstring）的同步 terminate 呼叫使用：
        `asyncio.to_thread` 包裝的呼叫若被取消，底層 thread pool worker 不受影響仍會跑完
        （asyncio 的取消只中斷呼叫端的 await，不會真的停止已提交給執行緒池的任務）；這個
        「孤兒」worker 可能在很久之後（跨過整個 session 結束、下一輪 `ensure_child()`
        已經 respawn 出全新 generation 的 child）才真正拿到 `_lock` 執行到這裡——沒有
        fencing 的話會把新 child 錯殺。呼叫端在**進入鎖之前**捕捉當下的 generation，這裡
        在鎖內（真正動 `_process`/`_conn` 之前）重新核對，不符即 no-op：完全不碰目前這一
        代 process/conn，等同於「這次 terminate 意圖已經過期」。不傳
        （`expected_generation=None`，既有的所有呼叫端——`start()` 失敗清理、
        `_poison()`、`ensure_child()`、`run_forever` 的 `ChildFrozenError` 處理——維持既有
        無條件終止語意，不受影響）。這個機制本身不是 `_recover()` 專屬——`ensure_child()`
        的既有 respawn 路徑（`ChildFrozenError` 後）本來就會呼叫 `terminate()`，一樣受益
        於這道 fencing；`_recover()` 移除後，`expected_generation` 這個選填參數暫時沒有
        production 呼叫端顯式傳值（保留介面，供未來需要精確世代核對的呼叫端使用）。

        R7-1（HIGH，codex 終審 round7，好衛生，shutdown/frozen 路徑仍在用）：舊版
        `kill()`+`join(timeout=5)` 後未檢查 `process.is_alive()` 就無條件把
        `self._process` 設 `None`——`join` 逾時只代表「這 5 秒內沒等到它退出」，不代表真的
        死了；舊版把「遺失 handle」誤當成「確認死亡」，讓 `alive` 屬性此後永遠回報
        `False`（因為 `self._process` 已經是 `None`），即使底層 OS 進程其實仍在跑（仍可能
        繼續寫 sentinel／落地事件）。原本這道二次確認主要是給 `_recover()`（已移除）用來
        判斷要不要繼續清 sentinel/latch；現在保留下來是單純的正確性/衛生修復——任何呼叫端
        （`ensure_child()` 的 respawn、`shutdown`）都不該在子程序其實還活著的情況下誤判
        「已死」。

        修法：保留 local `process` 參照；`kill()`+`join(timeout=5)` 後明確檢查
        `process.is_alive()`——仍活著就**不清** `self._process`/`self._conn`/failstop
        conn（保留 handle，讓呼叫端能重試、`alive` 屬性能誠實反映現況），回傳 `False`。
        只有在（沒有 process 需要處理，或 process 確認已死）時才清理資源。

        回傳值三態（既有呼叫端多半忽略回傳值，見下）：`None`＝generation 已過期／no-op
        （完全沒碰任何 process，比照舊版既有語意，見
        `test_terminate_generation_mismatch_after_concurrent_respawn_is_noop_for_new_child`
        ——這支測試鎖住這個既有回傳值契約，R7-1 沒有改動這一分支）；`True`＝已確認死亡
        （或本來就沒有 process）；`False`＝`kill()`+`join(5s)` 後仍驗到 `alive`（R7-1
        新增）。既有呼叫端（`_poison()`／`start()` 失敗清理／`ensure_child()`／
        `run_forever()` 的 `ChildFrozenError` 處理）目前都忽略回傳值——這些呼叫時機都是
        「pipe 已判死」或「即將整個 respawn」等場景，不需要靠這裡的結果決定要不要動
        durable 狀態；即使忽略回傳值，這些呼叫端也不會因此變得更不安全，因為 `alive`
        屬性本身現在更誠實了。"""
        with self._lock:
            if expected_generation is not None and expected_generation != self._generation:
                log.warning(
                    "N6-1: terminate 呼叫過期（expected_generation=%r，目前 generation="
                    "%r），no-op：不誤殺已換代的新 child", expected_generation,
                    self._generation,
                )
                return None
            if self._process is not None:
                process = self._process
                process.kill()
                process.join(timeout=5)
                if process.is_alive():
                    log.error(
                        "R7-1: ChildHandle.terminate kill()+join(5s) 後 child 仍存活"
                        "（pid=%r）——保留 handle、不清狀態，呼叫端須視為 terminate 失敗、"
                        "重試（不得沿用 process=None 誤判「已死」）",
                        getattr(process, "pid", None),
                    )
                    return False
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
            return True

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
        """RPC 可用（poisoned-aware）——`_poison()` 觸發後即使底層 OS process 還沒真的死
        （見 `process_alive`），這裡也回 False：pipe 已經不可信，不該再被當成可用 child。"""
        with self._lock:
            return (not self._poisoned) and self._process is not None and self._process.is_alive()

    @property
    def process_alive(self) -> bool:
        """N9-1（HIGH，codex 終審 round9）：純粹反映底層 OS process 是否仍在跑，**不**
        像 `alive` 一樣受 `_poisoned` 影響——`_poison()` 只把 RPC pipe 判死（`alive` 因此
        回 False），不代表 process 真的已經退出（唯有 `terminate()` 的 kill()+join()+
        `is_alive()` 驗證才能確認死亡；若驗死失敗，`terminate()` 回 False 且保留 handle，
        `self._process` 仍指向那個活著的 process）。`ensure_child()` 靠這個屬性判斷
        「(re)start 之前是否需要先 terminate 驗死」，不會被 poisoned 狀態遮蔽掉一個其實
        還活著的 process，避免雙 child 併發碰同一個 buffer/broker 帳號。"""
        with self._lock:
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
    if isinstance(msg, DownQueryQty):
        # G3/D8（Task 2 審查發現的缺口，本 task 補上）：DownQueryQty 是唯讀指令，
        # 與 DownReconcile 走同一條 _execute_readonly_command 路徑（回 UpQueryResult）。
        return {"op": "query_qty", "ordno": msg.ordno}
    raise ValueError(f"未知下行指令: {msg!r}")


class AgentRunner:
    def __init__(self, *, transport, buffer, child, mode: str = "sim",
                 pump_interval: float = 0.1, resend_after: float = 5.0,
                 child_command_timeout: float = 8.0, child_ping_interval: float = 10.0,
                 child_ping_timeout: float = 20.0, heartbeat_interval: float = 15.0,
                 backoff_base: float = 1.0, backoff_max: float = 60.0,
                 stable_session_seconds: float = 30.0,
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
        # （latch/epoch++/sentinel 寫入），絕不 await 任何網路 I/O（R3-3）——WS send 一律
        # 搬到 lock 外，見 `_health_sender`。2026-08-08（設計降級，使用者拍板）：G2 恢復
        # 不再有「session 進行中週期性重驗」這個概念——`_recovery_probe_interval`／
        # `_recovery_prober()`／`_recover()` 整組移除；恢復檢查改成只在 `run_forever()`
        # 開始前做一次（見 `_startup_recovery_probe`），這把鎖仍然保護 `_latch()` 與
        # `_startup_recovery_probe` 對 `_latched`/`_health_epoch`/sentinel 的互斥存取。
        self._failstop_poll_timeout = failstop_poll_timeout
        self._recovery_lock = asyncio.Lock()
        self._latched = False
        self._latch_detail: str | None = None
        self._health_epoch = 0
        self._health_queue: asyncio.Queue = asyncio.Queue()
        self._load_persisted_health()
        # Task 7：連線狀態觀測面（GUI snapshot 用）。合法值見 AgentSnapshot docstring；
        # 寫入點僅限 run_once()（連線成功/例外收攏）與 stop()，見各處註記——不得在
        # _pump/_receive_loop/_heartbeat/_child_watchdog 等個別 task 內 setattr。
        self._connection_state = "connecting"

    def _load_persisted_health(self) -> None:
        """啟動時（同步、建構子內）讀取 durable 狀態：sentinel 檔存在＝latch 仍在效（buffer
        之外路徑，見 buffer.py write_sentinel docstring）——epoch 取 sentinel 記的值與 buffer
        meta 存的值兩者較大者（sentinel 可能記著父程序來不及覆寫的佔位值 -1，這種情況下改信
        buffer meta；buffer meta 若因為同一次故障也寫不進去，則沿用 sentinel 的值）。沒有
        sentinel 時單純讀 buffer meta（預設 0，全新 buffer 或從未 latch 過）。

        N9-3（MEDIUM，codex 終審 round9）：`read_sentinel()` 現在區分「真的沒有 sentinel」
        （`None`，檔案不存在）與「sentinel 存在但讀不出來」（`SentinelUnreadableError`，
        權限錯誤／JSON 損毀）——舊版把兩者都當「無 latch」處理，可能讓一個其實還在
        failstop 的 agent 誤上報 `status="ok"`（fail-open）。這裡 fail-closed：讀取失敗
        一律視為仍在 latch，epoch 退回 buffer meta 記的值（sentinel 本身讀不到，唯一還能
        信的來源），記明確錯誤 log，供操作者人工排查底層 sentinel 檔案（可能需要修復權限，
        或確認內容損毀程度後決定要不要人工刪除重來——這裡不自動刪除，避免銷毀故障診斷
        證據）。"""
        try:
            sentinel = self._buffer.read_sentinel()
        except SentinelUnreadableError:
            log.error(
                "N9-3: sentinel 檔存在但讀取/解析失敗（權限錯誤或內容損毀），fail-closed"
                "：視為仍在 failstop latch，需要人工排查底層 sentinel 檔案（buffer=%s）",
                self._buffer.path,
            )
            self._latched = True
            self._latch_detail = "sentinel 檔讀取失敗（fail-closed，需人工排查底層檔案）"
            self._health_epoch = self._buffer.get_health_epoch()
            return
        if sentinel is not None:
            self._latched = True
            self._latch_detail = sentinel.get("detail")
            self._health_epoch = max(int(sentinel.get("epoch", 0)), self._buffer.get_health_epoch())
        else:
            self._health_epoch = self._buffer.get_health_epoch()

    def ensure_child(self) -> None:
        """child 未活則 (re)start；child alive 但 self._account 遺失（接手他人已在跑的
        child 的邊界情況）視同需要重啟。回傳後 self._account 必為非空。

        N9-1（HIGH，codex 終審 round9）：舊版只在 `self._child.alive` 為 True 時才呼叫
        `terminate()`——但 `alive` 是 `(not poisoned) and process.is_alive()`，`_poison()`
        可能已經把 `alive` 打成 False（RPC pipe 判死），底層 OS process 卻仍在跑（例如
        `_rpc()` 逾時觸發的 `terminate()` 驗死失敗，保留 handle 不變）。這種「poisoned 但
        process 其實還活著」的狀態下，舊碼會被 `alive` 遮蔽、整段 terminate 分支直接跳過，
        落到 `self._child.start()` 在舊 process 還沒死之前又 spawn 出第二個 child——雙
        child 併發碰同一個 buffer/broker 帳號。

        修法：一律用 `process_alive`（純 process 存活判斷，不受 poisoned 影響；`getattr`
        容錯——非 `ChildHandle` 的測試替身沒有這個屬性時退回 `alive`，維持既有行為，比照
        `_failstop_watchdog` 對 `poll_failstop` 的既有 `getattr` 容錯慣例）決定要不要
        terminate；terminate 之後**消費回傳值**：`False`（kill+join(5s) 後仍驗到存活）
        代表無法確認死亡，絕不允許在這個狀態下 start() 出第二個 child——raise
        `FatalAgentError`，讓 `run_forever` 直接停止（不 respawn、不 backoff），需要操作者
        人工處理殘留的 process 後才能重啟這個 agent 程序。"""
        if self._child.alive and self._account:
            return
        process_alive = getattr(self._child, "process_alive", self._child.alive)
        if process_alive:
            died = self._child.terminate()
            if died is False:
                raise FatalAgentError(
                    "agent 子程序無法終止（kill+join 逾時後仍驗到存活），拒絕啟動第二個"
                    "child（避免雙 child 併發碰同一 buffer/broker 帳號）——請人工處理殘留"
                    "的子程序後重啟這個 agent 程序"
                )
        self._account = self._child.start()

    async def _latch(self, detail: str, *, expected_generation: int | None = None) -> None:
        """G2①/⑤/⑦：本機原子轉移（latch=True、epoch+=1、sentinel 寫入），只受
        `_recovery_lock` 保護——**絕不 await 任何網路 I/O**（R3-3）：即使 `_health_sender`
        當下卡在一個緩慢/卡住的 `transport.send()`，也不會拖住這裡，因為 sender 只在
        「重驗」那一小段（純記憶體讀取）才持有這把鎖，實際送出永遠在鎖外（見
        `_health_sender`）。latch 完成後把新 epoch 推進健康佇列，交給單一序列化 sender
        擇機送出（不在這裡直接送）。

        R4-c（MEDIUM，codex 終審 round4）：`expected_generation`——呼叫端（`_failstop_
        watchdog`）在鎖外核對過 notice 的 generation 與當下 `self._child.generation` 相符
        後才呼叫這裡；但「核對通過」與「真正拿到 `_recovery_lock`」之間仍有一段沒有互斥
        的窗口（`_recovery_lock` 可能正被另一個 `_latch()` 呼叫佔住），這段
        期間 child 若換代，鎖外核對過的 generation 就過期了。這裡在拿到鎖之後、真正改動
        任何狀態之前，用同一個快照重新核對一次——不符即 no-op（完全不動 `_latched`/
        `_health_epoch`/sentinel），避免把一則過期通知套用到目前這一代健康的新 child 身上。
        呼叫端不傳（`expected_generation=None`，既有的直接呼叫路徑／測試）視為「呼叫端
        自己已經確保沒有這個競態」，照舊無條件 latch。"""
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
            await asyncio.to_thread(self._buffer.write_sentinel, epoch=epoch, detail=detail)
        self._health_queue.put_nowait(epoch)

    async def _startup_recovery_probe(self) -> None:
        """G2④ 手動重啟恢復（2026-08-08，設計降級，使用者拍板，取代 Round3-7 打磨出的
        session-restart 自動恢復）：**恢復檢查只在 agent 程序啟動時做一次**——`run_forever()`
        在進入主迴圈之前呼叫這裡，此刻 `ensure_child()` 尚未被呼叫過、沒有任何 child
        程序、也沒有任何並發的 sentinel writer（唯一的 writer 是 child 的 callback 執行緒，
        見 `native_runner.py::_trigger_failstop_latch`）。

        舊版 `_recover()`（Round3-7，8 輪終審逐輪打磨）處理的每一個縫——探針期間 child 又
        故障（dying-gasp/fault_token 比對）、terminate 是否真的驗死、`asyncio.to_thread`
        取消後孤兒 worker 遲到、清 sentinel 與 child 寫入之間的 replace/unlink TOCTOU——
        全部源自「必須在一個活著的 session 內、跟一個可能仍在寫 sentinel 的 child 打交道」
        這個前提。在程序啟動的這個時間點，這個前提結構性不成立：沒有 child 可以在 probe
        之後、之前又寫入新故障，也沒有其他 thread/process 會併發碰 buffer/sentinel——這裡
        因此不需要 terminate、不需要 dying-gasp 偵測、不需要 generation fencing，只是單純
        的「讀一次 sentinel、探一次 buffer、決定要不要清」。

        流程：
          1. 建構子的 `_load_persisted_health()` 已經讀過 sentinel——`self._latched` 為
             `False`（沒有 latch，或 buffer 本來就是全新的）時直接返回，不做任何探測（不對
             健康的 buffer 做無謂的探測寫入）。
          2. `latched` 時對同一 buffer 執行一次 storage probe（`DurableBuffer.probe()`：
             寫→commit→讀回）。
          3. 探針**失敗**：保持 latched，agent 以 failstop 模式運行（native 呼叫前的
             latch gate——`_execute_mutating_command`／`_dispatch`——繼續拒絕一切
             mutating 指令，health sender 繼續回報 `status="failstop"`）。這裡**不**排
             程重試——底層儲存問題需要操作者人工修復，修好後手動重啟這個 agent 程序（新一輪
             `run_forever()` 呼叫會再探一次）。
          4. 探針**通過**：持久化 epoch（`set_health_epoch`）→清 sentinel
             （`clear_sentinel`）→`self._latched = False`。持久化任一步失敗：保留
             latched（C3 的邏輯沿用——不能讓 in-memory 狀態「假裝恢復」但 durable 狀態其實
             沒有真的恢復），記錯，直接返回（同樣不重試，等下一次程序重啟）。全部成功才記一行
             info log「已從 failstop 恢復（啟動時儲存探測通過）」，供操作者從 log 確認。

        session 進行中 latch 後永不自動解除——這個方法只在 `run_forever()` 開始前被呼叫
        一次，之後任何一次 `_latch()`（child 經 failstop IPC 通知父程序觸發）都不會再被
        任何背景迴圈重新探測；唯一能讓 `self._latched` 回到 `False` 的路徑是重啟整個
        agent 程序（新的 `AgentRunner`/`run_forever()` 呼叫）。

        `_recover()`／`_recovery_prober()`／`SessionRestartRequested`／dying-gasp
        fault_token 比對／`ChildHandle.terminate(expected_generation=...)` 在 recovery
        路徑上的呼叫，隨此次降級整組移除——`fault_token` 欄位本身保留在 sentinel
        （`buffer.py::write_sentinel`）當純診斷資訊，`ChildHandle.terminate()` 的
        `expected_generation` fencing／is_alive 驗死機制也保留（`ensure_child()` 的
        respawn 路徑、shutdown 路徑仍在用），只是不再有 recovery 這個呼叫端。詳細推演見
        `.superpowers/sdd/codex-final-fixes-report.md` Round 8 fixes 段。"""
        async with self._recovery_lock:
            if not self._latched:
                return
            ok = await asyncio.to_thread(self._buffer.probe)
            if not ok:
                log.error(
                    "agent 啟動時 storage probe 失敗，保持 failstop latch——請修復底層"
                    "儲存問題後重啟這個 agent 程序（不會自動重試）"
                )
                return
            epoch = self._health_epoch
            try:
                await asyncio.to_thread(self._buffer.set_health_epoch, epoch)
                await asyncio.to_thread(self._buffer.clear_sentinel)
            except Exception:
                log.exception(
                    "G2④ 啟動時恢復的持久化步驟失敗（epoch 寫入／sentinel 清除），保持"
                    "failstop latch——請修復底層儲存問題後重啟這個 agent 程序"
                )
                return
            self._latched = False
            self._latch_detail = None
        log.info("已從 failstop 恢復（啟動時儲存探測通過）")

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
        重設重連 backoff（見 module docstring）。

        每次新 session 開始都重新 `_load_persisted_health()`——不只建構子跑一次：上一輪
        session 若因為 `ChildFrozenError`／一般例外中途打斷，這裡會重新讀到正確的 durable
        狀態，不會誤以為自己是健康的（2026-08-08 之後，唯一會改動 durable latch 狀態的
        路徑是 `_latch()`〔trip〕與 `run_forever()` 開頭的 `_startup_recovery_probe()`
        〔啟動時 probe〕，這裡的重讀是純防禦層，不承擔收斂任何 TOCTOU 的責任）。

        N6-3（MEDIUM，codex 終審 round6）：`asyncio.wait(FIRST_EXCEPTION)` 的 `done` 是個
        `set`，可能同時收攏多個例外——`set` 的迭代順序不保證，「取第一個」等同看 hash 排序
        碰運氣：`FatalAgentError` 若被別的例外蓋掉，不可重試的致命錯誤可能被當一般錯誤
        重試，持續燒 Shioaji 登入配額。改用 `_select_session_end_exception()` 收集 `done`
        裡全部例外、依確定性優先序（`FatalAgentError`＞其他）選一個 raise，不受 set 迭代
        順序影響。"""
        self._load_persisted_health()
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
            # Task 7：WS 連線成功、UpLogin 送出後 → "connected"（主 loop 上直接賦值，
            # 不需 call_soon_threadsafe，見 AgentSnapshot docstring）。
            self._connection_state = "connected"
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
            ]
            done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            exceptions = [exc for t in done if (exc := t.exception()) is not None]
            if exceptions:
                exc = _select_session_end_exception(exceptions)
                # Task 7：唯一允許寫入 self._connection_state 的例外收攏點——不得在
                # _pump/_receive_loop/_heartbeat/_child_watchdog 等個別 task 內各自
                # setattr（見 AgentSnapshot/_connection_state_for_session_exception
                # docstring）。
                self._connection_state = _connection_state_for_session_exception(exc)
                raise exc
        finally:
            for t in tasks:
                t.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await self._transport.close()

    async def run_forever(self, *, stop_on_token_reject: bool = False) -> None:
        """不斷跑 run_once()：連線/子程序凍結例外時依 backoff 重試（backoff×2 封頂
        backoff_max）；ChildFrozenError 則 terminate 子程序，下一輪 ensure_child()
        respawn＋（run_once 內）重新登入——server 端視角即 WS 斷線→重連→重新 login，
        觸發既有 login handler 的 reconcile（T0.2）。backoff 重設判準是本輪 session
        「存活時間達 stable_session_seconds」，而非「是否送出 login」（Task 13 修復：
        login 在四個 session task 啟動前就送出，幾乎所有失敗模式都發生在 login 送出之後，
        用它當判準會讓 backoff 每輪重設、指數退避失效——凍結 respawn 迴圈會無節制地重打
        Shioaji 登入，燒每日 1000 次額度）。stop() 後迴圈結束。

        2026-08-08（設計降級，使用者拍板）：進入主迴圈之前先呼叫一次
        `_startup_recovery_probe()`（G2 恢復——見其 docstring）——這是整條 agent 程序生命
        週期裡唯一一次自動恢復嘗試；此後任何一輪 `run_once()` 的失敗/重試都不會再觸發
        任何恢復檢查，session 進行中被 latch 就一路 latch 到程序被人工重啟為止。舊版
        `SessionRestartRequested`（`_recover()` 完成本機轉移後主動拋出、`run_forever` 需
        特別處理不重設 backoff 的受控訊號）隨 `_recover()` 一併移除——現在沒有任何路徑會
        因為「recovery 完成」而結束 session，下面的 except 分支只剩 `FatalAgentError`／
        `ChildFrozenError`／`TokenRejectedError`／一般例外四種既有情境。

        `stop_on_token_reject`（Task 13，預設 `False`，G5 紅線）：`TokenRejectedError`
        （WS 握手被 server 以 close code 1008 拒絕，見 ws_client.py）發生時是否停止重試。
        headless 呼叫端（`main.py`）完全不改、不傳這個參數，永遠是 `False`——沿用既有
        「當一般例外處理、照 backoff 繼續重連」的行為，逐位元組不變。只有 GUI coordinator
        的 kind="direct" 快速連線路徑顯式傳 `True`：token 已知失效時不該無限重試（每輪都是
        一次真的握手嘗試），而是讓使用者知道要重新授權——這是整條 agent 程式碼裡唯一允許
        停用無限重試的地方。"""
        await self._startup_recovery_probe()
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
                # N9-1（HIGH，codex 終審 round9）：消費 terminate() 的回傳值——`False`
                # （kill+join(5s) 後仍驗到存活）代表無法確認死亡，絕不能假裝乾淨後照舊
                # respawn（下一輪 `ensure_child()` 會在舊 process 還活著時又 start() 出
                # 第二個 child，雙 child 併發碰同一 buffer/broker 帳號）。改以明確的 fatal
                # 錯誤停止整個 agent 程序（不 respawn、不 backoff），需要操作者人工處理。
                died = self._child.terminate()
                if died is False:
                    log.error(
                        "N9-1: child 無法終止（kill+join 逾時後仍驗到存活），拒絕 respawn，"
                        "請人工處理"
                    )
                    self.stop()
                    raise FatalAgentError(
                        "agent 子程序無法終止，請人工處理（ChildFrozenError 後 terminate "
                        "驗死失敗，拒絕 respawn 避免雙 child）"
                    )
            except TokenRejectedError:
                if stop_on_token_reject:
                    log.error(
                        "agent WS 握手被 server 拒絕（token 無效/停用/非 owner），"
                        "不再重試，需要重新授權"
                    )
                    # 刻意不呼叫 self.stop()：那會把 self._connection_state 蓋成
                    # "offline"，蓋掉 run_once() 集中點剛寫入的 "rejected"（更精確的
                    # 診斷資訊——GUI probe_direct_connect() 靠這個值判斷要不要導去重新
                    # 授權）。只設 _stopping=True 讓主迴圈不再重試，等價於 stop() 的
                    # 「不再重試」語意，但保留 "rejected" 這個更有資訊量的終態。
                    self._stopping = True
                    return
                log.exception("agent WS session 異常結束，準備依 backoff 重連")
                # 不 stop_on_token_reject（headless 預設）：與既有「一般例外」分支寫一模
                # 一樣的 log 訊息、不 return，falls through 到下面共用的 elapsed/backoff
                # 計算，繼續正常重試——這是 G5 的直接保證：headless 呼叫端不傳這個參數，
                # 預設 False，控制流/日誌逐位不變。
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
        self._connection_state = "offline"

    async def snapshot(self) -> AgentSnapshot:
        """組出目前快照。buffer_pending 走 asyncio.to_thread(self._buffer.unsent_count)
        （SQLite 同步 I/O 不壓 loop，repo 既有鐵律）——這個 await 完成後才寫回，本身就在
        event loop 上執行，不需要 call_soon_threadsafe；目前程式庫裡沒有任何『子執行緒
        直接寫快照』的呼叫點（buffer 計數走 to_thread 但結果回到呼叫者所在的 loop 才處理），
        若未來新增子執行緒直接寫入的路徑，一律要包 loop.call_soon_threadsafe(...)，不得
        繞過——這是 spec §6.4 不變量，即使目前沒有具體呼叫點也要保留這條規則供未來遵守。"""
        pending = await asyncio.to_thread(self._buffer.unsent_count)
        return AgentSnapshot(
            connection=self._connection_state,
            account=self._account,
            mode=self._mode,
            latched=self._latched,
            latch_detail=self._latch_detail,
            health_epoch=self._health_epoch,
            buffer_pending=pending,
            updated_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )

    async def _pump(self) -> None:
        """check-first / sleep-after（延遲優化，取代舊版 sleep-first）：先查
        `self._buffer.pending(50)`；本輪只要有 ≥1 列被實際送出，就立刻回圈頂重新
        `pending(50)`（drain-until-empty），不會為了「湊滿一輪」讓剛落地、甚至已經
        backlog 多批的回報平白多等一輪 pump_interval。本輪「零送出」才
        `sleep(self._pump_interval)`——零送出有兩種成因：`pending()` 真的查到空批，
        或查到的列全部卡在 `resend_after` 冷卻窗內被下面的 `continue` 跳過（回歸修復：
        後者若誤判成「有事做」而不睡，會在冷卻窗內〔最長 resend_after〕密集空轉查
        SQLite）。睡眠判準因此是「這輪有沒有實際送出東西」，不是「pending() 是否為空」；
        `resend_after`/`self._inflight` 的補送判斷邏輯本身不變。"""
        while True:
            pending = await asyncio.to_thread(self._buffer.pending, 50)
            now = time.monotonic()
            sent_any = False
            for row in pending:
                sent_at = self._inflight.get(row.id)
                if sent_at is not None and (now - sent_at) < self._resend_after:
                    continue
                await self._transport.send(self._to_uplink(row).model_dump())
                self._inflight[row.id] = now
                sent_any = True
            if not sent_any:
                await asyncio.sleep(self._pump_interval)

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

    async def _child_watchdog(self) -> None:
        """定期 ping SDK 子程序（#203 凍結偵測）：N6（順帶，codex 終審 round6）改用
        `ping_detail()`（不再只看 `ping()` 的 bool——`ping_detail` 因此有了第一個
        production caller，解掉先前的 dead code diagnostic，見其 docstring）。逾時/例外
        （含 `ok=False`）一律視為凍結；child 本地 latch（`ChildFailstopLatch`，見
        native_runner.py）已 tripped 時，即使 pipe 本身仍活著、仍能正常回應 ping，也一律
        視為不健康——latch 理論上應該已經透過獨立的 failstop IPC channel
        （`_failstop_watchdog`）通知父程序並觸發 `_latch()`，這裡是額外一層安全網：萬一
        那條 IPC 通知本身失敗（`native_runner.py::_trigger_failstop_latch` 的
        `failstop_conn.send()` 是 best-effort、吞例外），watchdog 仍能靠 `ping_detail`
        獨立偵測到 latch。

        N9-2（HIGH，codex 終審 round9）：`ok=False`／例外兩種情況直接 raise
        `ChildFrozenError`，`run_forever` 接手 terminate+respawn+重新登入（既有處理，只是
        單純的凍結，不代表資料層真的落地失敗）。但 `latched=True` 這個分支語意不同——這是
        「sentinel＋failstop IPC 都可能已經失敗，只剩 ping 備援發現故障」的情境，若還是只
        raise `ChildFrozenError` 讓 respawn 走既有路徑，父程序自己從未真正 latch，新 child
        起來後會照常上報 `status="ok"`（假 healthy，故障被悄悄吞掉）。這裡先
        `await self._latch(...)` 提升成 parent latch（durable，session 進行中不會自動
        解除）之後才 raise，respawn 後的新 session 因此天然維持在 failstop，直到操作者
        人工重啟這個 agent 程序。"""
        while True:
            await asyncio.sleep(self._child_ping_interval)
            try:
                detail = await asyncio.to_thread(
                    self._child.ping_detail, timeout=self._child_ping_timeout
                )
            except Exception as exc:
                raise ChildFrozenError(
                    f"agent 子程序 ping 例外，疑似凍結（issue #203）: {exc}"
                ) from exc
            if not detail.get("ok"):
                raise ChildFrozenError("agent 子程序 ping 逾時/失敗，疑似凍結（issue #203）")
            if detail.get("latched"):
                # N9-2（HIGH，codex 終審 round9）：這裡本身就是 failstop IPC 通知失敗的
                # 安全網（見上）——舊版只 raise ChildFrozenError 讓 run_forever 走既有
                # respawn 路徑，父程序自己從未真正 latch（`self._latched` 仍是 False）。
                # respawn 出的新 child 起來後，`AgentRunner` 完全不知道舊 child 曾經 latch
                # 過，會照常上報 status="ok"——假 healthy：sentinel/IPC 都失敗、只剩這道
                # ping 備援發現故障，但故障本身沒有被記錄下來，操作者看到的儀表板仍是綠的。
                #
                # 修法：先以這個 generation `await self._latch(...)`（確立 parent
                # in-memory latch＋epoch++＋durable sentinel 補寫，見其 docstring——不會
                # await 任何網路 I/O，不拖住這個 watchdog 迴圈），latch 完成之後才 raise
                # ChildFrozenError。之後的 respawn／新 session 因此天然處於 failstop——
                # `self._latched` session 進行中永不自動解除（G2 手動重啟恢復的既有保證），
                # native 呼叫前的 latch gate 繼續拒絕 mutating 指令，health sender 繼續
                # 回報 status="failstop"，直到操作者人工重啟這個 agent 程序。
                await self._latch(
                    "watchdog ping_detail 偵測到 child 本地 latch 已 tripped（sentinel/"
                    f"failstop IPC 通知可能雙失敗，fault_seq={detail.get('fault_seq')}）",
                    expected_generation=detail.get("generation"),
                )
                raise ChildFrozenError(
                    "agent 子程序回報本地 latch 已 tripped（N9-2 watchdog 安全網，"
                    "sentinel/failstop IPC 通知可能雙失敗），已提升為 parent latch，"
                    "視為不健康，terminate 後下一輪 respawn（respawn 後仍維持 failstop，"
                    "需人工重啟這個 agent 程序才能恢復）"
                )
