"""SDK 子程序主迴圈（#203 隔離，Inc0 Task 11）：把所有直接碰 Shioaji SDK 的呼叫關進獨立
程序（本機 agent），父程序（web app）永不 import shioaji——SDK 內部的 GIL 卡死/未知崩潰
（#203）只會拖垮子程序，不會波及主服務。

`child_main` 是子程序的唯一進入點：對 `conn`（`multiprocessing.Connection`，測試用
`threading` 版一樣走 `multiprocessing.Pipe()`）逐一收 `{"op": ...}` dict、序列執行（native
SDK 呼叫本來就非執行緒安全，全部關在這條迴圈內序列化，不額外開執行緒池）、回覆 reply dict。
任何例外一律 catch 並回覆結構化錯誤，迴圈本身不因單一 op 失敗而中斷——避免一次暫時性錯誤
（例如 update 打到已結案委託）拖死整條子程序，父程序被迫重新 spawn。

mode!="sim" 時（防禦層 3，Increment 0 只做 sim）：完全不建立 native client（避免任何意外
碰到真實 SDK/真實帳戶），所有 op（含 connect 本身）一律回 mode_mismatch。

callback：native client 建構時傳入 `on_raw=buffer.append`——SDK callback 執行緒（Solace/
.NET）直接同步呼叫，`DurableBuffer.append()` 落地成功（INSERT+commit）才返回，符合 T0.1
「跨程序保存、零丟單」的設計（見 `agent/buffer.py`）。

Inc1 D9/G2①（Task 12）：callback 落地失敗時的雙層防線——主寫入（`buffer.append`，SQLite）
失敗 → 退化寫入（`_try_degraded_write`，純檔案 append，與 SQLite 完全獨立的 I/O 路徑）；
兩者皆失敗才是真正的「落地失敗」：寫 sentinel（`buffer.write_sentinel`，buffer 之外路徑，
durable）＋經**專用 IPC channel**（`failstop_conn`，`child_main` 的獨立參數，R2-8：絕不
與既有 RPC pipe 共用——那條 pipe 靠 `rpc_id` 比對，混用會讓遲到的 failstop 通知被誤判成
「上一輪逾時後才姍姍來遲的 reply」直接丟棄，或反過來污染正常 RPC 的 rpc_id 序列）通知父
程序。父程序收到通知後才是真正決定 latch／epoch++ 的權威（`runner.py` AgentRunner._latch）
——child 端只負責「盡力通知＋盡力寫 sentinel」，不持有 epoch 狀態（那需要跨程序協調，交給
父程序統一管理）。
"""
import json
import logging
import os
import threading
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from quanquant.agent.buffer import DurableBuffer
from quanquant.broker.base import TradeNotFoundError
from quanquant.broker.redaction import redact_secrets

log = logging.getLogger(__name__)

_MODE_MISMATCH_MESSAGE = "Increment 0 僅支援 sim"


def _default_native_factory(*, credentials: dict, symbol: str, mode: str,
                             on_raw: Callable[[str, dict], None]):
    from quanquant.broker.native import ShioajiNativeClient

    return ShioajiNativeClient(
        api_key=credentials["api_key"], secret_key=credentials["secret_key"],
        ca_path=None, ca_passwd=None, person_id=None,
        symbol=symbol, mode=mode, on_raw=on_raw,
    )


class ChildFailstopLatch:
    """C4（HIGH，codex 終審）：child 進程內 thread-safe latch——SDK callback 執行緒
    （Solace/.NET，與 `child_main` 主迴圈不同執行緒）偵測到 buffer 落地失敗時同步
    `trip()`（純記憶體 `threading.Event`，不必等父程序 IPC round-trip），`_dispatch` 在
    真正呼叫 native place/cancel/update **之前**再檢查一次，堵住「父程序最後一次
    `AgentRunner._latched` 檢查通過後、`asyncio.to_thread(child.request, ...)` 尚未真正
    排程執行前」這段 asyncio 排程邊界的競態窗——那段期間父程序自己的 `_latch()`（受
    `_recovery_lock` 保護、經 IPC round-trip 才會被 `_failstop_watchdog` 處理）可能還沒
    完成，但這個特定 RPC 已經送到子程序、來不及被父程序攔下。子程序本地判定天然比父程序
    更即時（同進程、無 IPC round-trip），是這道縫唯一堵得住的地方；父 latch（G2①⑤⑦）續管
    sentinel/epoch/health 這些跨程序協調責任不變，這裡只加一道「native 呼叫前再確認」的
    本地防線。`threading.Event` 天然 thread-safe，`child_main` 主迴圈（單執行緒）與 SDK
    callback 執行緒可安全共用同一份，不需要額外的鎖。"""

    def __init__(self) -> None:
        self._event = threading.Event()

    def trip(self) -> None:
        self._event.set()

    @property
    def tripped(self) -> bool:
        return self._event.is_set()


def _trigger_failstop_latch(buffer: DurableBuffer, failstop_conn, latch, detail: str) -> None:
    """C4/C5（HIGH，codex 終審）共用：callback 主寫入失敗（不論退化寫入是否成功）觸發的
    latch 動作——trip child 本地 thread-safe latch（C4，供 `_dispatch` 呼叫 native 前再檢查）
    ＋寫 sentinel（durable，buffer 之外路徑）＋經專用 IPC channel（R2-8）通知父程序。

    N1（HIGH，codex 終審 round2）：`latch.trip()` 必須是**第一個動作**，排在任何 I/O
    （sentinel fsync／IPC send）之前——`trip()` 只是設一個 `threading.Event`，純記憶體、
    不阻塞；若像舊版一樣把它排在 sentinel/IPC 之後，這兩段 I/O（尤其 sentinel 的
    `os.fsync`）卡住的期間，`_dispatch`（child 主迴圈，與 callback 不同執行緒）仍會讀到
    `latch.tripped is False`，放行新的 mutating native 呼叫——這正是 child latch「故障後
    沒有立即 trip」的縫。trip 本身冪等（`Event.set()` 重複呼叫安全），下面 `_wrap_on_raw`
    也會在呼叫本函式之前搶先 trip 一次，這裡重複 trip 不影響正確性，只是防禦性地確保
    「即使有其他呼叫路徑漏了搶先 trip，這裡仍第一手補上」。sentinel/IPC 通知各自吞例外
    （本身失敗不能讓呼叫端更難排錯，仍靠子程序心跳凍結偵測——issue #203 既有防線——當
    最後防線）。"""
    if latch is not None:
        latch.trip()
    log.error("callback 主寫入落地失敗，觸發 G2 fail-stop latch: %s", detail)
    try:
        # child 不持有 epoch 狀態（那由父程序統一管理）——這裡寫的 epoch=-1 只是佔位，
        # 父程序收到 IPC 通知後會用自己遞增後的權威值覆寫這個 sentinel（見 runner.py
        # AgentRunner._latch）；即使父程序來不及覆寫就再次崩潰，sentinel 存在本身已經
        # 足以讓下次啟動視為 latch（durable 的定義只看「存在與否」，不依賴這裡的 epoch
        # 值精確與否）。
        buffer.write_sentinel(epoch=-1, detail=detail)
    except Exception:
        log.error("sentinel 寫入也失敗，僅能靠 IPC 通知父程序（若 IPC 也失敗，"
                  "父程序仍會靠子程序心跳凍結偵測——issue #203 既有防線——察覺異常）")
    if failstop_conn is not None:
        try:
            failstop_conn.send({"type": "failstop", "detail": detail})
        except Exception:
            log.error("failstop IPC 通知也失敗——callback 執行緒已無法對外示警")


class _AccountBox:
    """account 只有 connect 成功後才知道，但 `on_raw` callback 在 native client 建構時
    （connect 之前）就要註冊完畢——用一個可變容器讓 wrapper 延遲讀取，connect 成功時才
    填值（Task 9 D5/I7：callback 落 outbox 當下蓋章 account/mode，來源端不可變）。"""
    __slots__ = ("value",)

    def __init__(self) -> None:
        self.value: str | None = None


def _degraded_write_path(buffer: DurableBuffer) -> Path:
    return Path(buffer.path + ".degraded.jsonl")


def _try_degraded_write(buffer: DurableBuffer, kind: str, payload: dict, *,
                         account: str | None, mode: str) -> None:
    """G2①退化寫入：SQLite 主寫入（`buffer.append`）失敗時的最後手段——純檔案
    append-only（獨立於 SQLite 之外的 I/O 路徑，SQLite 損壞——如 WAL 檔鎖死/磁碟配額
    ——不必然拖累這裡的成功率）。任何例外原樣往外拋，呼叫端（`_wrap_on_raw`）據以判定
    「雙寫皆失敗」，觸發 G2① 的 sentinel＋IPC 通知流程。"""
    path = _degraded_write_path(buffer)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(
        {"kind": kind, "payload": payload, "account": account, "mode": mode},
        ensure_ascii=False,
    )
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())


def _wrap_on_raw(buffer: DurableBuffer, *, mode: str, account_box: _AccountBox,
                  failstop_conn=None, latch: "ChildFailstopLatch | None" = None):
    """把 `buffer.append` 包一層，補上 D5/I7 要求的 account/mode 蓋章——SDK callback
    只給 (kind, payload)，account 從 `account_box`（connect 成功後才填）動態讀取。

    C5（HIGH，codex 終審，收緊自 spec 原案「雙寫失敗才 latch」）：**主寫入（SQLite）一旦
    失敗就直接 latch**——不再讓「退化寫入是否成功」決定要不要 latch。理由：退化寫入
    （`_try_degraded_write`）是純檔案 append-only，缺乏 SQLite 的交易/索引/查詢能力，
    只是「事後人工救援」的最後手段，不是與主寫入等價的替代落地路徑；agent 端 outbox
    at-least-once（`runner.py::AgentRunner._pump` 只讀 SQLite buffer）完全看不到只落在
    退化檔的事件，也沒有自動 reinjection 機制——舊版「退化寫入成功就不 latch」會讓這些
    事件在沒有任何顯式訊號的情況下悄悄從零丟單（I1）保證裡漏出去（codex 終審 C5 原話：
    「degraded JSONL 成功即回傳、不 latch、又無 reinjection → I1 中斷」）。退化寫入仍然
    嘗試（降低真的完全遺失的機率、留人工救援線索），但寫入結果只影響訊息內容與回傳值，
    不再影響是否 latch 的決定：
      - 主寫入失敗＋退化寫入成功：仍 latch（`_trigger_failstop_latch`）＋回傳 -1
        （沒有 SQLite row id 可回，呼叫端本就不依賴它），**不** raise（維持既有回傳語意，
        呼叫端/callback thread 不需要另外處理例外）。
      - 主寫入失敗＋退化寫入也失敗：仍 latch＋原樣 re-raise（callback 執行緒/呼叫端仍需要
        知道這次真的沒有落地——SDK callback 若因此失敗，`child_main`/`native.py::
        _on_order_cb` 的既有 try/except 會把它轉成結構化的 `error_kind="exception"`
        reply 或再試一次 `_unparsed` 退化重試，這是本來就有的正常錯誤回報路徑，本函式
        不改變它）。

    C4：`latch`（`ChildFailstopLatch`，child 進程內 thread-safe，見其 docstring）在任一次
    主寫入失敗時同步 trip——與父程序的 IPC round-trip 無關，`_dispatch` 在真正呼叫 native
    place/cancel/update 前會再檢查一次。"""
    def _on_raw(kind: str, payload: dict) -> int:
        account = account_box.value
        try:
            return buffer.append(kind, payload, account=account, mode=mode)
        except Exception as primary_exc:
            # N1（HIGH，codex 終審 round2）：主寫入一旦失敗，第一個動作就是 trip 本地
            # latch（純記憶體、不阻塞）——排在下面的退化寫入／`_trigger_failstop_latch`
            # 的 sentinel fsync／IPC send 這些 I/O 之前，堵住「I/O 阻塞期間 `_dispatch`
            # 仍讀到 `latch.tripped is False`、放行新 mutating native 呼叫」的縫。
            # `_trigger_failstop_latch` 內部也會再 trip 一次（冪等），這裡提早搶先是為了
            # 不必等退化寫入（同樣是一段 I/O）跑完才 trip。
            if latch is not None:
                latch.trip()
            try:
                _try_degraded_write(buffer, kind, payload, account=account, mode=mode)
            except Exception as degraded_exc:
                detail = (
                    f"buffer 落地失敗（主寫入: {primary_exc}；退化寫入: {degraded_exc}）"
                )
                _trigger_failstop_latch(buffer, failstop_conn, latch, detail)
                raise
            detail = (
                f"buffer 主寫入失敗（{primary_exc}），已改寫入退化檔供人工救援："
                f"{_degraded_write_path(buffer)}（純檔案 append-only、非 SQLite，不會被"
                "agent outbox 自動送達 server，需人工介入重新灌回或補送）"
            )
            _trigger_failstop_latch(buffer, failstop_conn, latch, detail)
            return -1  # 退化寫入成功：沒有 SQLite row id 可回，呼叫端本就不依賴它
    return _on_raw


def _dispatch(native, op: dict, *, latch: "ChildFailstopLatch | None" = None) -> dict:
    kind = op["op"]

    if kind in ("place", "cancel", "update") and latch is not None and latch.tripped:
        # C4（HIGH，codex 終審）：native 呼叫正前方再次檢查本地 latch（同進程、無 IPC
        # round-trip，比父程序的 `AgentRunner._latched` 更即時）——callback 執行緒偵測到
        # 落地失敗、trip() latch 的那一刻，與這筆 RPC 已經被父程序送到子程序的那一刻剛好
        # 交錯時的最後防線。回傳的 reply 形狀與父程序 `_reject_failstop` 送的
        # `UpCommandRejected` 用同一個 error_kind="failstop"，`agent_commands.py` 既有的
        # `_EXPLICIT_REJECT_KINDS` 轉移表天然吃得下（place→failed+release、cancel→不動
        # Order+audit、update→只 release delta）。
        return {"ok": False, "error_kind": "failstop",
                "message": "agent buffer 落地失敗（child 本地 latch），拒絕執行 native 呼叫"}

    if kind == "connect":
        return {"ok": True, "account": native.connect()}

    if kind == "place":
        price = Decimal(op["price"])
        result = native.place(
            action=op["action"], price=price, qty=op["qty"],
            price_type=op["price_type"], order_type=op["order_type"], octype=op["octype"],
        )
        return {"ok": True, "result": result}

    if kind == "cancel":
        native.cancel(op["ordno"])
        return {"ok": True, "result": {}}

    if kind == "update":
        price = Decimal(op["price"]) if op.get("price") is not None else None
        native.update(op["ordno"], price=price, qty=op["qty"], price_type=op.get("price_type"))
        return {"ok": True, "result": {}}

    if kind == "reconcile":
        after_raw = op.get("after")
        after = datetime.fromisoformat(after_raw) if after_raw else None
        payloads, newest = native.trades_snapshot(after)
        return {"ok": True, "result": {
            "payloads": payloads, "newest": newest.isoformat() if newest else None,
        }}

    if kind == "query_qty":
        # G3/D8：等價 shioaji_adapter._query_order_qty_blocking——查詢券商目前對這筆委託
        # 回報的口數，供 server 端 per-slot watchdog 比對改前/改後值收斂 unknown。查無
        # （已從 list_trades() 目前清單消失，如已完全結案）回 qty=None，不猜測。
        qty = native.query_order_qty(op["ordno"])
        return {"ok": True, "result": {"qty": qty}}

    if kind == "ping":
        return {"ok": True}

    if kind == "shutdown":
        native.close()
        return {"ok": True}

    return {"ok": False, "error_kind": "exception", "message": f"未知 op: {kind!r}"}


def child_main(
    conn,
    *,
    credentials: dict,
    symbol: str,
    mode: str,
    buffer_path: str,
    native_factory: Callable[..., Any] | None = None,
    failstop_conn=None,
) -> None:
    """conn: multiprocessing.Connection（子端，既有 RPC pipe）。
    credentials={"api_key","secret_key"}。`failstop_conn`（Inc1 D9/G2①，Task 12）：獨立於
    `conn` 之外的專用單向 IPC channel（R2-8，`ChildHandle.start()` 用 `ctx.Pipe(duplex=False)`
    建立、只送 callback 落地雙寫失敗的通知）——留 `None` 預設值以相容尚未接線這條 channel 的
    既有呼叫端（測試/舊呼叫），此時退化寫入也失敗只會寫 sentinel，不會有 IPC 通知（父程序仍
    可能靠既有子程序心跳凍結偵測——#203 防線——間接察覺異常，但不是即時的）。"""
    factory = native_factory or _default_native_factory
    secrets = [v for v in credentials.values() if v]
    buffer = DurableBuffer(buffer_path)
    latch = ChildFailstopLatch()  # C4：child 進程內 thread-safe latch，貫穿 callback/dispatch

    # (g) mode!="sim" 時不建 native client（防禦層 3）——所有 op 一律回 mode_mismatch。
    native = None
    account_box = _AccountBox()  # connect 成功後才填值，見 _wrap_on_raw docstring。
    if mode == "sim":
        native = factory(credentials=credentials, symbol=symbol, mode=mode,
                         on_raw=_wrap_on_raw(buffer, mode=mode, account_box=account_box,
                                              failstop_conn=failstop_conn, latch=latch))

    while True:
        op = conn.recv()
        kind = op.get("op")
        # codex round1 fix1(a)：原樣帶回呼叫端的 rpc_id（op 沒帶就回 None）——
        # ChildHandle._rpc 靠這個欄位辨識/丟棄逾時後才姍姍來遲的舊 reply，避免污染下一輪
        # RPC（見 runner.py ChildHandle docstring）。

        if native is None:
            conn.send({"ok": False, "error_kind": "mode_mismatch",
                       "message": _MODE_MISMATCH_MESSAGE, "rpc_id": op.get("rpc_id")})
            if kind == "shutdown":
                break
            continue

        try:
            reply = _dispatch(native, op, latch=latch)
        except TradeNotFoundError as exc:
            message = redact_secrets(str(exc), secrets=secrets)
            conn.send({"ok": False, "error_kind": "trade_not_found", "message": message,
                       "result": {"ordno": exc.ordno}, "rpc_id": op.get("rpc_id")})
            continue
        except Exception as exc:
            message = redact_secrets(str(exc), secrets=secrets)
            log.error("子程序執行 %s 失敗: %s", kind, message)
            conn.send({"ok": False, "error_kind": "exception", "message": message,
                       "rpc_id": op.get("rpc_id")})
            continue

        if kind == "connect" and reply.get("ok"):
            # 帳號切換 tripwire（codex round1 fix6）：outbox 有前一帳號未送回報時，拒絕
            # 以不同帳號啟動——避免這個 buffer 檔接下來收到的回報被錯配進新帳號的 session。
            try:
                buffer.assert_account(reply["account"])
            except RuntimeError as exc:
                # codex round2 fix4(a)：reply 帶可辨識標記 error_kind="account_mismatch"
                # （pipe 內部 reply，非 UpCmdAck，不受 protocol Literal 限制）——runner.py 的
                # ChildHandle.start() 靠這個欄位判斷要 raise FatalAgentError（停止重試），
                # 而不是把它當一般連線失敗、任由 run_forever 無限 backoff 重打券商登入。
                reply = {"ok": False, "error_kind": "account_mismatch", "message": str(exc)}
            else:
                # Task 9 D5/I7：帳號確認通過才填 account_box——之後任何 callback 落地的
                # report 都會蓋上這個帳號；mismatch 分支刻意不填（子程序即將被
                # ChildHandle.start() terminate，不該有機會用錯帳號蓋章）。
                account_box.value = reply["account"]

        reply["rpc_id"] = op.get("rpc_id")
        conn.send(reply)
        if kind == "shutdown":
            break
