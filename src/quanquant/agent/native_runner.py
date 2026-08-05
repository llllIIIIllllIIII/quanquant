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
"""
import logging
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
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


def _dispatch(native, op: dict) -> dict:
    kind = op["op"]

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
) -> None:
    """conn: multiprocessing.Connection（子端）。credentials={"api_key","secret_key"}。"""
    factory = native_factory or _default_native_factory
    secrets = [v for v in credentials.values() if v]
    buffer = DurableBuffer(buffer_path)

    # (g) mode!="sim" 時不建 native client（防禦層 3）——所有 op 一律回 mode_mismatch。
    native = None
    if mode == "sim":
        native = factory(credentials=credentials, symbol=symbol, mode=mode, on_raw=buffer.append)

    while True:
        op = conn.recv()
        kind = op.get("op")

        if native is None:
            conn.send({"ok": False, "error_kind": "mode_mismatch", "message": _MODE_MISMATCH_MESSAGE})
            if kind == "shutdown":
                break
            continue

        try:
            reply = _dispatch(native, op)
        except TradeNotFoundError as exc:
            message = redact_secrets(str(exc), secrets=secrets)
            conn.send({"ok": False, "error_kind": "trade_not_found", "message": message,
                       "result": {"ordno": exc.ordno}})
            continue
        except Exception as exc:
            message = redact_secrets(str(exc), secrets=secrets)
            log.exception("子程序 op 失敗（op=%s）", kind)
            conn.send({"ok": False, "error_kind": "exception", "message": message})
            continue

        conn.send(reply)
        if kind == "shutdown":
            break
