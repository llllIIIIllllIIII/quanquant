"""SDK 子程序測試替身：module-level 可 pickle（供 `multiprocessing.get_context("spawn")`
與整合測試共用），不碰網路、不 import shioaji（Inc0 Task 11）。

`FakeNativeClient` 的 `place()` 觸發的 order/deal 回報 payload 欄位形狀是 2026-07-28 對
真實 Shioaji SDK 實測定案（見 handoff §5）：巢狀 `order_report`
（`operation.op_type`＋`order.id`/`order.seqno`）與扁平 `deal_report`（`trade_id`/`action`/
`price`/`quantity`/`ts`(epoch 秒)/`account_id`）——刻意對齊
`ShioajiAdapter._map_order_report`/`_map_deal_report` 認得的鍵，讓依賴這個 fake 的整合測試
（Task 15）可以一路跑到 mapper 層而不必真的連 SDK。
"""
import time
from decimal import Decimal

from quanquant.broker.base import OrderError, TradeNotFoundError


class FakeNativeClient:
    """不碰網路的 native 替身：place 後同步觸發 order/deal 回報。"""

    def __init__(self, *, credentials=None, symbol: str = "TXF", mode: str = "sim", on_raw=None) -> None:
        self.credentials = credentials or {}
        self.symbol = symbol
        self.mode = mode
        self._on_raw = on_raw
        self.account = ""
        self._next_seq = 1
        self._orders: dict[str, dict] = {}

    def connect(self) -> str:
        self.account = "F1"
        return self.account

    def close(self) -> None:
        pass

    def place(self, *, action: str, price: Decimal, qty: int, price_type: str,
              order_type: str, octype: str) -> dict:
        ordno = f"101AA{self._next_seq}"
        self._next_seq += 1
        self._orders[ordno] = {"action": action, "price": price, "qty": qty}

        if self._on_raw is not None:
            self._on_raw("order_report", {
                "operation": {"op_type": "New"},
                "order": {"id": ordno, "seqno": ordno, "ordno": ordno},
                "status": {},
            })
            self._on_raw("deal_report", {
                "trade_id": f"D{ordno}", "seqno": ordno, "ordno": ordno,
                "exchange_seq": ordno, "action": action, "code": "TXFH6",
                "price": float(price), "quantity": qty, "ts": time.time(),
                "account_id": self.account or "F1",
            })

        return {"ordno": ordno, "broker_order_id": ordno}

    def cancel(self, ordno: str) -> None:
        if ordno == "BOOM":
            # 專供 redaction 測試：訊息內嵌憑證，驗證 child_main 送出前已 redact。
            raise RuntimeError(f"boom {self.credentials.get('api_key')}")
        if ordno not in self._orders:
            # codex round1 fix4(b)：對齊 production 契約——native.py 的 cancel()（227-233
            # 行）對找不到對應委託 raise 一般 OrderError，TradeNotFoundError 只用在
            # update()。這裡原本誤用 TradeNotFoundError，跟真實 SDK 行為不一致。
            raise OrderError(
                f"找不到券商對應委託（ordno={ordno!r}），可能已成交/已刪除/跨日，"
                "拒絕在無法確認對應委託的情況下送出取消"
            )
        del self._orders[ordno]

    def update(self, ordno: str, *, price, qty: int, price_type: str | None = None) -> None:
        if ordno not in self._orders:
            raise TradeNotFoundError(ordno)
        self._orders[ordno].update(price=price, qty=qty)

    def trades_snapshot(self, after) -> tuple[list, None]:
        return [], None

    def query_order_qty(self, ordno: str) -> "int | None":
        """G3/D8（Task 11）：等價 `ShioajiNativeClient.query_order_qty`——查無（已從
        `self._orders` 消失，如 `cancel()` 刪除）回 None。"""
        order = self._orders.get(ordno)
        return order["qty"] if order is not None else None


def fake_native_factory(*, credentials, symbol, mode, on_raw):
    return FakeNativeClient(credentials=credentials, symbol=symbol, mode=mode, on_raw=on_raw)
