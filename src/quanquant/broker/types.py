"""Broker 無關的 domain 型別（純資料、無 DB/框架依賴）。

mode 完整性：OrderRequest 刻意不帶 mode 欄位——執行 mode 只由 OrderService.mode
（server-side session）決定；Fill.mode 由 adapter 依 self.mode 蓋入，不接受外部輸入覆寫。
fill_id 與 ordno/broker_order_id 分離：fill_id 是成交去重鍵，ordno/broker_order_id 是
委託關聯鍵，兩者用途不同、不可共用（V3-3：只收真實 deal id 當 fill_id，不用
ordno/seqno fallback 冒充）。

canonical_payload_hash（V3-3）：全計畫唯一的「這張委託將送給券商的完整可執行內容」
hash 產生點。刻意涵蓋 symbol/action/qty/price/price_type/order_type/octype/account/mode
九個欄位、刻意不含 client_order_id/broker_order_id——委託身分鍵與「送給券商的內容」是
兩件事：place 用「即將建立的委託」算 hash，update 用「套用變更後的完整新內容」算 hash，
兩者用同一 helper 才能讓 real update 的 confirm token 驗證得過（BLOCKER#1），且任何單一
可執行欄位被篡改都會讓 hash 改變（HIGH#5）。

round3 HIGH#5 修正：Decimal 正規化不能只靠 format(value, "f")——`Decimal("18000.00")` 未
先 normalize() 仍會格式化成 "18000.00"（尾零不會被 format 吃掉），與 `Decimal("18000")` 得到
不同字串、算出不同 hash。正確做法：先 `.normalize()` 折疊尾零/指數，零值另外特判
（`Decimal("0").normalize()` 雖然會收斂成 `Decimal('0')`，但 `-0`/`0E+n` 等變體仍需保險起見
統一成 "0"），再用 format(..., "f") 展開——decimal 模組的 'f' presentation type 保證輸出固定
小數點字串、不會有科學記號（`Decimal('1E+1')` 經 normalize()+format(...,"f") 會得到 "10"）。
再以固定 schema 的 canonical JSON（sort_keys=True、separators=(',',':')、所有值統一字串化）
編碼後 hash，避免欄位順序或型別（int vs str）造成同義 payload 得到不同 hash。

YuantaAdapter 日後把 SendFutureOrder / RR_RealReport 映射到同一組型別即可接同介面。
"""
import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

Mode = Literal["sim", "real"]
Action = Literal["Buy", "Sell"]
PriceType = Literal["LMT", "MKT"]
OrderType = Literal["ROD", "IOC", "FOK"]
OcType = Literal["New", "Cover", "Auto"]

_MODES: frozenset[str] = frozenset(("sim", "real"))
_ACTIONS: frozenset[str] = frozenset(("Buy", "Sell"))
_PRICE_TYPES: frozenset[str] = frozenset(("LMT", "MKT"))
_ORDER_TYPES: frozenset[str] = frozenset(("ROD", "IOC", "FOK"))
_OCTYPES: frozenset[str] = frozenset(("New", "Cover", "Auto"))


@dataclass(frozen=True, slots=True)
class OrderRequest:
    """下單請求；mode 刻意不在此型別——執行 mode 由 OrderService.mode 決定（見 base.py）。"""

    client_order_id: str      # 伺服器產生的冪等鍵（表單首次渲染即生成的 UUID，見 Task 9）
    symbol: str
    action: Action
    qty: int
    price: Decimal
    price_type: PriceType
    order_type: OrderType
    octype: OcType
    user_id: int               # 下單者（授權後綁定，一路帶到 Order/Deal/Fill）

    def __post_init__(self) -> None:
        if self.qty <= 0:
            raise ValueError(f"qty 必須 > 0，收到 {self.qty}")
        if self.price <= 0:
            raise ValueError(f"price 必須 > 0，收到 {self.price}")
        if self.action not in _ACTIONS:
            raise ValueError(f"非法 action: {self.action!r}")
        if self.price_type not in _PRICE_TYPES:
            raise ValueError(f"非法 price_type: {self.price_type!r}")
        if self.order_type not in _ORDER_TYPES:
            raise ValueError(f"非法 order_type: {self.order_type!r}")
        if self.octype not in _OCTYPES:
            raise ValueError(f"非法 octype: {self.octype!r}")


@dataclass(frozen=True, slots=True)
class OrderAck:
    client_order_id: str
    broker_order_id: str
    ordno: str | None
    status: str          # "submitted" | "cancelled" | "updated" | "failed" | "unknown"


@dataclass(frozen=True, slots=True)
class Fill:
    """一筆成交；mode/user_id 由 adapter 依 order context 補上（未解析前 user_id 可為 None）。"""

    broker: str          # "shioaji"
    fill_id: str          # 券商成交唯一識別（去重鍵，與 ordno/broker_order_id 分離）
    ordno: str | None     # 委託關聯鍵之一（對應 Order.ordno，複合 scope 查詢用）
    broker_order_id: str | None   # 委託關聯鍵之二（對應 Order.broker_order_id；D5）
    symbol: str
    action: Action
    price: Decimal
    qty: int
    fee: Decimal | None
    octype: OcType
    ts: int               # 成交時間 epoch-ms UTC
    account: str
    mode: Mode             # 由下單 session 決定（server-side），不可信任外部輸入覆寫
    user_id: int | None    # 解析 order context 後補上；None 代表尚未解析（quarantine）

    def __post_init__(self) -> None:
        """型別層 fail-closed（V3-4/HIGH#8）：非法/缺值一律拒絕建構，不猜測成 Auto/空字串/0。
        這是 defense-in-depth——Task 5 的 raw payload mapper 會在建構 Fill 之前先做同等驗證並
        quarantine 不合法的原始 callback，本檢查防的是「任何未來呼叫端忘記先驗證」。"""
        if self.mode not in _MODES:
            raise ValueError(f"非法 mode: {self.mode!r}")
        if self.action not in _ACTIONS:
            raise ValueError(f"非法 action: {self.action!r}")
        if self.octype not in _OCTYPES:
            raise ValueError(f"非法 octype: {self.octype!r}")
        if self.qty <= 0:
            raise ValueError(f"qty 必須 > 0，收到 {self.qty}")
        if self.price <= 0:
            raise ValueError(f"price 必須 > 0，收到 {self.price}")
        if not self.fill_id:
            raise ValueError("fill_id 不可為空（缺真實 deal id 應 quarantine，不可建構 Fill）")
        if not self.account:
            raise ValueError("account 不可為空")
        if self.ts <= 0:
            raise ValueError(f"ts 必須 > 0，收到 {self.ts}")


@dataclass(frozen=True, slots=True)
class Position:
    symbol: str
    direction: str        # "long" | "short"
    qty: int
    avg_price: Decimal


@dataclass(frozen=True, slots=True)
class RiskDecision:
    allowed: bool
    reason: str | None
    needs_confirm: bool


def _canonical_decimal_str(value: Decimal) -> str:
    """把 Decimal 正規化成穩定、非科學記號的十進位字串（round3 HIGH#5）。

    - 零值（含 -0、0.00、0E+5 等變體）一律統一成 "0"。
    - 非零值先 normalize() 折疊尾零/指數差異，再用 format(..., "f") 展開成固定小數點
      字串——'f' presentation type 保證不會輸出科學記號（例如 Decimal('1E+1') 會得到 "10"）。
    """
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def canonical_payload_hash(
    *,
    symbol: str,
    action: Action,
    qty: int,
    price: Decimal,
    price_type: PriceType,
    order_type: OrderType,
    octype: OcType,
    account: str,
    mode: Mode,
) -> str:
    """涵蓋所有可執行欄位的 canonical hash；place/update 簽發與驗證共用（V3-3）。

    刻意不含 client_order_id/broker_order_id（委託身分鍵，非「將送給券商的內容」）。
    固定 schema 的 canonical JSON（sort_keys + 緊湊 separators + 全欄位字串化）編碼後
    sha256，避免欄位順序或型別差異（int vs str）讓同義 payload 得到不同 hash；
    Decimal 用 `_canonical_decimal_str` 正規化，修正 round3 HIGH#5 的科學記號/尾零陷阱。
    """
    payload = {
        "symbol": symbol,
        "action": action,
        "qty": str(int(qty)),
        "price": _canonical_decimal_str(price),
        "price_type": price_type,
        "order_type": order_type,
        "octype": octype,
        "account": account,
        "mode": mode,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
