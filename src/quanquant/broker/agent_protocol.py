"""server ↔ 本機 agent 的 WS 訊息協定（v1）。雙邊共用；wire 上價格一律字串（Decimal 安全）。"""
from typing import Annotated, Literal
from pydantic import BaseModel, Field, TypeAdapter

PROTOCOL_VERSION = 1

# ---------- 下行（server → agent） ----------
class PlaceNative(BaseModel):
    action: Literal["Buy", "Sell"]
    price: str                      # Decimal 字串；MKT 為 "0"
    qty: int = Field(gt=0)
    price_type: Literal["LMT", "MKT"]
    order_type: Literal["ROD", "IOC", "FOK"]
    octype: Literal["New", "Cover", "Auto"]

class DownPlace(BaseModel):
    type: Literal["place"] = "place"
    cmd_id: str
    mode: Literal["sim"]            # Inc0 鐵律：協定層就擋 real
    native: PlaceNative

class DownCancel(BaseModel):
    type: Literal["cancel"] = "cancel"
    cmd_id: str
    mode: Literal["sim"]
    ordno: str

class DownUpdate(BaseModel):
    type: Literal["update"] = "update"
    cmd_id: str
    mode: Literal["sim"]
    ordno: str
    price: str | None = None
    qty: int = Field(gt=0)
    price_type: str | None = None

class DownReconcile(BaseModel):
    type: Literal["reconcile"] = "reconcile"
    cmd_id: str
    mode: Literal["sim"]
    after: str | None = None        # naive-UTC ISO 字串

class DownReportAck(BaseModel):
    type: Literal["report_ack"] = "report_ack"
    event_id: int

class DownHealth(BaseModel):
    type: Literal["health"] = "health"
    cmd_id: str

DownlinkMessage = Annotated[
    DownPlace | DownCancel | DownUpdate | DownReconcile | DownReportAck | DownHealth,
    Field(discriminator="type"),
]
_down_adapter = TypeAdapter(DownlinkMessage)

def parse_downlink(data: dict):
    return _down_adapter.validate_python(data)

# ---------- 上行（agent → server） ----------
class UpLogin(BaseModel):
    type: Literal["login"] = "login"
    protocol: int = PROTOCOL_VERSION
    account: str
    mode: Literal["sim"]

class UpReport(BaseModel):
    type: Literal["report"] = "report"
    event_id: int                   # agent durable buffer 的列 id（ack 對齊鍵）
    kind: Literal["deal_report", "order_report"]
    payload: dict

class UpCmdAck(BaseModel):
    type: Literal["cmd_ack"] = "cmd_ack"
    cmd_id: str
    ok: bool
    result: dict | None = None      # place: {"ordno","broker_order_id"}；reconcile: {"payloads","newest"}
    error_kind: Literal["trade_not_found", "exception", "timeout", "mode_mismatch"] | None = None
    message: str | None = None      # 已經 agent 端 redact

class UpHealth(BaseModel):
    type: Literal["health"] = "health"

UplinkMessage = Annotated[
    UpLogin | UpReport | UpCmdAck | UpHealth, Field(discriminator="type")
]
_up_adapter = TypeAdapter(UplinkMessage)

def parse_uplink(data: dict):
    return _up_adapter.validate_python(data)
