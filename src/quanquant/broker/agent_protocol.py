"""server ↔ 本機 agent 的 WS 訊息協定（v2）。雙邊共用；wire 上價格一律字串（Decimal 安全）。

Inc1 D7：協定硬升 `PROTOCOL_VERSION = 2`，**不做 v1 相容**——v1 訊息一律在這一關就
ValidationError（正式環境未部署、存量 agent 只有開發者自己，雙版本相容是純負債）。
變更集（詳見 docs/superpowers/specs/2026-08-06-local-broker-agent-inc1-design.md D7）：
- `UpReport` +`account/mode`（必填，D5 回報歸屬蓋章不可變，I7）。
- `UpCmdAck`（僅 mutating 指令 place/cancel/update）+`event_id`（D4：走 outbox
  at-least-once，與 UpReport 共用補送機制）；`error_kind` 增 `expired`/`scope_mismatch`/
  `failstop`（Task 12：`agent_ws.py` 收到 `UpCommandRejected` 時，server 端合成一筆
  `error_kind="failstop"` 的 `UpCmdAck` 餵給既有 applier，走同一套 kind×outcome 轉移表，不
  另開一條路徑；`agent_commands._EXPLICIT_REJECT_KINDS` 已納入）。
- 新增 volatile `UpQueryResult`：reconcile 快照與 query_qty 結果共用（R1-7），無
  `event_id`、不進 outbox、不觸發 DownReportAck。
- 新增 volatile `UpCommandRejected`：failstop 期間拒新指令用（R2-5），無 `event_id`、
  不進 outbox、不觸發 DownReportAck——buffer 已壞時仍能拒絕；訊息遺失靠重連重播收斂。
- `UpHealth` +`status`/`detail`/`health_epoch`（G2 R2-3：健康狀態單調遞增；generation
  不進 payload，由 server 端連線 handler 自行 fencing，R3-2）。
- `UpLogin.protocol` 改 `Literal[2]`；+`health_epoch`（本 session 健康狀態基準宣告）。
- `DownPlace/DownCancel/DownUpdate` +`account/expires_at`（指令 scope 建立時凍結，
  agent 執行前核對，R1-2；`expires_at` 為 naive-UTC ISO 字串，agent 逾時拒執行）。
- 新增 `DownQueryQty`（G3，D8：per-slot watchdog 用來比對改前/改後口數收斂 unknown）。
- `DownReportAck` 只 ack durable event（`report` 與 `cmd_ack` 兩種共用同一補送機制）。

**這裡只定義協定本身**——command ledger／failstop 狀態機／unknown-resolver 等 runtime
接線是後續 task 的範圍，本檔與呼叫端只確保訊息在 wire 上合法。
"""
from typing import Annotated, Literal
from pydantic import BaseModel, Field, TypeAdapter

PROTOCOL_VERSION = 2

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
    account: str                    # D4/R1-2：指令 scope 建立時凍結，agent 執行前核對
    mode: Literal["sim"]            # Inc0 鐵律：協定層就擋 real
    expires_at: str                 # naive-UTC ISO 字串；agent 逾時拒執行回 error_kind=expired
    native: PlaceNative

class DownCancel(BaseModel):
    type: Literal["cancel"] = "cancel"
    cmd_id: str
    account: str
    mode: Literal["sim"]
    expires_at: str
    ordno: str

class DownUpdate(BaseModel):
    type: Literal["update"] = "update"
    cmd_id: str
    account: str
    mode: Literal["sim"]
    expires_at: str
    ordno: str
    price: str | None = None
    qty: int = Field(gt=0)
    price_type: str | None = None

class DownReconcile(BaseModel):
    type: Literal["reconcile"] = "reconcile"
    cmd_id: str
    mode: Literal["sim"]
    after: str | None = None        # naive-UTC ISO 字串

class DownQueryQty(BaseModel):
    """G3/D8：per-slot watchdog 查詢委託目前口數，用來比對改前/改後值收斂 unknown。"""
    type: Literal["query_qty"] = "query_qty"
    cmd_id: str
    ordno: str
    mode: Literal["sim"]            # 協定層仍鎖 sim（D7）

class DownReportAck(BaseModel):
    type: Literal["report_ack"] = "report_ack"
    event_id: int                   # 只 ack durable event：UpReport／UpCmdAck 共用此欄位空間

class DownHealth(BaseModel):
    type: Literal["health"] = "health"
    cmd_id: str

DownlinkMessage = Annotated[
    DownPlace | DownCancel | DownUpdate | DownReconcile | DownQueryQty | DownReportAck
    | DownHealth,
    Field(discriminator="type"),
]
_down_adapter = TypeAdapter(DownlinkMessage)

def parse_downlink(data: dict):
    return _down_adapter.validate_python(data)

# ---------- 上行（agent → server） ----------
class UpLogin(BaseModel):
    type: Literal["login"] = "login"
    # codex round1 fix5：錯版 agent 在 parse_uplink 這關就 ValidationError（端點既有
    # invalid-frame 路徑忽略，agent 永不 ready），不必等到跑起來才發現協定不合。
    # codex round2 fix1：拿掉 default——有 default 時缺 protocol 欄位的 login 會自動補 1
    # 通過驗證，等於版本協商可被繞過。必填後，任何呼叫端（runner.py 的 UpLogin(...)）都
    # 必須顯式帶 protocol=PROTOCOL_VERSION，缺就是建構期 ValidationError。
    # Inc1 D7：硬升 v2，不做 v1 相容——Literal[1] 改 Literal[2]，v1 agent 一律拒收。
    protocol: Literal[2]
    account: str
    mode: Literal["sim"]
    health_epoch: int               # R3-2：本 session 健康狀態基準宣告（buffer 重建歸零時
                                     # 由重宣告吸收，避免舊 epoch 永久拒收新健康訊息）

class UpReport(BaseModel):
    type: Literal["report"] = "report"
    event_id: int                   # agent durable buffer 的列 id（ack 對齊鍵）
    kind: Literal["deal_report", "order_report"]
    account: str                    # D5 S1/S2/S3：agent 在落 outbox 當下蓋章，回報歸屬
    mode: Literal["sim"]            # 不可變（I7）——換帳號/重連/重送都不改變歸屬
    payload: dict

class UpCmdAck(BaseModel):
    """僅 mutating 指令（place/cancel/update）的回覆——read-only 指令（reconcile/
    query_qty）改用下方 UpQueryResult。"""
    type: Literal["cmd_ack"] = "cmd_ack"
    cmd_id: str
    event_id: int                   # D4：走 outbox at-least-once，與 UpReport 共用補送機制
    ok: bool
    result: dict | None = None      # place: {"ordno","broker_order_id"}
    error_kind: Literal[
        "trade_not_found", "exception", "timeout", "mode_mismatch",
        "expired", "scope_mismatch", "failstop",
    ] | None = None
    message: str | None = None      # 已經 agent 端 redact

class UpQueryResult(BaseModel):
    """唯讀結果專用（volatile）：reconcile 快照與 query_qty 結果共用（R1-7）。無
    `event_id`、不進 outbox、只 resolve 同 user/generation 的 pending future、不觸發
    DownReportAck——避免「CAS 查無 command＝重複」誤把查詢結果丟掉。"""
    type: Literal["query_result"] = "query_result"
    cmd_id: str
    result: dict             # reconcile: {"payloads","newest"}；query_qty: {"qty": int}

class UpCommandRejected(BaseModel):
    """failstop 期間拒新指令（volatile，R2-5）：無 `event_id`、不進 outbox、不觸發
    DownReportAck——buffer 已壞時仍能拒絕；訊息遺失靠重連重播、agent 再拒收斂。"""
    type: Literal["cmd_rejected"] = "cmd_rejected"
    cmd_id: str
    error_kind: Literal["failstop"]

class UpHealth(BaseModel):
    type: Literal["health"] = "health"
    status: Literal["ok", "failstop"]
    detail: str | None = None
    health_epoch: int               # 單調遞增（R2-3）；generation 不進 payload——由 server
                                     # 連線 handler 以自己的 my_generation 對整條連線 fencing

UplinkMessage = Annotated[
    UpLogin | UpReport | UpCmdAck | UpQueryResult | UpCommandRejected | UpHealth,
    Field(discriminator="type"),
]
_up_adapter = TypeAdapter(UplinkMessage)

def parse_uplink(data: dict):
    return _up_adapter.validate_python(data)
