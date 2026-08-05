# 本機 Broker Agent — Increment 0 骨幹 實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 Shioaji 原始 I/O 搬到使用者本機 agent 程序（sim 登入、送單、收回報），server 只留政策/風控/持久化/UI，透過認證 WebSocket 交換下行指令與上行回報；證明 login(sim)→place→report→UI roundtrip + kill switch 擋新單。

**Architecture:** 三段切「唯一硬拆」：server DB 決策（不動）→ 下行 native 指令（新 WS 通道）→ agent 執行（native 序列化移 agent）→ cmd_ack → server 寫回（不動）。上行回報走「天然分界縫」：agent 本機 durable buffer 先落地 → WS 送 → server `commit_raw_callback` 落 RawInbox → `RawInboxWorker` 以下整條不動。#203 隔離：agent 內把 SDK 關進子程序，durable buffer（SQLite）兼作回報路徑的跨程序中介。

**Tech Stack:** Python 3.11+、FastAPI WebSocket（server）、`websockets`（agent client，新依賴）、pydantic v2（協定訊息）、stdlib `sqlite3`（agent durable buffer）、`multiprocessing`（SDK 子程序）、pytest + pytest-asyncio（asyncio_mode=auto）。

## Global Constraints（每個 Task 隱含適用）

- **測試底線**：起點 649 pytest 全綠；每個 Task 結束時 `uv run pytest` 必須全綠（0 failed），既有測試不得弱化或刪除。
- **禁 push / 禁 deploy**：只做本機 commit（分支 `feat/shioaji-order-integration`）。push/PR/deploy 需使用者明確要求。
- **Inc0 僅 sim**：三層 guard——server `order_channel=="agent"` 時強制 `order_mode=="sim"`；下行指令 `mode` 欄位僅允許 `"sim"`；agent CLI `--mode` choices 僅 `["sim"]`。
- **`import shioaji` 唯一落點**：Task 2 完成後，`src/quanquant/broker/` 內只有 `native.py` 允許 import shioaji（行情用的 `sources/shioaji_stream.py` 不在本計畫範圍、不動）。
- **零丟單鏈**：agent callback 執行緒必須「durable buffer 同步落地成功後才返回」；server 收 report 必須「RawInbox commit 成功後才回 report_ack」。at-least-once + 既有 Deal 層 `uq_deal_fill` 去重吸收重送。
- **WS 接收迴圈禁止取得 supervisor.lock、禁止 inline await 長工作**：login 觸發的 reconcile 必須 `asyncio.create_task`（否則 receive loop 等 reconcile、reconcile 等 cmd_ack、cmd_ack 需要 receive loop → 死鎖）。
- **憑證 session-only**：api_key/secret_key 只存 agent 程序記憶體（getpass 或 env 讀入），永不寫檔、不進 argv、不進 log；agent 上行錯誤訊息先用 `redact_secrets` 遮蔽。
- **server 端零新表/零 migration**：本計畫不新增任何 server DB 表或欄位（agent buffer 是 agent 本機自己的 SQLite 檔，與 app DB 無關，無雙方言可攜義務）。
- **新依賴 `websockets>=12`**：需使用者核可後才可改 pyproject（Task 14 執行前確認；uvicorn[standard] 已間接帶入，此舉是顯式化）。
- **pyproject hatch 設定不可加 force-include**（現況 `packages = ["src/quanquant"]`，只加 `[project.scripts]` 一行與依賴一行）。
- **UI/訊息一律繁體台灣中文**；commit message 格式 `<type>: <描述>`（feat/fix/refactor/test/chore），無 attribution。
- **GateGuard hook**：第一次 Bash、建新檔、每檔首次 edit 會被擋下要求陳述事實——照錯誤訊息列點補上（匯入者/受影響 API/schema/使用者指示）後重試同一操作，非故障。
- **決策 5 對應**：agent 模式下 supervisor.lock 背後沒有任何 native 呼叫（native 序列化由 agent 子程序序列迴圈保證），它的角色是「server DB 序列化鎖」，watchdog DB-only 工作（`_retry_quarantined`）沿用它＝與 native 序列化脫鉤。in-process 模式行為完全不變。
- **Inc0 已知限制（刻意，勿擴 scope）**：agent 模式停用 `_reconcile_unknown_quota`（需 native 查詢，留 Inc1；保守後果=配額維持保留、不會超賣）；agent 離線 → `session_state.mark_disabled`（/healthz 200，UI 顯示未連線，下單被擋）；ops 告警不新增 agent 生命週期事件（log.warning 代替，留 Inc1）。

---

## File Structure

**新增（server 端）**
- `src/quanquant/broker/native.py` — `ShioajiNativeClient`：純 SDK 操作＋callback→on_raw，零 DB import
- `src/quanquant/broker/agent_protocol.py` — 上/下行訊息 pydantic 模型 + parse helpers（雙邊共用）
- `src/quanquant/broker/agent_channel.py` — `AgentChannel`（連線態＋cmd 多工）＋ `AgentNativeGateway`（下行指令→ack 對映）
- `src/quanquant/web/routers/agent_ws.py` — `/ws/agent` WebSocket 端點＋上行 dispatch
- `src/quanquant/web/templates/partials/agent_status.html` — agent 連線狀態 badge

**新增（agent 端，同 repo 同 package）**
- `src/quanquant/agent/__init__.py`
- `src/quanquant/agent/buffer.py` — `DurableBuffer`（本機 SQLite/WAL outbox）
- `src/quanquant/agent/native_runner.py` — 子程序進入點 `child_main`（唯一擁有 SDK 的程序）
- `src/quanquant/agent/runner.py` — `ChildHandle`（子程序管理）＋`AgentRunner`（WS session/泵/監督）
- `src/quanquant/agent/ws_client.py` — `Transport` protocol ＋ `WebsocketsTransport`
- `src/quanquant/agent/main.py` — CLI 進入點（getpass 憑證、sim-only guard）
- `src/quanquant/agent/testing.py` — `FakeNativeClient`（可 pickle，供單元/整合測試）

**修改**
- `src/quanquant/broker/base.py` — 新例外 `TradeNotFoundError`/`AgentUnavailableError`/`AgentCommandTimeoutError`
- `src/quanquant/broker/shioaji_adapter.py` — native 委派到 `ShioajiNativeClient`；`remote_gateway` 支援；reconcile 拆分
- `src/quanquant/broker/watchdog.py` — 新增 `run_agent_watchdog`
- `src/quanquant/config.py` — `order_channel`/`agent_ws_token`/`agent_command_timeout_seconds`
- `src/quanquant/web/app.py` — `_start_order_subsystem` agent 分支；create_app 掛 agent_ws router
- `src/quanquant/web/routers/orders.py` — `GET /orders/agent-status`
- `src/quanquant/web/templates/orders.html` — agent 狀態 badge div
- `pyproject.toml` — `quanquant-agent` script ＋ `websockets>=12`

**測試**
- `tests/test_native_client.py`、`tests/test_agent_protocol.py`、`tests/test_agent_channel.py`、`tests/test_adapter_remote.py`、`tests/test_agent_ws.py`、`tests/test_agent_app_wiring.py`、`tests/test_agent_buffer.py`、`tests/test_agent_child.py`、`tests/test_agent_runner.py`、`tests/test_agent_cli.py`、`tests/test_agent_integration.py`

**任務相依**：1→2；3、4 獨立；5 需 3+4；6 需 2+3+5；7 需 4+5；8 需 6+7；9 需 8；10 獨立；11 需 1+10；12 需 4+10+11；13 需 12；14 需 13；15 需 8+9+13+14；16 人工。

---

### Task 1: `ShioajiNativeClient` 抽取（純新增，不動 adapter）

**Files:**
- Create: `src/quanquant/broker/native.py`
- Modify: `src/quanquant/broker/base.py`
- Test: `tests/test_native_client.py`

**Interfaces:**
- Consumes: `broker/types.py` 的 `Mode`；`broker/base.py` 的 `OrderError`；shioaji_adapter.py 既有實作為搬移來源（`_connect_blocking` L183-193、`_place_blocking` L494-504、`_cancel_blocking` L586-593、`_update_blocking` L709-720、`_refresh_and_list_trades` L554-565、`_find_trade_by_ordno` L567-584、`_probe_blocking` L259-268、`_query_order_qty_blocking` L352-377、`_contract_for` L207-217、`_json_safe` L778-818、`_ack_fields_from_trade` L506-511、`_trade_watermark` L336-350、`_reconcile_blocking` L293-334 的 native 半段、`_on_order_cb` L754-776 的 kind 判斷+json_safe 半段）
- Produces（Task 2/11 依賴，簽名固定）:

```python
# src/quanquant/broker/base.py 新增
class TradeNotFoundError(OrderError):
    """cancel/update 時在券商 list_trades 找不到對應委託（可能已終結或不存在）。"""
    def __init__(self, ordno: str | None = None) -> None:
        super().__init__(f"找不到 ordno={ordno!r} 對應的委託")
        self.ordno = ordno

# src/quanquant/broker/native.py
class ShioajiNativeClient:
    def __init__(self, *, api_key: str, secret_key: str, ca_path: str | None,
                 ca_passwd: str | None, person_id: str | None, symbol: str,
                 mode: Mode, on_raw: Callable[[str, dict], None]) -> None
    # 屬性：api（connect 前 None）、contract、account（connect 後 account_id）
    def connect(self) -> str            # login(+real 才 activate_ca)+set_order_callback；回 account_id
    def close(self) -> None             # api 換 None 後 logout（吞例外）
    def probe(self) -> None             # list_accounts 或讀 futopt_account；失敗 raise
    def place(self, *, action: str, price: Decimal, qty: int, price_type: str,
              order_type: str, octype: str) -> dict     # {"ordno","broker_order_id"}
    def cancel(self, ordno: str) -> None                # 找不到 → OrderError（沿用現行 cancel 語意）
    def update(self, ordno: str, *, price, qty: int, price_type: str | None = None) -> None
                                                        # 找不到 → TradeNotFoundError
    def trades_snapshot(self, after: datetime | None) -> tuple[list[dict], datetime | None]
        # _reconcile_blocking 的 native 半段：update_status+list_trades → 過濾 watermark>after
        # → 回 ([{"order_id","seqno","status"}...], newest_watermark)；api None 回 ([], None)
    def query_order_qty(self, ordno: str) -> int | None
    @staticmethod
    def json_safe(msg) -> dict          # 原 _json_safe 逐字搬移
    @staticmethod
    def ack_fields_from_trade(trade) -> dict
    @staticmethod
    def trade_watermark(trade) -> "datetime | None"
    def _on_order_cb(self, stat, msg) -> None
        # kind 判斷 + json_safe + self._on_raw(kind, payload)；on_raw 或 json_safe 例外時
        # 退化重試 self._on_raw(kind, {"_unparsed": True, "repr": repr(msg)})，再失敗只 log
```

搬移原則：程式碼**逐字搬**（`self._api`→`self.api`、`self._contract`→`self.contract`、`self.mode`/`self.symbol`/`self.account` 照舊；credentials 屬性名沿用 `_api_key` 等）。`_on_order_cb` 的差異：原本直呼 `commit_raw_callback`，改呼 `self._on_raw(kind, payload)`（落地責任移交呼叫端），degradation 邏輯等價保留。

- [ ] **Step 1: 寫失敗測試**

`tests/test_native_client.py`——複製 `tests/test_shioaji_adapter.py:46-97` 的 `_FakeApi`/`_FakeTrade` 模式（本檔自帶一份，勿跨檔 import 測試私有類）：

```python
from decimal import Decimal
from quanquant.broker.base import OrderError, TradeNotFoundError
from quanquant.broker.native import ShioajiNativeClient


def _client(on_raw=None):
    c = ShioajiNativeClient(
        api_key="k", secret_key="s", ca_path=None, ca_passwd=None, person_id=None,
        symbol="TXF", mode="sim", on_raw=on_raw or (lambda kind, payload: None),
    )
    c.api = _FakeApi()
    c.contract = object()
    c.account = "F1"
    return c


def test_place_mkt_sends_zero_price_and_returns_ack_fields():
    c = _client()
    ack = c.place(action="Buy", price=Decimal("0"), qty=1,
                  price_type="MKT", order_type="IOC", octype="Auto")
    assert set(ack) == {"ordno", "broker_order_id"}
    assert c.api.placed[-1]["price"] == 0.0


def test_cancel_unknown_ordno_raises_order_error():
    c = _client()
    try:
        c.cancel("NOPE")
        assert False, "應該 raise"
    except OrderError:
        pass


def test_update_unknown_ordno_raises_trade_not_found():
    c = _client()
    try:
        c.update("NOPE", price=Decimal("21000"), qty=2)
        assert False, "應該 raise"
    except TradeNotFoundError as exc:
        assert exc.ordno == "NOPE"


def test_on_order_cb_calls_on_raw_with_kind_and_json_safe_payload():
    seen = []
    c = _client(on_raw=lambda kind, payload: seen.append((kind, payload)))
    c._on_order_cb("OrderState.FuturesDeal", {"trade_id": "T1", "price": 100.0})
    assert seen == [("deal_report", {"trade_id": "T1", "price": 100.0})]


def test_on_order_cb_degrades_when_on_raw_raises_once():
    calls = []
    def flaky(kind, payload):
        calls.append(payload)
        if len(calls) == 1:
            raise RuntimeError("db down")
    c = _client(on_raw=flaky)
    c._on_order_cb("OrderState.FuturesDeal", {"trade_id": "T1"})
    assert calls[1]["_unparsed"] is True and "repr" in calls[1]


def test_trades_snapshot_filters_by_watermark_and_returns_newest():
    c = _client()
    # _FakeApi 需支援 list_trades 回帶 status.order_datetime 的 trade（測試檔內自建）
    payloads, newest = c.trades_snapshot(after=None)
    assert isinstance(payloads, list) and (newest is None or hasattr(newest, "year"))
```

（`_FakeApi` 需比照 test_shioaji_adapter 版本補 `placed`/`_live_trades`/`update_status`/`list_trades`/`cancel_order`/`update_order`；trades_snapshot 測試自建帶 `order.id`/`order.seqno`/`status.status`/`status.order_datetime` 的假 trade。）

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_native_client.py -v`
Expected: FAIL（`ModuleNotFoundError: quanquant.broker.native` / `ImportError: TradeNotFoundError`）

- [ ] **Step 3: 實作**

1. `base.py` 加 `TradeNotFoundError`（如上簽名）。
2. 建 `native.py`：把 Interfaces 列出的方法自 `shioaji_adapter.py` 對應行段逐字搬入（來源行號見 Consumes），只改屬性名映射與 `_on_order_cb` 的 on_raw 委派。`connect()` 末尾 `return self.account`。`trades_snapshot` 從 `_reconcile_blocking` L300-324 搬 native 半段（`update_status`→`list_trades`→逐 trade 抽 `order.id`/`order.seqno`/`status.status`/watermark、過濾 `after`），**不含**任何 `session_factory` 行；回傳 `(payloads, newest)`。
3. 檔頭 docstring 標注：「本檔是全 broker 子系統唯一允許 import shioaji 的模組；不得 import 任何 DB/repository/session 相關符號」。

- [ ] **Step 4: 跑測試確認通過**

Run: `uv run pytest tests/test_native_client.py -v` → PASS；`uv run pytest` → 全綠（既有 649 + 新增）。

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/broker/native.py src/quanquant/broker/base.py tests/test_native_client.py
git commit -m "feat(agent): 抽出 ShioajiNativeClient——純 SDK 操作、零 DB 相依（Inc0 Task 1）"
```

---

### Task 2: ShioajiAdapter 委派重構（行為不變，649 綠當安全網）

**Files:**
- Modify: `src/quanquant/broker/shioaji_adapter.py`
- Test: 既有全部測試（不新增檔；本 task 的「測試」就是既有套件不變綠）

**Interfaces:**
- Consumes: Task 1 的 `ShioajiNativeClient`
- Produces（Task 6/8 依賴）: adapter 新增內部屬性 `self._native: ShioajiNativeClient`；新方法 `_read_reconcile_cursor(self) -> datetime | None`、`_stage_reconcile_results(self, payloads: list[dict], newest: "datetime | None") -> int`；`_api`/`_contract`/`account` 變成委派 property（getter/setter 皆通，既有測試 `a._api = _FakeApi()` 注入法不變）。

- [ ] **Step 1: 委派改寫**

1. `__init__` 尾端建 `self._native = ShioajiNativeClient(api_key=api_key, secret_key=secret_key, ca_path=ca_path, ca_passwd=ca_passwd, person_id=person_id, symbol=symbol, mode=mode, on_raw=self._persist_raw)`；移除 `self._api = None`/`self._contract = None`/`self.account = ""` 直接屬性。
2. 加 property 墊片（相容既有測試注入）：

```python
@property
def _api(self):
    return self._native.api

@_api.setter
def _api(self, value):
    self._native.api = value

@property
def _contract(self):
    return self._native.contract

@_contract.setter
def _contract(self, value):
    self._native.contract = value

@property
def account(self) -> str:
    return self._native.account

@account.setter
def account(self, value: str) -> None:
    self._native.account = value
```

3. 落地 handler（原 `_on_order_cb` 的 DB 半段）：

```python
def _persist_raw(self, kind: str, payload: dict) -> None:
    commit_raw_callback(self._session_factory, kind=kind, broker=self.broker, payload=payload)
```

4. 方法改為薄委派（原 body 刪除，來源已搬 native.py）：`_connect_blocking` → `self._native.connect()`；`_place_blocking(req)` → `return self._native.place(action=req.action, price=req.price, qty=req.qty, price_type=req.price_type, order_type=req.order_type, octype=req.octype)`；`_cancel_blocking(ordno)` → `self._native.cancel(ordno)`；`_update_blocking(...)` → `self._native.update(...)`；`_probe_blocking` → `self._native.probe()`；`_query_order_qty_blocking(ordno)` → `return self._native.query_order_qty(ordno)`（保留「呼叫端須已持鎖」docstring）；`close()` 鎖內改呼 `asyncio.to_thread(self._native.close)`；`_on_order_cb(stat, msg)` → `self._native._on_order_cb(stat, msg)`（相容既有 hardening 測試直呼）。刪除已搬走的 `_contract_for`/`_refresh_and_list_trades`/`_find_trade_by_ordno`/`_json_safe`/`_ack_fields_from_trade`/`_trade_watermark`（若有測試直呼舊名，保留同名薄委派）。`_TradeNotFoundError = TradeNotFoundError`（import 自 base，保留舊名別名供既有 except 與測試）。
5. reconcile 拆分（行為不變）：

```python
def _read_reconcile_cursor(self) -> "datetime | None":
    with self._session_factory() as session:
        return brepo.get_reconcile_cursor(session, broker=self.broker,
                                          account=self.account, mode=self.mode)

def _stage_reconcile_results(self, payloads: list[dict], newest) -> int:
    if not payloads:
        return 0
    with self._session_factory() as session:
        for p in payloads:
            brepo.stage_raw_inbox(session, kind="order_report", broker=self.broker,
                                  payload=json.dumps(p))
        if newest is not None:
            brepo.upsert_reconcile_cursor(session, broker=self.broker,
                                          account=self.account, mode=self.mode, at=newest)
        session.commit()
    return len(payloads)

def _reconcile_blocking(self) -> int:
    if self._api is None:
        return 0
    after = self._read_reconcile_cursor()
    payloads, newest = self._native.trades_snapshot(after)
    return self._stage_reconcile_results(payloads, newest)
```

（`get_reconcile_cursor`/`upsert_reconcile_cursor` 的實際簽名以 `repository.py` 現況為準——沿用原 `_reconcile_blocking` L297-333 的呼叫寫法。）

- [ ] **Step 2: 全套件驗證**

Run: `uv run pytest`
Expected: 全綠。若有測試因 monkeypatch adapter 私有方法而 FAIL：優先在 adapter 保留同名薄委派使測試不改而過；確實過時者才小幅改測試（逐筆說明原因）。

- [ ] **Step 3: 驗證 import 邊界**

Run: `grep -rn "import shioaji" src/quanquant/broker/`
Expected: 只剩 `src/quanquant/broker/native.py` 一筆。

- [ ] **Step 4: Commit**

```bash
git add -A src/quanquant/broker/ tests/
git commit -m "refactor(agent): ShioajiAdapter 委派 ShioajiNativeClient，adapter 端不再直接碰 SDK（Inc0 Task 2）"
```

---

### Task 3: 通道例外 + `_classify_place_failure` 擴充

**Files:**
- Modify: `src/quanquant/broker/base.py`、`src/quanquant/broker/shioaji_adapter.py`
- Test: `tests/test_adapter_remote.py`（新檔，先放分類測試）

**Interfaces:**
- Produces（Task 5/6 依賴）:

```python
# base.py
class AgentUnavailableError(OrderError):
    """指令送出前 agent 即不在線——保證未送達券商，可安全判 failed。"""

class AgentCommandTimeoutError(OrderError):
    """指令可能已送達 agent/券商但未收到 ack——必須保守判 unknown。"""
```

- 分類語意（安全關鍵）：`AgentUnavailableError → "failed"`（未送出，退配額安全）；`AgentCommandTimeoutError → "unknown"`（可能已送出，保留配額）；既有 `code:4xx` 規則不變。

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_adapter_remote.py
from quanquant.broker.base import AgentCommandTimeoutError, AgentUnavailableError
from quanquant.broker.shioaji_adapter import _classify_place_failure


def test_agent_unavailable_classified_failed():
    assert _classify_place_failure(AgentUnavailableError("agent 未連線")) == "failed"


def test_agent_timeout_classified_unknown():
    assert _classify_place_failure(AgentCommandTimeoutError("ack 逾時")) == "unknown"


def test_broker_reject_code_still_failed():
    assert _classify_place_failure(Exception("code: 406 not signed")) == "failed"
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_adapter_remote.py -v` → FAIL（ImportError）

- [ ] **Step 3: 實作**

base.py 加兩例外（docstring 如上）；`_classify_place_failure`（shioaji_adapter.py L65-91）開頭插入：

```python
    if isinstance(exc, AgentUnavailableError):
        return "failed"
```

- [ ] **Step 4: 跑測試確認通過** — `uv run pytest tests/test_adapter_remote.py -v` → PASS；全套件綠。

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/broker/base.py src/quanquant/broker/shioaji_adapter.py tests/test_adapter_remote.py
git commit -m "feat(agent): 通道例外與失敗分類——unavailable=failed、timeout=unknown（Inc0 Task 3）"
```

---

### Task 4: `agent_protocol.py` 訊息模型

**Files:**
- Create: `src/quanquant/broker/agent_protocol.py`
- Test: `tests/test_agent_protocol.py`

**Interfaces:**
- Produces（Task 5/7/12 依賴，全文即契約）:

```python
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
```

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_agent_protocol.py
import pytest
from pydantic import ValidationError
from quanquant.broker.agent_protocol import (
    DownPlace, PlaceNative, UpCmdAck, UpReport, parse_downlink, parse_uplink,
)


def test_place_roundtrip():
    cmd = DownPlace(cmd_id="c1", mode="sim",
                    native=PlaceNative(action="Buy", price="21500", qty=1,
                                       price_type="LMT", order_type="ROD", octype="Auto"))
    parsed = parse_downlink(cmd.model_dump())
    assert isinstance(parsed, DownPlace) and parsed.native.price == "21500"


def test_mode_real_rejected():
    with pytest.raises(ValidationError):
        DownPlace(cmd_id="c1", mode="real",
                  native=PlaceNative(action="Buy", price="0", qty=1,
                                     price_type="MKT", order_type="IOC", octype="Auto"))


def test_uplink_discriminates_by_type():
    assert isinstance(parse_uplink({"type": "report", "event_id": 3,
                                    "kind": "deal_report", "payload": {"a": 1}}), UpReport)
    assert isinstance(parse_uplink({"type": "cmd_ack", "cmd_id": "c1", "ok": True,
                                    "result": {"ordno": "101AA1"}}), UpCmdAck)


def test_unknown_type_raises():
    with pytest.raises(ValidationError):
        parse_uplink({"type": "evil"})
```

- [ ] **Step 2: 跑測試確認失敗** — `uv run pytest tests/test_agent_protocol.py -v` → FAIL
- [ ] **Step 3: 實作** — 依 Produces 全文建檔。
- [ ] **Step 4: 跑測試確認通過** — PASS；全套件綠。
- [ ] **Step 5: Commit**

```bash
git add src/quanquant/broker/agent_protocol.py tests/test_agent_protocol.py
git commit -m "feat(agent): WS 協定訊息模型 v1（下行指令/上行事件，sim-only）（Inc0 Task 4）"
```

---

### Task 5: `AgentChannel` + `AgentNativeGateway`（transport 無關）

**Files:**
- Create: `src/quanquant/broker/agent_channel.py`
- Test: `tests/test_agent_channel.py`

**Interfaces:**
- Consumes: Task 3 例外、Task 4 訊息模型、`broker/types.py` 的 `OrderRequest`、`broker/base.py` 的 `OrderError`/`TradeNotFoundError`
- Produces（Task 6/7/8 依賴，全文即契約）:

```python
"""server 端 agent 連線態與指令多工。

不碰 WS 框架——由 web/routers/agent_ws.py 注入 send_json callable，
故可用純 asyncio 單元測試。上行解析後由端點呼叫 resolve_ack / mark_logged_in。
"""
import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime

from quanquant.broker.agent_protocol import (
    DownCancel, DownPlace, DownReconcile, DownUpdate, PlaceNative, UpCmdAck,
)
from quanquant.broker.base import (
    AgentCommandTimeoutError, AgentUnavailableError, OrderError, TradeNotFoundError,
)
from quanquant.broker.types import OrderRequest


class AgentChannel:
    def __init__(self) -> None:
        self._send: Callable[[dict], Awaitable[None]] | None = None
        self._pending: dict[str, asyncio.Future] = {}
        self.logged_in = False
        self.account = ""
        self.last_heartbeat: float | None = None

    @property
    def connected(self) -> bool:
        return self._send is not None

    @property
    def ready(self) -> bool:
        return self._send is not None and self.logged_in

    def attach(self, send_json: Callable[[dict], Awaitable[None]]) -> None:
        self._send = send_json

    def detach(self) -> None:
        self._send = None
        self.logged_in = False
        pending, self._pending = self._pending, {}
        for fut in pending.values():
            if not fut.done():
                fut.set_exception(AgentCommandTimeoutError("agent 連線中斷，指令結果未知"))

    def mark_logged_in(self, account: str) -> None:
        self.logged_in = True
        self.account = account

    def note_heartbeat(self) -> None:
        self.last_heartbeat = time.monotonic()

    def resolve_ack(self, ack: UpCmdAck) -> None:
        fut = self._pending.pop(ack.cmd_id, None)
        if fut is not None and not fut.done():
            fut.set_result(ack)

    async def request(self, cmd: dict, *, cmd_id: str, timeout: float) -> UpCmdAck:
        if not self.ready:
            raise AgentUnavailableError("agent 未連線或未登入")
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[cmd_id] = fut
        try:
            try:
                await self._send(cmd)
            except Exception as exc:  # socket 已壞：可能已部分送出 → 保守 unknown
                raise AgentCommandTimeoutError(f"下行送出失敗: {exc}") from exc
            try:
                return await asyncio.wait_for(fut, timeout)
            except TimeoutError as exc:
                raise AgentCommandTimeoutError(f"等待 cmd_ack 逾時（{timeout}s）") from exc
        finally:
            self._pending.pop(cmd_id, None)


class AgentNativeGateway:
    """把 adapter 的 native 需求翻成下行指令，把 cmd_ack 翻回結果或既有語意的例外。"""

    def __init__(self, channel: AgentChannel, *, timeout_seconds: float) -> None:
        self._channel = channel
        self._timeout = timeout_seconds

    @property
    def ready(self) -> bool:
        return self._channel.ready

    async def place(self, req: OrderRequest) -> dict:
        cmd = DownPlace(
            cmd_id=uuid.uuid4().hex, mode="sim",
            native=PlaceNative(action=req.action, price=str(req.price), qty=req.qty,
                               price_type=req.price_type, order_type=req.order_type,
                               octype=req.octype),
        )
        ack = await self._channel.request(cmd.model_dump(), cmd_id=cmd.cmd_id,
                                          timeout=self._timeout)
        result = self._unwrap(ack)
        return {"ordno": result.get("ordno"), "broker_order_id": result.get("broker_order_id")}

    async def cancel(self, ordno: str) -> None:
        cmd = DownCancel(cmd_id=uuid.uuid4().hex, mode="sim", ordno=ordno)
        self._unwrap(await self._channel.request(cmd.model_dump(), cmd_id=cmd.cmd_id,
                                                 timeout=self._timeout))

    async def update(self, ordno: str, *, price, qty: int, price_type: str | None = None) -> None:
        cmd = DownUpdate(cmd_id=uuid.uuid4().hex, mode="sim", ordno=ordno,
                         price=(str(price) if price is not None else None),
                         qty=qty, price_type=price_type)
        self._unwrap(await self._channel.request(cmd.model_dump(), cmd_id=cmd.cmd_id,
                                                 timeout=self._timeout))

    async def trades_snapshot(self, after: "datetime | None") -> tuple[list[dict], "datetime | None"]:
        cmd = DownReconcile(cmd_id=uuid.uuid4().hex, mode="sim",
                            after=(after.isoformat() if after is not None else None))
        result = self._unwrap(await self._channel.request(cmd.model_dump(), cmd_id=cmd.cmd_id,
                                                          timeout=self._timeout))
        newest = result.get("newest")
        return result.get("payloads", []), (datetime.fromisoformat(newest) if newest else None)

    @staticmethod
    def _unwrap(ack: UpCmdAck) -> dict:
        if ack.ok:
            return ack.result or {}
        if ack.error_kind == "trade_not_found":
            raise TradeNotFoundError((ack.result or {}).get("ordno"))
        # message 內含券商原始錯誤字串（agent 端已 redact），
        # `code: 4xx` 交給既有 _classify_place_failure 判 failed。
        raise OrderError(ack.message or f"agent 指令失敗（{ack.error_kind}）")
```

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_agent_channel.py
import asyncio
import pytest
from quanquant.broker.agent_channel import AgentChannel, AgentNativeGateway
from quanquant.broker.agent_protocol import UpCmdAck
from quanquant.broker.base import (
    AgentCommandTimeoutError, AgentUnavailableError, OrderError, TradeNotFoundError,
)
from quanquant.broker.shioaji_adapter import _classify_place_failure


class _Sink:
    def __init__(self):
        self.msgs = []
    async def __call__(self, msg):
        self.msgs.append(msg)


def _ready_channel():
    ch, sink = AgentChannel(), _Sink()
    ch.attach(sink)
    ch.mark_logged_in("F1")
    return ch, sink


async def test_request_before_attach_raises_unavailable():
    with pytest.raises(AgentUnavailableError):
        await AgentChannel().request({"type": "health", "cmd_id": "c1"}, cmd_id="c1", timeout=1)


async def test_request_resolves_when_ack_arrives():
    ch, sink = _ready_channel()
    task = asyncio.create_task(ch.request({"type": "health", "cmd_id": "c1"},
                                          cmd_id="c1", timeout=1))
    await asyncio.sleep(0)
    ch.resolve_ack(UpCmdAck(cmd_id="c1", ok=True, result={"x": 1}))
    ack = await task
    assert ack.ok and sink.msgs[0]["cmd_id"] == "c1"


async def test_request_timeout_raises_command_timeout():
    ch, _ = _ready_channel()
    with pytest.raises(AgentCommandTimeoutError):
        await ch.request({"type": "health", "cmd_id": "c1"}, cmd_id="c1", timeout=0.01)


async def test_detach_fails_pending_with_timeout_error():
    ch, _ = _ready_channel()
    task = asyncio.create_task(ch.request({"type": "health", "cmd_id": "c1"},
                                          cmd_id="c1", timeout=5))
    await asyncio.sleep(0)
    ch.detach()
    with pytest.raises(AgentCommandTimeoutError):
        await task


class _StubChannel(AgentChannel):
    """request 直接回 canned ack / raise，測 gateway 對映。"""
    def __init__(self, ack=None, exc=None):
        super().__init__()
        self._ack, self._exc = ack, exc
        self.sent = []
    async def request(self, cmd, *, cmd_id, timeout):
        self.sent.append(cmd)
        if self._exc:
            raise self._exc
        return self._ack


def _req():
    from decimal import Decimal
    from quanquant.broker.types import OrderRequest
    return OrderRequest(client_order_id="c-1", symbol="TXF", action="Buy", qty=1,
                        price=Decimal("21500"), price_type="LMT", order_type="ROD",
                        octype="Auto", user_id=1)


async def test_gateway_place_maps_ack_fields_and_serializes_price_as_str():
    ch = _StubChannel(ack=UpCmdAck(cmd_id="x", ok=True,
                                   result={"ordno": "101AA1", "broker_order_id": "101AA1"}))
    gw = AgentNativeGateway(ch, timeout_seconds=1)
    assert await gw.place(_req()) == {"ordno": "101AA1", "broker_order_id": "101AA1"}
    assert ch.sent[0]["native"]["price"] == "21500"


async def test_gateway_trade_not_found_raises_typed():
    ch = _StubChannel(ack=UpCmdAck(cmd_id="x", ok=False, error_kind="trade_not_found",
                                   result={"ordno": "NOPE"}))
    gw = AgentNativeGateway(ch, timeout_seconds=1)
    with pytest.raises(TradeNotFoundError):
        await gw.cancel("NOPE")


async def test_gateway_error_message_preserves_broker_code_for_classification():
    ch = _StubChannel(ack=UpCmdAck(cmd_id="x", ok=False, error_kind="exception",
                                   message="code: 406 Please sign F002 first"))
    gw = AgentNativeGateway(ch, timeout_seconds=1)
    with pytest.raises(OrderError) as ei:
        await gw.place(_req())
    assert _classify_place_failure(ei.value) == "failed"
```

- [ ] **Step 2: 跑測試確認失敗** — `uv run pytest tests/test_agent_channel.py -v` → FAIL（ImportError）
- [ ] **Step 3: 實作** — 依 Produces 全文建 `agent_channel.py`。
- [ ] **Step 4: 跑測試確認通過** — PASS；全套件綠。
- [ ] **Step 5: Commit**

```bash
git add src/quanquant/broker/agent_channel.py tests/test_agent_channel.py
git commit -m "feat(agent): AgentChannel 指令多工 + AgentNativeGateway（timeout→unknown、斷線→保守）（Inc0 Task 5）"
```

---

### Task 6: ShioajiAdapter `remote_gateway` 支援（三段切完成）

**Files:**
- Modify: `src/quanquant/broker/shioaji_adapter.py`
- Test: `tests/test_adapter_remote.py`（擴充）

**Interfaces:**
- Consumes: Task 5 gateway（依 `_NativeGatewayLike` Protocol 鬆耦合，adapter 不 import agent_channel）
- Produces（Task 8 依賴）: `ShioajiAdapter.__init__` 新增 keyword-only 參數 `remote_gateway: "_NativeGatewayLike | None" = None`；行為矩陣——

| 情境 | 結果 |
|---|---|
| gateway.ready=False（place 進入時） | 立刻 `OrderError("agent 未連線，無法下單")`，**不建任何 Order/配額列** |
| gateway.place 成功 | Order submitted + confirm_quota（與 in-process 第 3 段完全同碼） |
| gateway raise `AgentUnavailableError`（鎖內） | Order failed + release_quota（既有 failed 分支） |
| gateway raise `AgentCommandTimeoutError` | Order unknown + 配額保留（既有 unknown 分支） |
| kill switch ON | `_send_gate` raise RiskError，gateway **不被呼叫**，Order failed + release |
| cancel/update | 同 place 分流；`TradeNotFoundError` 語意與 in-process 相同 |
| reconcile（remote） | 讀 cursor → gateway.trades_snapshot → `_stage_reconcile_results`，全程 supervisor.run 內 |

- [ ] **Step 1: 寫失敗測試**（附掛到 `tests/test_adapter_remote.py`）

```python
import pytest
from decimal import Decimal
from datetime import datetime
from sqlmodel import Session, select
from quanquant.broker.base import (
    AgentCommandTimeoutError, AgentUnavailableError, OrderError, RiskError,
)
from quanquant.broker.risk import RiskGuard
from quanquant.broker.shioaji_adapter import ShioajiAdapter
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import OrderRequest
from quanquant.db.models import Order, QuotaReservation, RawInbox


class _FakeGateway:
    def __init__(self):
        self.ready = True
        self.place_calls, self.cancel_calls = [], []
        self.result = {"ordno": "101AA1", "broker_order_id": "101AA1"}
        self.raise_exc = None
        self.snapshot = ([], None)
    async def place(self, req):
        self.place_calls.append(req)
        if self.raise_exc:
            raise self.raise_exc
        return self.result
    async def cancel(self, ordno):
        self.cancel_calls.append(ordno)
        if self.raise_exc:
            raise self.raise_exc
    async def update(self, ordno, *, price, qty, price_type=None):
        if self.raise_exc:
            raise self.raise_exc
    async def trades_snapshot(self, after):
        return self.snapshot


def _guard(engine):
    return RiskGuard(session_factory=lambda: Session(engine), secret="s",
                     owner_user_ids=frozenset({1}), symbol_whitelist=frozenset({"TXF"}),
                     max_qty_per_order=5, max_qty_per_day=20, max_orders_per_day=20)


def _adapter(engine, gw, guard=None):
    a = ShioajiAdapter(api_key="", secret_key="", ca_path=None, ca_passwd=None,
                       person_id=None, symbol="TXF", mode="sim",
                       session_factory=lambda: Session(engine),
                       supervisor=BrokerSupervisor(), risk_guard=guard,
                       sim_fee_per_lot=Decimal("20"), remote_gateway=gw)
    a.account = "F1"
    return a


def _req(cid="c-1"):
    return OrderRequest(client_order_id=cid, symbol="TXF", action="Buy", qty=1,
                        price=Decimal("21500"), price_type="LMT", order_type="ROD",
                        octype="Auto", user_id=1)


async def test_remote_place_success_submitted_and_quota_confirmed(engine):
    gw = _FakeGateway()
    a = _adapter(engine, gw, _guard(engine))
    ack = await a.place(_req(), actor_user_id=1)
    assert ack.status == "submitted" and ack.ordno == "101AA1"
    with Session(engine) as s:
        order = s.exec(select(Order)).one()
        assert order.status == "submitted" and order.ordno == "101AA1"
        assert s.exec(select(QuotaReservation)).one().state == "confirmed"


async def test_remote_place_timeout_unknown_and_quota_reserved(engine):
    gw = _FakeGateway()
    gw.raise_exc = AgentCommandTimeoutError("ack 逾時")
    a = _adapter(engine, gw, _guard(engine))
    with pytest.raises(AgentCommandTimeoutError):
        await a.place(_req(), actor_user_id=1)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "unknown"
        assert s.exec(select(QuotaReservation)).one().state == "reserved"


async def test_remote_place_unavailable_midflight_failed_and_quota_released(engine):
    gw = _FakeGateway()
    gw.raise_exc = AgentUnavailableError("斷線")
    a = _adapter(engine, gw, _guard(engine))
    with pytest.raises(AgentUnavailableError):
        await a.place(_req(), actor_user_id=1)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "failed"
        assert s.exec(select(QuotaReservation)).one().state == "released"


async def test_remote_place_offline_fails_fast_no_db_rows(engine):
    gw = _FakeGateway()
    gw.ready = False
    a = _adapter(engine, gw, _guard(engine))
    with pytest.raises(OrderError):
        await a.place(_req(), actor_user_id=1)
    with Session(engine) as s:
        assert s.exec(select(Order)).all() == []
        assert s.exec(select(QuotaReservation)).all() == []


async def test_kill_switch_blocks_before_gateway_called(engine):
    gw = _FakeGateway()
    guard = _guard(engine)
    guard.set_kill_switch(True)
    a = _adapter(engine, gw, guard)
    with pytest.raises(RiskError):
        await a.place(_req(), actor_user_id=1)
    assert gw.place_calls == []


async def test_remote_reconcile_stages_payloads_and_returns_count(engine):
    gw = _FakeGateway()
    gw.snapshot = ([{"order_id": "101AA1", "seqno": "101AA1", "status": "Filled"}],
                   datetime(2026, 8, 4, 9, 0))
    a = _adapter(engine, gw, _guard(engine))
    await a.reconcile()
    with Session(engine) as s:
        rows = s.exec(select(RawInbox)).all()
        assert len(rows) == 1 and rows[0].kind == "order_report"
```

- [ ] **Step 2: 跑測試確認失敗** — `uv run pytest tests/test_adapter_remote.py -v` → FAIL（`__init__` 不接受 remote_gateway）

- [ ] **Step 3: 實作**

1. adapter 檔內加 Protocol（比照既有 `_RiskGuardLike` 寫法）：

```python
class _NativeGatewayLike(Protocol):
    @property
    def ready(self) -> bool: ...
    async def place(self, req: OrderRequest) -> dict: ...
    async def cancel(self, ordno: str) -> None: ...
    async def update(self, ordno: str, *, price, qty: int, price_type: str | None = None) -> None: ...
    async def trades_snapshot(self, after): ...
```

2. `__init__` 加 `remote_gateway: "_NativeGatewayLike | None" = None`，存 `self._remote_gateway`。
3. `place()` 開頭（L392 hash 計算之前）插入 fail-fast：

```python
    if self._remote_gateway is not None and not self._remote_gateway.ready:
        raise OrderError("agent 未連線，無法下單")
```

4. `_send_gate` 改寫（原 L381-385）：

```python
async def _send_gate(self) -> None:
    if self._remote_gateway is not None:
        if not self._remote_gateway.ready:
            raise AgentUnavailableError("agent 未連線或未登入")
    elif self._api is None:
        raise OrderError("下單 session 尚未就緒")
    if self._risk_guard is not None and self._risk_guard.kill_switch:
        raise RiskError("kill switch 已啟動，拒絕送出")
```

5. `_do_place` 內 native 呼叫行（原 L429-431）改：

```python
            await self._send_gate()
            if self._remote_gateway is not None:
                return await self._remote_gateway.place(req)
            return await asyncio.to_thread(self._place_blocking, req)
```

6. `cancel()`：`_api is None` 檢查（L540-544）改為與 `_send_gate` 相同的 remote/in-process 分流（remote 不 ready → `OrderError("agent 未連線")`；kill switch 依然刻意不擋取消）；native 呼叫行改 `await self._remote_gateway.cancel(ordno)` / 原 to_thread 分流。
7. `update()`：native 呼叫行同法分流成 `await self._remote_gateway.update(ordno, price=new_price, qty=new_qty, price_type=...)`；`TradeNotFoundError` 分支不動（gateway 丟同型別）。
8. `reconcile()`：內部改為

```python
async def _reconcile_inner(self) -> int:
    if self._remote_gateway is None:
        return await asyncio.to_thread(self._reconcile_blocking)
    if not self._remote_gateway.ready:
        return 0
    after = await asyncio.to_thread(self._read_reconcile_cursor)
    payloads, newest = await self._remote_gateway.trades_snapshot(after)
    return await asyncio.to_thread(self._stage_reconcile_results, payloads, newest)
```

`reconcile()` 的 supervisor.run 改跑 `self._reconcile_inner`，count>0 的 ops 告警邏輯不動。

- [ ] **Step 4: 跑測試確認通過** — `uv run pytest tests/test_adapter_remote.py -v` → PASS；`uv run pytest` 全綠（in-process 行為零變化）。

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/broker/shioaji_adapter.py tests/test_adapter_remote.py
git commit -m "feat(agent): adapter 三段切——DB 決策/寫回留 server，native 經 remote gateway 下行（Inc0 Task 6）"
```

---

### Task 7: `/ws/agent` WebSocket 端點 + 設定欄位

**Files:**
- Create: `src/quanquant/web/routers/agent_ws.py`
- Modify: `src/quanquant/config.py`、`src/quanquant/web/app.py`（create_app 只加 include_router）
- Test: `tests/test_agent_ws.py`

**Interfaces:**
- Consumes: Task 4 協定、Task 5 `AgentChannel`、既有 `commit_raw_callback(session_factory, *, kind, broker, payload)`、`OrderSessionState`、`OrderEventHub.publish()`
- Produces: `config.Settings` 新欄位（Task 8/14 依賴）——

```python
# config.py（比照 L84 慣例，加在 order_* 區塊之後）
# --- 本機 broker agent 通道（Increment 0，單一信任使用者） ---
order_channel: str = "inprocess"    # inprocess | agent（agent=Shioaji I/O 在使用者本機執行）
agent_ws_token: str = ""            # agent WS 靜態 token；空字串=agent 通道停用
agent_command_timeout_seconds: float = 10.0  # server 等 cmd_ack 逾時（逾時→unknown 保守）
```

- 端點契約：連上後先 accept；token（header `x-agent-token`）錯誤/通道未啟用 → `close(code=1008)`。上行 dispatch：`login`→mark_logged_in+`adapter.account`+`mark_ready`+publish+**create_task(reconcile)**；`report`→`to_thread(commit_raw_callback)` 成功後回 `report_ack`；`cmd_ack`→`channel.resolve_ack`；`health`→`note_heartbeat`；斷線→`detach`+`mark_disabled("agent 離線")`+publish。新連線取代舊連線（先 detach 舊的）。

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_agent_ws.py
import time
import pytest
from sqlmodel import Session, select
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from quanquant.broker.agent_channel import AgentChannel
from quanquant.broker.session_state import OrderSessionState
from quanquant.config import get_settings
from quanquant.db.models import RawInbox
from quanquant.web.app import create_app
from quanquant.web.deps import get_session


class _FakeHub:
    def __init__(self):
        self.publishes = 0
    def publish(self):
        self.publishes += 1


class _FakeAdapter:
    def __init__(self):
        self.account = ""
        self.reconcile_calls = 0
        self.block = None            # asyncio.Event 時卡住 reconcile（測非 inline）
    async def reconcile(self):
        self.reconcile_calls += 1
        if self.block is not None:
            await self.block.wait()


class _SpyChannel(AgentChannel):
    def __init__(self):
        super().__init__()
        self.acks = []
    def resolve_ack(self, ack):
        self.acks.append(ack)
        super().resolve_ack(ack)


def _wait(cond, timeout=2.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.02)
    return False


@pytest.fixture
def ws_env(engine, monkeypatch):
    monkeypatch.setenv("AGENT_WS_TOKEN", "tok")
    get_settings.cache_clear()
    app = create_app()

    def _session_override():
        with Session(engine) as s:
            yield s

    app.dependency_overrides[get_session] = _session_override
    app.state.agent_channel = _SpyChannel()
    app.state.order_session_state = OrderSessionState()
    app.state.order_events = _FakeHub()
    app.state.order_service = _FakeAdapter()
    app.state.order_session_factory = lambda: Session(engine)
    yield app
    get_settings.cache_clear()


def test_bad_token_closed(ws_env):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "wrong"}) as ws:
        with pytest.raises(WebSocketDisconnect):
            ws.receive_json()


def test_login_marks_ready_sets_account_schedules_reconcile(ws_env):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
        assert _wait(lambda: ws_env.state.order_session_state.ready)
        assert ws_env.state.order_service.account == "F1"
        assert _wait(lambda: ws_env.state.order_service.reconcile_calls == 1)
        assert ws_env.state.order_events.publishes >= 1
    assert _wait(lambda: ws_env.state.order_session_state.disabled)  # 斷線 → disabled


def test_report_staged_then_acked(ws_env, engine):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        ws.send_json({"type": "report", "event_id": 7, "kind": "deal_report",
                      "payload": {"trade_id": "T1"}})
        assert ws.receive_json() == {"type": "report_ack", "event_id": 7}
    with Session(engine) as s:
        rows = s.exec(select(RawInbox)).all()
        assert len(rows) == 1 and rows[0].kind == "deal_report"


def test_duplicate_report_resend_both_staged_and_acked(ws_env, engine):
    # at-least-once：staging 層允許重複列，去重由既有 Deal 層 uq_deal_fill 吸收
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        for _ in range(2):
            ws.send_json({"type": "report", "event_id": 7, "kind": "deal_report",
                          "payload": {"trade_id": "T1"}})
            assert ws.receive_json()["event_id"] == 7
    with Session(engine) as s:
        assert len(s.exec(select(RawInbox)).all()) == 2


def test_cmd_ack_routed_to_channel(ws_env):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        ws.send_json({"type": "cmd_ack", "cmd_id": "c9", "ok": True, "result": {}})
        assert _wait(lambda: len(ws_env.state.agent_channel.acks) == 1)
        assert ws_env.state.agent_channel.acks[0].cmd_id == "c9"


def test_login_reconcile_not_inline_receive_loop_stays_responsive(ws_env, engine):
    import asyncio
    adapter = ws_env.state.order_service
    adapter.block = asyncio.Event()   # reconcile 永久卡住
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
        ws.send_json({"type": "report", "event_id": 1, "kind": "order_report",
                      "payload": {"k": 1}})
        # reconcile 卡住時 report 仍被處理 → 證明 login 用 create_task 非 inline await
        assert ws.receive_json() == {"type": "report_ack", "event_id": 1}
    adapter.block.set()


def test_invalid_frame_ignored_connection_survives(ws_env):
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": "tok"}) as ws:
        ws.send_json({"type": "evil"})
        ws.send_json({"type": "report", "event_id": 2, "kind": "order_report",
                      "payload": {}})
        assert ws.receive_json()["event_id"] == 2
```

- [ ] **Step 2: 跑測試確認失敗** — `uv run pytest tests/test_agent_ws.py -v` → FAIL（404 route 不存在）

- [ ] **Step 3: 實作**

1. config.py 加三欄位（見 Produces）。
2. 建 `agent_ws.py`：

```python
"""本機 broker agent 的 WebSocket 端點（上行回報/ack、下行指令的傳輸層）。

鐵律：本 receive 迴圈絕不取得 supervisor.lock、絕不 inline await 長工作
（login 觸發的 reconcile 一律 create_task）——否則 receive 迴圈等 reconcile、
reconcile 等 cmd_ack、cmd_ack 需要 receive 迴圈 → 死鎖。
"""
import asyncio
import logging
import secrets as _secrets

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import ValidationError

from quanquant.broker.agent_protocol import (
    DownReportAck, UpCmdAck, UpHealth, UpLogin, UpReport, parse_uplink,
)
from quanquant.broker.inbox_worker import commit_raw_callback
from quanquant.config import get_settings

log = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/ws/agent")
async def agent_ws(websocket: WebSocket) -> None:
    settings = get_settings()
    state = websocket.app.state
    channel = getattr(state, "agent_channel", None)
    token = websocket.headers.get("x-agent-token", "")
    await websocket.accept()
    if (channel is None or not settings.agent_ws_token
            or not _secrets.compare_digest(token, settings.agent_ws_token)):
        await websocket.close(code=1008)
        return
    if channel.connected:
        channel.detach()   # 新連線取代殘留半開連線（agent 重啟）
    channel.attach(websocket.send_json)
    order_state = state.order_session_state
    hub = getattr(state, "order_events", None)
    adapter = state.order_service
    session_factory = state.order_session_factory
    try:
        while True:
            data = await websocket.receive_json()
            try:
                msg = parse_uplink(data)
            except ValidationError:
                log.warning("agent 上行訊息格式不符，忽略：%s", str(data)[:200])
                continue
            if isinstance(msg, UpLogin):
                channel.mark_logged_in(msg.account)
                adapter.account = msg.account
                order_state.mark_ready()
                if hub is not None:
                    hub.publish()
                asyncio.create_task(_reconcile_after_login(adapter))
            elif isinstance(msg, UpReport):
                await asyncio.to_thread(
                    commit_raw_callback, session_factory,
                    kind=msg.kind, broker="shioaji", payload=msg.payload,
                )
                await websocket.send_json(DownReportAck(event_id=msg.event_id).model_dump())
            elif isinstance(msg, UpCmdAck):
                channel.resolve_ack(msg)
            elif isinstance(msg, UpHealth):
                channel.note_heartbeat()
    except WebSocketDisconnect:
        pass
    finally:
        channel.detach()
        order_state.mark_disabled("agent 離線")
        if hub is not None:
            hub.publish()
        log.warning("agent WS 連線中斷，下單暫停（等待 agent 重連）")


async def _reconcile_after_login(adapter) -> None:
    try:
        await adapter.reconcile()
    except Exception:
        log.exception("agent 登入後 reconcile 失敗（best-effort，不影響連線）")
```

3. app.py `create_app`：在 `app.include_router(health.router)` 之後加 `app.include_router(agent_ws_routes.router)`（**public**——認證靠 token，非 cookie；import 比照既有 router import 寫法）。

- [ ] **Step 4: 跑測試確認通過** — `uv run pytest tests/test_agent_ws.py -v` → PASS；全套件綠。

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/web/routers/agent_ws.py src/quanquant/config.py src/quanquant/web/app.py tests/test_agent_ws.py
git commit -m "feat(agent): /ws/agent 端點——token 認證、上行落 RawInbox 後 ack、login 觸發 reconcile（Inc0 Task 7）"
```

---

### Task 8: app 啟動配線 agent 分支 + agent 模式 watchdog

**Files:**
- Modify: `src/quanquant/web/app.py`、`src/quanquant/broker/watchdog.py`
- Test: `tests/test_agent_app_wiring.py`

**Interfaces:**
- Consumes: Task 5-7 全部；既有 `_start_order_subsystem`（app.py L172-300）、`_retry_quarantined`（watchdog.py L124-126）
- Produces: `app.py` 新 helper `_start_agent_channel_subsystem(app, settings, tasks, order_state, ops_alerter) -> None`（async）；`watchdog.py` 新 `run_agent_watchdog(adapter, *, unquarantine_after_seconds: float) -> None`（async）；新 `app.state.order_session_factory`（agent 分支設定；Task 7 端點依賴）。

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_agent_app_wiring.py
import asyncio
from types import SimpleNamespace
import pytest
from quanquant.broker.session_state import OrderSessionState
from quanquant.config import Settings
from quanquant.web.app import _start_agent_channel_subsystem


def _settings(**kw):
    base = dict(order_channel="agent", order_mode="sim", agent_ws_token="tok",
                order_owner_user_ids="1", session_secret="s")
    base.update(kw)
    return Settings(**base)


def _app():
    return SimpleNamespace(state=SimpleNamespace(order_events=None))


async def _run(settings, engine, monkeypatch):
    # 防呆：wiring 測試絕不能碰真實 quanquant.db —— get_engine 換成 in-memory
    monkeypatch.setattr("quanquant.web.app.get_engine", lambda: engine)
    app, state, tasks = _app(), OrderSessionState(), []
    await _start_agent_channel_subsystem(app, settings, tasks, state, None)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    return app, state, tasks


async def test_agent_mode_requires_sim(engine, monkeypatch):
    _, state, tasks = await _run(_settings(order_mode="real"), engine, monkeypatch)
    assert not state.ready and not state.disabled          # 真故障 → /healthz 503
    assert "僅支援" in (state.last_error or "") and tasks == []


async def test_agent_mode_without_token_disabled(engine, monkeypatch):
    _, state, tasks = await _run(_settings(agent_ws_token=""), engine, monkeypatch)
    assert state.disabled and tasks == []                   # 刻意停用 → /healthz 200


async def test_agent_mode_without_owner_disabled(engine, monkeypatch):
    _, state, _ = await _run(_settings(order_owner_user_ids=""), engine, monkeypatch)
    assert state.disabled and "order_owner_user_ids" in (state.last_error or "")


async def test_agent_mode_happy_path_wires_state(engine, monkeypatch):
    app, state, tasks = await _run(_settings(), engine, monkeypatch)
    from quanquant.broker.agent_channel import AgentChannel
    from quanquant.broker.shioaji_adapter import ShioajiAdapter
    assert isinstance(app.state.agent_channel, AgentChannel)
    assert isinstance(app.state.order_service, ShioajiAdapter)
    assert app.state.order_risk_guard is not None
    assert callable(app.state.order_session_factory)
    assert state.disabled and "agent 未連線" in (state.last_error or "")
    assert len(tasks) >= 2                                  # inbox worker + agent watchdog
```

- [ ] **Step 2: 跑測試確認失敗** — `uv run pytest tests/test_agent_app_wiring.py -v` → FAIL（ImportError）

- [ ] **Step 3: 實作**

1. `watchdog.py` 加：

```python
async def run_agent_watchdog(adapter, *, unquarantine_after_seconds: float) -> None:
    """agent 通道模式的精簡 watchdog：只做 DB-only 背景工作。

    連線/重連/健康是 agent 端與 WS 端點的責任；`_reconcile_unknown_quota`
    需要 native 查詢，Increment 0 在 agent 模式停用（保守後果：unknown 委託的
    配額維持保留、不會超賣），Increment 1 以下行 query_qty 指令補回。
    supervisor.lock 在 agent 模式背後沒有 native → 即決策 5 的「獨立 server 鎖」。
    """
    while True:
        await asyncio.sleep(unquarantine_after_seconds)
        try:
            await _retry_quarantined(adapter, unquarantine_after_seconds)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("agent watchdog：retry_quarantined 失敗")
```

2. `app.py`：`_start_order_subsystem` 在 preflight 之前插入分流（`ops_alerter = getattr(app.state, "ops_alerter", None)` 先取）：

```python
    if settings.order_channel not in ("inprocess", "agent"):
        order_state.mark_unhealthy(f"ORDER_CHANNEL 設定錯誤: {settings.order_channel!r}")
        return
    if settings.order_channel == "agent":
        await _start_agent_channel_subsystem(app, settings, tasks, order_state, ops_alerter)
        return
    # 以下既有 in-process 路徑一行不動
```

3. `app.py` 新 helper（延遲 import 比照 L201-206 慣例）：

```python
async def _start_agent_channel_subsystem(app, settings, tasks, order_state, ops_alerter) -> None:
    from decimal import Decimal

    from quanquant.broker.agent_channel import AgentChannel, AgentNativeGateway
    from quanquant.broker.inbox_worker import RawInboxWorker
    from quanquant.broker.risk import RiskGuard, parse_owner_ids, parse_whitelist
    from quanquant.broker.shioaji_adapter import ShioajiAdapter
    from quanquant.broker.supervisor import BrokerSupervisor
    from quanquant.broker.watchdog import run_agent_watchdog

    if settings.order_mode != "sim":
        order_state.mark_unhealthy("agent 通道 Increment 0 僅支援 ORDER_MODE=sim")
        return
    if not settings.agent_ws_token:
        order_state.mark_disabled("未設定 AGENT_WS_TOKEN，agent 通道停用")
        return
    owner_ids = parse_owner_ids(settings.order_owner_user_ids)
    if not owner_ids:
        order_state.mark_disabled("order_owner_user_ids 未設定，下單子系統未啟用")
        return

    supervisor = BrokerSupervisor()

    def _order_session() -> Session:
        return Session(get_engine())

    risk_guard = RiskGuard(
        session_factory=_order_session,
        secret=settings.session_secret or "dev-only-insecure",
        owner_user_ids=owner_ids,
        symbol_whitelist=parse_whitelist(settings.order_symbol_whitelist),
        max_qty_per_order=settings.order_max_qty_per_order,
        max_qty_per_day=settings.order_max_qty_per_day,
        max_orders_per_day=settings.order_max_orders_per_day,
        confirm_token_ttl_seconds=settings.order_confirm_token_ttl_seconds,
        kill_switch_initial=settings.order_kill_switch_initial,
    )
    channel = AgentChannel()
    gateway = AgentNativeGateway(channel,
                                 timeout_seconds=settings.agent_command_timeout_seconds)
    adapter = ShioajiAdapter(
        api_key="", secret_key="", ca_path=None, ca_passwd=None, person_id=None,
        symbol=settings.symbol, mode="sim", session_factory=_order_session,
        supervisor=supervisor, risk_guard=risk_guard,
        sim_fee_per_lot=Decimal(settings.order_sim_fee_per_lot),
        ops_alerter=ops_alerter, remote_gateway=gateway,
    )
    inbox_worker = RawInboxWorker(
        session_factory=_order_session, supervisor=supervisor,
        deal_mapper=adapter._map_deal_report,
        order_report_mapper=adapter._map_order_report,
        order_events=getattr(app.state, "order_events", None),
        ops_alerter=ops_alerter,
    )
    app.state.agent_channel = channel
    app.state.order_service = adapter
    app.state.order_risk_guard = risk_guard
    app.state.order_inbox_worker = inbox_worker
    app.state.order_session_factory = _order_session
    order_state.mark_disabled("agent 未連線")     # 等 agent 上線；/healthz 200
    tasks.append(asyncio.create_task(inbox_worker.run()))
    tasks.append(asyncio.create_task(run_agent_watchdog(
        adapter, unquarantine_after_seconds=settings.order_unquarantine_after_seconds)))
```

4. 孤兒掃描（T0.2 可視性）：把既有 in-process 的孤兒委託掃描 block（app.py L277-289）抽成模組級 helper（例如 `_scan_orphan_orders_once(session_factory, ops_alerter)`，body 逐字搬），in-process 原位改呼 helper，agent 分支結尾同樣 `tasks.append(asyncio.create_task(...))` 排一次。
5. 注意：mapper 仍在 adapter（server 端）——上行是原始 payload、mapping 在 `RawInboxWorker`，agent 端不需要 mapper 與 `_classify_place_failure`。

- [ ] **Step 4: 跑測試確認通過** — `uv run pytest tests/test_agent_app_wiring.py -v` → PASS；全套件綠（in-process 啟動路徑不變）。

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/web/app.py src/quanquant/broker/watchdog.py tests/test_agent_app_wiring.py
git commit -m "feat(agent): ORDER_CHANNEL=agent 啟動分支——registry 單 user、DB-only watchdog、sim 硬 guard（Inc0 Task 8）"
```

---

### Task 9: UI agent 連線狀態

**Files:**
- Create: `src/quanquant/web/templates/partials/agent_status.html`
- Modify: `src/quanquant/web/routers/orders.py`、`src/quanquant/web/templates/orders.html`
- Test: `tests/test_agent_app_wiring.py`（擴充）

**Interfaces:**
- Consumes: `app.state.order_session_state`、orders.py 既有 `render_partial`、orders.html 既有 sse 容器（L82-89）
- Produces: `GET /orders/agent-status` 回 partial；orders 頁 badge 隨 `sse:orders-changed`/`refreshorders` 自動刷新（login/斷線時 WS 端點會 publish → SSE 觸發）。

- [ ] **Step 1: 寫失敗測試**（附掛 `tests/test_agent_app_wiring.py`；fixture 比照 `tests/test_orders_routes.py:133-147` 的 `order_client` 模式建登入 client 並塞 `app.state`）

```python
def test_agent_status_partial_offline_and_online(engine, user, monkeypatch):
    from quanquant.broker.session_state import OrderSessionState
    from quanquant.config import get_settings
    monkeypatch.setenv("ORDER_CHANNEL", "agent")
    get_settings.cache_clear()
    client, app = _make_logged_in_client(engine, user)      # 依 test_orders_routes 慣例建
    state = OrderSessionState()
    state.mark_disabled("agent 未連線")
    app.state.order_session_state = state
    body = client.get("/orders/agent-status").text
    assert "agent 未連線" in body
    state.mark_ready()
    assert "agent 已連線" in client.get("/orders/agent-status").text
    get_settings.cache_clear()


def test_agent_status_hidden_when_inprocess(engine, user, monkeypatch):
    monkeypatch.setenv("ORDER_CHANNEL", "inprocess")
    from quanquant.config import get_settings
    get_settings.cache_clear()
    client, app = _make_logged_in_client(engine, user)
    assert "agent" not in client.get("/orders/agent-status").text
    get_settings.cache_clear()


def test_orders_page_contains_agent_status_div(engine, user):
    # 比照 test_orders_routes 的頁面測試：orders.html 有 hx-get="/orders/agent-status"
    client, app = _make_logged_in_client(engine, user)
    app.state.order_service = None
    assert 'hx-get="/orders/agent-status"' in client.get("/orders").text
```

- [ ] **Step 2: 跑測試確認失敗** — 404 / assert 失敗

- [ ] **Step 3: 實作**

1. orders.py（import `get_settings` 比照既有 config import）：

```python
@router.get("/orders/agent-status", response_class=HTMLResponse)
def orders_agent_status(request: Request):
    state = getattr(request.app.state, "order_session_state", None)
    return HTMLResponse(render_partial(
        "partials/agent_status.html",
        channel=get_settings().order_channel,
        ready=bool(state and state.ready),
        reason=(state.last_error if state else None),
    ))
```

2. `partials/agent_status.html`（樣式 class 比照 kill_switch_control.html 鄰近慣例，不造新 CSS）：

```html
{% if channel == "agent" %}
  {% if ready %}
    <span class="agent-status-ok">🟢 agent 已連線</span>
  {% else %}
    <span class="agent-status-warn">🔴 agent 未連線{% if reason and reason != "agent 未連線" %}（{{ reason }}）{% endif %}</span>
  {% endif %}
{% endif %}
```

3. orders.html：sse 容器（L82-89）內加第三個 div：

```html
<div id="agent-status-box"
     hx-get="/orders/agent-status"
     hx-trigger="load, refreshorders from:body, sse:orders-changed"
     hx-swap="innerHTML"></div>
```

- [ ] **Step 4: 跑測試確認通過** — PASS；全套件綠。
- [ ] **Step 5: Commit**

```bash
git add src/quanquant/web/routers/orders.py src/quanquant/web/templates/ tests/test_agent_app_wiring.py
git commit -m "feat(agent): orders 頁 agent 連線狀態 badge（SSE 驅動刷新）（Inc0 Task 9）"
```

---

### Task 10: agent durable buffer（零丟單根基）

**Files:**
- Create: `src/quanquant/agent/__init__.py`（空檔）、`src/quanquant/agent/buffer.py`
- Test: `tests/test_agent_buffer.py`

**Interfaces:**
- Produces（Task 11/12 依賴，全文即契約）:

```python
"""agent 本機 durable outbox：券商 callback 落地點（T0.1 零丟單的跨網路對應）。

- append() 由 SDK 子程序的 callback 執行緒呼叫：同步 INSERT+commit 成功才返回。
- pending()/mark_sent() 由父程序（WS 泵）呼叫：跨程序經同一 SQLite 檔（WAL）。
- 每次操作短連線 + busy_timeout，避免跨程序鎖競爭複雜化。
"""
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  sent_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_outbox_unsent ON outbox(id) WHERE sent_at IS NULL;
"""


@dataclass(frozen=True)
class BufferRow:
    id: int
    kind: str
    payload: dict


class DurableBuffer:
    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(_SCHEMA)

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self._path, timeout=5)

    def append(self, kind: str, payload: dict) -> int:
        with self._conn() as conn:
            cur = conn.execute("INSERT INTO outbox (kind, payload) VALUES (?, ?)",
                               (kind, json.dumps(payload, ensure_ascii=False)))
            return int(cur.lastrowid)

    def pending(self, limit: int = 50) -> list[BufferRow]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id, kind, payload FROM outbox WHERE sent_at IS NULL "
                "ORDER BY id LIMIT ?", (limit,)).fetchall()
        return [BufferRow(id=r[0], kind=r[1], payload=json.loads(r[2])) for r in rows]

    def mark_sent(self, event_id: int) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE outbox SET sent_at = datetime('now') WHERE id = ?",
                         (event_id,))

    def unsent_count(self) -> int:
        with self._conn() as conn:
            return int(conn.execute(
                "SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL").fetchone()[0])
```

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_agent_buffer.py
import threading
from quanquant.agent.buffer import DurableBuffer


def test_append_survives_reopen(tmp_path):
    p = tmp_path / "outbox.db"
    eid = DurableBuffer(p).append("deal_report", {"trade_id": "T1", "中文": "好"})
    rows = DurableBuffer(p).pending()          # 全新連線（模擬程序重啟）
    assert rows[0].id == eid and rows[0].payload["trade_id"] == "T1"
    assert rows[0].payload["中文"] == "好"


def test_mark_sent_removes_from_pending(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    eid = buf.append("order_report", {"k": 1})
    buf.mark_sent(eid)
    assert buf.pending() == [] and buf.unsent_count() == 0


def test_pending_orders_by_id_and_respects_limit(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    ids = [buf.append("deal_report", {"n": i}) for i in range(5)]
    got = buf.pending(limit=3)
    assert [r.id for r in got] == ids[:3]


def test_append_from_thread_visible_to_main(tmp_path):
    buf = DurableBuffer(tmp_path / "o.db")
    t = threading.Thread(target=lambda: buf.append("deal_report", {"x": 1}))
    t.start(); t.join()
    assert buf.unsent_count() == 1


def test_cross_instance_visibility_same_file(tmp_path):
    # 模擬「子程序寫、父程序讀」的跨程序共享（同檔不同連線）
    p = tmp_path / "o.db"
    writer, reader = DurableBuffer(p), DurableBuffer(p)
    eid = writer.append("deal_report", {"x": 1})
    assert [r.id for r in reader.pending()] == [eid]
    reader.mark_sent(eid)
    assert writer.unsent_count() == 0
```

- [ ] **Step 2: 跑測試確認失敗** — `uv run pytest tests/test_agent_buffer.py -v` → FAIL（ModuleNotFoundError）
- [ ] **Step 3: 實作** — 建 `agent/__init__.py`（空）與 `buffer.py`（Produces 全文）。
- [ ] **Step 4: 跑測試確認通過** — PASS；全套件綠。
- [ ] **Step 5: Commit**

```bash
git add src/quanquant/agent/ tests/test_agent_buffer.py
git commit -m "feat(agent): 本機 durable outbox——callback 同步落地、跨程序共享、補送標記（Inc0 Task 10）"
```

---

### Task 11: SDK 子程序（#203 隔離）+ FakeNativeClient

**Files:**
- Create: `src/quanquant/agent/native_runner.py`、`src/quanquant/agent/testing.py`
- Test: `tests/test_agent_child.py`

**Interfaces:**
- Consumes: Task 1 `ShioajiNativeClient`、Task 10 `DurableBuffer`、`broker/base.py` 的 `TradeNotFoundError`、`broker/redaction.py` 的 `redact_secrets(text, *, secrets)`
- Produces（Task 12/13/15 依賴）:

```python
# native_runner.py
def child_main(conn, *, credentials: dict, symbol: str, mode: str,
               buffer_path: str, native_factory=None) -> None
# conn: multiprocessing.Connection（子端）。credentials={"api_key","secret_key"}。
# 迴圈：conn.recv() → {"op": ...} dict → 序列執行 → conn.send(reply dict)。
# op 契約（request → reply）：
#   {"op":"connect"}                          → {"ok":True,"account":str} | {"ok":False,"message":str}
#   {"op":"place","action","price"(str),"qty","price_type","order_type","octype"}
#                                             → {"ok":True,"result":{"ordno","broker_order_id"}}
#   {"op":"cancel","ordno"}                   → {"ok":True,"result":{}}
#   {"op":"update","ordno","price"(str|None),"qty","price_type"}
#                                             → {"ok":True,"result":{}}
#   {"op":"reconcile","after"(iso|None)}      → {"ok":True,"result":{"payloads":[...],"newest":iso|None}}
#   {"op":"ping"}                             → {"ok":True}
#   {"op":"shutdown"}                         → {"ok":True} 後 break（close native、結束程序）
# 任何例外 → {"ok":False,"error_kind":"trade_not_found"|"exception","message":redacted str}，迴圈續行。
# child 以 mode!="sim" 啟動時，所有 op（含 connect）一律回
# {"ok":False,"error_kind":"mode_mismatch","message":"Increment 0 僅支援 sim"}（防禦層 3）。
# callback：native_factory 建 client 時傳 on_raw=lambda kind,payload: buffer.append(kind,payload)
#          ——SDK callback 執行緒直接同步寫 buffer，落地成功才返回（T0.1 跨程序保存）。

def _default_native_factory(*, credentials, symbol, mode, on_raw):
    from quanquant.broker.native import ShioajiNativeClient
    return ShioajiNativeClient(api_key=credentials["api_key"],
                               secret_key=credentials["secret_key"],
                               ca_path=None, ca_passwd=None, person_id=None,
                               symbol=symbol, mode=mode, on_raw=on_raw)

# testing.py（module-level、可 pickle，供 spawn 與整合測試）
class FakeNativeClient:
    """不碰網路的 native 替身：place 後同步觸發 order/deal 回報。"""
    def __init__(self, *, credentials=None, symbol="TXF", mode="sim", on_raw=None): ...
    def connect(self) -> str            # 回 "F1"
    def close(self) -> None
    def place(self, *, action, price, qty, price_type, order_type, octype) -> dict
        # 回 {"ordno":"101AA1","broker_order_id":"101AA1"}；並依序 on_raw：
        # ("order_report", 巢狀 {operation:{op_type:"New"},order:{id,seqno,ordno,...},status:{}})
        # ("deal_report", 扁平 {trade_id,seqno,ordno,exchange_seq,action,code:"TXFH6",
        #                       price(float),quantity,ts(epoch 秒 float),account_id:"F1"})
        # 欄位形狀 = 2026-07-28 實測定案（見 handoff §5），必須能被
        # adapter._map_order_report/_map_deal_report 成功解析。
    def cancel(self, ordno) -> None     # 未知 ordno → TradeNotFoundError；
                                        # ordno=="BOOM" → RuntimeError(f"boom {api_key}")
                                        #（訊息內嵌憑證，專供 redaction 測試）
    def update(self, ordno, *, price, qty, price_type=None) -> None
    def trades_snapshot(self, after) -> tuple[list, None]   # 回 ([], None)

def fake_native_factory(*, credentials, symbol, mode, on_raw):
    return FakeNativeClient(credentials=credentials, symbol=symbol, mode=mode, on_raw=on_raw)
```

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_agent_child.py
import multiprocessing as mp
import threading
from quanquant.agent.buffer import DurableBuffer
from quanquant.agent.native_runner import child_main
from quanquant.agent.testing import fake_native_factory


def _start_child_thread(tmp_path):
    """用執行緒跑 child_main（測迴圈邏輯；真 Process spawn 另測一條 smoke）。"""
    parent_conn, child_conn = mp.Pipe()
    t = threading.Thread(
        target=child_main, args=(child_conn,),
        kwargs=dict(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                    mode="sim", buffer_path=str(tmp_path / "o.db"),
                    native_factory=fake_native_factory),
        daemon=True)
    t.start()
    return parent_conn, t


def _rpc(conn, msg, timeout=5):
    conn.send(msg)
    assert conn.poll(timeout), f"子程序 {msg['op']} 無回應"
    return conn.recv()


def test_connect_then_place_acks_and_persists_reports(tmp_path):
    conn, t = _start_child_thread(tmp_path)
    assert _rpc(conn, {"op": "connect"}) == {"ok": True, "account": "F1"}
    reply = _rpc(conn, {"op": "place", "action": "Buy", "price": "0", "qty": 1,
                        "price_type": "MKT", "order_type": "IOC", "octype": "Auto"})
    assert reply["ok"] and reply["result"]["ordno"] == "101AA1"
    buf = DurableBuffer(tmp_path / "o.db")
    kinds = [r.kind for r in buf.pending()]
    assert kinds == ["order_report", "deal_report"]        # callback 已同步落地
    _rpc(conn, {"op": "shutdown"}); t.join(timeout=5)


def test_cancel_unknown_maps_trade_not_found(tmp_path):
    conn, t = _start_child_thread(tmp_path)
    _rpc(conn, {"op": "connect"})
    reply = _rpc(conn, {"op": "cancel", "ordno": "NOPE"})
    assert reply == {"ok": False, "error_kind": "trade_not_found",
                     "message": reply["message"], "result": {"ordno": "NOPE"}}
    _rpc(conn, {"op": "shutdown"}); t.join(timeout=5)


def test_exception_reply_is_redacted_and_loop_survives(tmp_path):
    # 用可辨識的憑證值 + FakeNativeClient 的 BOOM 後門（cancel("BOOM") raise 內嵌 api_key）
    parent_conn, child_conn = mp.Pipe()
    t = threading.Thread(
        target=child_main, args=(child_conn,),
        kwargs=dict(credentials={"api_key": "SECRET-KEY-123", "secret_key": "SECRET-VAL-456"},
                    symbol="TXF", mode="sim", buffer_path=str(tmp_path / "o.db"),
                    native_factory=fake_native_factory),
        daemon=True)
    t.start()
    _rpc(parent_conn, {"op": "connect"})
    reply = _rpc(parent_conn, {"op": "cancel", "ordno": "BOOM"})
    assert reply["ok"] is False and reply["error_kind"] == "exception"
    assert "SECRET-KEY-123" not in reply["message"]         # redact_secrets 已遮蔽
    assert "SECRET-VAL-456" not in reply["message"]
    assert _rpc(parent_conn, {"op": "ping"}) == {"ok": True}  # 迴圈仍活著
    _rpc(parent_conn, {"op": "shutdown"}); t.join(timeout=5)


def test_child_refuses_non_sim_mode(tmp_path):
    parent_conn, child_conn = mp.Pipe()
    t = threading.Thread(
        target=child_main, args=(child_conn,),
        kwargs=dict(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                    mode="real", buffer_path=str(tmp_path / "o.db"),
                    native_factory=fake_native_factory),
        daemon=True)
    t.start()
    reply = _rpc(parent_conn, {"op": "connect"})
    assert reply["ok"] is False and reply["error_kind"] == "mode_mismatch"
    _rpc(parent_conn, {"op": "shutdown"}); t.join(timeout=5)


def test_real_process_spawn_smoke(tmp_path):
    """真 multiprocessing spawn 一次：驗 pickle/進入點/管線暢通。"""
    ctx = mp.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe()
    p = ctx.Process(target=child_main, args=(child_conn,),
                    kwargs=dict(credentials={"api_key": "k", "secret_key": "s"},
                                symbol="TXF", mode="sim",
                                buffer_path=str(tmp_path / "o.db"),
                                native_factory=fake_native_factory))
    p.start()
    assert _rpc(parent_conn, {"op": "connect"}, timeout=30)["ok"]
    assert _rpc(parent_conn, {"op": "ping"})["ok"]
    _rpc(parent_conn, {"op": "shutdown"})
    p.join(timeout=10)
    assert p.exitcode == 0
```

（憑證遮蔽斷言以實作時的 `redact_secrets` 行為為準微調——原則：`credentials` 內任一值不得出現在 message。mode_mismatch 測試依實作的指令欄位形狀補完斷言。）

- [ ] **Step 2: 跑測試確認失敗** — `uv run pytest tests/test_agent_child.py -v` → FAIL
- [ ] **Step 3: 實作** — 依 Produces 契約寫 `native_runner.py` 與 `testing.py`。`child_main` 要點：(a) 先建 `DurableBuffer(buffer_path)` 與 native client（`on_raw=buffer.append`）；(b) `while True: msg = conn.recv()` 逐一處理，全 op try/except；(c) 例外回覆的 message 先 `redact_secrets(str(exc), secrets=[v for v in credentials.values() if v])`；(d) `TradeNotFoundError` → `error_kind="trade_not_found"` 並附 `result={"ordno": exc.ordno}`；(e) `shutdown` → `close()` native 後 break；(f) place op 的 `price` 是 wire 字串，先 `Decimal(op["price"])` 再呼 `native.place`（update 的 price 同理，None 直接透傳）；(g) `mode!="sim"` 時所有 op 回 mode_mismatch（不建 native client）。
- [ ] **Step 4: 跑測試確認通過** — PASS；全套件綠。
- [ ] **Step 5: Commit**

```bash
git add src/quanquant/agent/native_runner.py src/quanquant/agent/testing.py tests/test_agent_child.py
git commit -m "feat(agent): SDK 子程序迴圈——#203 隔離、callback 直寫 durable buffer、錯誤 redact（Inc0 Task 11）"
```

---

### Task 12: agent runner——WS session、上行泵、下行 dispatch

**Files:**
- Create: `src/quanquant/agent/ws_client.py`、`src/quanquant/agent/runner.py`
- Test: `tests/test_agent_runner.py`

**Interfaces:**
- Consumes: Task 4 協定、Task 10 buffer、Task 11 `child_main`/`fake_native_factory`
- Produces（Task 13/14/15 依賴）:

```python
# ws_client.py
class Transport(Protocol):
    async def connect(self) -> None: ...
    async def send(self, msg: dict) -> None: ...
    async def receive(self) -> dict: ...
    async def close(self) -> None: ...

class WebsocketsTransport:
    """websockets 套件實作；header x-agent-token 帶 token。"""
    def __init__(self, url: str, *, token: str) -> None
    # connect: websockets.connect(url, additional_headers={"x-agent-token": token})
    #（websockets 舊版參數名 extra_headers——以實際安裝版本為準）
    # send/receive: json.dumps / json.loads；close: await ws.close()

# runner.py
class ChildFrozenError(RuntimeError):
    """SDK 子程序無回應（疑似 issue #203 凍結），需 respawn。"""

class ChildHandle:
    """SDK 子程序的擁有者：spawn(spawn ctx)、序列 RPC（threading.Lock）、ping、terminate。"""
    def __init__(self, *, credentials: dict, symbol: str, mode: str, buffer_path: str,
                 native_factory=None) -> None
    def start(self) -> str                       # spawn + {"op":"connect"} → account；失敗 raise RuntimeError
    def request(self, op: dict, *, timeout: float) -> dict   # conn.send + poll(timeout)；逾時 raise TimeoutError
    def ping(self, *, timeout: float) -> bool
    def terminate(self) -> None                  # kill + join
    @property
    def alive(self) -> bool

class AgentRunner:
    def __init__(self, *, transport: Transport, buffer, child, mode: str = "sim",
                 pump_interval: float = 0.5, resend_after: float = 5.0,
                 child_command_timeout: float = 8.0, child_ping_interval: float = 10.0,
                 child_ping_timeout: float = 20.0, heartbeat_interval: float = 15.0,
                 backoff_base: float = 1.0, backoff_max: float = 60.0) -> None
    def ensure_child(self) -> None               # sync：child 未活則 (re)start，記 self._account
    async def run_once(self) -> None             # 單一 WS session：connect→login→pump/receive/heartbeat 直到斷線/例外
    async def run_forever(self) -> None          # Task 13 實作
    def stop(self) -> None
```

- 行為契約：
  - `run_once` 連上後**先送 `UpLogin(account, mode="sim")`**，再啟動 `_pump`/`_receive_loop`/`_heartbeat` 三個 task，任一 task 例外→全取消、close transport、例外上拋。
  - `_pump`：每 `pump_interval` 讀 `buffer.pending(50)`；`_inflight[event_id]` 在 `resend_after` 內不重送（防 ack 未到前重複洗頻）；送 `UpReport`。
  - `_receive_loop`：`report_ack`→`buffer.mark_sent`＋清 inflight；`health`→回 `UpHealth`；place/cancel/update/reconcile→`_execute_command`（**inline 序列執行＝agent 端 native 序列化第一層**；child pipe lock 為第二層）。
  - `_execute_command`：轉 op dict → `to_thread(child.request, op, timeout=child_command_timeout)`；`TimeoutError`→`UpCmdAck(ok=False, error_kind="timeout", message="agent 子程序無回應")`（server 端保守判 unknown）；其餘 reply 原樣轉 `UpCmdAck`。
  - `child_command_timeout(8s) < server agent_command_timeout_seconds(10s)`：正常失敗路徑是 agent 回 error ack，而非 server 逾時。

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_agent_runner.py
import asyncio
import pytest
from quanquant.agent.buffer import DurableBuffer
from quanquant.agent.runner import AgentRunner


class _FakeTransport:
    def __init__(self):
        self.sent, self.incoming, self.connects = [], asyncio.Queue(), 0
        self.fail_connects = 0                    # 前 N 次 connect 丟例外（Task 13 用）
    async def connect(self):
        self.connects += 1
        if self.connects <= self.fail_connects:
            raise ConnectionError("連不上")
    async def send(self, msg):
        self.sent.append(msg)
    async def receive(self):
        return await self.incoming.get()
    async def close(self):
        pass
    def reports(self):
        return [m for m in self.sent if m["type"] == "report"]


class _FakeChild:
    def __init__(self):
        self.ops, self.starts, self.alive = [], 0, False
        self.ping_ok = True
        self.request_exc = None
    def start(self):
        self.starts += 1
        self.alive = True
        return "F1"
    def request(self, op, *, timeout):
        self.ops.append(op)
        if self.request_exc:
            raise self.request_exc
        return {"ok": True, "result": {"ordno": "101AA1", "broker_order_id": "101AA1"}}
    def ping(self, *, timeout):
        return self.ping_ok
    def terminate(self):
        self.alive = False


async def _until(cond, timeout=3.0):
    async def _poll():
        while not cond():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_poll(), timeout)


def _runner(tr, child, buf):
    return AgentRunner(transport=tr, buffer=buf, child=child,
                       pump_interval=0.02, resend_after=0.5,
                       child_command_timeout=0.5, heartbeat_interval=30,
                       child_ping_interval=0.05, child_ping_timeout=0.1,
                       backoff_base=0.01, backoff_max=0.05)


async def test_run_once_sends_login_first(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.sent) >= 1)
    assert tr.sent[0]["type"] == "login"
    assert tr.sent[0]["account"] == "F1" and tr.sent[0]["mode"] == "sim"
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_pump_sends_pending_and_marks_sent_on_ack(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    ids = [buf.append("deal_report", {"n": i}) for i in range(2)]
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len(tr.reports()) >= 2)
    assert [m["event_id"] for m in tr.reports()[:2]] == ids
    for eid in ids:
        tr.incoming.put_nowait({"type": "report_ack", "event_id": eid})
    await _until(lambda: buf.unsent_count() == 0)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_pump_resends_when_no_ack(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    eid = buf.append("deal_report", {"n": 1})
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    await _until(lambda: len([m for m in tr.reports() if m["event_id"] == eid]) >= 2,
                 timeout=5)                       # resend_after=0.5 後重送
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_crash_before_ack_resent_by_next_session(tmp_path):
    """零丟單核心測試：送出未 ack 就崩潰 → 重啟後補送。"""
    tr1, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    eid = buf.append("deal_report", {"n": 1})
    r1 = _runner(tr1, child, buf)
    r1.ensure_child()
    t1 = asyncio.create_task(r1.run_once())
    await _until(lambda: len(tr1.reports()) >= 1)
    t1.cancel()                                   # 模擬 agent 崩潰（未收 ack）
    await asyncio.gather(t1, return_exceptions=True)
    tr2 = _FakeTransport()
    r2 = _runner(tr2, child, DurableBuffer(tmp_path / "o.db"))
    r2.ensure_child()
    t2 = asyncio.create_task(r2.run_once())
    await _until(lambda: any(m["event_id"] == eid for m in tr2.reports()))
    t2.cancel()
    await asyncio.gather(t2, return_exceptions=True)


async def test_downlink_place_dispatched_to_child_and_acked(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    tr.incoming.put_nowait({"type": "place", "cmd_id": "c1", "mode": "sim",
                            "native": {"action": "Buy", "price": "0", "qty": 1,
                                       "price_type": "MKT", "order_type": "IOC",
                                       "octype": "Auto"}})
    await _until(lambda: any(m.get("type") == "cmd_ack" for m in tr.sent))
    ack = next(m for m in tr.sent if m.get("type") == "cmd_ack")
    assert ack["cmd_id"] == "c1" and ack["ok"] and ack["result"]["ordno"] == "101AA1"
    assert child.ops[0]["op"] == "place" and child.ops[0]["price"] == "0"
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_child_timeout_yields_error_ack(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    child.request_exc = TimeoutError()
    r = _runner(tr, child, buf)
    r.ensure_child()
    task = asyncio.create_task(r.run_once())
    tr.incoming.put_nowait({"type": "cancel", "cmd_id": "c2", "mode": "sim",
                            "ordno": "101AA1"})
    await _until(lambda: any(m.get("type") == "cmd_ack" for m in tr.sent))
    ack = next(m for m in tr.sent if m.get("type") == "cmd_ack")
    assert ack["ok"] is False and ack["error_kind"] == "timeout"
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def test_real_child_handle_spawn_roundtrip(tmp_path):
    """ChildHandle 對真 spawn 子程序的 smoke（fake native factory）。"""
    from quanquant.agent.runner import ChildHandle
    from quanquant.agent.testing import fake_native_factory
    child = ChildHandle(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                        mode="sim", buffer_path=str(tmp_path / "o.db"),
                        native_factory=fake_native_factory)
    assert child.start() == "F1"
    reply = child.request({"op": "ping"}, timeout=10)
    assert reply == {"ok": True}
    child.terminate()
    assert not child.alive
```

- [ ] **Step 2: 跑測試確認失敗** — `uv run pytest tests/test_agent_runner.py -v` → FAIL
- [ ] **Step 3: 實作** — 依 Produces 契約與行為契約實作 `ws_client.py`、`runner.py`（`run_forever` 先放 `raise NotImplementedError`，Task 13 補）。`_pump`/`_receive_loop`/`_execute_command`/`_heartbeat` 為 runner 私有 async 方法；`run_once` 用 `asyncio.wait(..., return_when=FIRST_EXCEPTION)` 收攏、finally 全取消＋`transport.close()`。
- [ ] **Step 4: 跑測試確認通過** — PASS；全套件綠。
- [ ] **Step 5: Commit**

```bash
git add src/quanquant/agent/ws_client.py src/quanquant/agent/runner.py tests/test_agent_runner.py
git commit -m "feat(agent): AgentRunner——login/上行泵/report_ack 標記/下行 dispatch/crash 補送（Inc0 Task 12）"
```

---

### Task 13: agent 韌性——重連 backoff + 子程序凍結偵測/respawn

**Files:**
- Modify: `src/quanquant/agent/runner.py`
- Test: `tests/test_agent_runner.py`（擴充）

**Interfaces:**
- Consumes: Task 12 全部
- Produces（Task 14/15 依賴）: `run_forever` 完整實作；`run_once` 的 task 集合加入 `_child_watchdog`。行為契約：
  - `_child_watchdog`：每 `child_ping_interval` 呼叫 `to_thread(child.ping, timeout=child_ping_timeout)`；False/例外 → raise `ChildFrozenError`（→ run_once 崩出）。
  - `run_forever`：迴圈——`ensure_child()`（凍結後 terminate+start+重新登入，憑證仍在父程序記憶體）→ `run_once()`；`ChildFrozenError` → `child.terminate()` 標記需重啟；任何例外 → log 後 `sleep(backoff)`、backoff×2 封頂 `backoff_max`；成功送出 login 的 session 把 backoff 重設為 `backoff_base`；`stop()` 後結束。
  - server 端視角：凍結 respawn = WS 斷線→重連→重新 `login` → 既有 login handler 自動觸發 reconcile（T0.2 斷線 reconcile 免費保存）。

- [ ] **Step 1: 寫失敗測試**（附掛 `tests/test_agent_runner.py`）

```python
async def test_run_forever_reconnects_after_transport_error(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    tr.fail_connects = 1                          # 第一次 connect 失敗
    r = _runner(tr, child, buf)
    task = asyncio.create_task(r.run_forever())
    await _until(lambda: tr.connects >= 2 and any(m["type"] == "login" for m in tr.sent))
    r.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_child_frozen_triggers_respawn_and_relogin(tmp_path):
    tr, child, buf = _FakeTransport(), _FakeChild(), DurableBuffer(tmp_path / "o.db")
    child.ping_ok = False                         # 第一個 session 內就判凍結
    r = _runner(tr, child, buf)
    task = asyncio.create_task(r.run_forever())
    await _until(lambda: child.starts >= 2)       # respawn 過
    child.ping_ok = True
    await _until(lambda: len([m for m in tr.sent if m["type"] == "login"]) >= 2)
    r.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
```

- [ ] **Step 2: 跑測試確認失敗** — 新測試 FAIL（run_forever NotImplementedError / 無 watchdog）
- [ ] **Step 3: 實作** — 依行為契約補 `_child_watchdog` 與 `run_forever`。
- [ ] **Step 4: 跑測試確認通過** — PASS；全套件綠。
- [ ] **Step 5: Commit**

```bash
git add src/quanquant/agent/runner.py tests/test_agent_runner.py
git commit -m "feat(agent): 重連 backoff + 子程序凍結偵測/respawn/重登入（#203 防線）（Inc0 Task 13）"
```

---

### Task 14: CLI 進入點 + 打包（⚠️ 動 pyproject 前先取得使用者核可）

**Files:**
- Create: `src/quanquant/agent/main.py`
- Modify: `pyproject.toml`
- Test: `tests/test_agent_cli.py`

**Interfaces:**
- Consumes: Task 10/12/13
- Produces: console script `quanquant-agent`；pyproject 依賴加一行 `"websockets>=12",`（**新依賴，執行本 Task 前必須先問使用者**；uvicorn[standard] 已間接帶入，此為顯式化）。

```python
# main.py
"""quanquant-agent：本機 broker agent CLI（Increment 0，僅 simtrade）。

憑證 session-only：getpass/env 讀入記憶體，不落地、不進 argv、不進 log。
"""
import argparse
import asyncio
import getpass
import os


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="quanquant-agent",
                                description="QuanQuant 本機 broker agent（simtrade）")
    p.add_argument("--server", default=os.environ.get(
        "QQ_AGENT_SERVER", "ws://127.0.0.1:8000/ws/agent"))
    p.add_argument("--mode", choices=["sim"], default="sim")   # Inc0 硬 guard：real 不存在
    p.add_argument("--symbol", default="TXF")
    p.add_argument("--buffer", default=os.environ.get(
        "QQ_AGENT_BUFFER", os.path.expanduser("~/.quanquant-agent/outbox.db")))
    return p


def main() -> None:
    args = build_parser().parse_args()
    token = os.environ.get("QQ_AGENT_TOKEN") or getpass.getpass("Agent token: ")
    api_key = os.environ.get("QQ_AGENT_API_KEY") or getpass.getpass("Shioaji API Key: ")
    secret_key = os.environ.get("QQ_AGENT_SECRET_KEY") or getpass.getpass("Shioaji Secret Key: ")

    from quanquant.agent.buffer import DurableBuffer
    from quanquant.agent.runner import AgentRunner, ChildHandle
    from quanquant.agent.ws_client import WebsocketsTransport

    runner = AgentRunner(
        transport=WebsocketsTransport(args.server, token=token),
        buffer=DurableBuffer(args.buffer),
        child=ChildHandle(credentials={"api_key": api_key, "secret_key": secret_key},
                          symbol=args.symbol, mode=args.mode, buffer_path=args.buffer),
        mode=args.mode,
    )
    print(f"agent 啟動（simtrade）→ {args.server}；Ctrl-C 結束（憑證僅存記憶體）")
    try:
        asyncio.run(runner.run_forever())
    except KeyboardInterrupt:
        print("agent 結束")
```

pyproject 兩處修改（逐字）：dependencies 清單加 `"websockets>=12",`；`[project.scripts]` 加 `quanquant-agent = "quanquant.agent.main:main"`。

- [ ] **Step 0: 確認使用者已核可新依賴 `websockets`**（未核可就停在這裡回報）
- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_agent_cli.py
import pytest
from quanquant.agent.main import build_parser, main


def test_mode_real_rejected_by_argparse():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--mode", "real"])


def test_defaults():
    args = build_parser().parse_args([])
    assert args.mode == "sim" and args.server.endswith("/ws/agent")


def test_main_reads_credentials_from_env_not_argv(monkeypatch, tmp_path):
    monkeypatch.setenv("QQ_AGENT_TOKEN", "tok")
    monkeypatch.setenv("QQ_AGENT_API_KEY", "AK")
    monkeypatch.setenv("QQ_AGENT_SECRET_KEY", "SK")
    monkeypatch.setenv("QQ_AGENT_BUFFER", str(tmp_path / "o.db"))
    captured = {}

    class _FakeRunner:
        def __init__(self, **kw):
            captured.update(kw)
        async def run_forever(self):
            return None

    monkeypatch.setattr("quanquant.agent.runner.AgentRunner", _FakeRunner)
    monkeypatch.setattr("sys.argv", ["quanquant-agent"])
    main()
    assert captured["mode"] == "sim"
    # 憑證只進 ChildHandle 記憶體，不在 argv
    assert captured["child"]._credentials["api_key"] == "AK"
```

（`_credentials` 屬性名以 Task 12 實作為準；若為私有命名不同，改斷言建構參數側錄。getpass 路徑不測互動、只測 env 短路。）

- [ ] **Step 2: 跑測試確認失敗** — FAIL（ModuleNotFoundError）
- [ ] **Step 3: 實作** — main.py 如上；pyproject 兩行；`uv sync --extra dev`。
- [ ] **Step 4: 跑測試確認通過** — PASS；全套件綠；`uv run quanquant-agent --help` 正常輸出。
- [ ] **Step 5: Commit**

```bash
git add src/quanquant/agent/main.py pyproject.toml uv.lock tests/test_agent_cli.py
git commit -m "feat(agent): quanquant-agent CLI——getpass 憑證 session-only、sim-only guard（Inc0 Task 14）"
```

---

### Task 15: 全線整合測試（真 socket、fake native）

**Files:**
- Create: `tests/test_agent_integration.py`
- Test: 本檔即測試

**Interfaces:**
- Consumes: Task 8 wiring 元件、Task 12/13 runner、Task 11 `fake_native_factory`、conftest 的 `engine`/`user` fixtures、`auth/tokens.py` 的 `SESSION_COOKIE`/`sign_session`

骨幹自動化證明：uvicorn 起在隨機 port（thread）→ 真 `WebsocketsTransport` 接上 `/ws/agent` → `POST /orders`（簽章 cookie）→ 下行 place → fake native 回報 → 上行 report → RawInbox → RawInboxWorker → Order=filled + UI 局部 + kill switch 擋單。

- [ ] **Step 1: 寫測試（本 task 無「先紅後綠」——它驗的是已完成的整條鏈；紅=發現 bug 即修）**

```python
# tests/test_agent_integration.py
"""Increment 0 骨幹自動化證明（真 WS socket；SDK 以 FakeNativeClient 替身）。"""
import asyncio
import threading
from contextlib import asynccontextmanager
from decimal import Decimal

import httpx
import pytest
import uvicorn
from sqlmodel import Session, select

from quanquant.agent.buffer import DurableBuffer
from quanquant.agent.runner import AgentRunner
from quanquant.agent.testing import fake_native_factory
from quanquant.agent.ws_client import WebsocketsTransport
from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.broker.agent_channel import AgentChannel, AgentNativeGateway
from quanquant.broker.inbox_worker import RawInboxWorker
from quanquant.broker.order_events import OrderEventHub
from quanquant.broker.risk import RiskGuard
from quanquant.broker.session_state import OrderSessionState
from quanquant.broker.shioaji_adapter import ShioajiAdapter
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.config import get_settings
from quanquant.db.models import BrokerPosition, Order
from quanquant.web.app import create_app
from quanquant.web.deps import get_session


class _ThreadChild:
    """ChildHandle 同介面、但用執行緒跑 child_main（重用 Task 11 測試手法）。"""
    def __init__(self, buffer_path):
        import multiprocessing as mp
        from quanquant.agent.native_runner import child_main
        self._parent, child_conn = mp.Pipe()
        self._lock = threading.Lock()
        self.ops = []
        self._thread = threading.Thread(
            target=child_main, args=(child_conn,),
            kwargs=dict(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                        mode="sim", buffer_path=buffer_path,
                        native_factory=fake_native_factory),
            daemon=True)
        self._thread.start()
        self.alive = True
    def start(self):
        reply = self.request({"op": "connect"}, timeout=10)
        assert reply["ok"], reply
        return reply["account"]
    def request(self, op, *, timeout):
        with self._lock:
            self.ops.append(op)
            self._parent.send(op)
            if not self._parent.poll(timeout):
                raise TimeoutError
            return self._parent.recv()
    def ping(self, *, timeout):
        try:
            return self.request({"op": "ping"}, timeout=timeout).get("ok", False)
        except TimeoutError:
            return False
    def terminate(self):
        self.alive = False


@pytest.fixture
def live_server(engine, user, monkeypatch):
    monkeypatch.setenv("AGENT_WS_TOKEN", "tok")
    get_settings.cache_clear()
    app = create_app()

    def _session_override():
        with Session(engine) as s:
            yield s

    app.dependency_overrides[get_session] = _session_override
    session_factory = lambda: Session(engine)
    supervisor = BrokerSupervisor()
    guard = RiskGuard(session_factory=session_factory, secret="s",
                      owner_user_ids=frozenset({user.id}),
                      symbol_whitelist=frozenset({"TXF"}), max_qty_per_order=5,
                      max_qty_per_day=20, max_orders_per_day=20)
    channel = AgentChannel()
    gateway = AgentNativeGateway(channel, timeout_seconds=5)
    adapter = ShioajiAdapter(api_key="", secret_key="", ca_path=None, ca_passwd=None,
                             person_id=None, symbol="TXF", mode="sim",
                             session_factory=session_factory, supervisor=supervisor,
                             risk_guard=guard, sim_fee_per_lot=Decimal("20"),
                             remote_gateway=gateway)
    hub = OrderEventHub()
    worker = RawInboxWorker(session_factory=session_factory, supervisor=supervisor,
                            deal_mapper=adapter._map_deal_report,
                            order_report_mapper=adapter._map_order_report,
                            order_events=hub, idle_interval=0.05)
    state = OrderSessionState()
    state.mark_disabled("agent 未連線")
    app.state.agent_channel = channel
    app.state.order_service = adapter
    app.state.order_risk_guard = guard
    app.state.order_inbox_worker = worker
    app.state.order_session_state = state
    app.state.order_session_factory = session_factory
    app.state.order_events = hub

    @asynccontextmanager
    async def _lifespan(app):
        t = asyncio.create_task(worker.run())
        yield
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)

    app.router.lifespan_context = _lifespan
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0,
                                           log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    import time
    for _ in range(200):
        if server.started:
            break
        time.sleep(0.05)
    assert server.started, "uvicorn 未啟動"
    port = server.servers[0].sockets[0].getsockname()[1]
    yield f"127.0.0.1:{port}", app
    server.should_exit = True
    thread.join(timeout=10)
    get_settings.cache_clear()


async def _until(cond, timeout=10.0):
    async def _poll():
        while not cond():
            await asyncio.sleep(0.05)
    await asyncio.wait_for(_poll(), timeout)


async def test_skeleton_roundtrip_and_kill_switch(live_server, engine, user, tmp_path):
    host, app = live_server
    buf = DurableBuffer(tmp_path / "o.db")
    child = _ThreadChild(str(tmp_path / "o.db"))
    runner = AgentRunner(transport=WebsocketsTransport(f"ws://{host}/ws/agent", token="tok"),
                         buffer=buf, child=child, pump_interval=0.05, resend_after=1.0,
                         child_command_timeout=5, child_ping_interval=30,
                         child_ping_timeout=5, heartbeat_interval=30)
    runner.ensure_child()
    run_task = asyncio.create_task(runner.run_once())
    try:
        await _until(lambda: app.state.agent_channel.ready)
        assert app.state.order_session_state.ready          # login → mark_ready

        async with httpx.AsyncClient(base_url=f"http://{host}") as client:
            client.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
            r = await client.post("/orders", data={
                "client_order_id": "e2e-1", "symbol": "TXF", "action": "Buy",
                "qty": "1", "price": "", "price_type": "MKT", "order_type": "IOC",
                "octype": "Auto"})
            assert r.status_code == 200

            def _filled():
                with Session(engine) as s:
                    o = s.exec(select(Order).where(
                        Order.client_order_id == "e2e-1")).first()
                    return o is not None and o.status == "filled"
            await _until(_filled)                            # 全鏈：place→report→worker

            with Session(engine) as s:
                assert s.exec(select(BrokerPosition)).first() is not None

            page = await client.get("/orders/list?mode=sim")
            assert "1/1" in page.text                        # UI 成交欄

            assert "agent 已連線" in (await client.get("/orders/agent-status")).text

            # kill switch：ON 後新單被擋、child 未收到新 place
            await client.post("/orders/kill-switch", data={"enabled": "true"})
            n_ops = len(child.ops)
            r = await client.post("/orders", data={
                "client_order_id": "e2e-2", "symbol": "TXF", "action": "Buy",
                "qty": "1", "price": "", "price_type": "MKT", "order_type": "IOC",
                "octype": "Auto"})
            assert "kill switch" in r.text
            assert len([op for op in child.ops[n_ops:] if op["op"] == "place"]) == 0
    finally:
        run_task.cancel()
        await asyncio.gather(run_task, return_exceptions=True)
        child.request({"op": "shutdown"}, timeout=5)
```

- [ ] **Step 2: 跑測試** — `uv run pytest tests/test_agent_integration.py -v`
Expected: PASS（若在此環境 uvicorn-in-thread 不穩：備援＝改用 `TestClient.websocket_connect` 由測試手動扮演 agent 側收發協定訊框，涵蓋同一條鏈；先試真 socket 版）。
- [ ] **Step 3: 全套件** — `uv run pytest` 全綠。
- [ ] **Step 4: Commit**

```bash
git add tests/test_agent_integration.py
git commit -m "test(agent): Inc0 骨幹整合證明——真 WS、place→fill→UI 全鏈 + kill switch（Inc0 Task 15）"
```

---

### Task 16: 人工 sim 實測（人類驗收，不可由 subagent 自動化）

**前置**：使用者本人的 Shioaji sim key（F002 已簽）、交易時段內（日盤 08:45–13:45 / 夜盤 15:00–次日 05:00；sim 掛單不撮合→驗成交用 MKT/IOC）。

- [ ] **Step 1: 啟動**

```bash
# 終端 1（server，本機）
AGENT_WS_TOKEN=<隨機長字串> ORDER_CHANNEL=agent ORDER_MODE=sim \
ORDER_OWNER_USER_IDS=<你的 user id> uv run quanquant-web
# 終端 2（agent）
QQ_AGENT_SERVER=ws://127.0.0.1:8000/ws/agent QQ_AGENT_TOKEN=<同 token> uv run quanquant-agent
# → 依提示輸入 sim API Key/Secret（不需 CA）
```

- [ ] **Step 2: 逐項驗收（每項記錄實際輸出）**

1. agent 登入後 orders 頁 badge 🟢「agent 已連線」；`curl -s localhost:8000/healthz` → 200。
2. 盤中下 1 口 Buy MKT/IOC：委託表 filled、成交 1/1、部位表出現；server DB `sqlite3 quanquant.db "SELECT kind,processed,quarantine FROM raw_inbox ORDER BY id DESC LIMIT 5;"` → 全 processed=1、quarantine=0。
3. kill switch ON → 再下單顯示錯誤、agent 終端無新 place 指令；OFF → 恢復可下。
4. Ctrl-C 關 agent → badge 🔴「agent 未連線」、下單被擋（表單錯誤）；重啟 agent（重新輸入憑證＝session-only 證明）→ badge 恢復、server log 出現 reconcile。
5. `sqlite3 ~/.quanquant-agent/outbox.db "SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL;"` → 0（回報全數送達）。

- [ ] **Step 3: 驗收記錄** — 寫 `docs/superpowers/reviews/<日期>-local-broker-agent-inc0-sim-verification.md`（逐項結果＋異常），commit（`docs:` 前綴）。

---

## 完成定義（Increment 0）

1. Task 1-15 全部 commit、`uv run pytest` 全綠（649 基線 + 新增全部）。
2. Task 15 整合測試證明骨幹鏈路（自動化）。
3. Task 16 人工 sim 實測全項通過（人類證據）。
4. 未 push、未 deploy、未動 real 模式、server 零新表。

