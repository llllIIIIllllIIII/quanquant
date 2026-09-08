# Agent 設定精靈（GUI Onboarding）實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把「登入網站複製 token→終端機 export→命令列啟動」三段斷裂流程，改成使用者點桌面捷徑→本機瀏覽器精靈（device-code 授權＋永豐憑證輸入）→連線成功自動轉狀態儀表板，全程零終端機輸入；既有技術路徑（env vars + headless）行為逐位不變。

**Architecture:** agent 本機新增一個單一 async 協調器：先預綁 `127.0.0.1:0` 拿到實際 port，啟動一個極簡 FastAPI+HTMX 本機 server（`/setup` 精靈、`/status` 儀表板），精靈完成後才建構既有 `ChildHandle`/`WebsocketsTransport`/`AgentRunner` 並跑 `run_forever`。token 取得走 server 端新增的 device-code 授權流程（RFC 8628 精神簡化版）：agent 發起→使用者在正式站核准頁手動輸碼核准→agent 輪詢兌換 token；憑證（token、永豐 API_KEY/SECRET）opt-in 存 OS keychain（`keyring` 套件），本機 `profiles.json` 只存非秘密 metadata 解「keyring 定位」的雞生蛋問題。server 端只新增一張表＋兩個公開 endpoint＋一個核准頁，`agent_tokens.py`／WS `/ws/agent` v1 協定完全不動。

**Tech Stack:** Python 3.11+、FastAPI + uvicorn（agent 本機 GUI，與 server 同套件版本，零新增）、`keyring`（新依賴，D3，僅供 keychain 存取）、SQLModel（新表 `agent_device_codes`）、pytest + pytest-asyncio（asyncio_mode=auto）、httpx（agent 端 device-flow client；`ASGITransport` 供偽造來源 IP 測試）。

## Global Constraints（每個 Task 隱含適用）

- **測試底線**：起點以 `uv run pytest` 目前全綠為準；每個 Task 結束時必須 0 failed，既有測試不得弱化/刪除/跳過。
- **憑證永不落地明文檔案**：token／永豐 API_KEY／API_SECRET 只能在程序記憶體或 OS keychain（經 `keyring`）；不可寫入任何明文檔案、不進 argv、不進 log、不進 server DB（DB 只存 hash）。GUI 422/error body 絕不回帶輸入值；所有 setup/status 回應 `Cache-Control: no-store`。
- **WS `/ws/agent` v1 wire frame 完全不動**：`agent_protocol.py` 的訊息模型、握手語意、token revoke 不中斷既有連線的不變量，本計畫不觸碰。
- **`agent_tokens.py` 公開介面與語意不變**：`issue_token`/`validate_token`/`get_active_token` 簽名、行為、docstring 描述的不變量都不變；既有 `tests/test_agent_tokens.py` 全部測試不改一行、全綠。允許新增函式（`stage_rotation`）。
- **headless 路徑行為逐位一致（G5）**：`--no-gui` 與現行 env/getpass 行為完全相同；`--server`/`QQ_AGENT_SERVER`/預設值 `ws://127.0.0.1:8000/ws/agent` 優先序不變；GUI 模式的新增邏輯不得改變這條路徑任何一個判斷分支。
- **keyring 無安全 backend 即 fail closed**：偵測到 fail/plaintext/未鎖定/檔案型 backend 時完全不寫入，UI 明示「無法記住」，絕不退化成明文檔案。
- **DB 雙方言可攜**：新 raw SQL／conditional UPDATE 一律用 SQLAlchemy Core（`sqlalchemy.update(table).where(...).values(...)`，比照 `broker/agent_commands.py::mark_timeout_observed` 既有範式）或具名參數，不寫死任一方言語法。
- **新表走 SQLModel `create_all`**：`AgentDeviceCode` 比照 `AgentToken`/`AgentCommand` 既有寫法，不進 `db/migrate.py` 的 `_MIGRATIONS`（那只用於既有表補欄位）。
- **mode 四層鎖 sim 不動**：本計畫完全不碰 `order_mode`/`--mode` 相關 guard；device flow 核准的是「連線 agent（sim 模式）」，不涉及交易模式切換。
- **新依賴僅 `keyring`**：不可為 Windows `.lnk` 產生器等其他子任務引入 `pywin32` 或任何其他套件——Windows 捷徑改用 `subprocess` 呼叫系統內建 PowerShell（`New-Object -ComObject WScript.Shell`）產生，零新增 Python 依賴。
- **pyproject 的 hatch wheel 設定不可加 `force-include`**（現況 `packages = ["src/quanquant"]`，本計畫只加 `keyring` 一行依賴）。
- **部署測試指令**：`uv run pytest`（全綠才可部署）；個別 task 測試一律 `uv run pytest tests/<路徑> -v`。
- **UI/訊息一律繁體台灣中文**；commit message 格式 `<type>: <描述>`（feat/fix/refactor/test/chore），不加 attribution。
- **GateGuard hook**：第一次 Bash、建新檔會被擋下要求陳述事實——照錯誤訊息列點補上（匯入者/受影響 API/schema/使用者指示）後重試同一操作，非故障；所有寫入內容禁用簡體字。
- **禁 push / 禁 deploy**：只做本機 commit（沿用現有分支或使用者指定分支）。push/PR/deploy 需使用者明確要求。

---

## File Structure

**新增（server 端）**
- `src/quanquant/auth/device_flow.py` — device flow 服務層：發起／輪詢五步入口／claim 交易／approve/deny（Task 3/4/6）
- `src/quanquant/web/routers/agent_device.py` — `POST /api/agent/device-code`、`POST /api/agent/device-token`（Task 5）
- `src/quanquant/web/routers/agent_authorize.py` — `/agent/authorize` 核准頁（Task 6）
- `src/quanquant/web/csrf.py` — 一次性 synchronizer CSRF token（double-submit cookie，Task 6）
- `src/quanquant/web/rate_limit.py` — 每 IP token bucket（Task 5）
- `src/quanquant/web/templates/agent_authorize.html`、`src/quanquant/web/templates/partials/agent_authorize_confirm.html` — 核准頁（Task 6）

**新增（agent 端，同 repo 同 package）**
- `src/quanquant/agent/gui/__init__.py`
- `src/quanquant/agent/gui/security.py` — bootstrap exchange＋本機安全邊界（Task 8）
- `src/quanquant/agent/gui/coordinator.py` — 單一 async 協調器（Task 8）
- `src/quanquant/agent/gui/setup_routes.py` — `/setup` 三步精靈（Task 9）
- `src/quanquant/agent/gui/status_routes.py` — `/status` 儀表板（Task 10）
- `src/quanquant/agent/gui/templates/*.html` — 精靈／狀態頁模板（Task 9/10）
- `src/quanquant/agent/device_flow_client.py` — agent 端 device flow client（Task 9）
- `src/quanquant/agent/keyring_store.py` — keychain 存取層（Task 11）
- `src/quanquant/agent/profile_registry.py` — profile registry＋per-profile buffer 路徑＋單實例鎖（Task 12）
- `src/quanquant/agent/gui/startup_flow.py` — GUI 啟動決策樹＋profile 選擇頁＋fallback 規則（Task 13）
- `src/quanquant/agent/gui/templates/profile_select.html` — profile 選擇頁模板（Task 13）
- `src/quanquant/agent/startup.py` — CLI 優先序決策樹＋`--site` canonical 解析（Task 14）
- `src/quanquant/agent/shortcut_gen.py` — 桌面捷徑產生器（Task 15）

**修改**
- `src/quanquant/auth/agent_tokens.py` — 抽出 `stage_rotation`（Task 1）
- `src/quanquant/db/models.py` — 新增 `AgentDeviceCode`（Task 2）
- `src/quanquant/config.py` — 新增 device-code TTL／限流／`forwarded_allow_ips` 等設定（Task 3/5/16）
- `src/quanquant/web/app.py` — 掛新 router、`run()` 帶 `forwarded_allow_ips`（Task 5/6/16）
- `src/quanquant/agent/runner.py` — `AgentSnapshot`＋`AgentRunner.snapshot()`（Task 7）＋新增 `"rejected"` 連線態（Task 13）
- `src/quanquant/agent/ws_client.py` — 新增 `TokenRejectedError`（WS 1008 close code 分類，Task 13）
- `src/quanquant/agent/gui/coordinator.py` — `run_gui()` 串入決策樹（Task 13）
- `src/quanquant/agent/main.py` — CLI 全面改寫（Task 14）
- `docker-compose.yml` — 固定 IP 網路＋`FORWARDED_ALLOW_IPS`（Task 16）
- `docs/deployment.md` — 信任鏈設定說明（Task 16）
- `pyproject.toml` — `keyring` 依賴（Task 11 執行前）

**新增（測試）**
- `tests/test_agent_tokens.py`（既有檔追加，含 Task 4 補的 WS revoke 不中斷連線測試）、`tests/test_agent_ws.py`（既有檔追加，Task 4）、`tests/test_agent_device_code_model.py`、`tests/test_device_flow_initiate.py`、`tests/test_device_flow_poll.py`、`tests/test_agent_device_api.py`、`tests/test_agent_authorize_page.py`、`tests/test_agent_runner_snapshot.py`、`tests/test_agent_gui_security.py`、`tests/test_device_flow_client.py`、`tests/test_agent_setup_wizard.py`（含 Task 9 補的秘密洩漏 log 掃描測試）、`tests/test_agent_status_page.py`、`tests/test_agent_keyring_store.py`、`tests/test_agent_profile_registry.py`、`tests/test_agent_gui_startup_flow.py`、`tests/test_agent_startup_resolution.py`、`tests/test_agent_shortcut_gen.py`、`tests/test_deployment_trust_chain.py`

**新增（文件）**
- `docs/superpowers/reviews/2026-08-12-agent-setup-gui-manual-test-plan.md`（Task 17）

**任務相依**（編號為 skeleton 順序，此為真實執行順序——subagent-driven-development 依此圖排程，非純數字序）：
1→2→3→4→(5、6 皆需 4)；7 獨立；8 需 7；11、12 獨立（可與 7/8 並行）；9 需 5+8+11+12+14；13（GUI 決策樹＋profile 選擇頁＋fallback）需 7+8+9+11+12；10 需 7+8+9+11+12；14（CLI）獨立（可提早做，9/13 需要它）；15 需 12+14；16 需 5（真實 client IP 才有意義，但可獨立先寫）；17 需 1-16 全部。

---

### Task 1：`agent_tokens.py` 抽出不自行 commit 的 rotation primitive

**Files:**
- Modify: `src/quanquant/auth/agent_tokens.py`
- Test: `tests/test_agent_tokens.py`（既有檔追加，既有測試一行不改）

**Interfaces:**
- Consumes：`db/models.py` 既有 `AgentToken`/`_utcnow`；`sqlalchemy.exc.IntegrityError`
- Produces（Task 4 依賴）:

```python
def stage_rotation(session: Session, *, user_id: int, ttl_days: int) -> tuple[str, AgentToken]:
    """rotation 的『不自行 commit』半段：把該 user 目前有效的舊列標 revoked_at=now，
    建一筆新 AgentToken（session.add，不 commit）。呼叫端負責決定何時 commit／要不要跟
    別的寫入包在同一交易（Task 4 的 claim_and_issue_device_token 用這點把『搶占 device
    code』與『簽發 token』綁進單一交易）。撞 uq_agent_tokens_active_per_user 的重試/
    commit 邏輯不在這裡——那是呼叫端（issue_token 或 claim_and_issue_device_token）的
    責任，因為重試需要知道『要不要連同呼叫端自己的其他寫入一起 rollback』。"""
```

`issue_token()` 改為呼叫 `stage_rotation()` 再自行 commit/重試，**回傳值、docstring 描述的不變量、撞鍵重試語意完全不變**（既有測試作為安全網逐字驗證）。

- [ ] **Step 1：寫失敗測試**

追加到 `tests/test_agent_tokens.py`：

```python
def test_stage_rotation_does_not_commit(session, user):
    from quanquant.auth.agent_tokens import get_active_token, stage_rotation

    raw, token = stage_rotation(session, user_id=user.id, ttl_days=30)
    assert raw and token.token_hash
    session.rollback()
    # rollback 撤銷了尚未 commit 的 insert——沒有任何 active token 留下
    assert get_active_token(session, user_id=user.id) is None


def test_issue_token_still_rotates_via_stage_rotation(session, user):
    from quanquant.auth.agent_tokens import issue_token, validate_token

    first = issue_token(session, user_id=user.id, ttl_days=30)
    second = issue_token(session, user_id=user.id, ttl_days=30)
    assert first != second
    assert validate_token(session, raw=first) is None       # 舊枚已撤銷
    assert validate_token(session, raw=second) is not None  # 新枚有效
```

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_agent_tokens.py -v`
Expected: FAIL（`ImportError: cannot import name 'stage_rotation'`）

- [ ] **Step 3：實作**

在 `agent_tokens.py` 新增 `stage_rotation()`（上方簽名，內容=現行 `issue_token()` 迴圈內「revoke 舊列＋建新列＋session.add」那段，不含 `session.commit()`/`session.refresh()`/重試迴圈）；`issue_token()` 改寫為：

```python
def issue_token(session: Session, *, user_id: int, ttl_days: int) -> str:
    for attempt in range(2):
        raw, token = stage_rotation(session, user_id=user_id, ttl_days=ttl_days)
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            if attempt == 1:
                raise
            continue
        session.refresh(token)
        return raw
    raise AssertionError("unreachable")  # pragma: no cover
```

模組頂部 C10 說明段落原樣保留（不變量描述仍正確——只是實作換了個殼）。

- [ ] **Step 4：跑測試確認通過**

Run: `uv run pytest tests/test_agent_tokens.py -v` → 全部（既有 8 條＋新增 2 條）PASS；`uv run pytest` → 全綠。

- [ ] **Step 5：Commit**

```bash
git add src/quanquant/auth/agent_tokens.py tests/test_agent_tokens.py
git commit -m "refactor: agent_tokens 抽出不自行 commit 的 stage_rotation primitive"
```

---

### Task 2：`AgentDeviceCode` model＋雙方言建表測試

**Files:**
- Modify: `src/quanquant/db/models.py`
- Test: `tests/test_agent_device_code_model.py`

**Interfaces:**
- Consumes：既有 `_utcnow`、`Field`/`SQLModel`/`CheckConstraint`（已在檔頭 import）
- Produces（Task 3/4/5/6 依賴）:

```python
class AgentDeviceCode(SQLModel, table=True):
    """Device-code 授權流程狀態表（spec §4.2）。走 SQLModel.metadata.create_all 自動建
    （比照 AgentToken/AgentCommand 既有範式），不進 db/migrate.py 的 _MIGRATIONS。"""

    __tablename__ = "agent_device_codes"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending','approved','denied')",
            name="ck_agent_device_codes_status",
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    device_code_hash: str = Field(unique=True, index=True)
    code_challenge: str                                    # sha256(code_verifier) hex，PoP（§8）
    user_code: str = Field(unique=True, index=True)         # "XXXX-XXXX" 人類可讀
    user_id: int | None = Field(default=None, index=True)   # 核准時綁定
    request_ip: str = Field(index=True)                     # 限流計數用（§4.3）
    status: str = Field(default="pending", index=True)
    created_at: datetime = Field(default_factory=_utcnow)
    expires_at: datetime
    consumed_at: datetime | None = None
    last_polled_at: datetime | None = None
    current_interval: int = Field(default=5)
    consecutive_violations: int = Field(default=0)
    blocked_until: datetime | None = None
```

- [ ] **Step 1：寫失敗測試**

```python
# tests/test_agent_device_code_model.py
import pytest
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.schema import CreateTable
from sqlmodel import select

from quanquant.db.models import AgentDeviceCode


def test_agent_device_code_ddl_compiles_on_both_dialects_with_status_check():
    for dialect in (sqlite.dialect(), postgresql.dialect()):
        ddl = str(CreateTable(AgentDeviceCode.__table__).compile(dialect=dialect))
        assert "agent_device_codes" in ddl
        assert "ck_agent_device_codes_status" in ddl


def test_agent_device_code_defaults_and_roundtrip(session):
    import datetime as dt

    row = AgentDeviceCode(
        device_code_hash="h" * 64, code_challenge="c" * 64, user_code="ABCD-EFGH",
        request_ip="203.0.113.5", expires_at=dt.datetime(2099, 1, 1),
    )
    session.add(row)
    session.commit()
    got = session.exec(select(AgentDeviceCode)).one()
    assert got.status == "pending"
    assert got.current_interval == 5
    assert got.consecutive_violations == 0
    assert got.user_id is None and got.consumed_at is None and got.blocked_until is None


def test_agent_device_code_rejects_invalid_status_value(session):
    import datetime as dt
    from sqlalchemy.exc import IntegrityError

    row = AgentDeviceCode(
        device_code_hash="x" * 64, code_challenge="c" * 64, user_code="ZZZZ-9999",
        request_ip="127.0.0.1", status="bogus", expires_at=dt.datetime(2099, 1, 1),
    )
    session.add(row)
    with pytest.raises(IntegrityError):
        session.commit()
```

（`session`/`engine` fixture 沿用 `tests/conftest.py`；`SQLModel.metadata.create_all(eng)` 自動建到這張新表，不需額外 fixture 改動。）

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_agent_device_code_model.py -v`
Expected: FAIL（`ImportError: cannot import name 'AgentDeviceCode'`）

- [ ] **Step 3：實作**

在 `db/models.py` 尾端（`AgentCommand`／既有 agent 相關表之後）加入上方 `AgentDeviceCode` 定義。

- [ ] **Step 4：跑測試確認通過**

Run: `uv run pytest tests/test_agent_device_code_model.py -v` → PASS（第三條測試 IntegrityError 需注意 SQLite 預設不強制 CHECK——`tests/conftest.py` 的 `create_engine("sqlite://", ...)` 未關閉 foreign_keys/check 強制，SQLite 3.x 原生支援 CHECK constraint 強制執行，不需額外 PRAGMA；若本機 SQLite 版本過舊導致此測試意外 PASS-without-raise，之情況需回報而非靜默跳過）；`uv run pytest` → 全綠。

- [ ] **Step 5：Commit**

```bash
git add src/quanquant/db/models.py tests/test_agent_device_code_model.py
git commit -m "feat: 新增 AgentDeviceCode 表（device-code 授權流程狀態，spec 4.2）"
```

---

### Task 3：device flow 服務——發起（user_code 產生、request_ip 計數、清理）

**Files:**
- Create: `src/quanquant/auth/device_flow.py`
- Modify: `src/quanquant/config.py`
- Test: `tests/test_device_flow_initiate.py`

**Interfaces:**
- Consumes：Task 2 的 `AgentDeviceCode`；`db/models._utcnow`
- Produces（Task 4/5/6 依賴）:

```python
DEVICE_CODE_TTL_SECONDS = 600
POLL_INTERVAL_SECONDS = 5
_USER_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # 剔除 0/O/1/I（spec §4.1）


def generate_user_code() -> str:
    """8 碼 XXXX-XXXX，大寫、剔除混淆字元。secrets.choice——每碼獨立均勻取樣。"""


def count_active_pending_for_ip(session: Session, *, request_ip: str) -> int:
    """status='pending' 且 expires_at>now 的列數（§4.3 per-IP 限流用）。"""


def count_active_pending_global(session: Session) -> int:
    """同上，不篩 request_ip（全域上限用）。"""


def create_device_code(
    session: Session, *, request_ip: str, code_challenge: str,
) -> tuple[str, AgentDeviceCode]:
    """建立一筆 pending device code；回傳 (device_code 明文, row)。呼叫端（Task 5 router）
    必須先完成限流檢查（token bucket＋上面兩個計數函式）才呼叫本函式——本函式不做限流
    判斷，純粹『建立一筆』。副作用：同一交易內順手 DELETE expires_at < now-1day 的過期列
    （spec §4.2 清理策略，不加背景任務）。user_code 撞唯一鍵時重試（機率極低，32^8 空間），
    上限 3 次，仍撞則往外拋 IntegrityError（不可能發生，防禦性程式碼）。"""
```

Settings 新增（`config.py`，緊鄰既有 `agent_token_ttl_days` 一段）：

```python
agent_device_code_ttl_seconds: int = 600
agent_device_code_poll_interval_seconds: int = 5
agent_device_code_rate_per_minute: int = 10   # 每 IP token bucket 補充速率（Task 5 用）
agent_device_code_rate_burst: int = 5         # 每 IP token bucket burst 容量（Task 5 用）
agent_device_code_max_active_per_ip: int = 10
agent_device_code_max_active_global: int = 500
```

- [ ] **Step 1：寫失敗測試**

```python
# tests/test_device_flow_initiate.py
import datetime as dt

import pytest
from sqlmodel import select

from quanquant.auth.device_flow import (
    count_active_pending_for_ip, count_active_pending_global,
    create_device_code, generate_user_code,
)
from quanquant.db.models import AgentDeviceCode, _utcnow


def test_generate_user_code_format_excludes_confusing_chars():
    for _ in range(200):
        code = generate_user_code()
        assert len(code) == 9 and code[4] == "-"
        assert not (set(code) & set("0O1I"))


def test_create_device_code_persists_hash_not_plaintext(session):
    raw, row = create_device_code(session, request_ip="203.0.113.5", code_challenge="c" * 64)
    assert row.device_code_hash != raw and len(row.device_code_hash) == 64
    assert row.status == "pending" and row.request_ip == "203.0.113.5"
    assert row.code_challenge == "c" * 64


def test_create_device_code_deletes_stale_expired_rows(session):
    stale = AgentDeviceCode(
        device_code_hash="s" * 64, code_challenge="c" * 64, user_code="STAL-EONE",
        request_ip="203.0.113.5", expires_at=_utcnow() - dt.timedelta(days=2),
    )
    session.add(stale)
    session.commit()
    create_device_code(session, request_ip="203.0.113.5", code_challenge="c" * 64)
    remaining = session.exec(select(AgentDeviceCode).where(AgentDeviceCode.user_code == "STAL-EONE")).first()
    assert remaining is None


def test_count_active_pending_for_ip_only_counts_unexpired_pending(session):
    create_device_code(session, request_ip="203.0.113.5", code_challenge="c" * 64)
    expired = AgentDeviceCode(
        device_code_hash="e" * 64, code_challenge="c" * 64, user_code="EXPI-REDX",
        request_ip="203.0.113.5", expires_at=_utcnow() - dt.timedelta(seconds=1),
    )
    session.add(expired)
    session.commit()
    assert count_active_pending_for_ip(session, request_ip="203.0.113.5") == 1
    assert count_active_pending_for_ip(session, request_ip="198.51.100.1") == 0
    assert count_active_pending_global(session) == 1
```

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_device_flow_initiate.py -v`
Expected: FAIL（`ModuleNotFoundError: quanquant.auth.device_flow`）

- [ ] **Step 3：實作**

`device_flow.py` 檔頭 import `hashlib`、`secrets`、`datetime`/`timedelta`、`sqlalchemy.delete`/`func`、`sqlmodel.Session`/`select`、`quanquant.db.models.AgentDeviceCode`/`_utcnow`。`_hash_device_code(raw: str) -> str` 為模組內 `hashlib.sha256(raw.encode()).hexdigest()`（與 `agent_tokens._hash` 同邏輯、不共用，模組邊界各自獨立，比照既有風格）。`create_device_code` 內先 `session.exec(delete(AgentDeviceCode).where(AgentDeviceCode.expires_at < _utcnow() - timedelta(days=1)))`，再迴圈（最多 3 次）試插入新列，撞 `user_code` 唯一鍵（`IntegrityError`）時 `session.rollback()` 換碼重試。

- [ ] **Step 4：跑測試確認通過**

Run: `uv run pytest tests/test_device_flow_initiate.py -v` → PASS；`uv run pytest` → 全綠。

- [ ] **Step 5：Commit**

```bash
git add src/quanquant/auth/device_flow.py src/quanquant/config.py tests/test_device_flow_initiate.py
git commit -m "feat: device flow 發起——user_code 產生＋active pending 計數＋過期清理"
```

---

### Task 4：device flow 服務——輪詢五步入口（PoP／terminal／slow_down／claim）

這是本計畫最關鍵的併發正確性任務：PoP 驗證零副作用、slow_down 單一原子 UPDATE、claim 交易與 rollback 重搶占，三者都要求逐位照 spec §4.1 落地。

**Files:**
- Modify: `src/quanquant/auth/device_flow.py`
- Test: `tests/test_device_flow_poll.py`
- Test: `tests/test_agent_ws.py`（既有檔追加一支測試，補 spec §3 正式化不變量的缺口——見 Step 4b）

**Interfaces:**
- Consumes：Task 1 的 `stage_rotation`；Task 3 的常數與 `_hash_device_code`；`db/models.User`/`AgentToken`；`sqlalchemy.update`
- Produces（Task 5/9 依賴，回應 dict 的 `"state"` 鍵為以下七值之一：`invalid`/`pending`/`slow_down`/`approved`/`expired`/`denied`/`consumed`）:

```python
def claim_and_issue_device_token(
    session: Session, *, device_code_id: int, user_id: int, ttl_days: int,
) -> tuple[str, AgentToken] | None:
    """claim 交易（spec §4.1 step⑤ 核心）：①conditional UPDATE 搶占 consumed_at
    （WHERE id=:id AND user_id=:user_id AND consumed_at IS NULL AND status='approved'）
    ②呼叫 stage_rotation() ③單次 commit。任何 IntegrityError rollback 後必須從步驟①
    重新開始（搶占與簽發同生共死——絕不能出現『device code 已標 consumed 但 token 沒發
    出』的狀態）。回傳 None＝搶占失敗（rowcount=0，代表已被另一併發輪詢搶走，或狀態已
    變更）——呼叫端據此重讀最新狀態決定回應。"""


def poll_device_token(
    session: Session, *, device_code: str, code_verifier: str, ttl_days: int,
) -> dict:
    """輪詢五步入口（spec §4.1 step2 全部語意）。"""
```

- [ ] **Step 1：寫失敗測試**

```python
# tests/test_device_flow_poll.py
import datetime as dt
import hashlib

import pytest
from sqlmodel import select

from quanquant.auth import service as auth_service
from quanquant.auth.agent_tokens import validate_token
from quanquant.auth.device_flow import claim_and_issue_device_token, create_device_code, poll_device_token
from quanquant.db.models import AgentDeviceCode, _utcnow


def _approved_row(session, user, *, verifier="v" * 43, interval=5):
    challenge = hashlib.sha256(verifier.encode()).hexdigest()
    raw, row = create_device_code(session, request_ip="203.0.113.5", code_challenge=challenge)
    row.status, row.user_id, row.current_interval = "approved", user.id, interval
    session.add(row)
    session.commit()
    return raw, row, verifier


def test_poll_wrong_verifier_returns_invalid_and_zero_side_effects(session, user):
    raw, row, _ = _approved_row(session, user)
    before = row.consecutive_violations
    result = poll_device_token(session, device_code=raw, code_verifier="wrong", ttl_days=30)
    assert result == {"state": "invalid"}
    session.refresh(row)
    assert row.consecutive_violations == before and row.last_polled_at is None
    assert row.consumed_at is None and row.status == "approved"


def test_poll_unknown_device_code_returns_invalid(session):
    assert poll_device_token(session, device_code="nope", code_verifier="v", ttl_days=30) == {"state": "invalid"}


def test_poll_pending_row_returns_pending_with_interval(session, user):
    challenge = hashlib.sha256(b"v").hexdigest()
    raw, row = create_device_code(session, request_ip="203.0.113.5", code_challenge=challenge)
    result = poll_device_token(session, device_code=raw, code_verifier="v", ttl_days=30)
    assert result == {"state": "pending", "interval": 5}


def test_poll_approved_row_claims_token_and_reports_metadata(session, user):
    raw, row, verifier = _approved_row(session, user, interval=0)
    result = poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30)
    assert result["state"] == "approved"
    assert result["profile_id"] == str(user.id)
    assert result["username"] == user.username
    assert validate_token(session, raw=result["token"]) is not None


def test_poll_after_consumed_is_readonly_idempotent(session, user):
    raw, row, verifier = _approved_row(session, user, interval=0)
    first = poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30)
    assert first["state"] == "approved"
    second = poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30)
    third = poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30)
    assert second == {"state": "consumed"} == third


def test_poll_expired_terminal_state(session, user):
    raw, row, verifier = _approved_row(session, user, interval=0)
    row.expires_at = _utcnow() - dt.timedelta(seconds=1)
    session.add(row); session.commit()
    assert poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30) == {"state": "expired"}


def test_poll_denied_terminal_state(session, user):
    raw, row, verifier = _approved_row(session, user, interval=0)
    row.status = "denied"
    session.add(row); session.commit()
    assert poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30) == {"state": "denied"}


def test_slow_down_escalates_interval_and_blocks_after_five_violations(session, user):
    raw, row, verifier = _approved_row(session, user, interval=30)  # 30s 起跳，之後每次都算太快
    for i in range(5):
        result = poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30)
        assert result["state"] == "slow_down"
    session.refresh(row)
    assert row.blocked_until is not None and row.blocked_until > _utcnow()
    assert row.consecutive_violations == 0  # 達 5 次後歸零
    blocked = poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30)
    assert blocked["state"] == "slow_down" and "blocked_until" in blocked


def test_concurrent_claim_only_one_winner(session, user):
    """R3-1 精神的併發搶占測試：兩個獨立 Session 對同一列同時呼叫 claim，只有一方成功。"""
    raw, row, verifier = _approved_row(session, user, interval=0)
    from sqlmodel import Session
    with Session(session.get_bind()) as s2:
        r1 = claim_and_issue_device_token(session, device_code_id=row.id, user_id=user.id, ttl_days=30)
        r2 = claim_and_issue_device_token(s2, device_code_id=row.id, user_id=user.id, ttl_days=30)
    assert (r1 is None) != (r2 is None)  # 恰一方贏
```

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_device_flow_poll.py -v`
Expected: FAIL（`ImportError: cannot import name 'poll_device_token'`）

- [ ] **Step 3：實作**

在 `device_flow.py` 追加：

```python
import hmac
from datetime import timedelta

from sqlalchemy import update as sa_update
from sqlalchemy.exc import IntegrityError

from quanquant.auth.agent_tokens import stage_rotation
from quanquant.db.models import AgentToken, User


def claim_and_issue_device_token(
    session: Session, *, device_code_id: int, user_id: int, ttl_days: int,
) -> tuple[str, AgentToken] | None:
    t = AgentDeviceCode.__table__
    for attempt in range(2):
        now = _utcnow()
        stmt = (
            sa_update(t)
            .where(t.c.id == device_code_id, t.c.user_id == user_id,
                   t.c.consumed_at.is_(None), t.c.status == "approved")
            .values(consumed_at=now)
        )
        result = session.exec(stmt)  # type: ignore[call-overload]
        if result.rowcount == 0:
            session.rollback()
            return None
        raw, token = stage_rotation(session, user_id=user_id, ttl_days=ttl_days)
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            if attempt == 1:
                raise
            continue
        session.refresh(token)
        return raw, token
    raise AssertionError("unreachable")  # pragma: no cover


def _apply_poll_throttle(session: Session, *, row: AgentDeviceCode, now) -> dict | None:
    """單一 conditional UPDATE（spec §4.1 step④）。以樂觀鎖（WHERE last_polled_at=
    當初讀到的值）保證『讀→算新值→寫回』對同一列的併發輪詢是原子的——輸家 rowcount=0，
    重讀最新列狀態直接回應，不重算（避免雙重懲罰同一次遲到請求）。回傳 None＝不節流
    （放行到 claim 步驟）。"""
    if row.blocked_until is not None and row.blocked_until > now:
        return {"state": "slow_down", "interval": row.current_interval,
                "blocked_until": row.blocked_until.isoformat()}

    too_fast = (row.last_polled_at is not None
                and (now - row.last_polled_at).total_seconds() < row.current_interval)
    if too_fast:
        new_interval = min(row.current_interval + 5, 30)
        new_violations = row.consecutive_violations + 1
        if new_violations >= 5:
            new_blocked_until, new_violations = now + timedelta(seconds=60), 0
        else:
            new_blocked_until = row.blocked_until
    else:
        new_interval, new_violations, new_blocked_until = row.current_interval, 0, row.blocked_until

    t = AgentDeviceCode.__table__
    prev = row.last_polled_at
    where_prev = t.c.last_polled_at.is_(None) if prev is None else t.c.last_polled_at == prev
    stmt = (
        sa_update(t).where(t.c.id == row.id, where_prev)
        .values(last_polled_at=now, current_interval=new_interval,
                consecutive_violations=new_violations, blocked_until=new_blocked_until)
    )
    result = session.exec(stmt)  # type: ignore[call-overload]
    session.commit()
    if result.rowcount == 0:
        session.refresh(row)
        return {"state": "slow_down", "interval": row.current_interval} if too_fast else None
    row.last_polled_at, row.current_interval = now, new_interval
    row.consecutive_violations, row.blocked_until = new_violations, new_blocked_until
    return {"state": "slow_down", "interval": new_interval} if too_fast else None


def poll_device_token(
    session: Session, *, device_code: str, code_verifier: str, ttl_days: int,
) -> dict:
    now = _utcnow()
    row = session.exec(
        select(AgentDeviceCode).where(AgentDeviceCode.device_code_hash == _hash_device_code(device_code))
    ).first()
    # ① hash 查無
    if row is None:
        return {"state": "invalid"}
    # ② constant-time 驗 code_verifier；失敗零副作用——這裡之前絕不能有任何 session.add/commit
    challenge = hashlib.sha256(code_verifier.encode("utf-8")).hexdigest()
    if not hmac.compare_digest(challenge, row.code_challenge):
        return {"state": "invalid"}
    # ③ 唯讀 terminal 狀態，恆同冪等，不寫入
    if row.consumed_at is not None:
        return {"state": "consumed"}
    if row.status == "denied":
        return {"state": "denied"}
    if row.expires_at <= now:
        return {"state": "expired"}
    # ④ slow_down／封鎖判定與更新
    throttled = _apply_poll_throttle(session, row=row, now=now)
    if throttled is not None:
        return throttled
    # ⑤ pending／approved claim
    if row.status == "pending":
        return {"state": "pending", "interval": row.current_interval}
    claimed = claim_and_issue_device_token(
        session, device_code_id=row.id, user_id=row.user_id, ttl_days=ttl_days,
    )
    if claimed is None:
        session.refresh(row)
        if row.consumed_at is not None:
            return {"state": "consumed"}
        if row.status == "denied":
            return {"state": "denied"}
        if row.expires_at <= now:
            return {"state": "expired"}
        return {"state": "pending"}
    raw, token = claimed
    owner = session.get(User, row.user_id)
    return {
        "state": "approved", "token": raw, "profile_id": str(row.user_id),
        "username": owner.username if owner else "", "token_expires_at": token.expires_at.isoformat(),
    }
```

`profile_id` 選定為 `str(user_id)`：spec §4.1 允許「server user id 或不可變 opaque ID」，`user_id` 在帳號存續期間不變，符合定位需求（§5.3 profile registry／§5.2 keyring 用它當 key 的一部分）。

- [ ] **Step 4：跑測試確認通過**

Run: `uv run pytest tests/test_device_flow_poll.py -v` → 全部 PASS（含併發搶占測試——SQLite `StaticPool` 共用同一底層連線，兩個 Session 的交易序列化由 SQLAlchemy 層級鎖處理，仍能驗證『恰一方 rowcount=1』的邏輯正確性）；`uv run pytest` → 全綠。

- [ ] **Step 4b：補正式化不變量的整合測試（spec §3「token revoke 不中斷既有 WS 連線」，fresh read-back 覆核發現的缺口）**

既有 `tests/test_agent_ws.py::test_rotation_invalidates_old_token_new_token_still_works` 只驗證「rotation 後**新開**連線」的兩種結果（舊枚被拒、新枚成功），沒有驗證「rotation 當下**已經連上**的那條連線是否被中斷」——這正是 spec §3 這條不變量的核心主張（token 只在握手驗，不是每則訊息都驗）。`claim_and_issue_device_token`（本 task 新增）內部呼叫 `stage_rotation` 觸發的 revoke 語意與既有 `issue_token` 完全相同，因此這條不變量對 device flow 簽發的 token 同樣成立，屬於本 task 的正確性範圍，在此補上：

```python
# tests/test_agent_ws.py 追加（沿用既有 ws_env/_wait/_slot fixture，本檔頂部已 import
# issue_token/Session/TestClient/WebSocketDisconnect，不需再加 import）
def test_revoke_does_not_interrupt_existing_ws_connection(ws_env, engine):
    """spec §3 正式化不變量：『token revoke 不中斷既有 WS 連線（token 只在握手驗）』——
    既有 test_rotation_invalidates_old_token_new_token_still_works 只測了「rotation 後
    新連線」，這裡補「rotation 當下已連上的那條連線是否存活」半段。"""
    owner_id = ws_env.state.agent_test_owner_id
    old_token = ws_env.state.agent_test_token
    client = TestClient(ws_env)
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": old_token}) as ws:
        ws.send_json({"type": "login", "account": "F1", "mode": "sim", "protocol": 2, "health_epoch": 0})
        ws.send_json({"type": "health", "status": "ok", "health_epoch": 0})
        assert _wait(lambda: _slot(ws_env).session_state.ready)

        with Session(engine) as s:
            issue_token(s, user_id=owner_id, ttl_days=30)  # rotation：revoke old_token

        # 既有連線只在握手驗 token；revoke 之後仍可繼續收送，不會被 server 主動踢掉——
        # 若這裡拋 WebSocketDisconnect 就是不變量被打破。
        ws.send_json({"type": "health", "status": "ok", "health_epoch": 1})
        assert _wait(lambda: _slot(ws_env).session_state.ready)

    # 斷線後舊 token 重連必失敗（重申前提，讓本測試自成一體，不需跳去另一支確認）
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": old_token}) as ws_old:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            ws_old.receive_json()
    assert exc_info.value.code == 1008
```

Run: `uv run pytest tests/test_agent_ws.py -v` → 全部 PASS（既有全部＋新增這支）；`uv run pytest` → 全綠。

- [ ] **Step 5：Commit**

```bash
git add src/quanquant/auth/device_flow.py tests/test_device_flow_poll.py tests/test_agent_ws.py
git commit -m "feat: device flow 輪詢五步入口——PoP 零副作用/slow_down 原子節流/claim 交易"
```

---

### Task 5：兩個 API endpoint＋token bucket＋trusted IP 讀取

**Files:**
- Create: `src/quanquant/web/rate_limit.py`
- Create: `src/quanquant/web/routers/agent_device.py`
- Modify: `src/quanquant/web/app.py`（掛新 router，public——不經 `protected` 依賴）
- Test: `tests/test_agent_device_api.py`

**Interfaces:**
- Consumes：Task 3/4 的 `create_device_code`/`poll_device_token`/`count_active_pending_for_ip`/`count_active_pending_global`/`DEVICE_CODE_TTL_SECONDS`；`config.get_settings()`
- Produces（Task 9/16 依賴）:

```python
# src/quanquant/web/rate_limit.py
class TokenBucket:
    """每 key（IP）一個 bucket；純記憶體，重啟歸零可接受（頻率限制而非持久計數，持久
    上限由 DB active pending 計數負責，見 device_flow.py）。"""
    def __init__(self, *, rate_per_minute: int, burst: int) -> None: ...
    def allow(self, key: str) -> bool:
        """True＝放行並消耗一個 token；False＝超限。內部以 (tokens, last_refill_ts) dict
        存每個 key，取用前先按經過時間補充 tokens（封頂 burst）。"""


# src/quanquant/web/routers/agent_device.py
router = APIRouter()  # 掛在 app.py 的 public 區（比照 /ws/agent，不經 protected）

def client_ip(request: Request) -> str:
    """trusted IP 讀取：只用 request.client.host——uvicorn 的 ProxyHeadersMiddleware
    （forwarded_allow_ips，見 Task 16）已經把可信來源的 X-Forwarded-For 換算好，這裡
    絕不自行解析任何 X-Forwarded-* 標頭（spec §4.3 鐵律）。"""

@router.post("/api/agent/device-code")
async def initiate_device_code(request: Request, body: DeviceCodeInitiateRequest) -> ...: ...

@router.post("/api/agent/device-token")
async def poll_device_code(request: Request, body: DeviceTokenPollRequest) -> ...: ...
```

pydantic body/response 模型（同檔案）：`DeviceCodeInitiateRequest{code_challenge: str}`、
`DeviceCodeInitiateResponse{device_code: str, user_code: str, verification_path: str,
interval: int, expires_in: int}`、`DeviceTokenPollRequest{device_code: str, code_verifier: str}`。
輪詢回應直接把 `poll_device_token()` 的 dict 序列化回去（FastAPI 對 `dict` 回傳自動轉
JSON，`state` 為 `"invalid"` 時回 404（不透露細節）、`"expired"/"denied"/"consumed"` 回
200（唯讀終態，body 帶 state）、`"pending"/"slow_down"` 回 200、`"approved"` 回 200。

- [ ] **Step 1：寫失敗測試**

```python
# tests/test_agent_device_api.py
import hashlib

from quanquant.auth import service as auth_service


def _initiate(client, verifier="v" * 43):
    challenge = hashlib.sha256(verifier.encode()).hexdigest()
    resp = client.post("/api/agent/device-code", json={"code_challenge": challenge})
    return resp, verifier


def test_initiate_returns_device_code_and_fixed_verification_path(anon_client):
    resp, _ = _initiate(anon_client)
    assert resp.status_code == 200
    body = resp.json()
    assert body["verification_path"] == "/agent/authorize"
    assert body["interval"] == 5 and body["expires_in"] == 600
    assert len(body["device_code"]) > 20 and len(body["user_code"]) == 9


def test_initiate_does_not_require_login(anon_client):
    resp, _ = _initiate(anon_client)
    assert resp.status_code == 200


def test_initiate_rate_limited_after_burst(anon_client, monkeypatch):
    from quanquant.config import get_settings
    get_settings.cache_clear()
    monkeypatch.setenv("AGENT_DEVICE_CODE_RATE_BURST", "2")
    monkeypatch.setenv("AGENT_DEVICE_CODE_RATE_PER_MINUTE", "2")
    get_settings.cache_clear()
    for _ in range(2):
        assert _initiate(anon_client)[0].status_code == 200
    assert _initiate(anon_client)[0].status_code == 429
    get_settings.cache_clear()


def test_poll_unknown_device_code_returns_404(anon_client):
    resp = anon_client.post("/api/agent/device-token", json={"device_code": "nope", "code_verifier": "v"})
    assert resp.status_code == 404


def test_poll_pending_then_approved_flow(anon_client, session, user):
    resp, verifier = _initiate(anon_client)
    body = resp.json()
    poll = anon_client.post("/api/agent/device-token",
                             json={"device_code": body["device_code"], "code_verifier": verifier})
    assert poll.json()["state"] == "pending"

    from quanquant.db.models import AgentDeviceCode
    from sqlmodel import select
    row = session.exec(select(AgentDeviceCode).where(AgentDeviceCode.user_code == body["user_code"])).first()
    row.status, row.user_id, row.current_interval = "approved", user.id, 0
    session.add(row); session.commit()

    poll2 = anon_client.post("/api/agent/device-token",
                              json={"device_code": body["device_code"], "code_verifier": verifier})
    assert poll2.json()["state"] == "approved" and poll2.json()["username"] == user.username
```

（`anon_client` 沿用 `tests/conftest.py` 既有 fixture——未登入 client，證明這兩支 endpoint 不需認證。）

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_agent_device_api.py -v`
Expected: FAIL（404 not found，router 未掛載）

- [ ] **Step 3：實作**

1. `rate_limit.py`：`TokenBucket.__init__` 存 `_rate`/`_burst`/`_state: dict[str, tuple[float, float]]`（tokens, last_ts）；`allow(key)` 用 `time.monotonic()` 算經過秒數補充 `tokens = min(burst, tokens + elapsed * rate/60)`，`tokens>=1` 則扣 1 回 True，否則 False。無鎖——FastAPI async 路由於同一 event loop 循序執行到 `await` 點前不會交錯，`allow()` 內無 `await`，天然原子。
2. `agent_device.py`：`initiate_device_code` 流程：`ip = client_ip(request)` → `bucket.allow(ip)` 否則 429 → `async with app.state.device_code_lock:`（`asyncio.Lock`，序列化「計數→insert」）→ 查 `count_active_pending_for_ip`/`count_active_pending_global` 超過設定值否則 429 → 呼叫 `create_device_code` → 組回應。`app.state.device_code_lock`/`app.state.device_code_bucket` 在 `create_app()` 的 lifespan 或直接建構時初始化（比照既有 `app.state.agent_registry` 掛法）。
3. `poll_device_code`：直接呼叫 `poll_device_token(...)`，依 `state` 映射 HTTP status（`invalid`→404、其餘→200）。
4. `app.py`：`app.include_router(agent_device_routes.router)`（public 區，緊鄰 `/ws/agent` 那行）；建構 `app.state.device_code_bucket = TokenBucket(rate_per_minute=settings.agent_device_code_rate_per_minute, burst=settings.agent_device_code_rate_burst)`、`app.state.device_code_lock = asyncio.Lock()`。

- [ ] **Step 4：跑測試確認通過**

Run: `uv run pytest tests/test_agent_device_api.py -v` → PASS；`uv run pytest` → 全綠。

- [ ] **Step 5：Commit**

```bash
git add src/quanquant/web/rate_limit.py src/quanquant/web/routers/agent_device.py src/quanquant/web/app.py tests/test_agent_device_api.py
git commit -m "feat: device-code/device-token API endpoint＋per-IP token bucket 限流"
```

---

### Task 6：`/agent/authorize` 核准頁（CSRF synchronizer、手動輸碼、owner-only）

**Files:**
- Create: `src/quanquant/web/csrf.py`
- Create: `src/quanquant/web/routers/agent_authorize.py`
- Create: `src/quanquant/web/templates/agent_authorize.html`
- Create: `src/quanquant/web/templates/partials/agent_authorize_confirm.html`
- Modify: `src/quanquant/auth/device_flow.py`（追加 approve/deny/lookup）
- Modify: `src/quanquant/web/app.py`（掛新 router，走 `protected`——需登入）
- Test: `tests/test_agent_authorize_page.py`

**Interfaces:**
- Consumes：Task 2 的 `AgentDeviceCode`；`web/deps.get_current_user`；`web/routers/orders.get_order_risk_guard`（既有 owner-only 依賴，直接 import 重用，同 repo 既有跨 router import 前例——見 `orders.py` 對 `AuthorizationError` 的用法）
- Produces（本 task 內部消化，不供其他 task 依賴，除 CSRF 模組本身可重用）:

```python
# src/quanquant/web/csrf.py（double-submit cookie；spec §4.1 要求「不只靠 cookie 行為」，
# 這裡是「cookie 值必須與表單隱藏欄位值相等」——跨站偽造請求即使 cookie 自動帶上，攻擊者
# 讀不到 HttpOnly cookie 內容，無法在偽造表單裡填出相符的隱藏欄位值）
#
# 用語對照（避免實作者困惑）：spec 用語是「synchronizer token」，這裡以 double-submit-
# cookie 實作其意圖——伺服端本來就無 session 儲存（session cookie 是 itsdangerous 簽名的
# 無狀態 payload，見 auth/tokens.py），沒有地方存「synchronizer 該存哪」；double-submit
# 版本安全性質等價：攻擊者無法讀寫受害者的 HttpOnly cookie，就無法在偽造表單裡填出相符的
# 隱藏欄位值，等同傳統 synchronizer token 的防偽造效果。
CSRF_COOKIE = "qq_csrf_authorize"

def issue_csrf_token(response: Response) -> str:
    """產生新 token，寫入 HttpOnly/SameSite=Strict/max_age=600s cookie，回傳同值供模板
    塞進隱藏欄位。"""

def verify_csrf_token(request: Request, submitted: str | None) -> bool:
    """cookie 值與 submitted 用 hmac.compare_digest 比對；任一缺失回 False。"""
```

```python
# device_flow.py 追加
def find_pending_by_user_code(session: Session, *, user_code: str) -> AgentDeviceCode | None:
    """唯讀查詢：status='pending' 且未過期。找不到／已過期／已處理一律回 None（頁面顯示
    generic「找不到此代碼或已過期」，不區分是打錯碼還是碼已核准過——避免資訊洩漏助攻
    枚舉）。"""

def approve_device_code(session: Session, *, user_code: str, user_id: int) -> AgentDeviceCode | None:
    """conditional UPDATE：WHERE user_code=:code AND status='pending' AND expires_at>now
    → SET status='approved', user_id=:uid（spec §4.1 step2）。回傳 None＝搶占失敗（已被
    處理或過期，讓 router 顯示「已處理」）。"""

def deny_device_code(session: Session, *, user_code: str) -> bool:
    """同上條件 → SET status='denied'。回傳 True＝成功。"""
```

- [ ] **Step 1：寫失敗測試**

```python
# tests/test_agent_authorize_page.py
import hashlib

from quanquant.auth.device_flow import create_device_code


def _pending_code(session, *, verifier="v" * 43):
    challenge = hashlib.sha256(verifier.encode()).hexdigest()
    raw, row = create_device_code(session, request_ip="203.0.113.5", code_challenge=challenge)
    return raw, row


def test_get_authorize_page_requires_login(anon_client):
    resp = anon_client.get("/agent/authorize", follow_redirects=False)
    assert resp.status_code in (303, 401)


def test_lookup_shows_confirmation_for_valid_pending_code(client, session):
    raw, row = _pending_code(session)
    get_resp = client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = client.post("/agent/authorize", data={"user_code": row.user_code, "csrf_token": csrf})
    assert resp.status_code == 200 and row.user_code in resp.text
    assert "核准" in resp.text and "拒絕" in resp.text


def test_lookup_unknown_code_shows_generic_error(client):
    get_resp = client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = client.post("/agent/authorize", data={"user_code": "ZZZZ-0000", "csrf_token": csrf})
    assert resp.status_code == 200 and "找不到" in resp.text


def test_decide_without_valid_csrf_rejected(client, session):
    raw, row = _pending_code(session)
    resp = client.post("/agent/authorize/decide",
                        data={"device_code_id": row.id, "decision": "approve", "csrf_token": "forged"})
    assert resp.status_code == 403
    session.refresh(row)
    assert row.status == "pending"


def test_decide_approve_binds_current_user_and_conditional_update(client, session, user):
    raw, row = _pending_code(session)
    get_resp = client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = client.post("/agent/authorize/decide",
                        data={"device_code_id": row.id, "decision": "approve", "csrf_token": csrf})
    assert resp.status_code == 200
    session.refresh(row)
    assert row.status == "approved" and row.user_id == user.id


def test_decide_deny_sets_denied(client, session):
    raw, row = _pending_code(session)
    get_resp = client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = client.post("/agent/authorize/decide",
                        data={"device_code_id": row.id, "decision": "deny", "csrf_token": csrf})
    assert resp.status_code == 200
    session.refresh(row)
    assert row.status == "denied"


def test_decide_already_processed_is_rejected_not_overwritten(client, session, user):
    raw, row = _pending_code(session)
    row.status, row.user_id = "approved", user.id
    session.add(row); session.commit()
    get_resp = client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = client.post("/agent/authorize/decide",
                        data={"device_code_id": row.id, "decision": "deny", "csrf_token": csrf})
    assert resp.status_code == 200 and "已處理" in resp.text
    session.refresh(row)
    assert row.status == "approved"  # 沒被 deny 蓋掉
```

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_agent_authorize_page.py -v`
Expected: FAIL（404，router/csrf 模組不存在）

- [ ] **Step 3：實作**

1. `csrf.py`：`issue_csrf_token` 用 `secrets.token_urlsafe(32)`；`Response.set_cookie(CSRF_COOKIE, token, max_age=600, httponly=True, samesite="strict", secure=<比照 auth/routers.py:set_session_cookie 的 x-forwarded-proto 判斷>)`。
2. `device_flow.py` 追加三個函式（`find_pending_by_user_code` 用 `select(...).where(status=='pending', expires_at>now)`；`approve_device_code`/`deny_device_code` 用 `sa_update` conditional UPDATE，比照 Task 4 `claim_and_issue_device_token` 的 `result.rowcount` 判斷寫法）。
3. `agent_authorize.py`：
   - `GET /agent/authorize`：`user: User = Depends(get_current_user)` 觸發未登入 303/401（既有 `_auth_failure` 行為）；`risk_guard.assert_owner(user.id)`（403 若非 owner，比照 `orders.py` 既有寫法）；render 空表單，`issue_csrf_token(response)` 寫 cookie，模板塞入同值隱藏欄位。
   - `POST /agent/authorize`（body: `user_code`, `csrf_token`）：`verify_csrf_token` 失敗 → 403；owner 檢查同上；`find_pending_by_user_code` 找不到 → render 「找不到此代碼或已過期」局部；找到 → render 確認片段（`created_at`、文案「此裝置請求連線你的下單 agent（sim 模式）」、隱藏欄位 `device_code_id`＋新 `issue_csrf_token`＋核准/拒絕兩個 submit）。
   - `POST /agent/authorize/decide`（body: `device_code_id`, `decision`, `csrf_token`）：CSRF/owner 檢查同上；`decision=="approve"` 呼叫 `approve_device_code(session, user_code=<由 device_code_id 反查>, user_id=user.id)`；`decision=="deny"` 呼叫 `deny_device_code`；回傳 None/False → render 「已處理，請請對方重新開始授權流程」；成功 → render 「已核准／已拒絕」確認畫面。
4. `app.py`：`app.include_router(agent_authorize_routes.router, dependencies=protected)`（掛在既有 `protected` 區塊，與 `orders_routes` 同一段）。
5. 模板：純文字＋表單，無 JS；user_code 輸入框無任何預填值（防釣魚，spec §4.1/§8）。

- [ ] **Step 4：跑測試確認通過**

Run: `uv run pytest tests/test_agent_authorize_page.py -v` → PASS；`uv run pytest` → 全綠。

- [ ] **Step 5：Commit**

```bash
git add src/quanquant/web/csrf.py src/quanquant/web/routers/agent_authorize.py \
        src/quanquant/web/templates/agent_authorize.html \
        src/quanquant/web/templates/partials/agent_authorize_confirm.html \
        src/quanquant/auth/device_flow.py src/quanquant/web/app.py tests/test_agent_authorize_page.py
git commit -m "feat: /agent/authorize 核准頁——CSRF synchronizer＋手動輸碼＋owner-only"
```

---

### Task 7：`AgentRunner` 狀態快照

**Files:**
- Modify: `src/quanquant/agent/runner.py`
- Test: `tests/test_agent_runner_snapshot.py`

**Interfaces:**
- Consumes：既有 `AgentRunner` 內部欄位（`self._account`、`self._latched`/`_latch_detail`/`_health_epoch`、`self._buffer`、`self._mode`）；`agent/buffer.py` 既有 `unsent_count() -> int`
- Produces（Task 10 依賴）:

```python
from dataclasses import dataclass, replace

@dataclass(frozen=True)
class AgentSnapshot:
    """整份替換的不可變快照，由主 event loop 單一擁有（GUI 與 runner 同 loop，讀取
    不需 lock）。connection 的合法值目前為 "connecting"/"connected"/"reconnecting"/
    "offline"；Task 13 會再追加 "rejected"（WS 握手被拒，token 無效/停用/非 owner）——
    寫入這個欄位永遠只能經過下面 _connection_state_for_session_exception() 這一個集中
    判斷點，不得在 _pump/_receive_loop/_heartbeat/_child_watchdog 等個別 task 各自
    setattr（多處同時寫同一欄位、例外同步收攏無 await 讓出點，會有後寫覆蓋先寫的競態，
    這是 fresh read-back 覆核抓到的教訓，Task 13 段落有完整根因分析）。"""
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
    規則都只改這一個函式，杜絕分散設定造成的競態。本 task 只建立預設 fallback：
    "reconnecting"；TokenRejectedError→"rejected" 這個分支由 Task 13 補上（因為
    TokenRejectedError 定義在 Task 13 才新增的 ws_client.py，本 task 尚未 import 它）。"""
    return "reconnecting"


class AgentRunner:
    async def snapshot(self) -> AgentSnapshot:
        """組出目前快照。buffer_pending 走 asyncio.to_thread(self._buffer.unsent_count)
        （SQLite 同步 I/O 不壓 loop，repo 既有鐵律）——這個 await 完成後才寫回，本身就在
        event loop 上執行，不需要 call_soon_threadsafe；目前程式庫裡沒有任何『子執行緒
        直接寫快照』的呼叫點（buffer 計數走 to_thread 但結果回到呼叫者所在的 loop 才處理），
        若未來新增子執行緒直接寫入的路徑，一律要包 loop.call_soon_threadsafe(...)，不得
        繞過——這是 spec §6.4 不變量，即使目前沒有具體呼叫點也要保留這條規則供未來遵守。"""
```

- [ ] **Step 1：寫失敗測試**

```python
# tests/test_agent_runner_snapshot.py
import asyncio

import pytest

from quanquant.agent.runner import AgentRunner


class _FakeBuffer:
    def __init__(self, pending=0):
        self._pending = pending
        self.path = "/tmp/fake.db"
    def unsent_count(self):
        return self._pending
    def read_sentinel(self):
        return None
    def get_health_epoch(self):
        return 0


class _FakeChild:
    alive = False
    def start(self):
        return "F1"


@pytest.mark.asyncio
async def test_snapshot_reflects_initial_state():
    runner = AgentRunner(transport=None, buffer=_FakeBuffer(pending=3), child=_FakeChild(), mode="sim")
    snap = await runner.snapshot()
    assert snap.mode == "sim"
    assert snap.buffer_pending == 3
    assert snap.latched is False
    assert snap.connection == "connecting"


@pytest.mark.asyncio
async def test_snapshot_reflects_latched_state_from_persisted_health():
    class _LatchedBuffer(_FakeBuffer):
        def read_sentinel(self):
            return {"epoch": 2, "detail": "buffer 目錄唯讀"}
    runner = AgentRunner(transport=None, buffer=_LatchedBuffer(), child=_FakeChild(), mode="sim")
    snap = await runner.snapshot()
    assert snap.latched is True and snap.latch_detail == "buffer 目錄唯讀" and snap.health_epoch == 2


@pytest.mark.asyncio
async def test_snapshot_is_immutable_and_each_call_is_fresh_instance():
    runner = AgentRunner(transport=None, buffer=_FakeBuffer(pending=1), child=_FakeChild(), mode="sim")
    s1 = await runner.snapshot()
    with pytest.raises(Exception):
        s1.buffer_pending = 99  # frozen dataclass 拒絕賦值
    runner._buffer._pending = 5
    s2 = await runner.snapshot()
    assert s1.buffer_pending == 1 and s2.buffer_pending == 5  # 不是同一份被 mutate
```

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_agent_runner_snapshot.py -v`
Expected: FAIL（`AttributeError: 'AgentRunner' object has no attribute 'snapshot'`）

- [ ] **Step 3：實作**

1. 檔頭加 `from dataclasses import dataclass, replace`，定義 `AgentSnapshot`（上方全欄位）與模組層級函式 `_connection_state_for_session_exception`（上方全碼，本 task 先只有預設 fallback 分支）。
2. `AgentRunner.__init__` 尾端（`_load_persisted_health()` 之後）加 `self._connection_state = "connecting"`。
3. 在既有連線生命週期關鍵點更新 `self._connection_state`（皆已在主 loop 上，直接賦值，不需 call_soon_threadsafe）：`run_once()` WS 連線成功、`UpLogin` 送出後 →`"connected"`；`run_once()` 收攏本輪 session 結束的例外、準備 `raise exc` 給 `run_forever` **之前**，插入唯一一行 `self._connection_state = _connection_state_for_session_exception(exc)`（這是整個計畫**唯一**允許寫入這個欄位的地方——`_pump`/`_receive_loop`/`_heartbeat`/`_child_watchdog` 等個別 task 一律只管 raise，不直接碰這個欄位）；`stop()` 內 →`"offline"`。
4. 新增 `async def snapshot(self) -> AgentSnapshot`：`pending = await asyncio.to_thread(self._buffer.unsent_count)`，組回 `AgentSnapshot(connection=self._connection_state, account=self._account, mode=self._mode, latched=self._latched, latch_detail=self._latch_detail, health_epoch=self._health_epoch, buffer_pending=pending, updated_at=datetime.now(timezone.utc).replace(tzinfo=None))`。

- [ ] **Step 4：跑測試確認通過**

Run: `uv run pytest tests/test_agent_runner_snapshot.py -v` → PASS；`uv run pytest` → 全綠（含既有 `tests/test_agent_runner.py` 系列不受影響——`snapshot()` 是純新增方法，不改任何既有分支的控制流）。

- [ ] **Step 5：Commit**

```bash
git add src/quanquant/agent/runner.py tests/test_agent_runner_snapshot.py
git commit -m "feat: AgentRunner 加入 immutable snapshot（連線狀態/latch/buffer pending）"
```

---

### Task 8：agent GUI coordinator（bootstrap exchange＋本機安全邊界＋生命週期）

**Files:**
- Create: `src/quanquant/agent/gui/__init__.py`
- Create: `src/quanquant/agent/gui/security.py`
- Create: `src/quanquant/agent/gui/coordinator.py`
- Test: `tests/test_agent_gui_security.py`

**Interfaces:**
- Consumes：Task 7 的 `AgentRunner.snapshot()`（協調器持有 runner 實例，供 Task 9/10 掛的路由取用）
- Produces（Task 9/10 依賴，兩個模組協作）:

```python
# src/quanquant/agent/gui/security.py
GUI_SESSION_COOKIE = "qq_agent_gui_session"

class GuiSecurityState:
    """一個 agent GUI 程序的生命週期內單例，掛在 app.state.gui_security。"""
    def __init__(self, *, port: int) -> None:
        self.port = port
        self.bootstrap_secret = secrets.token_urlsafe(32)
        self.bootstrap_consumed = False
        self.session_token: str | None = None

def bootstrap_url(state: GuiSecurityState) -> str:
    """自動開瀏覽器要打開的一次性 URL：http://127.0.0.1:<port>/bootstrap?secret=..."""

def require_gui_session(request: Request) -> None:
    """FastAPI dependency，掛在除 /bootstrap 外的所有本機 API/頁面：驗 session cookie＋
    exact Host: 127.0.0.1:<port>＋same-origin Origin/Sec-Fetch-Site。任一失敗 → 403。"""

def install_security_headers(app: FastAPI) -> None:
    """middleware：所有回應加 Referrer-Policy: no-referrer、Cache-Control: no-store、
    Content-Security-Policy: default-src 'self'、X-Frame-Options/frame-ancestors: 'none'。"""
```

```python
# src/quanquant/agent/gui/coordinator.py
async def run_gui(*, site_origin: str, profile: str | None, reset: bool) -> None:
    """①預綁 127.0.0.1:0 拿實際 port ②建 GuiSecurityState＋FastAPI app（掛 security
    middleware、/bootstrap、Task 9 的 /setup、Task 10 的 /status）③uvicorn.Server.serve
    (sockets=[sock])，access log 關閉 ④webbrowser.open(bootstrap_url(...)) ⑤精靈完成、
    憑證齊備後才建構 ChildHandle/WebsocketsTransport/AgentRunner，啟動 run_forever 背景
    task ⑥runner fatal 時 GUI 保持存活顯示錯誤（不 raise 出協調器）；『停止 Agent』
    （Task 10）與 OS signal 走同一個關閉序列：stop runner → 關 WS → 驗證 child 終止 →
    最後停 uvicorn.Server。"""
```

- [ ] **Step 1：寫失敗測試**

```python
# tests/test_agent_gui_security.py
import httpx
import pytest

from quanquant.agent.gui.security import GUI_SESSION_COOKIE, GuiSecurityState, bootstrap_url, install_security_headers, require_gui_session
from fastapi import Depends, FastAPI


def _build_app(state):
    app = FastAPI()
    install_security_headers(app)

    @app.get("/bootstrap")
    def bootstrap(secret: str, response):
        ...  # 實作時完成；測試只驗證下面兩支

    @app.get("/protected", dependencies=[Depends(require_gui_session)])
    def protected():
        return {"ok": True}

    app.state.gui_security = state
    return app


def test_bootstrap_url_contains_secret_once():
    state = GuiSecurityState(port=54321)
    url = bootstrap_url(state)
    assert f"127.0.0.1:{54321}" in url and state.bootstrap_secret in url


def test_protected_endpoint_rejects_missing_session_cookie():
    state = GuiSecurityState(port=54321)
    app = _build_app(state)
    client = httpx.Client(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:54321")
    resp = client.get("/protected", headers={"Host": "127.0.0.1:54321"})
    assert resp.status_code == 403


def test_protected_endpoint_rejects_wrong_host_header():
    state = GuiSecurityState(port=54321)
    state.session_token = "s3cr3t"
    app = _build_app(state)
    client = httpx.Client(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:54321",
                           cookies={GUI_SESSION_COOKIE: "s3cr3t"})
    resp = client.get("/protected", headers={"Host": "evil.example:54321"})
    assert resp.status_code == 403


def test_protected_endpoint_allows_matching_session_and_host():
    state = GuiSecurityState(port=54321)
    state.session_token = "s3cr3t"
    app = _build_app(state)
    client = httpx.Client(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:54321",
                           cookies={GUI_SESSION_COOKIE: "s3cr3t"})
    resp = client.get("/protected", headers={"Host": "127.0.0.1:54321"})
    assert resp.status_code == 200


def test_responses_carry_no_store_and_csp_headers():
    state = GuiSecurityState(port=54321)
    app = _build_app(state)
    client = httpx.Client(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:54321")
    resp = client.get("/bootstrap", params={"secret": "x"}, headers={"Host": "127.0.0.1:54321"})
    assert resp.headers.get("cache-control") == "no-store"
    assert "default-src 'self'" in resp.headers.get("content-security-policy", "")
```

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_agent_gui_security.py -v`
Expected: FAIL（`ModuleNotFoundError: quanquant.agent.gui.security`）

- [ ] **Step 3：實作**（bootstrap exchange，spec §6.2 全語意）

```python
# src/quanquant/agent/gui/security.py
import secrets
from fastapi import FastAPI, HTTPException, Request, Response

GUI_SESSION_COOKIE = "qq_agent_gui_session"


class GuiSecurityState:
    def __init__(self, *, port: int) -> None:
        self.port = port
        self.bootstrap_secret = secrets.token_urlsafe(32)
        self.bootstrap_consumed = False
        self.session_token: str | None = None


def bootstrap_url(state: GuiSecurityState) -> str:
    return f"http://127.0.0.1:{state.port}/bootstrap?secret={state.bootstrap_secret}"


def consume_bootstrap(state: GuiSecurityState, response: Response, *, secret: str) -> None:
    """驗證後立即失效、種 session cookie。secret 不符或已消費過 → 一律 404（不透露是
    『不符』還是『已用過』，避免時序/存在性洩漏）。"""
    import hmac
    if state.bootstrap_consumed or not hmac.compare_digest(secret, state.bootstrap_secret):
        raise HTTPException(status_code=404)
    state.bootstrap_consumed = True
    state.session_token = secrets.token_urlsafe(32)
    response.set_cookie(GUI_SESSION_COOKIE, state.session_token, httponly=True,
                         samesite="strict", secure=False)  # 127.0.0.1 loopback，無 TLS


def require_gui_session(request: Request) -> None:
    state: GuiSecurityState = request.app.state.gui_security
    cookie = request.cookies.get(GUI_SESSION_COOKIE)
    import hmac
    if state.session_token is None or cookie is None or not hmac.compare_digest(cookie, state.session_token):
        raise HTTPException(status_code=403)
    expected_host = f"127.0.0.1:{state.port}"
    if request.headers.get("host") != expected_host:
        raise HTTPException(status_code=403)
    origin = request.headers.get("origin")
    if origin is not None and origin != f"http://{expected_host}":
        raise HTTPException(status_code=403)
    fetch_site = request.headers.get("sec-fetch-site")
    if fetch_site is not None and fetch_site not in ("same-origin", "none"):
        raise HTTPException(status_code=403)


def install_security_headers(app: FastAPI) -> None:
    @app.middleware("http")
    async def _headers(request: Request, call_next):
        response = await call_next(request)
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Cache-Control"] = "no-store"
        response.headers["Content-Security-Policy"] = "default-src 'self'; frame-ancestors 'none'"
        return response
```

`/bootstrap` GET route（Task 8 在 `coordinator.py` 內定義，呼叫上面 `consume_bootstrap`）驗證後 303 到 `/setup`（乾淨 URL，不帶 secret）。`coordinator.py` 的 `run_gui()` 用 `socket.socket()` 手動 bind `("127.0.0.1", 0)` 取實際 port，`uvicorn.Config(app, ...).server_class(config).serve(sockets=[sock])`；access log 經 `uvicorn.Config(..., access_log=False)` 關閉。

- [ ] **Step 4：跑測試確認通過**

Run: `uv run pytest tests/test_agent_gui_security.py -v` → PASS；`uv run pytest` → 全綠。

- [ ] **Step 5：Commit**

```bash
git add src/quanquant/agent/gui/__init__.py src/quanquant/agent/gui/security.py \
        src/quanquant/agent/gui/coordinator.py tests/test_agent_gui_security.py
git commit -m "feat: agent GUI 本機安全邊界——bootstrap exchange＋Host/Origin 檢查＋安全標頭"
```

---

### Task 9：`/setup` 精靈＋device flow client（單一 in-flight、consumed 自動重開、verification_path exact-match）

**Files:**
- Create: `src/quanquant/agent/device_flow_client.py`
- Create: `src/quanquant/agent/gui/setup_routes.py`
- Create: `src/quanquant/agent/gui/templates/setup_step1.html`、`setup_step2.html`、`setup_step3.html`
- Test: `tests/test_device_flow_client.py`
- Test: `tests/test_agent_setup_wizard.py`

**Interfaces:**
- Consumes：Task 5 的 `/api/agent/device-code`／`/api/agent/device-token`；Task 8 的 `require_gui_session`；Task 12 的 `upsert_profile`（精靈完成才寫 registry，見 Task 12）；Task 14 的 `canonicalize_site`（本 task 直接消費已由 CLI 層算好的 `site_origin` 字串，不重算）
- Produces（Task 10 消費 `DeviceFlowClient` 的 state 供狀態頁顯示重新授權進度）:

```python
# src/quanquant/agent/device_flow_client.py
AGENT_AUTHORIZE_PATH = "/agent/authorize"  # 內建常數；server 回傳值僅供 exact-match 核對


class DeviceFlowClient:
    """單一 device flow 嘗試的擁有者。同一實例任一時刻只允許一個 in-flight 輪詢——
    設計上用一個循序 asyncio loop（await sleep(interval) → await 一次 POST，逾時才重送）
    達成，不使用可能重疊呼叫的 API，天然滿足『只允許一個 in-flight』。"""

    def __init__(self, *, site_origin: str, http_client: "httpx.AsyncClient") -> None: ...

    async def initiate(self) -> dict:
        """產生 code_verifier（記憶體，token_urlsafe(32)）＋code_challenge=sha256 hex，
        POST /api/agent/device-code。驗證回應的 verification_path == AGENT_AUTHORIZE_PATH
        （exact-match，不符 → raise VerificationPathMismatchError，絕不對回傳值做任何
        URL resolve）。回傳 server 回應 dict（含 device_code/user_code/interval/
        expires_in）。approval_url 由 site_origin ＋ 內建常數自行拼接：
        f'{site_origin}{AGENT_AUTHORIZE_PATH}'。"""

    async def poll_until_done(self) -> dict:
        """迴圈：sleep(當下 interval) → POST 一次（client timeout=interval+5s）→
        依 state 分派：pending/slow_down → 更新 interval 繼續迴圈；approved → 回傳；
        expired/denied → raise 對應例外；consumed → 呼叫 initiate() 自動重開新一輪（沿用
        同一 DeviceFlowClient 實例，重設 device_code/user_code/verifier），繼續迴圈；
        invalid → raise DeviceFlowProtocolError（不應發生，防禦性）。"""
```

- [ ] **Step 1：寫失敗測試（client 單元，httpx mock transport，不需真server）**

```python
# tests/test_device_flow_client.py
import hashlib

import httpx
import pytest

from quanquant.agent.device_flow_client import (
    AGENT_AUTHORIZE_PATH, DeviceFlowClient, VerificationPathMismatchError,
)


def _mock_transport(responses):
    calls = []
    def handler(request):
        calls.append(request)
        return responses.pop(0)
    return httpx.MockTransport(handler), calls


@pytest.mark.asyncio
async def test_initiate_rejects_verification_path_mismatch():
    resp = httpx.Response(200, json={
        "device_code": "d", "user_code": "AAAA-BBBB",
        "verification_path": "//evil.example/agent/authorize",  # network-path reference 攻擊
        "interval": 5, "expires_in": 600,
    })
    transport, _ = _mock_transport([resp])
    async with httpx.AsyncClient(transport=transport, base_url="https://quant.example") as http:
        client = DeviceFlowClient(site_origin="https://quant.example", http_client=http)
        with pytest.raises(VerificationPathMismatchError):
            await client.initiate()


@pytest.mark.asyncio
async def test_initiate_builds_approval_url_from_builtin_constant_not_server_value():
    resp = httpx.Response(200, json={
        "device_code": "d", "user_code": "AAAA-BBBB",
        "verification_path": AGENT_AUTHORIZE_PATH, "interval": 5, "expires_in": 600,
    })
    transport, calls = _mock_transport([resp])
    async with httpx.AsyncClient(transport=transport, base_url="https://quant.example") as http:
        client = DeviceFlowClient(site_origin="https://quant.example", http_client=http)
        state = await client.initiate()
        assert client.approval_url == f"https://quant.example{AGENT_AUTHORIZE_PATH}"
    body = calls[0].content
    import json
    challenge = json.loads(body)["code_challenge"]
    assert challenge == hashlib.sha256(client._code_verifier.encode()).hexdigest()


@pytest.mark.asyncio
async def test_poll_until_done_auto_restarts_on_consumed():
    init_resp = httpx.Response(200, json={
        "device_code": "d1", "user_code": "AAAA-BBBB",
        "verification_path": AGENT_AUTHORIZE_PATH, "interval": 0, "expires_in": 600,
    })
    consumed_resp = httpx.Response(200, json={"state": "consumed"})
    reinit_resp = httpx.Response(200, json={
        "device_code": "d2", "user_code": "CCCC-DDDD",
        "verification_path": AGENT_AUTHORIZE_PATH, "interval": 0, "expires_in": 600,
    })
    approved_resp = httpx.Response(200, json={
        "state": "approved", "token": "tok", "profile_id": "1",
        "username": "tester", "token_expires_at": "2099-01-01T00:00:00",
    })
    transport, calls = _mock_transport([init_resp, consumed_resp, reinit_resp, approved_resp])
    async with httpx.AsyncClient(transport=transport, base_url="https://quant.example") as http:
        client = DeviceFlowClient(site_origin="https://quant.example", http_client=http)
        await client.initiate()
        result = await client.poll_until_done()
    assert result["token"] == "tok"
    assert client.user_code == "CCCC-DDDD"  # 已切到重開後的新一輪
```

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_device_flow_client.py -v`
Expected: FAIL（`ModuleNotFoundError: quanquant.agent.device_flow_client`）

- [ ] **Step 3：實作**

`DeviceFlowClient.initiate()`：`self._code_verifier = secrets.token_urlsafe(32)`、`challenge = hashlib.sha256(self._code_verifier.encode()).hexdigest()`、POST body `{"code_challenge": challenge}`；驗證 `resp.json()["verification_path"] == AGENT_AUTHORIZE_PATH` 字串完全相等（`!=` 就 raise，**不呼叫 urljoin/urlsplit 對它做任何 resolve**）；`self.approval_url = f"{self._site_origin}{AGENT_AUTHORIZE_PATH}"`（純字串接，用內建常數不用回傳值）；存 `self._device_code`/`self.user_code`/`self._interval`/`self._expires_at`。`poll_until_done()` 迴圈骨架：

```python
async def poll_until_done(self) -> dict:
    while True:
        await asyncio.sleep(self._interval)
        resp = await self._http.post(
            "/api/agent/device-token",
            json={"device_code": self._device_code, "code_verifier": self._code_verifier},
            timeout=self._interval + 5,
        )
        body = resp.json() if resp.status_code != 404 else {"state": "invalid"}
        state = body.get("state")
        if state in ("pending", "slow_down"):
            self._interval = body.get("interval", self._interval)
            continue
        if state == "approved":
            return body
        if state == "expired":
            raise DeviceFlowExpiredError()
        if state == "denied":
            raise DeviceFlowDeniedError()
        if state == "consumed":
            await self.initiate()
            continue
        raise DeviceFlowProtocolError(f"未知 state: {state!r}")
```

三個例外類別（`VerificationPathMismatchError`/`DeviceFlowExpiredError`/`DeviceFlowDeniedError`/`DeviceFlowProtocolError`）皆繼承 `RuntimeError`，定義於同檔案頂部。

- [ ] **Step 4：跑測試確認通過（client 單元）**

Run: `uv run pytest tests/test_device_flow_client.py -v` → PASS。

- [ ] **Step 5：精靈路由測試與實作（HTMX 頁面骨架）**

`setup_routes.py`（`GET /setup`：Step ①連線授權——顯示 `user_code` 大字、「開核准頁」按鈕（`target="_blank" href=approval_url`）、輪詢進度（HTMX `hx-trigger="load, every 2s"` 打 `/setup/poll-status` 局部）；Step ②永豐憑證——`type=password`＋`autocomplete=off` 兩欄位＋兩個獨立 checkbox「記住裝置授權」「記住永豐 API 憑證」＋教學連結；Step ③確認啟動——顯示 site/mode=sim/symbol/username，「啟動」按鈕）；`POST /setup/step1/start`（背景啟動 `DeviceFlowClient.initiate()`+`poll_until_done()` 為 `asyncio.Task`，存 `app.state.device_flow_task`）；`GET /setup/poll-status`（回傳目前 client 狀態局部，approved 後自動導到 Step②）；`POST /setup/step2`（收永豐憑證進記憶體，422 時 body 不回帶輸入值，見 Global Constraints）；`POST /setup/step3/launch`（呼叫 Task 12 `upsert_profile`，依 opt-in 呼叫 Task 11 keyring 寫入，最後建構 `ChildHandle`/`AgentRunner` 交回 coordinator）。

```python
# tests/test_agent_setup_wizard.py（節錄關鍵案例；422 洩漏面掃描）
def test_step2_validation_error_does_not_echo_credentials(gui_client):
    resp = gui_client.post("/setup/step2", data={"api_key": "", "secret_key": "SECRET123"})
    assert resp.status_code == 422
    assert "SECRET123" not in resp.text


def test_setup_responses_are_no_store(gui_client):
    resp = gui_client.get("/setup")
    assert resp.headers.get("cache-control") == "no-store"
```

（`gui_client` fixture：本 task 於 `tests/conftest.py` 或本檔追加一個組好 `GuiSecurityState`＋已核發 session cookie 的 `httpx` client，供 Task 9/10 測試共用；照 Task 8 `test_agent_gui_security.py` 的 `ASGITransport` 手法建構。）

- [ ] **Step 5b：秘密洩漏 log 掃描（spec §5.1，必修——fresh read-back 覆核要求從 Task 17 的文件備忘改為正式程式碼）**

Task 8 的 `install_security_headers`／`access_log=False` 已經讓本機 GUI server 完全沒有 access log 記任何請求（§6.1），所以本機這一側唯一還有洩漏可能的是 **application log**（`logging.getLogger(...)` 呼叫裡不小心把秘密值直接內插進訊息字串）。以下測試用 `caplog` 掃過整輪精靈表單提交（成功路徑＋錯誤路徑）產生的全部 log record，斷言不含任何一項秘密明文——涵蓋範圍是本 task 新增的 `setup_routes.py` 這個表面；正式站主 server 的 access log 由既有 uvicorn 設定管理（不記 body，既有風險面不變，不在本 task 範圍）。

```python
# tests/test_agent_setup_wizard.py 追加
def test_wizard_flow_logs_do_not_leak_secrets(gui_client, caplog):
    """spec §5.1『掃 access/application log 不含三項秘密』——application log 這一半，
    跑一輪成功＋錯誤路徑的 step2 表單提交，斷言 caplog 全部 record 都不含任何一項秘密
    明文（token 這項用一個假明文樣本模擬，因為本 task 尚未實際持有真 token——Task 10/11
    另外各自針對自己新增的路徑補齊，這裡只保證本 task 引入的程式碼不洩漏）。"""
    import logging

    caplog.set_level(logging.DEBUG)
    secret_api_key = "SAPI-SUPER-SECRET-KEY-0001"
    secret_secret_key = "SSEC-SUPER-SECRET-VALUE-0002"
    secret_token_sample = "TOKEN-SUPER-SECRET-VALUE-0003"

    gui_client.post("/setup/step2", data={"api_key": secret_api_key, "secret_key": secret_secret_key})
    gui_client.post("/setup/step2", data={"api_key": "", "secret_key": secret_secret_key})  # 錯誤路徑（422）

    for record in caplog.records:
        message = record.getMessage()
        assert secret_api_key not in message
        assert secret_secret_key not in message
        assert secret_token_sample not in message
```

實作面：`setup_routes.py` 的任何 `log.*(...)` 呼叫（例如記錄步驟切換、錯誤原因）一律只能記結構性資訊（`user_id`/步驟編號/錯誤類別名），秘密欄位本身或原樣 exception message（可能夾帶輸入值的例外，如某些 pydantic ValidationError 字串化）一律先過濾或改記固定通用訊息，不得原樣 `log.exception(exc)`/`log.error(str(exc))` 這類可能夾帶輸入值的寫法。

- [ ] **Step 6：跑測試確認通過**

Run: `uv run pytest tests/test_agent_setup_wizard.py tests/test_device_flow_client.py -v` → PASS；`uv run pytest` → 全綠。

- [ ] **Step 7：Commit**

```bash
git add src/quanquant/agent/device_flow_client.py src/quanquant/agent/gui/setup_routes.py \
        src/quanquant/agent/gui/templates/setup_step1.html src/quanquant/agent/gui/templates/setup_step2.html \
        src/quanquant/agent/gui/templates/setup_step3.html \
        tests/test_device_flow_client.py tests/test_agent_setup_wizard.py
git commit -m "feat: /setup 三步精靈＋device flow client——單一 in-flight／consumed 自動重開"
```

---

### Task 10：`/status` 儀表板＋停止 Agent＋fail-stop 顯示

**Files:**
- Create: `src/quanquant/agent/gui/status_routes.py`
- Create: `src/quanquant/agent/gui/templates/status.html`
- Test: `tests/test_agent_status_page.py`

**Interfaces:**
- Consumes：Task 7 的 `AgentRunner.snapshot()`；Task 8 的 `require_gui_session`／coordinator 關閉序列；Task 9 的 `DeviceFlowClient`（重新授權按鈕觸發新一輪）；Task 11 的 keyring 清除函式；Task 12 的 `remove_profile`／buffer `unsent_count`
- Produces（本 task 內部消化）:

```python
# src/quanquant/agent/gui/status_routes.py
@router.get("/status", dependencies=[Depends(require_gui_session)])
async def status_page(request: Request): ...
    """HTMX 每 1 秒輪詢（本機、無負載疑慮，spec §6.4）。渲染：連線 badge（connecting/
    connected/reconnecting/offline 四態文案）、mode=sim 標示、buffer pending 筆數、
    latched=True 時大紅警示＋latch_detail 指引文案、token_expires_at 倒數（<3 天變黃）、
    「重新授權」「清除已存憑證／刪除 profile」「停止 Agent」按鈕。"""

@router.post("/status/stop", dependencies=[Depends(require_gui_session)])
async def stop_agent(request: Request): ...
    """呼叫 coordinator 的關閉序列：runner.stop() → 等目前 session task 結束（關 WS）→
    ChildHandle.terminate() 驗證回傳非 False（否則顯示錯誤，不謊稱已停止）→ 排程
    uvicorn.Server 的 should_exit=True（延遲一小段，讓這個 HTTP response 先送出）。"""

@router.post("/status/reauth", dependencies=[Depends(require_gui_session)])
async def reauth(request: Request): ...
    """依 opt-in 分流（spec §5.4）：已勾『記住裝置授權』→ 立即背景跑新一輪 DeviceFlowClient
    → 成功後呼叫 Task 11 rotate_token_secret（先刪後寫）→ 顯示『重啟後套用』；未勾 →
    不預先取 token，顯示『停止後重新啟動，啟動時會重新引導授權』＋補選『改為記住』checkbox。"""

@router.post("/status/clear-credential", dependencies=[Depends(require_gui_session)])
async def clear_credential(request: Request, which: str): ...
    """which ∈ {"token","broker"}；呼叫 Task 11 對應清除函式，registry entry 保留
    （spec §5.2 分項清除）。"""

@router.post("/status/delete-profile", dependencies=[Depends(require_gui_session)])
async def delete_profile(request: Request): ...
    """buffer 有未送資料（unsent_count()>0）→ 拒絕並警示；否則清 registry entry＋該
    profile 全部 keyring 筆（Task 11 clear_profile ＋ Task 12 remove_profile）。"""
```

- [ ] **Step 1：寫失敗測試**

```python
# tests/test_agent_status_page.py（沿用 Task 9 建立的 gui_client fixture 手法；runner
# 用 Task 7 的 _FakeBuffer/_FakeChild 組一個真 AgentRunner 掛到 app.state）

@pytest.mark.asyncio
async def test_status_page_shows_connection_badge_and_pending_count(gui_status_client):
    resp = await gui_status_client.get("/status")
    assert resp.status_code == 200
    assert "connecting" in resp.text or "連線中" in resp.text


@pytest.mark.asyncio
async def test_status_page_shows_failstop_warning_when_latched(gui_status_client_latched):
    resp = await gui_status_client_latched.get("/status")
    assert "buffer 目錄唯讀" in resp.text  # latch_detail 文案出現


@pytest.mark.asyncio
async def test_delete_profile_blocked_when_buffer_has_unsent_rows(gui_status_client_pending_buffer):
    resp = await gui_status_client_pending_buffer.post("/status/delete-profile")
    assert resp.status_code == 409
    assert "尚未送出" in resp.text or "拒絕" in resp.text


@pytest.mark.asyncio
async def test_stop_agent_calls_runner_stop_and_reports_result(gui_status_client):
    resp = await gui_status_client.post("/status/stop")
    assert resp.status_code == 200
    assert gui_status_client.app.state.agent_runner._stopping is True


@pytest.mark.asyncio
async def test_status_responses_are_no_store(gui_status_client):
    resp = await gui_status_client.get("/status")
    assert resp.headers.get("cache-control") == "no-store"
```

（四個 fixture 差異只在 `_FakeBuffer`/`AgentRunner._latched` 初始值——本 task 在測試檔內用小 helper 組出，不新增 conftest 全域 fixture，避免污染其他測試檔。）

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_agent_status_page.py -v`
Expected: FAIL（`ModuleNotFoundError: quanquant.agent.gui.status_routes`）

- [ ] **Step 3：實作**

`status_page` 呼叫 `snap = await request.app.state.agent_runner.snapshot()`，模板依 `snap.connection`/`snap.latched`/`snap.buffer_pending` 條件渲染。`delete_profile` 呼叫 `pending = await asyncio.to_thread(buffer.unsent_count)`，`pending>0` 回 `HTMLResponse(..., status_code=409)`。`stop_agent` 呼叫 `request.app.state.agent_runner.stop()`，然後 `await asyncio.sleep(0)` 讓事件迴圈有機會結束目前 session，再確認 `ChildHandle.terminate()` 回傳值決定顯示文案（複用 Task 7 snapshot 觀察不到 child 存活狀態，這裡直接呼叫 `runner._child.terminate()`——現況 `ChildHandle` 已是 public 介面上的既有方法，僅本檔案存取，不需再加新公開 API）。

- [ ] **Step 4：跑測試確認通過**

Run: `uv run pytest tests/test_agent_status_page.py -v` → PASS；`uv run pytest` → 全綠。

- [ ] **Step 5：Commit**

```bash
git add src/quanquant/agent/gui/status_routes.py src/quanquant/agent/gui/templates/status.html \
        tests/test_agent_status_page.py
git commit -m "feat: /status 儀表板——連線/latch/buffer 可視化＋停止 Agent＋清除憑證"
```

---

### Task 11：keyring 層（backend 能力檢查＋兩 opt-in 逐筆自治＋token 先刪後寫＋清除粒度）

**Files:**
- Create: `src/quanquant/agent/keyring_store.py`
- Modify: `pyproject.toml`（加 `keyring` 依賴）
- Test: `tests/test_agent_keyring_store.py`

**Interfaces:**
- Consumes：`profile_id`/`site_origin` 定位鍵（Task 4/12 提供）
- Produces（Task 9/10 依賴）:

```python
_SERVICE_NAME = "quanquant-agent"

@dataclass(frozen=True)
class KeyringResult:
    ok: bool
    error: str | None = None

def check_secure_backend() -> bool:
    """啟動時能力檢查（spec §5.2）：明確拒絕 fail/plaintext/未鎖定/檔案型 backend。"""

def save_token(*, site_origin: str, profile_id: str, token: str, expires_at: str, username: str) -> KeyringResult: ...
def rotate_token_secret(*, site_origin: str, profile_id: str, new_token: str, expires_at: str, username: str) -> KeyringResult:
    """先刪後寫（spec §5.2 例外規則，見下方全碼）。"""
def load_token(*, site_origin: str, profile_id: str) -> dict | None: ...
def save_broker_credentials(*, site_origin: str, profile_id: str, api_key: str, secret_key: str) -> KeyringResult:
    """逐筆自治：寫入前先讀既有值快照，失敗時只復原這一筆自己的快照（見下方全碼）。"""
def load_broker_credentials(*, site_origin: str, profile_id: str) -> dict | None: ...
def clear_secret(*, site_origin: str, profile_id: str, which: str) -> KeyringResult:
    """which ∈ {"token","broker"}；只刪這一筆，registry entry 由呼叫端另外處理（不動）。"""
def clear_profile(*, site_origin: str, profile_id: str) -> KeyringResult:
    """整個 profile 兩筆都刪（供刪除 profile 用）。"""
```

- [ ] **Step 1：寫失敗測試**

```python
# tests/test_agent_keyring_store.py
import keyring
import keyring.errors
import pytest

from quanquant.agent import keyring_store as ks


class _FakeSecureBackend(keyring.backend.KeyringBackend):
    priority = 1
    def __init__(self):
        self._store: dict[tuple[str, str], str] = {}
    def get_password(self, service, key):
        return self._store.get((service, key))
    def set_password(self, service, key, value):
        self._store[(service, key)] = value
    def delete_password(self, service, key):
        if (service, key) not in self._store:
            raise keyring.errors.PasswordDeleteError("not found")
        del self._store[(service, key)]


class _FlakyBackend(_FakeSecureBackend):
    """第二次 set_password 必失敗，模擬寫入失敗場景。"""
    def __init__(self):
        super().__init__()
        self._set_calls = 0
    def set_password(self, service, key, value):
        self._set_calls += 1
        if self._set_calls == 2:
            raise keyring.errors.PasswordSetError("disk full")
        super().set_password(service, key, value)


@pytest.fixture
def fake_backend(monkeypatch):
    backend = _FakeSecureBackend()
    monkeypatch.setattr(keyring, "get_keyring", lambda: backend)
    monkeypatch.setattr(keyring, "get_password", backend.get_password)
    monkeypatch.setattr(keyring, "set_password", backend.set_password)
    monkeypatch.setattr(keyring, "delete_password", backend.delete_password)
    return backend


def test_check_secure_backend_rejects_fail_backend(monkeypatch):
    import keyring.backends.fail
    monkeypatch.setattr(keyring, "get_keyring", lambda: keyring.backends.fail.Keyring())
    assert ks.check_secure_backend() is False


def test_check_secure_backend_accepts_fake_secure_backend(fake_backend):
    assert ks.check_secure_backend() is True


def test_save_and_load_token_roundtrip(fake_backend):
    result = ks.save_token(site_origin="https://q.example", profile_id="1",
                            token="tok123", expires_at="2099-01-01T00:00:00", username="u")
    assert result.ok
    loaded = ks.load_token(site_origin="https://q.example", profile_id="1")
    assert loaded == {"token": "tok123", "expires_at": "2099-01-01T00:00:00", "username": "u"}


def test_rotate_token_deletes_old_before_writing_new(fake_backend):
    ks.save_token(site_origin="https://q.example", profile_id="1",
                   token="old", expires_at="2020-01-01T00:00:00", username="u")
    result = ks.rotate_token_secret(site_origin="https://q.example", profile_id="1",
                                     new_token="new", expires_at="2099-01-01T00:00:00", username="u")
    assert result.ok
    assert ks.load_token(site_origin="https://q.example", profile_id="1")["token"] == "new"


def test_rotate_token_delete_failure_is_fail_closed(fake_backend, monkeypatch):
    ks.save_token(site_origin="https://q.example", profile_id="1",
                   token="old", expires_at="2020-01-01T00:00:00", username="u")
    def _boom(service, key):
        raise keyring.errors.PasswordDeleteError("locked")
    monkeypatch.setattr(keyring, "delete_password", _boom)
    result = ks.rotate_token_secret(site_origin="https://q.example", profile_id="1",
                                     new_token="new", expires_at="2099-01-01T00:00:00", username="u")
    assert result.ok is False and "手動清除" in result.error


def test_broker_credentials_partial_write_failure_keeps_successful_field_and_restores_only_failed_snapshot(
    monkeypatch,
):
    backend = _FlakyBackend()
    monkeypatch.setattr(keyring, "get_keyring", lambda: backend)
    monkeypatch.setattr(keyring, "get_password", backend.get_password)
    monkeypatch.setattr(keyring, "set_password", backend.set_password)
    monkeypatch.setattr(keyring, "delete_password", backend.delete_password)
    # 先寫一次成功值當快照基準
    ks.save_broker_credentials(site_origin="https://q.example", profile_id="1",
                                api_key="k1", secret_key="s1")
    backend._set_calls = 0  # 重置：下一次 save_broker_credentials 呼叫時第二個 set_password 失敗

    result = ks.save_broker_credentials(site_origin="https://q.example", profile_id="1",
                                         api_key="k2", secret_key="s2")
    assert result.ok is False
    loaded = ks.load_broker_credentials(site_origin="https://q.example", profile_id="1")
    assert loaded == {"api_key": "k1", "secret_key": "s1"}  # 復原成寫入前快照
```

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_agent_keyring_store.py -v`
Expected: FAIL（`ModuleNotFoundError: quanquant.agent.keyring_store`）

- [ ] **Step 3：實作（token 先刪後寫，spec §5.2 必給全碼）**

```python
# src/quanquant/agent/keyring_store.py
import json
from dataclasses import dataclass

import keyring
import keyring.errors

_SERVICE_NAME = "quanquant-agent"
_UNSAFE_BACKEND_MODULE_PREFIXES = (
    "keyring.backends.fail", "keyring.backends.null", "keyring.backends.chainer",
    "keyrings.alt",
)


@dataclass(frozen=True)
class KeyringResult:
    ok: bool
    error: str | None = None


def check_secure_backend() -> bool:
    backend = keyring.get_keyring()
    module = type(backend).__module__
    return not any(module.startswith(prefix) for prefix in _UNSAFE_BACKEND_MODULE_PREFIXES)


def _token_key(site_origin: str, profile_id: str) -> str:
    return f"token:{site_origin}:{profile_id}"


def _broker_key(site_origin: str, profile_id: str) -> str:
    return f"broker:{site_origin}:{profile_id}"


def save_token(*, site_origin: str, profile_id: str, token: str, expires_at: str, username: str) -> KeyringResult:
    payload = json.dumps({"token": token, "expires_at": expires_at, "username": username})
    try:
        keyring.set_password(_SERVICE_NAME, _token_key(site_origin, profile_id), payload)
    except keyring.errors.KeyringError as exc:
        return KeyringResult(ok=False, error=f"儲存失敗：{exc}")
    return KeyringResult(ok=True)


def rotate_token_secret(
    *, site_origin: str, profile_id: str, new_token: str, expires_at: str, username: str,
) -> KeyringResult:
    """先刪後寫：舊枚已被 server 撤銷，順序固定──①先刪 ②再寫。刪除失敗 → fail closed，
    不繼續寫入。各 backend 對『刪除不存在的 key』與『真正刪除失敗』的例外語意不一致，
    先用 get_password 探測是否存在：不存在則跳過刪除（視為成功）；存在則呼叫
    delete_password，任何例外都視為刪除失敗（寧可誤判也不要在無法確認舊值已清除的情況
    下寫入新值）。"""
    key = _token_key(site_origin, profile_id)
    try:
        existing = keyring.get_password(_SERVICE_NAME, key)
    except keyring.errors.KeyringError:
        existing = None
    if existing is not None:
        try:
            keyring.delete_password(_SERVICE_NAME, key)
        except keyring.errors.KeyringError:
            return KeyringResult(
                ok=False, error="無法安全更新授權，請至『清除已存憑證』手動清除後重試",
            )
    payload = json.dumps({"token": new_token, "expires_at": expires_at, "username": username})
    try:
        keyring.set_password(_SERVICE_NAME, key, payload)
    except keyring.errors.KeyringError as exc:
        return KeyringResult(
            ok=False, error=f"儲存失敗，請點『重試儲存』；儲存成功前請勿關閉：{exc}",
        )
    return KeyringResult(ok=True)


def load_token(*, site_origin: str, profile_id: str) -> dict | None:
    raw = keyring.get_password(_SERVICE_NAME, _token_key(site_origin, profile_id))
    return json.loads(raw) if raw else None


def save_broker_credentials(*, site_origin: str, profile_id: str, api_key: str, secret_key: str) -> KeyringResult:
    """逐筆自治：寫入前先讀既有值快照；失敗時只復原這一筆自己的快照（呼叫端另外分別呼叫
    save_token，兩者互不回滾——這裡只管永豐這一筆自身）。快照復原本身也失敗 → 顯式錯誤，
    不得宣稱原值已保留。"""
    key = _broker_key(site_origin, profile_id)
    try:
        snapshot = keyring.get_password(_SERVICE_NAME, key)
    except keyring.errors.KeyringError:
        snapshot = None
    payload = json.dumps({"api_key": api_key, "secret_key": secret_key})
    try:
        keyring.set_password(_SERVICE_NAME, key, payload)
    except keyring.errors.KeyringError as exc:
        if snapshot is not None:
            try:
                keyring.set_password(_SERVICE_NAME, key, snapshot)
            except keyring.errors.KeyringError:
                return KeyringResult(
                    ok=False,
                    error="儲存失敗且原值可能遺失——請至『清除已存憑證』檢查後重新設定",
                )
        return KeyringResult(ok=False, error=f"儲存失敗，原值已保留：{exc}")
    return KeyringResult(ok=True)


def load_broker_credentials(*, site_origin: str, profile_id: str) -> dict | None:
    raw = keyring.get_password(_SERVICE_NAME, _broker_key(site_origin, profile_id))
    return json.loads(raw) if raw else None


def clear_secret(*, site_origin: str, profile_id: str, which: str) -> KeyringResult:
    key = _token_key(site_origin, profile_id) if which == "token" else _broker_key(site_origin, profile_id)
    try:
        keyring.delete_password(_SERVICE_NAME, key)
    except keyring.errors.PasswordDeleteError:
        pass  # 本來就沒有這筆，視同已清除
    except keyring.errors.KeyringError as exc:
        return KeyringResult(ok=False, error=f"清除失敗：{exc}")
    return KeyringResult(ok=True)


def clear_profile(*, site_origin: str, profile_id: str) -> KeyringResult:
    token_result = clear_secret(site_origin=site_origin, profile_id=profile_id, which="token")
    broker_result = clear_secret(site_origin=site_origin, profile_id=profile_id, which="broker")
    if not (token_result.ok and broker_result.ok):
        return KeyringResult(ok=False, error=token_result.error or broker_result.error)
    return KeyringResult(ok=True)
```

- [ ] **Step 4：跑測試確認通過**

Run: `uv run pytest tests/test_agent_keyring_store.py -v` → PASS；`uv run pytest` → 全綠。

- [ ] **Step 5：加依賴＋Commit**

```bash
uv add keyring
```

```bash
git add src/quanquant/agent/keyring_store.py pyproject.toml uv.lock tests/test_agent_keyring_store.py
git commit -m "feat: keyring 存取層——backend 能力檢查＋token 先刪後寫＋逐筆自治復原"
```

---

### Task 12：profile registry（全域鎖＋原子替換）＋per-profile buffer 路徑＋單實例鎖

**Files:**
- Create: `src/quanquant/agent/profile_registry.py`
- Test: `tests/test_agent_profile_registry.py`

**Interfaces:**
- Consumes：`profile_id`/`site_origin`（Task 4 approved 回應、Task 14 canonical site）
- Produces（Task 9/10/13 依賴）:

```python
REGISTRY_PATH = Path.home() / ".quanquant-agent" / "profiles.json"
LOCK_PATH = Path.home() / ".quanquant-agent" / "profiles.lock"

@dataclass(frozen=True)
class ProfileEntry:
    profile_id: str
    username: str
    buffer_path: str
    created_at: str

class AgentAlreadyRunningError(RuntimeError): ...

def upsert_profile(*, site_origin: str, profile_id: str, username: str, buffer_path: str) -> None:
    """(site_origin, profile_id) 唯一鍵 upsert（spec §5.3）。"""
def remove_profile(*, site_origin: str, profile_id: str) -> None: ...
def list_profiles(*, site_origin: str) -> list[ProfileEntry]: ...
def find_profile(*, site_origin: str, profile_id: str) -> ProfileEntry | None: ...
def buffer_path_for(*, site_origin: str, profile_id: str) -> Path:
    """origin_dir = sha256(site_origin) 前 16 hex；profile_dir = sha256(profile_id) 前
    16 hex——固定長度 hex，filesystem-safe（spec §5.3）。"""

class InstanceLock:
    """綁定 buffer 路徑的單實例 process lock；acquire() 非阻塞，拿不到就
    raise AgentAlreadyRunningError。"""
    def __init__(self, buffer_path: Path) -> None: ...
    def acquire(self) -> None: ...
    def release(self) -> None: ...
```

- [ ] **Step 1：寫失敗測試**

```python
# tests/test_agent_profile_registry.py
import json
import threading

import pytest

from quanquant.agent import profile_registry as pr


@pytest.fixture(autouse=True)
def _isolated_home(tmp_path, monkeypatch):
    monkeypatch.setattr(pr, "REGISTRY_PATH", tmp_path / "profiles.json")
    monkeypatch.setattr(pr, "LOCK_PATH", tmp_path / "profiles.lock")
    return tmp_path


def test_upsert_then_list_roundtrip():
    pr.upsert_profile(site_origin="https://q.example", profile_id="1", username="alice",
                       buffer_path="/x/outbox.db")
    entries = pr.list_profiles(site_origin="https://q.example")
    assert len(entries) == 1 and entries[0].profile_id == "1" and entries[0].username == "alice"


def test_upsert_same_profile_id_updates_not_duplicates():
    pr.upsert_profile(site_origin="https://q.example", profile_id="1", username="alice", buffer_path="/x")
    pr.upsert_profile(site_origin="https://q.example", profile_id="1", username="alice2", buffer_path="/x")
    entries = pr.list_profiles(site_origin="https://q.example")
    assert len(entries) == 1 and entries[0].username == "alice2"


def test_remove_profile_deletes_entry():
    pr.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x")
    pr.remove_profile(site_origin="https://q.example", profile_id="1")
    assert pr.list_profiles(site_origin="https://q.example") == []


def test_registry_file_is_valid_json_after_write(_isolated_home):
    pr.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x")
    data = json.loads((_isolated_home / "profiles.json").read_text())
    assert "https://q.example" in data


def test_buffer_path_for_is_fixed_length_hex_and_stable():
    p1 = pr.buffer_path_for(site_origin="https://q.example:443", profile_id="1")
    p2 = pr.buffer_path_for(site_origin="https://q.example:443", profile_id="1")
    assert p1 == p2
    assert len(p1.parent.name) == 16 and all(c in "0123456789abcdef" for c in p1.parent.name)


def test_buffer_path_differs_by_port_even_with_same_host():
    p_http = pr.buffer_path_for(site_origin="http://q.example:8000", profile_id="1")
    p_other = pr.buffer_path_for(site_origin="http://q.example:9000", profile_id="1")
    assert p_http != p_other  # 同 host 異 port 不共用 outbox（spec §5.3 BLOCKER 修復）


def test_concurrent_upserts_from_multiple_threads_do_not_corrupt_registry(_isolated_home):
    def _worker(i):
        pr.upsert_profile(site_origin="https://q.example", profile_id=str(i),
                           username=f"user{i}", buffer_path=f"/x{i}")
    threads = [threading.Thread(target=_worker, args=(i,)) for i in range(20)]
    for t in threads: t.start()
    for t in threads: t.join()
    data = json.loads((_isolated_home / "profiles.json").read_text())
    assert len(data["https://q.example"]) == 20  # 無互蓋、無遺失


def test_instance_lock_second_acquire_raises():
    lock_path = pr.buffer_path_for(site_origin="https://q.example", profile_id="1")
    lock1 = pr.InstanceLock(lock_path)
    lock1.acquire()
    try:
        lock2 = pr.InstanceLock(lock_path)
        with pytest.raises(pr.AgentAlreadyRunningError):
            lock2.acquire()
    finally:
        lock1.release()
```

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_agent_profile_registry.py -v`
Expected: FAIL（`ModuleNotFoundError: quanquant.agent.profile_registry`）

- [ ] **Step 3：實作（全域鎖＋fsync＋os.replace 原子替換，spec §5.3 必給全碼）**

```python
# src/quanquant/agent/profile_registry.py
import hashlib
import json
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

REGISTRY_PATH = Path.home() / ".quanquant-agent" / "profiles.json"
LOCK_PATH = Path.home() / ".quanquant-agent" / "profiles.lock"


@dataclass(frozen=True)
class ProfileEntry:
    profile_id: str
    username: str
    buffer_path: str
    created_at: str


class AgentAlreadyRunningError(RuntimeError):
    """同一 profile 已有另一個 agent 程序持有 instance lock（spec §5.3 單實例）。"""


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


def _load(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _atomic_write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)  # 同檔系統內 rename，POSIX/NTFS 皆原子


@contextmanager
def _registry_lock(*, timeout: float = 10.0):
    """全域跨程序鎖：flock（POSIX）/ msvcrt.locking（Windows），保護
    「重新載入→修改→fsync→os.replace」整段。"""
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(LOCK_PATH, os.O_CREAT | os.O_RDWR)
    deadline = time.monotonic() + timeout
    try:
        while True:
            try:
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise TimeoutError("無法取得 profile registry 鎖（逾時，另一個 agent 程序可能卡住）")
                time.sleep(0.05)
        yield
    finally:
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def upsert_profile(*, site_origin: str, profile_id: str, username: str, buffer_path: str) -> None:
    with _registry_lock():
        data = _load(REGISTRY_PATH)
        entries = data.setdefault(site_origin, [])
        for entry in entries:
            if entry["profile_id"] == profile_id:
                entry["username"] = username
                entry["buffer_path"] = buffer_path
                break
        else:
            entries.append({"profile_id": profile_id, "username": username,
                             "buffer_path": buffer_path, "created_at": _utcnow_iso()})
        _atomic_write(REGISTRY_PATH, data)


def remove_profile(*, site_origin: str, profile_id: str) -> None:
    with _registry_lock():
        data = _load(REGISTRY_PATH)
        remaining = [e for e in data.get(site_origin, []) if e["profile_id"] != profile_id]
        if remaining:
            data[site_origin] = remaining
        else:
            data.pop(site_origin, None)
        _atomic_write(REGISTRY_PATH, data)


def list_profiles(*, site_origin: str) -> list[ProfileEntry]:
    with _registry_lock():
        data = _load(REGISTRY_PATH)
    return [ProfileEntry(**e) for e in data.get(site_origin, [])]


def find_profile(*, site_origin: str, profile_id: str) -> ProfileEntry | None:
    return next((e for e in list_profiles(site_origin=site_origin) if e.profile_id == profile_id), None)


def buffer_path_for(*, site_origin: str, profile_id: str) -> Path:
    origin_dir = hashlib.sha256(site_origin.encode("utf-8")).hexdigest()[:16]
    profile_dir = hashlib.sha256(profile_id.encode("utf-8")).hexdigest()[:16]
    return Path.home() / ".quanquant-agent" / origin_dir / profile_dir / "outbox.db"


class InstanceLock:
    def __init__(self, buffer_path: Path) -> None:
        self._lock_path = Path(str(buffer_path) + ".instance.lock")
        self._fd: int | None = None

    def acquire(self) -> None:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._lock_path, os.O_CREAT | os.O_RDWR)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            raise AgentAlreadyRunningError(
                f"同一 profile 已有另一個 agent 程序在跑（lock={self._lock_path}）"
            )
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(self._fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None
```

- [ ] **Step 4：跑測試確認通過**

Run: `uv run pytest tests/test_agent_profile_registry.py -v` → PASS；`uv run pytest` → 全綠。

- [ ] **Step 5：Commit**

```bash
git add src/quanquant/agent/profile_registry.py tests/test_agent_profile_registry.py
git commit -m "feat: profile registry——全域鎖/fsync/原子替換＋per-profile buffer hash 路徑"
```

---

### Task 13：GUI 啟動決策樹＋profile 選擇頁＋fallback 規則（spec §5.3 全部＋WS 握手被拒引導 reauth）

fresh read-back 覆核發現的必修缺口：spec §5.3「GUI 決策樹」與 profile fallback 規則整組沒有對應 task；本 task 補上，串接 Task 11（keyring）／Task 12（registry）／Task 9（wizard，接手決策樹判定的起始步驟）／Task 8（`run_gui()` 生命週期，本 task 在其中插入決策樹呼叫點）。

**Files:**
- Create: `src/quanquant/agent/gui/startup_flow.py`
- Create: `src/quanquant/agent/gui/templates/profile_select.html`
- Modify: `src/quanquant/agent/ws_client.py`（新增 `TokenRejectedError`）
- Modify: `src/quanquant/agent/runner.py`（`AgentSnapshot.connection` 新增 `"rejected"` 值；`_connection_state_for_session_exception` 補 `TokenRejectedError` 分支；`run_forever(stop_on_token_reject: bool = False)` 新參數＋新 except 分支）
- Modify: `src/quanquant/agent/gui/coordinator.py`（`run_gui()` 串入決策樹＋`probe_direct_connect`＋`stop_on_token_reject=True`）
- Modify: `src/quanquant/agent/gui/setup_routes.py`（`GET /setup` 接受 `start_step`/`notice`；新增 `_finalize_approved_profile`／`_guard_legacy_buffer_before_first_launch`；新增 `GET /profiles`／`POST /profiles/select`）
- Test: `tests/test_agent_gui_startup_flow.py`
- Test: `tests/test_agent_ws_client.py`（新檔，測 `TokenRejectedError` 分類邏輯）
- Test: `tests/test_agent_runner_snapshot.py`（既有檔追加，測單一集中判斷點與 `stop_on_token_reject` 契約）
- Test: `tests/test_agent_setup_wizard.py`（既有檔追加，測 `_finalize_approved_profile`／legacy buffer guard／`/profiles`）

**Interfaces:**
- Consumes：Task 7 的 `AgentRunner.snapshot()`/`AgentSnapshot`/`_connection_state_for_session_exception`；Task 8 的 `run_gui()`／`require_gui_session`；Task 9 的 `DeviceFlowClient`／`gui_client` fixture（本 task 追加 `site_origin`/`profile_id` 屬性）；Task 11 的 `load_token`/`load_broker_credentials`；Task 12 的 `list_profiles`/`find_profile`/`buffer_path_for`/`ProfileEntry`；`agent/buffer.DurableBuffer.unsent_count`
- Produces（Task 9 的 `setup_routes.py` 內部消費 `start_step`/`notice`／`check_legacy_buffer_conflict`／`reconcile_profile_after_approval`——實際呼叫點見下方 Step 3b/3c，不是只掛在 `direct` 分支）:

```python
# src/quanquant/agent/ws_client.py 追加
class TokenRejectedError(RuntimeError):
    """WS 握手被 server 以 close code 1008 拒絕（token 無效/停用/非 owner，見
    web/routers/agent_ws.py::_authenticate 的既有語意）。GUI 決策樹用它判斷『不能再信
    metadata 說 token 未過期』，headless 路徑不特別處理（沿用既有例外傳播/backoff 行為，
    不影響 G5）。"""
```

```python
# src/quanquant/agent/gui/startup_flow.py
LEGACY_DEFAULT_BUFFER = Path.home() / ".quanquant-agent" / "outbox.db"

@dataclass(frozen=True)
class GuiStartupDecision:
    kind: str                        # "profile_select" | "setup" | "direct"
    profile: "ProfileEntry | None" = None
    start_step: int = 1              # 精靈從哪一步開始（1=裝置授權/2=永豐憑證/3=確認）
    notice: str | None = None        # 給 UI 顯示的提示文案

def resolve_gui_startup(
    *, site_origin: str, profile_hint: str | None, reset: bool,
) -> GuiStartupDecision:
    """spec §5.3 GUI 決策樹＋profile fallback miss 分支（見下方全碼）。"""

def reconcile_profile_after_approval(
    *, site_origin: str, expected_profile: "ProfileEntry | None", approved_profile_id: str, username: str,
) -> tuple["ProfileEntry", bool]:
    """核准回應 profile_id 與預期不符時的切換規則（見下方全碼）。回傳
    (要沿用/新建的 ProfileEntry, is_new_profile)；is_new_profile=False 且
    entry.profile_id == expected_profile.profile_id 時＝與預期相符（多數情況）。"""

def check_legacy_buffer_conflict() -> str | None:
    """舊 buffer 遷移政策（見下方全碼）。None＝無衝突。"""

async def probe_direct_connect(runner: "AgentRunner", *, timeout: float = 5.0) -> str:
    """kind="direct" 啟動時的短窗觀察（見下方全碼）。回傳最後觀察到的 connection 值。"""
```

- [ ] **Step 1：寫失敗測試**

```python
# tests/test_agent_gui_startup_flow.py
import datetime as dt

import pytest

from quanquant.agent import keyring_store, profile_registry
from quanquant.agent.gui.startup_flow import (
    check_legacy_buffer_conflict, reconcile_profile_after_approval, resolve_gui_startup,
)


@pytest.fixture(autouse=True)
def _isolated_home(tmp_path, monkeypatch):
    monkeypatch.setattr(profile_registry, "REGISTRY_PATH", tmp_path / "profiles.json")
    monkeypatch.setattr(profile_registry, "LOCK_PATH", tmp_path / "profiles.lock")
    import quanquant.agent.gui.startup_flow as sf
    monkeypatch.setattr(sf, "LEGACY_DEFAULT_BUFFER", tmp_path / "legacy_outbox.db")


@pytest.fixture
def fake_keyring_backend(monkeypatch):
    import keyring
    class _Fake(keyring.backend.KeyringBackend):
        priority = 1
        def __init__(self): self._store = {}
        def get_password(self, service, key): return self._store.get((service, key))
        def set_password(self, service, key, value): self._store[(service, key)] = value
        def delete_password(self, service, key): self._store.pop((service, key), None)
    backend = _Fake()
    monkeypatch.setattr(keyring, "get_keyring", lambda: backend)
    monkeypatch.setattr(keyring, "get_password", backend.get_password)
    monkeypatch.setattr(keyring, "set_password", backend.set_password)
    monkeypatch.setattr(keyring, "delete_password", backend.delete_password)
    return backend


def test_zero_profiles_goes_to_setup_step1():
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint=None, reset=False)
    assert decision.kind == "setup" and decision.start_step == 1


def test_profile_hint_miss_shows_notice_and_goes_to_setup():
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint="ghost", reset=False)
    assert decision.kind == "setup" and decision.start_step == 1
    assert "找不到此帳號設定" in decision.notice


def test_multiple_profiles_without_hint_goes_to_profile_select():
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x1")
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="2", username="b", buffer_path="/x2")
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint=None, reset=False)
    assert decision.kind == "profile_select"


def test_single_profile_with_complete_unexpired_credentials_goes_direct(fake_keyring_backend):
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x1")
    keyring_store.save_token(site_origin="https://q.example", profile_id="1", token="t",
                              expires_at="2099-01-01T00:00:00", username="a")
    keyring_store.save_broker_credentials(site_origin="https://q.example", profile_id="1",
                                           api_key="k", secret_key="s")
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint=None, reset=False)
    assert decision.kind == "direct" and decision.profile.profile_id == "1"


def test_single_profile_missing_token_restarts_at_step1(fake_keyring_backend):
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x1")
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint=None, reset=False)
    assert decision.kind == "setup" and decision.start_step == 1


def test_single_profile_missing_broker_creds_restarts_at_step2(fake_keyring_backend):
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x1")
    keyring_store.save_token(site_origin="https://q.example", profile_id="1", token="t",
                              expires_at="2099-01-01T00:00:00", username="a")
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint=None, reset=False)
    assert decision.kind == "setup" and decision.start_step == 2


def test_expired_token_restarts_at_step1_even_with_broker_creds_present(fake_keyring_backend):
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x1")
    keyring_store.save_token(site_origin="https://q.example", profile_id="1", token="t",
                              expires_at="2000-01-01T00:00:00", username="a")
    keyring_store.save_broker_credentials(site_origin="https://q.example", profile_id="1",
                                           api_key="k", secret_key="s")
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint=None, reset=False)
    assert decision.kind == "setup" and decision.start_step == 1


def test_reset_flag_forces_setup_even_when_profile_complete(fake_keyring_backend):
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x1")
    keyring_store.save_token(site_origin="https://q.example", profile_id="1", token="t",
                              expires_at="2099-01-01T00:00:00", username="a")
    keyring_store.save_broker_credentials(site_origin="https://q.example", profile_id="1",
                                           api_key="k", secret_key="s")
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint="1", reset=True)
    assert decision.kind == "setup" and decision.start_step == 1


def test_externally_deleted_keyring_entry_falls_back_to_setup_not_crash(fake_keyring_backend):
    # registry 還在，但沒寫 keyring（模擬外部工具清空了 keychain）——不得 crash，回精靈。
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x1")
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint="1", reset=False)
    assert decision.kind == "setup" and decision.profile.profile_id == "1"  # 沿用原 buffer 路徑


def test_reconcile_reuses_existing_profile_when_approved_id_already_registered():
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="42", username="bob", buffer_path="/x42")
    entry, is_new = reconcile_profile_after_approval(
        site_origin="https://q.example", expected_profile=None, approved_profile_id="42", username="bob",
    )
    assert is_new is False and entry.buffer_path == "/x42"


def test_reconcile_creates_isolated_new_profile_when_approved_id_unknown():
    entry, is_new = reconcile_profile_after_approval(
        site_origin="https://q.example", expected_profile=None, approved_profile_id="99", username="carol",
    )
    assert is_new is True and entry.profile_id == "99"


def test_legacy_buffer_conflict_blocks_when_unsent_rows_present(tmp_path, monkeypatch):
    import quanquant.agent.gui.startup_flow as sf
    from quanquant.agent.buffer import DurableBuffer
    legacy = tmp_path / "legacy_outbox.db"
    monkeypatch.setattr(sf, "LEGACY_DEFAULT_BUFFER", legacy)
    buf = DurableBuffer(str(legacy))
    buf.append("order_report", {"x": 1})
    notice = check_legacy_buffer_conflict()
    assert notice is not None and "不會自動搬移" in notice


def test_legacy_buffer_conflict_none_when_no_legacy_file(tmp_path, monkeypatch):
    import quanquant.agent.gui.startup_flow as sf
    monkeypatch.setattr(sf, "LEGACY_DEFAULT_BUFFER", tmp_path / "nope.db")
    assert check_legacy_buffer_conflict() is None
```

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_agent_gui_startup_flow.py -v`
Expected: FAIL（`ModuleNotFoundError: quanquant.agent.gui.startup_flow`）

- [ ] **Step 3：實作（決策樹＋fallback 規則，spec §5.3 必給全碼）**

```python
# src/quanquant/agent/gui/startup_flow.py
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from quanquant.agent import keyring_store, profile_registry
from quanquant.agent.buffer import DurableBuffer
from quanquant.agent.profile_registry import ProfileEntry

LEGACY_DEFAULT_BUFFER = Path.home() / ".quanquant-agent" / "outbox.db"


@dataclass(frozen=True)
class GuiStartupDecision:
    kind: str
    profile: ProfileEntry | None = None
    start_step: int = 1
    notice: str | None = None


def _is_expired(expires_at_iso: str) -> bool:
    deadline = datetime.fromisoformat(expires_at_iso)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return now >= deadline


def resolve_gui_startup(
    *, site_origin: str, profile_hint: str | None, reset: bool,
) -> GuiStartupDecision:
    entries = profile_registry.list_profiles(site_origin=site_origin)

    if profile_hint is not None:
        match = next((e for e in entries if e.profile_id == profile_hint), None)
        if match is None:
            return GuiStartupDecision(kind="setup", start_step=1, notice="找不到此帳號設定")
        profile = match
    elif len(entries) == 0:
        return GuiStartupDecision(kind="setup", start_step=1)
    elif len(entries) == 1:
        profile = entries[0]
    else:
        return GuiStartupDecision(kind="profile_select")

    if reset:
        return GuiStartupDecision(kind="setup", start_step=1, profile=profile)

    token = keyring_store.load_token(site_origin=site_origin, profile_id=profile.profile_id)
    if token is None or _is_expired(token["expires_at"]):
        # 涵蓋兩種情境：從沒存過 token，或 keyring entry 被外部刪除／已過期——一律回精靈
        # 步驟①重建，profile 帶著原本的 buffer_path 一起傳回去，完成後沿用同一路徑。
        return GuiStartupDecision(kind="setup", start_step=1, profile=profile)

    broker = keyring_store.load_broker_credentials(site_origin=site_origin, profile_id=profile.profile_id)
    if broker is None:
        return GuiStartupDecision(kind="setup", start_step=2, profile=profile)

    return GuiStartupDecision(kind="direct", profile=profile)


def reconcile_profile_after_approval(
    *, site_origin: str, expected_profile: ProfileEntry | None, approved_profile_id: str, username: str,
) -> tuple[ProfileEntry, bool]:
    """核准頁登入了另一個帳號（approved_profile_id 與 expected_profile 不符，或本來就是
    全新精靈沒有 expected_profile）時的切換規則：已存在該 profile_id → 沿用其
    buffer_path（不沿用 expected_profile 的任何東西）；不存在 → 隔離新建。"""
    existing = profile_registry.find_profile(site_origin=site_origin, profile_id=approved_profile_id)
    if existing is not None:
        return existing, False
    buffer_path = str(profile_registry.buffer_path_for(site_origin=site_origin, profile_id=approved_profile_id))
    new_entry = ProfileEntry(profile_id=approved_profile_id, username=username,
                              buffer_path=buffer_path, created_at=datetime.now(timezone.utc).isoformat())
    return new_entry, True


def check_legacy_buffer_conflict() -> str | None:
    if not LEGACY_DEFAULT_BUFFER.exists():
        return None
    pending = DurableBuffer(str(LEGACY_DEFAULT_BUFFER)).unsent_count()
    if pending == 0:
        return None
    return (
        f"偵測到舊路徑 {LEGACY_DEFAULT_BUFFER} 有 {pending} 筆尚未送出的回報——為避免遺漏，"
        "本精靈不會自動搬移或忽略這批資料。請先以原本的指令列方式（headless，沿用舊設定）"
        "啟動 agent 跑到這批資料送完，或聯絡維運人員人工處理，確認清空後再重新執行本精靈。"
    )
```

`reconcile_profile_after_approval` 的「沿用既有 profile 的 buffer」與 spec §5.3「registry 指向的 keyring entry 被外部刪除→回精靈重建、重建拿到同 profile_id 則沿用原 buffer 路徑」是**同一機制**：只要 server 這次核准仍回傳同一個 `profile_id`（穩定，因為 `profile_id=str(user_id)`，見 Task 4），`find_profile` 就會命中原本的 registry 列，自動帶回原 `buffer_path`，不需要額外分支——這是刻意的設計收斂，實作者不需要為「重建」另開一條路徑。

- [ ] **Step 3b：把 `reconcile_profile_after_approval` 接進 Task 9 的核准完成流程（必修 B1——不能只 upsert_profile）**

Task 9 原骨架把「device flow 收到 `approved`」到「建 registry」寫成一句話帶過（直接拿 `approved["profile_id"]` upsert）；這裡補上真正的收斂點，強制經過比對/切換邏輯：

```python
# src/quanquant/agent/gui/setup_routes.py 追加（延伸 Task 9 骨架，取代原本『approved 後
# 直接 upsert_profile』的簡化描述）
from quanquant.agent.gui.startup_flow import reconcile_profile_after_approval


def _finalize_approved_profile(
    app_state, *, site_origin: str, expected_profile: "ProfileEntry | None", approved: dict,
) -> "ProfileEntry":
    """device flow 核准完成（approved dict 含 profile_id/username/token/token_expires_at）
    後的唯一收斂點：一律呼叫 reconcile_profile_after_approval 決定要沿用哪個 profile
    （expected_profile 是精靈啟動當下的預期——0 筆分支/miss 分支為 None，1 筆分支/
    --profile 命中為 resolve_gui_startup 回傳的 decision.profile）。回傳值存進
    app_state.gui_current_profile，後續 step2/step3 一律讀這個欄位取得 buffer_path，
    不得再各自讀 approved["profile_id"] 另外組路徑。"""
    entry, _is_new = reconcile_profile_after_approval(
        site_origin=site_origin, expected_profile=expected_profile,
        approved_profile_id=approved["profile_id"], username=approved["username"],
    )
    app_state.gui_current_profile = entry
    return entry
```

**掛點（明確呼叫位置）**：`setup_routes.py` 的 `POST /setup/poll-status` handler——`DeviceFlowClient` 輪詢收到 `approved`（拿到 token＋profile_id/username/token_expires_at dict）的當下、寫入任何 keyring/app_state 之前，呼叫 `_finalize_approved_profile(app_state, site_origin=..., expected_profile=decision.profile, approved=approved)`。全精靈只有這一個呼叫點；step2/step3 handler 禁止各自 upsert 或另組 buffer 路徑，一律讀 `app_state.gui_current_profile`。

```python
# tests/test_agent_setup_wizard.py 追加（沿用 tests/test_agent_profile_registry.py 的
# _isolated_home fixture 手法：autouse monkeypatch profile_registry.REGISTRY_PATH/
# LOCK_PATH 到 tmp_path，避免污染真的 ~/.quanquant-agent/）
def test_finalize_approved_profile_reuses_existing_when_id_matches_registry(_isolated_home):
    from quanquant.agent import profile_registry
    from quanquant.agent.gui import setup_routes as sr

    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="7",
                                     username="dave", buffer_path="/x7")
    result = sr._finalize_approved_profile(
        object(), site_origin="https://q.example", expected_profile=None,
        approved={"profile_id": "7", "username": "dave", "token": "t",
                  "token_expires_at": "2099-01-01T00:00:00"},
    )
    assert result.buffer_path == "/x7"


def test_finalize_approved_profile_creates_isolated_new_profile_when_mismatched(_isolated_home):
    from quanquant.agent.gui import setup_routes as sr
    from quanquant.agent.profile_registry import ProfileEntry

    expected = ProfileEntry(profile_id="1", username="old", buffer_path="/x1", created_at="2020-01-01T00:00:00")
    result = sr._finalize_approved_profile(
        object(), site_origin="https://q.example", expected_profile=expected,
        approved={"profile_id": "2", "username": "new", "token": "t",
                  "token_expires_at": "2099-01-01T00:00:00"},
    )
    assert result.profile_id == "2" and result.buffer_path != expected.buffer_path
```

Run: `uv run pytest tests/test_agent_setup_wizard.py -v` → 新增這兩支先 FAIL（`AttributeError: module 'setup_routes' has no attribute '_finalize_approved_profile'`）→ 實作後 PASS。

- [ ] **Step 3c：把 `check_legacy_buffer_conflict` 接進『精靈首次建立 profile』的 launch 路徑（必修 B2——不是 direct 分支）**

正確掛點是 `POST /setup/step3/launch`，且只在**這是這台機器第一次建立這個 `(site_origin, profile_id)`**時才檢查（`profile_registry.find_profile(...)` 查無）——`direct` 分支代表 profile 早就存在過，不會走到這裡；`reset`／既有 profile 缺永豐憑證補問（步驟②起）等情境也不是「首次建立」，同樣不擋：

```python
# src/quanquant/agent/gui/setup_routes.py 追加（延伸 Task 9 骨架的 POST /setup/step3/launch）
from quanquant.agent.gui.startup_flow import check_legacy_buffer_conflict


def _guard_legacy_buffer_before_first_launch(*, site_origin: str, profile_id: str) -> str | None:
    """回傳非 None＝擋下 launch（HTTP 409，顯示這段文案，不建立 profile、不啟動 runner）；
    None＝放行。只在『這個 (site_origin, profile_id) 在 registry 裡還不存在』時才檢查——
    舊 buffer 衝突只對『這台機器第一次從 headless 轉 GUI』有意義。"""
    if profile_registry.find_profile(site_origin=site_origin, profile_id=profile_id) is not None:
        return None
    return check_legacy_buffer_conflict()
```

`POST /setup/step3/launch` 的既有骨架（Task 9）在呼叫 `profile_registry.upsert_profile(...)` **之前**先呼叫這支函式，非 None 就直接回 `HTMLResponse(conflict_message, status_code=409)`，不繼續建 registry／寫 keyring／建構 `AgentRunner`。

```python
# tests/test_agent_setup_wizard.py 追加
def test_launch_blocked_when_legacy_buffer_has_unsent_rows_and_profile_is_new(
    gui_client, tmp_path, monkeypatch, _isolated_home,
):
    import quanquant.agent.gui.startup_flow as sf
    from quanquant.agent.buffer import DurableBuffer
    legacy = tmp_path / "legacy_outbox.db"
    monkeypatch.setattr(sf, "LEGACY_DEFAULT_BUFFER", legacy)
    DurableBuffer(str(legacy)).append("order_report", {"x": 1})

    resp = gui_client.post("/setup/step3/launch", data={"remember_token": "on", "remember_broker": "on"})
    assert resp.status_code == 409
    assert "不會自動搬移" in resp.text


def test_launch_not_blocked_when_profile_already_exists_in_registry(
    gui_client, tmp_path, monkeypatch, _isolated_home,
):
    """既有 profile（reset 或補問憑證流程）不是『首次建立』，即使舊 buffer 有 pending 也
    不擋——這條規則只保護真正的新使用者。"""
    import quanquant.agent.gui.startup_flow as sf
    from quanquant.agent.buffer import DurableBuffer
    from quanquant.agent import profile_registry

    legacy = tmp_path / "legacy_outbox.db"
    monkeypatch.setattr(sf, "LEGACY_DEFAULT_BUFFER", legacy)
    DurableBuffer(str(legacy)).append("order_report", {"x": 1})
    profile_registry.upsert_profile(site_origin=gui_client.site_origin, profile_id=gui_client.profile_id,
                                     username="tester", buffer_path="/existing")

    resp = gui_client.post("/setup/step3/launch", data={"remember_token": "on", "remember_broker": "on"})
    assert resp.status_code != 409
```

（`gui_client.site_origin`/`gui_client.profile_id`：本 task 的 `gui_client` fixture 需固定暴露這兩個屬性，方便測試組出與 fixture 內部一致的 registry key；沿用 Task 9 已建立的 fixture，本 task 只是追加這兩個屬性。）

Run: `uv run pytest tests/test_agent_setup_wizard.py -v` → 新增測試先 FAIL → 實作後 PASS。

`probe_direct_connect` 與 `AgentSnapshot`/`ws_client.py`/`runner.py` 修改（WS 握手被拒 → 不信 metadata；**本節是 fresh read-back 覆核 Necessary A 的修正版，取代前一版有競態的短窗觀察設計**）：

```python
# src/quanquant/agent/ws_client.py：WebsocketsTransport.receive() 改寫（與前一版相同，
# 這段沒有問題，問題在下面 runner.py 端如何處理這個例外）
async def receive(self) -> dict:
    try:
        data = await self._ws.recv()
    except websockets.exceptions.ConnectionClosed as exc:
        code = getattr(exc, "code", None)
        if code is None:
            code = getattr(getattr(exc, "rcvd", None), "code", None)
        if code == 1008:
            raise TokenRejectedError("agent WS 握手被拒（token 無效/停用/非 owner）") from exc
        raise
    return json.loads(data)
```

**根因**（fresh read-back 覆核找到的競態）：`run_once()` 內 `asyncio.wait(..., FIRST_EXCEPTION)` 收攏 `_pump`/`_receive_loop`/`_heartbeat`/`_child_watchdog` 任一 task 的例外是同步發生、中間沒有 `await` 讓出點；若 `_receive_loop` 自己 catch `TokenRejectedError` 就地把 `self._connection_state` 設成 `"rejected"` 再 raise，`run_once()` 收攏例外後緊接著執行 Task 7 原本「例外→`"reconnecting"`」的判斷，會在同一個事件迴圈 tick 內把它蓋回 `"reconnecting"`——`probe_direct_connect` 的輪詢幾乎不可能撞見那個瞬間值。**修法：只設一個集中判斷點，個別 task 一律只管 raise、不碰這個欄位**：

```python
# src/quanquant/agent/runner.py：run_once() 的唯一狀態判斷點（Task 7 已建立
# _connection_state_for_session_exception() 骨架、預設一律回 "reconnecting"；本 task
# 在這裡加 TokenRejectedError 分支）：
def _connection_state_for_session_exception(exc: BaseException) -> str:
    if isinstance(exc, TokenRejectedError):
        return "rejected"
    return "reconnecting"
```

`run_once()` 既有的例外收攏點目前是**單行內聯** `raise _select_session_end_exception(exceptions)`（runner.py 約 L755-758，沒有已綁定的 `exc` 變數）——本 task 把它拆成三行：`exc = _select_session_end_exception(exceptions)` → `self._connection_state = _connection_state_for_session_exception(exc)` → `raise exc`。`_receive_loop`／`_pump`／`_heartbeat`／`_child_watchdog` 對 `TokenRejectedError` 不做任何特殊處理，原樣讓它往外傳播即可——這是本 task 對 `run_once()` 唯一的修改，取代前一版「`_receive_loop` 自己 catch 並直接寫欄位」的做法。

```python
# src/quanquant/agent/runner.py：run_forever() 新增關鍵字參數，並在既有 except 鏈插入
# TokenRejectedError 專屬分支（位置在 except ChildFrozenError 之後、
# except asyncio.CancelledError 之前；三者是互斥的 RuntimeError 子類、順序不影響正確性）：
async def run_forever(self, *, stop_on_token_reject: bool = False) -> None:
    # ...既有簽名其餘參數、既有邏輯全部不動，只新增這個關鍵字參數...
    # 迴圈內既有 try/except 加一段：
    #     except TokenRejectedError:
    #         if stop_on_token_reject:
    #             log.error("agent WS 握手被 server 拒絕（token 無效/停用/非 owner），"
    #                       "不再重試，需要重新授權")
    #             self.stop()
    #             return
    #         log.exception("agent WS session 異常結束，準備依 backoff 重連")
    #         # 不 stop_on_token_reject（headless 預設）：與既有「一般例外」分支寫一模一樣
    #         # 的 log 訊息、不 return，falls through 到下面共用的 elapsed/backoff 計算，
    #         # 繼續正常重試——這是 G5 的直接保證：headless 呼叫端不傳這個參數，預設
    #         # False，控制流/日誌逐位不變。
```

headless 呼叫端（`main.py`）完全不改，不傳這個參數；GUI coordinator 呼叫時**必須**顯式傳 `stop_on_token_reject=True`。

```python
# src/quanquant/agent/gui/startup_flow.py 追加
import asyncio
import time


async def probe_direct_connect(runner, *, timeout: float = 5.0) -> str:
    """kind="direct" 啟動時的觀察：poll runner.snapshot().connection 直到出現
    connected/rejected 或逾時。**這裡不再是競態**（見上方根因分析）：TokenRejectedError
    只會透過 run_once() 那唯一一個集中點寫入 "rejected"，而且呼叫端一律傳
    stop_on_token_reject=True，run_forever() 觀察到後立刻 stop()＋return，不會再有下一輪
    run_once() 把狀態改回 "reconnecting"——"rejected" 一旦出現就是穩定終態，這個輪詢迴圈
    保證看得到，不是短窗賭運氣。逾時仍未出現 connected/rejected（純網路延遲、server 暫時
    不可達）→ 當暫時性問題處理，交給背景 run_forever 繼續照既有 backoff 重試。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snap = await runner.snapshot()
        if snap.connection in ("connected", "rejected"):
            return snap.connection
        await asyncio.sleep(0.1)
    snap = await runner.snapshot()
    return snap.connection
```

`coordinator.py::run_gui()`（Task 8 的既有骨架）在「④自動開瀏覽器」之前插入：`decision = resolve_gui_startup(site_origin=site_origin, profile_hint=profile, reset=reset)`；`decision.kind == "profile_select"` → 開瀏覽器到 `/profiles`；`decision.kind == "setup"` → 開瀏覽器到 `/setup`，帶 `decision.start_step`/`decision.notice`（Task 9 的 `GET /setup` 改為讀 `app.state.gui_startup_decision`；`profile_hint` miss 完成精靈後額外顯示「捷徑指向的帳號已變更，請重新產生捷徑或修改 --profile」；`check_legacy_buffer_conflict`／`reconcile_profile_after_approval` 的實際掛點見上方 Step 3b/3c，不在這裡）；`decision.kind == "direct"` → 用 `keyring_store.load_token`/`load_broker_credentials` 組憑證，建構 `ChildHandle`/`AgentRunner`、`task = asyncio.create_task(runner.run_forever(stop_on_token_reject=True))`（**必須**傳 `True`——GUI 路徑唯一允許停用無限重試的地方）、`await probe_direct_connect(runner)`：結果 `"rejected"` → `run_forever` 內部已經 `stop()`＋`return`（不需要、也不應該再對一個已經正常結束的 task 呼叫 `cancel()`；保險起見可 `await asyncio.wait_for(task, timeout=1.0)` 確認收尾，逾時才 `task.cancel()`）→ 改開瀏覽器到 `/setup`（`start_step=1`，`notice="先前記住的授權已失效（token 可能已被撤銷），請重新授權"`，**不信任 keyring 裡的 `expires_at`**）；其餘結果（`"connected"` 或逾時未定案）→ 開瀏覽器到 `/status`（既有連線背景持續，`run_forever` 已在跑）。

- [ ] **Step 4：跑測試確認通過**

Run: `uv run pytest tests/test_agent_gui_startup_flow.py tests/test_agent_setup_wizard.py -v` → PASS；`uv run pytest` → 全綠。

- [ ] **Step 5：`TokenRejectedError` 分類＋單一集中判斷點＋`stop_on_token_reject` 契約測試**

```python
# tests/test_agent_ws_client.py
import pytest
import websockets

from quanquant.agent.ws_client import TokenRejectedError, WebsocketsTransport


class _FakeWs:
    def __init__(self, exc):
        self._exc = exc
    async def recv(self):
        raise self._exc
    async def send(self, data): ...
    async def close(self): ...


@pytest.mark.asyncio
async def test_receive_raises_token_rejected_on_close_code_1008():
    transport = WebsocketsTransport("ws://x", token="t")
    transport._ws = _FakeWs(websockets.exceptions.ConnectionClosedError(
        rcvd=websockets.frames.Close(1008, "policy violation"), sent=None,
    ))
    with pytest.raises(TokenRejectedError):
        await transport.receive()


@pytest.mark.asyncio
async def test_receive_reraises_other_close_codes_unchanged():
    transport = WebsocketsTransport("ws://x", token="t")
    transport._ws = _FakeWs(websockets.exceptions.ConnectionClosedError(
        rcvd=websockets.frames.Close(1011, "internal error"), sent=None,
    ))
    with pytest.raises(websockets.exceptions.ConnectionClosed):
        await transport.receive()
```

（`websockets.frames.Close(code, reason)` 建構子在 `websockets>=14` 存在；若實測發現本專案釘的版本建構參數名不同，改用 `unittest.mock.Mock(code=1008)` 當 `rcvd` 替身即可，不影響 `receive()` 的判斷邏輯本身。）

```python
# tests/test_agent_runner_snapshot.py 追加（既有檔，Task 7 建立）：單一集中判斷點的純
# 函式測試——不需要碰任何 task 編排細節，直接鎖住「這個 mapping 只有一處」的契約。
def test_connection_state_for_session_exception_maps_token_rejected_to_rejected():
    from quanquant.agent.ws_client import TokenRejectedError
    from quanquant.agent.runner import _connection_state_for_session_exception
    assert _connection_state_for_session_exception(TokenRejectedError("x")) == "rejected"


def test_connection_state_for_session_exception_maps_other_exceptions_to_reconnecting():
    from quanquant.agent.runner import _connection_state_for_session_exception
    assert _connection_state_for_session_exception(RuntimeError("x")) == "reconnecting"
    assert _connection_state_for_session_exception(ConnectionError("x")) == "reconnecting"


# tests/test_agent_runner_snapshot.py 追加：run_forever() 的 stop_on_token_reject 契約，
# 用 monkeypatch runner.run_once 隔離、不依賴 _pump/_receive_loop/_heartbeat/_child_watchdog
# 的內部編排細節（必修 A 覆核要求的「TokenRejectedError 後 state 恆為 rejected 且 runner
# 已停止」關鍵測試）。
@pytest.mark.asyncio
async def test_run_forever_stops_and_latches_rejected_when_flag_set():
    from quanquant.agent.ws_client import TokenRejectedError

    runner = AgentRunner(transport=None, buffer=_FakeBuffer(), child=_FakeChild(), mode="sim")

    async def _boom():
        runner._connection_state = "rejected"  # run_once() 集中點會做的事，這裡直接模擬其副作用
        raise TokenRejectedError("rejected")
    runner.run_once = _boom

    await runner.run_forever(stop_on_token_reject=True)  # 不得往外拋、必須正常 return

    snap = await runner.snapshot()
    assert snap.connection == "rejected"
    assert runner._stopping is True  # 已呼叫 stop()，迴圈不會再重試


@pytest.mark.asyncio
async def test_run_forever_default_flag_retries_normally_on_token_rejected_g5():
    """G5：stop_on_token_reject 預設 False 時，TokenRejectedError 走既有一般例外/backoff
    路徑——不 stop、不提早 return，迴圈照舊繼續重試（用呼叫次數證明有進第二輪）。"""
    from quanquant.agent.ws_client import TokenRejectedError

    runner = AgentRunner(transport=None, buffer=_FakeBuffer(), child=_FakeChild(), mode="sim",
                          backoff_base=0.01, backoff_max=0.01, stable_session_seconds=999)
    calls = []
    async def _boom():
        calls.append(1)
        if len(calls) >= 2:
            runner.stop()
            return
        raise TokenRejectedError("rejected")
    runner.run_once = _boom

    await runner.run_forever()  # stop_on_token_reject 未傳，預設 False

    assert len(calls) == 2  # 第一輪拒絕後仍然重試了第二輪，backoff 行為與既有一般例外一致
```

Run: `uv run pytest tests/test_agent_ws_client.py tests/test_agent_runner_snapshot.py -v` → PASS；`uv run pytest` → 全綠。

- [ ] **Step 6：`/profiles` 選擇頁（spec §6.3，補強 D——紅→綠，與其他 task 一致）**

```python
# tests/test_agent_setup_wizard.py 追加（沿用 Task 9 的 gui_client fixture；本測試需要
# gui_client 已綁定的 site_origin 有 >1 筆 profile，見下方 fixture 前置）
def test_profiles_page_lists_usernames_and_add_account_link(gui_client, _isolated_home):
    from quanquant.agent import profile_registry
    profile_registry.upsert_profile(site_origin=gui_client.site_origin, profile_id="1",
                                     username="alice", buffer_path="/x1")
    profile_registry.upsert_profile(site_origin=gui_client.site_origin, profile_id="2",
                                     username="bob", buffer_path="/x2")

    resp = gui_client.get("/profiles")

    assert resp.status_code == 200
    assert "alice" in resp.text and "bob" in resp.text
    assert "新增帳號" in resp.text and "/setup" in resp.text


def test_profiles_page_requires_gui_session(gui_anon_client):
    resp = gui_anon_client.get("/profiles")
    assert resp.status_code == 403


# `gui_anon_client`：本 task 新增的小 fixture，與既有 `gui_client`（Task 9）共用同一個
# app 建構邏輯，唯一差異是不呼叫 cookies.set(...)——同一支 app、無 session cookie，
# 用來驗證 require_gui_session 擋未登入請求（比照 Task 8 test_agent_gui_security.py 對
# 「缺 session cookie」情境的既有測法，這裡包成具名 fixture 方便本檔多處重用）。


def test_select_profile_posts_profile_id_and_redirects_toward_resolved_decision(gui_client, _isolated_home):
    from quanquant.agent import profile_registry
    profile_registry.upsert_profile(site_origin=gui_client.site_origin, profile_id="1",
                                     username="alice", buffer_path="/x1")

    resp = gui_client.post("/profiles/select", data={"profile_id": "1"}, follow_redirects=False)

    assert resp.status_code in (302, 303)
```

Run: `uv run pytest tests/test_agent_setup_wizard.py -v -k profiles`
Expected: FAIL（`404 Not Found`，`/profiles` 路由不存在）

實作：`GET /profiles`（掛 `require_gui_session`）讀 `list_profiles(site_origin=...)`（`site_origin` 從 coordinator 建構 app 時存的 `app.state.gui_site_origin` 取得，與 `gui_client` fixture 綁定的值一致），模板 `profile_select.html` 逐列 username＋隱藏 `profile_id` 的「使用此帳號」表單（`POST /profiles/select`）＋一個固定「新增帳號」連結（`href="/setup"`，不帶任何 profile 相關參數，等同全新精靈）；`POST /profiles/select`（掛 `require_gui_session`，body 帶 `profile_id`）以該 `profile_id` 重呼 `resolve_gui_startup(..., profile_hint=profile_id, ...)`，依回傳的 `decision.kind` 303 導向對應頁面（`setup`→`/setup`、`direct`→比照 coordinator 既有 direct 分支邏輯）。

Run: `uv run pytest tests/test_agent_setup_wizard.py -v -k profiles` → PASS；`uv run pytest` → 全綠。

- [ ] **Step 7：Commit**

```bash
git add src/quanquant/agent/gui/startup_flow.py src/quanquant/agent/gui/templates/profile_select.html \
        src/quanquant/agent/ws_client.py src/quanquant/agent/runner.py src/quanquant/agent/gui/coordinator.py \
        src/quanquant/agent/gui/setup_routes.py \
        tests/test_agent_gui_startup_flow.py tests/test_agent_ws_client.py tests/test_agent_runner_snapshot.py \
        tests/test_agent_setup_wizard.py
git commit -m "feat: GUI 啟動決策樹＋profile 選擇頁＋fallback 規則＋WS 握手被拒 latch＋stop_on_token_reject"
```

---

### Task 14：CLI 優先序七層＋`--site` canonical 解析／驗證＋GUI 禁 `--server`/`--buffer`＋headless 迴歸

**Files:**
- Create: `src/quanquant/agent/startup.py`
- Modify: `src/quanquant/agent/main.py`
- Test: `tests/test_agent_startup_resolution.py`

**Interfaces:**
- Consumes：無新外部相依（純 argparse/urllib.parse/os.environ）
- Produces（Task 8/9/13/15 依賴）:

```python
# src/quanquant/agent/startup.py
def canonicalize_site(raw: str) -> str:
    """--site canonical 定義（spec §3）：格式限 https://host[:port]（loopback host 例外
    允許 http）；禁 path/query/userinfo/fragment；預設 port（443/80）正規化省略。不合規
    → raise ValueError（訊息供 argparse.error 顯示）。"""

def ws_url_for(site_origin: str) -> str:
    """https→wss、http→ws，路徑固定 '/ws/agent'。"""

@dataclass(frozen=True)
class StartupPlan:
    headless: bool
    site: str | None      # GUI 分支：canonical origin；headless 分支：None
    profile: str | None
    reset: bool
    server: str | None    # headless 專用（--server > env > 預設）
    buffer: str | None    # headless 專用（--buffer > env > 預設）

def build_parser() -> argparse.ArgumentParser:
    """七層優先序中「層1 互斥錯誤」由 mutually_exclusive_group 在這裡擋（--no-gui 與
    --gui/--reset 同時指定）；GUI 模式必填 --site 由 parser.error 擋（--gui/--reset 沒帶
    --site）；GUI 禁 --server／--buffer 由 parser.error 擋（同時指定 --site 與
    --server，或 --gui/--reset 與 --buffer 同時指定）。"""

def resolve_startup_plan(args: argparse.Namespace, *, env: Mapping[str, str], is_tty: bool) -> StartupPlan:
    """層2-7（spec §5.3 表格），由上而下先命中先生效。"""
```

- [ ] **Step 1：寫失敗測試**

```python
# tests/test_agent_startup_resolution.py
import argparse

import pytest

from quanquant.agent.startup import build_parser, canonicalize_site, resolve_startup_plan, ws_url_for


# ---- canonicalize_site ----

@pytest.mark.parametrize("raw,expected", [
    ("https://quant.example", "https://quant.example"),
    ("https://quant.example:443", "https://quant.example"),   # 預設 port 省略
    ("https://quant.example:8443", "https://quant.example:8443"),
    ("http://127.0.0.1:8000", "http://127.0.0.1:8000"),        # loopback 允許 http
    ("http://127.0.0.1:80", "http://127.0.0.1"),
])
def test_canonicalize_site_normalizes(raw, expected):
    assert canonicalize_site(raw) == expected


@pytest.mark.parametrize("raw", [
    "https://quant.example/path",       # 禁 path
    "https://quant.example?x=1",        # 禁 query
    "https://user:pw@quant.example",    # 禁 userinfo
    "https://quant.example#frag",       # 禁 fragment
    "http://quant.example",             # 非 loopback 禁 http
    "ftp://quant.example",              # 非 http(s)
])
def test_canonicalize_site_rejects_invalid(raw):
    with pytest.raises(ValueError):
        canonicalize_site(raw)


def test_ws_url_for_derives_wss_from_https():
    assert ws_url_for("https://quant.example") == "wss://quant.example/ws/agent"


def test_ws_url_for_derives_ws_from_http_loopback():
    assert ws_url_for("http://127.0.0.1:8000") == "ws://127.0.0.1:8000/ws/agent"


# ---- argparse 互斥/必填 ----

def test_no_gui_and_gui_together_is_parser_error():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--no-gui", "--gui", "--site", "https://q.example"])


def test_gui_without_site_is_parser_error():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--gui"])


def test_gui_with_server_is_parser_error():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--gui", "--site", "https://q.example", "--server", "ws://x"])


def test_gui_with_buffer_is_parser_error():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--gui", "--site", "https://q.example", "--buffer", "/x"])


# ---- 七層優先序 ----

def _args(**overrides):
    defaults = dict(no_gui=False, gui=False, reset=False, site=None, profile=None,
                     server=None, buffer=None)
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def test_layer2_no_gui_forces_headless_ignoring_everything_else():
    plan = resolve_startup_plan(_args(no_gui=True), env={}, is_tty=True)
    assert plan.headless is True


def test_layer3_reset_forces_gui():
    plan = resolve_startup_plan(_args(reset=True, site="https://q.example"), env={}, is_tty=False)
    assert plan.headless is False and plan.reset is True and plan.site == "https://q.example"


def test_layer4_gui_flag_forces_gui_even_with_env_trio_present():
    env = {"QQ_AGENT_TOKEN": "t", "QQ_AGENT_API_KEY": "k", "QQ_AGENT_SECRET_KEY": "s"}
    plan = resolve_startup_plan(_args(gui=True, site="https://q.example"), env=env, is_tty=False)
    assert plan.headless is False   # GUI 模式不採用 env 三件套（單一來源原則）


def test_layer5_env_trio_complete_without_gui_flag_goes_headless():
    env = {"QQ_AGENT_TOKEN": "t", "QQ_AGENT_API_KEY": "k", "QQ_AGENT_SECRET_KEY": "s"}
    plan = resolve_startup_plan(_args(), env=env, is_tty=True)
    assert plan.headless is True


def test_layer6_tty_with_site_and_no_env_trio_goes_gui():
    plan = resolve_startup_plan(_args(site="https://q.example"), env={}, is_tty=True)
    assert plan.headless is False and plan.site == "https://q.example"


def test_layer7_fallback_headless_when_no_tty_no_site_no_env():
    plan = resolve_startup_plan(_args(), env={}, is_tty=False)
    assert plan.headless is True


def test_headless_server_priority_flag_beats_env_beats_default():
    plan = resolve_startup_plan(_args(no_gui=True, server="ws://flag"),
                                 env={"QQ_AGENT_SERVER": "ws://env"}, is_tty=True)
    assert plan.server == "ws://flag"
    plan2 = resolve_startup_plan(_args(no_gui=True), env={"QQ_AGENT_SERVER": "ws://env"}, is_tty=True)
    assert plan2.server == "ws://env"
    plan3 = resolve_startup_plan(_args(no_gui=True), env={}, is_tty=True)
    assert plan3.server == "ws://127.0.0.1:8000/ws/agent"   # G5：現行預設值逐位不變
```

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_agent_startup_resolution.py -v`
Expected: FAIL（`ModuleNotFoundError: quanquant.agent.startup`）

- [ ] **Step 3：實作**

`canonicalize_site`（用 `urllib.parse.urlsplit`）、`ws_url_for`（字串取代 `https`→`wss`/`http`→`ws` 開頭＋接 `/ws/agent`）、`resolve_startup_plan` 依 spec §5.3 七層 if-elif 鏈（`layer2: args.no_gui` → `layer3: args.reset` → `layer4: args.gui` → `layer5: env 三件套齊全` → `layer6: is_tty and args.site` → `layer7: 其餘`），headless 分支一律呼叫 `_resolve_headless_server(args, env)`/`_resolve_headless_buffer(args, env)`（`args.server or env.get("QQ_AGENT_SERVER") or "ws://127.0.0.1:8000/ws/agent"`；buffer 同理，預設 `os.path.expanduser("~/.quanquant-agent/outbox.db")`——與現行 `main.py` 逐位相同）。`build_parser()`：加 `--gui`/`--reset`/`--no-gui`（三者用 `add_mutually_exclusive_group()` 包 `--no-gui` 對 `--gui`/`--reset`）、`--site`（default `None`）、`--profile`（default `None`）；`--server` default 改 `None`（不再是 ws:// 預設值，注意這代表 `_resolve_headless_server` 是「預設值改由 resolve 階段補回」，不是拿掉——headless 行為仍逐位不變）；`parser.parse_args()` 後手動檢查：`(args.gui or args.reset) and not args.site` → `parser.error(...)`；`args.site and args.server` → `parser.error(...)`；`(args.gui or args.reset) and args.buffer` → `parser.error(...)`。`main.py` 的 `main()` 改為：`plan = resolve_startup_plan(parsed_args, env=os.environ, is_tty=sys.stdin.isatty())`；`plan.headless` 為 True 時走現行邏輯逐字不動（`getpass`/`ChildHandle`/`AgentRunner` 建構完全比照原本 `main.py`，只是 `args.server`/`args.buffer` 換成 `plan.server`/`plan.buffer`）；False 時呼叫 Task 8 的 `asyncio.run(run_gui(site_origin=plan.site, profile=plan.profile, reset=plan.reset))`。

- [ ] **Step 4：跑測試確認通過**

Run: `uv run pytest tests/test_agent_startup_resolution.py -v` → PASS；`uv run pytest` → 全綠（既有 `tests/test_agent_cli.py` 系列——headless 路徑逐位不變——必須不動全綠，這是 G5 的直接驗收）。

- [ ] **Step 5：Commit**

```bash
git add src/quanquant/agent/startup.py src/quanquant/agent/main.py tests/test_agent_startup_resolution.py
git commit -m "feat: CLI 優先序七層＋--site canonical 解析＋GUI 禁 --server/--buffer"
```

---

### Task 15：捷徑產生器（macOS `.command` chmod 0700／Windows `.lnk` Start in）

**Files:**
- Create: `src/quanquant/agent/shortcut_gen.py`
- Test: `tests/test_agent_shortcut_gen.py`

**Interfaces:**
- Consumes：`shutil.which("uv")`；Task 14 的 `canonicalize_site`（產生器接收的 `site` 參數必須已是 canonical 字串，呼叫端負責）
- Produces（供人工/文件使用，不供其他 task import）:

```python
def generate_macos_shortcut(*, repo_path: Path, site: str, profile: str | None, out_path: Path) -> Path:
    """cd 絕對 repo 路徑＋exec 絕對 uv 路徑；產生後 chmod 0700。找不到 uv → raise
    RuntimeError（不產生半成品檔案）。"""

def generate_windows_shortcut(*, repo_path: Path, site: str, profile: str | None, out_path: Path) -> Path:
    """透過 subprocess 呼叫系統內建 PowerShell（WScript.Shell COM）產生 .lnk——刻意不用
    pywin32，遵守『新依賴僅 keyring』的 Global Constraint。TargetPath=絕對 uv.exe，
    WorkingDirectory=repo_path（等同 Start in）。"""
```

- [ ] **Step 1：寫失敗測試**

```python
# tests/test_agent_shortcut_gen.py
import stat
import sys

import pytest

from quanquant.agent.shortcut_gen import generate_macos_shortcut, generate_windows_shortcut


def test_generate_macos_shortcut_content_and_permission(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: "/usr/local/bin/uv")
    out = tmp_path / "QuanQuant Agent.command"
    result = generate_macos_shortcut(repo_path=tmp_path / "repo", site="https://q.example",
                                      profile=None, out_path=out)
    content = result.read_text()
    assert 'cd "' in content and str(tmp_path / "repo") in content
    assert '"/usr/local/bin/uv" run quanquant-agent --gui --site "https://q.example"' in content
    mode = stat.S_IMODE(result.stat().st_mode)
    assert mode == 0o700


def test_generate_macos_shortcut_includes_profile_flag_when_given(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: "/usr/local/bin/uv")
    out = tmp_path / "QuanQuant Agent.command"
    result = generate_macos_shortcut(repo_path=tmp_path / "repo", site="https://q.example",
                                      profile="alice", out_path=out)
    assert '--profile "alice"' in result.read_text()


def test_generate_macos_shortcut_raises_when_uv_not_found(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: None)
    with pytest.raises(RuntimeError):
        generate_macos_shortcut(repo_path=tmp_path / "repo", site="https://q.example",
                                 profile=None, out_path=tmp_path / "x.command")


def test_generate_windows_shortcut_invokes_powershell_with_expected_args(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: "C:\\uv\\uv.exe")
    captured = {}
    def _fake_run(cmd, check):
        captured["cmd"] = cmd
        captured["check"] = check
    monkeypatch.setattr("subprocess.run", _fake_run)
    out = tmp_path / "QuanQuant Agent.lnk"
    generate_windows_shortcut(repo_path=tmp_path / "repo", site="https://q.example",
                               profile="bob", out_path=out)
    assert captured["check"] is True
    script = captured["cmd"][-1]
    assert "C:\\uv\\uv.exe" in script and "--profile" in script and str(tmp_path / "repo") in script


def test_generate_windows_shortcut_raises_when_uv_not_found(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: None)
    with pytest.raises(RuntimeError):
        generate_windows_shortcut(repo_path=tmp_path / "repo", site="https://q.example",
                                   profile=None, out_path=tmp_path / "x.lnk")
```

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_agent_shortcut_gen.py -v`
Expected: FAIL（`ModuleNotFoundError: quanquant.agent.shortcut_gen`）

- [ ] **Step 3：實作**

```python
# src/quanquant/agent/shortcut_gen.py
import shutil
import subprocess
from pathlib import Path


def _uv_or_raise() -> str:
    uv_path = shutil.which("uv")
    if uv_path is None:
        raise RuntimeError("找不到 uv 可執行檔（PATH 未包含 uv），無法產生捷徑")
    return uv_path


def generate_macos_shortcut(*, repo_path: Path, site: str, profile: str | None, out_path: Path) -> Path:
    uv_path = _uv_or_raise()
    profile_arg = f' --profile "{profile}"' if profile else ""
    content = (
        "#!/bin/sh\n"
        f'cd "{repo_path}" && exec "{uv_path}" run quanquant-agent --gui --site "{site}"{profile_arg}\n'
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")
    out_path.chmod(0o700)
    return out_path


def generate_windows_shortcut(*, repo_path: Path, site: str, profile: str | None, out_path: Path) -> Path:
    uv_path = _uv_or_raise()
    args = f'run quanquant-agent --gui --site "{site}"'
    if profile:
        args += f' --profile "{profile}"'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ps_script = (
        "$WshShell = New-Object -ComObject WScript.Shell; "
        f"$Shortcut = $WshShell.CreateShortcut('{out_path}'); "
        f"$Shortcut.TargetPath = '{uv_path}'; "
        f"$Shortcut.Arguments = '{args}'; "
        f"$Shortcut.WorkingDirectory = '{repo_path}'; "
        "$Shortcut.Save()"
    )
    subprocess.run(["powershell", "-NoProfile", "-Command", ps_script], check=True)
    return out_path
```

- [ ] **Step 4：跑測試確認通過**

Run: `uv run pytest tests/test_agent_shortcut_gen.py -v` → PASS；`uv run pytest` → 全綠。

- [ ] **Step 5：Commit**

```bash
git add src/quanquant/agent/shortcut_gen.py tests/test_agent_shortcut_gen.py
git commit -m "feat: 桌面捷徑產生器——macOS .command chmod 0700／Windows .lnk Start in"
```

---

### Task 16：部署信任鏈（docker-compose ipam 固定 IP＋`FORWARDED_ALLOW_IPS`）＋偽造 XFF/雙來源測試＋`docs/deployment.md`

**Files:**
- Modify: `src/quanquant/config.py`（新增 `forwarded_allow_ips` 設定）
- Modify: `src/quanquant/web/app.py`（`run()` 帶入 `forwarded_allow_ips`）
- Modify: `docker-compose.yml`（固定網路＋env）
- Modify: `docs/deployment.md`
- Test: `tests/test_deployment_trust_chain.py`

**Interfaces:**
- Consumes：Task 5 的 `client_ip(request)`（本 task 驗證它在有/無信任鏈設定下的行為差異）
- Produces：無其他 task 依賴（M1 的收尾）

Settings 新增：

```python
forwarded_allow_ips: str = "127.0.0.1"   # uvicorn 只信任這個 IP/CIDR 字面值送來的 X-Forwarded-*
```

`web/app.py::run()` 改為：

```python
uvicorn.run(
    "quanquant.web.app:create_app",
    factory=True, host=settings.host, port=settings.port, reload=False,
    forwarded_allow_ips=settings.forwarded_allow_ips,
)
```

`docker-compose.yml` 加固定子網＋caddy 固定 IP＋app 讀到的 `FORWARDED_ALLOW_IPS` 指向該 IP：

```yaml
networks:
  quanquant_net:
    ipam:
      config:
        - subnet: 172.28.0.0/24

services:
  app:
    networks:
      quanquant_net:
    environment:
      DB_URL: postgresql+psycopg://quanquant:${POSTGRES_PASSWORD}@postgres:5432/quanquant
      HOST: 0.0.0.0
      FORWARDED_ALLOW_IPS: 172.28.0.10   # 必須是 IP/CIDR 字面值，不得用服務別名（uvicorn 不解析 DNS）
    ...
  postgres:
    networks:
      quanquant_net:
    ...
  caddy:
    networks:
      quanquant_net:
        ipv4_address: 172.28.0.10
    ...
```

（既有 `services.*` 其餘欄位不變，只新增 `networks:` 頂層鍵與各服務底下的 `networks:` 段。）

- [ ] **Step 1：寫失敗測試**

```python
# tests/test_deployment_trust_chain.py
import httpx
import pytest

from quanquant.web.routers.agent_device import client_ip
from fastapi import FastAPI, Request


def _build_probe_app():
    app = FastAPI()

    @app.get("/whoami")
    def whoami(request: Request):
        return {"ip": client_ip(request)}

    return app


@pytest.mark.asyncio
async def test_client_ip_reflects_direct_peer_when_no_proxy_trusted():
    """本機開發情境（無 proxy 設定）：不解析任何 X-Forwarded-For，直接回 socket peer IP，
    即使外部偽造 XFF 也不被採信（spec §4.3「app 端不得解析任意 XFF」）。"""
    app = _build_probe_app()
    transport = httpx.ASGITransport(app=app, client=("203.0.113.5", 12345))
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        resp = await c.get("/whoami", headers={"X-Forwarded-For": "9.9.9.9"})
    assert resp.json()["ip"] == "203.0.113.5"   # 偽造的 9.9.9.9 未被採信


@pytest.mark.asyncio
async def test_two_different_source_ips_are_recorded_distinctly_through_caddy_ip():
    """雙來源測試（spec §4.3 驗收）：模擬兩個不同來源 IP 都「經過」同一個 Caddy 容器 IP
    （172.28.0.10）送出、各自帶自己的 X-Forwarded-For；uvicorn 的
    ProxyHeadersMiddleware（forwarded_allow_ips=172.28.0.10）採信該標頭時，兩者記到的
    client IP 必須不同、都不等於 Caddy 自己的 IP——限流不合流。"""
    from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

    app = ProxyHeadersMiddleware(_build_probe_app(), trusted_hosts=["172.28.0.10"])
    transport = httpx.ASGITransport(app=app, client=("172.28.0.10", 443))
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        r1 = await c.get("/whoami", headers={"X-Forwarded-For": "198.51.100.1"})
        r2 = await c.get("/whoami", headers={"X-Forwarded-For": "198.51.100.2"})
    ip1, ip2 = r1.json()["ip"], r2.json()["ip"]
    assert ip1 != ip2
    assert "172.28.0.10" not in (ip1, ip2)


@pytest.mark.asyncio
async def test_untrusted_proxy_ip_is_not_honored():
    """偽造來源測試：request 不是從被信任的 172.28.0.10 送來（例如攻擊者直連 app 容器），
    ProxyHeadersMiddleware 不採信其 XFF，client IP 仍是連線本身的 peer。"""
    from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

    app = ProxyHeadersMiddleware(_build_probe_app(), trusted_hosts=["172.28.0.10"])
    transport = httpx.ASGITransport(app=app, client=("6.6.6.6", 555))
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        resp = await c.get("/whoami", headers={"X-Forwarded-For": "1.2.3.4"})
    assert resp.json()["ip"] == "6.6.6.6"   # 未被信任來源送的 XFF 不採信


def test_settings_forwarded_allow_ips_defaults_to_loopback():
    from quanquant.config import Settings
    assert Settings().forwarded_allow_ips == "127.0.0.1"


def test_docker_compose_pins_caddy_ip_and_app_forwarded_allow_ips():
    import yaml
    compose = yaml.safe_load(open("docker-compose.yml"))
    assert compose["networks"]["quanquant_net"]["ipam"]["config"][0]["subnet"] == "172.28.0.0/24"
    caddy_ip = compose["services"]["caddy"]["networks"]["quanquant_net"]["ipv4_address"]
    assert compose["services"]["app"]["environment"]["FORWARDED_ALLOW_IPS"] == caddy_ip
```

- [ ] **Step 2：跑測試確認失敗**

Run: `uv run pytest tests/test_deployment_trust_chain.py -v`
Expected: FAIL（`client_ip` 不存在於當時的 import 路徑判斷／`docker-compose.yml` 尚無 `networks` 鍵）

- [ ] **Step 3：實作**

1. `config.py` 加 `forwarded_allow_ips: str = "127.0.0.1"`。
2. `web/app.py::run()` 的 `uvicorn.run(...)` 呼叫加 `forwarded_allow_ips=settings.forwarded_allow_ips` 參數（uvicorn 原生支援此 kwarg，內部掛上 `ProxyHeadersMiddleware`）。
3. `docker-compose.yml` 依上方 YAML 加 `networks:` 頂層鍵＋三個 service 各自的 `networks:` 段；`app.environment` 追加 `FORWARDED_ALLOW_IPS: 172.28.0.10`。
4. `docs/deployment.md`：在既有「部署架構」段落後新增一小節「反向代理信任鏈」，說明 Caddy 預設忽略外來 XFF、uvicorn `forwarded_allow_ips` 只信任 Caddy 固定 IP、為何不能用服務別名（uvicorn 只做 IP/CIDR 字面比對不解析 DNS）、換 Caddy 容器 IP 時要同步更新 `FORWARDED_ALLOW_IPS`。

- [ ] **Step 4：跑測試確認通過**

Run: `uv run pytest tests/test_deployment_trust_chain.py -v` → PASS；`uv run pytest` → 全綠。

- [ ] **Step 5：Commit**

```bash
git add src/quanquant/config.py src/quanquant/web/app.py docker-compose.yml docs/deployment.md \
        tests/test_deployment_trust_chain.py
git commit -m "feat: 部署信任鏈——docker-compose 固定 Caddy IP＋uvicorn forwarded_allow_ips"
```

---

### Task 17：端到端驗收——人工測試計畫檔（spec §10 情境 1-19）

**Files:**
- Create: `docs/superpowers/reviews/2026-08-12-agent-setup-gui-manual-test-plan.md`

**Interfaces:**
- Consumes：spec `docs/superpowers/specs/2026-08-12-agent-setup-gui-design.md` §10（情境 1-19 原文）；Task 1-16 產出的所有自動化測試檔名（供交叉標註）

**產出步驟（非程式碼 task，checkbox 對應「整理」動作而非紅/綠測試）：**

- [ ] **Step 1：建立文件骨架**

比照既有 `docs/superpowers/reviews/2026-08-08-inc1-manual-test-plan.md` 的章節結構（前言：目的／前置條件／環境；逐情境一段），新建 `docs/superpowers/reviews/2026-08-12-agent-setup-gui-manual-test-plan.md`，前言寫明：本文件對應 `2026-08-12-agent-setup-gui-design.md` §10、依賴 Task 1-16 全部實作完成、需要至少一台 macOS 與一台 Windows 機器（D5 驗收矩陣）。

- [ ] **Step 2：逐情境展開（19 條，每條固定欄位）**

每條情境輸出「情境編號與標題／前置條件／操作步驟（條列，含預期畫面文案）／預期結果／自動化覆蓋」五欄，`自動化覆蓋` 欄位精確列出對應 pytest（無自動化覆蓋則寫「純人工」＋原因，例如需要真實 Finder/檔案總管雙擊或真實 macOS Keychain/Windows 憑證管理員）。對照表（供撰寫時查核，非文件最終格式）：

| 情境 | 對應自動化測試 |
|---|---|
| 1 全新使用者端到端 | 純人工（跨 Task 1-16 全鏈路，無單一測試覆蓋整條） |
| 2 二次啟動免精靈 | `tests/test_agent_gui_startup_flow.py::test_single_profile_with_complete_unexpired_credentials_goes_direct`（決策樹本身）＋`tests/test_agent_profile_registry.py`／`tests/test_agent_keyring_store.py`（底層機制）＋人工（真的雙擊捷徑觀察） |
| 3 只記裝置授權 | `tests/test_agent_gui_startup_flow.py::test_single_profile_missing_broker_creds_restarts_at_step2`＋人工（GUI 畫面確認精靈從步驟②開始） |
| 4 user_code 釣魚辨識 | `tests/test_agent_authorize_page.py::test_lookup_unknown_code_shows_generic_error` ＋人工（UI 可讀性判斷） |
| 5 token rotation 不中斷連線 | 既有 `tests/test_agent_ws.py::test_rotation_invalidates_old_token_new_token_still_works`（不變量本身）＋人工（GUI 重新授權流程） |
| 6 fail-stop GUI 存活 | `tests/test_agent_status_page.py::test_status_page_shows_failstop_warning_when_latched`＋人工（真實唯讀目錄） |
| 7 headless 迴歸 | `tests/test_agent_startup_resolution.py`（全部）＋既有 `tests/test_agent_cli.py`／`tests/test_agent_integration.py` |
| 8 keyring 不可用平台 | `tests/test_agent_keyring_store.py::test_check_secure_backend_rejects_fail_backend`＋人工（真實 Linux 無 Secret Service 環境） |
| 9 consumed 復原＋並行 poll | `tests/test_device_flow_poll.py::test_poll_after_consumed_is_readonly_idempotent`／`test_concurrent_claim_only_one_winner`＋`tests/test_device_flow_client.py::test_poll_until_done_auto_restarts_on_consumed` |
| 10 同機雙 profile | `tests/test_agent_profile_registry.py`（全部）＋`tests/test_agent_gui_startup_flow.py::test_multiple_profiles_without_hint_goes_to_profile_select`／`test_reconcile_reuses_existing_profile_when_approved_id_already_registered`／`test_reconcile_creates_isolated_new_profile_when_approved_id_unknown`＋人工（真兩個程序＋真兩個捷徑） |
| 11 keyring 寫入失敗 | `tests/test_agent_keyring_store.py::test_broker_credentials_partial_write_failure_...` |
| 12 秘密洩漏掃描 | `tests/test_agent_setup_wizard.py::test_step2_validation_error_does_not_echo_credentials`＋`test_wizard_flow_logs_do_not_leak_secrets`（Task 9 Step 5b，application log 掃描，已是正式 pytest，非文件備忘） |
| 13 slow_down 封鎖 | `tests/test_device_flow_poll.py::test_slow_down_escalates_interval_and_blocks_after_five_violations` |
| 14 信任鏈 | `tests/test_deployment_trust_chain.py`（全部） |
| 15 捷徑實跑 | `tests/test_agent_shortcut_gen.py`（內容/權限覆蓋）＋人工（真實雙擊） |
| 16 PoP | `tests/test_device_flow_poll.py::test_poll_wrong_verifier_returns_invalid_and_zero_side_effects` |
| 17 reauth 儲存失敗 | `tests/test_agent_keyring_store.py::test_rotate_token_delete_failure_is_fail_closed`＋人工（GUI 重試按鈕流程） |
| 18 reauth 先刪後寫 | `tests/test_agent_keyring_store.py::test_rotate_token_deletes_old_before_writing_new`＋`tests/test_agent_ws_client.py::test_receive_raises_token_rejected_on_close_code_1008`（WS 1008 分類）＋人工（GUI 端到端：`probe_direct_connect` 觀察到 rejected 後真的導回精靈步驟①） |
| 19 registry 併發 | `tests/test_agent_profile_registry.py::test_concurrent_upserts_from_multiple_threads_do_not_corrupt_registry` |

情境 12 的自動化覆蓋已在 Task 9 Step 5b 落地為正式 pytest（`test_wizard_flow_logs_do_not_leak_secrets`，caplog 掃描 application log），不再是本 task 待補的文件備忘；人工測試僅需額外覆核 Task 9 範圍**之外**的表面（例如 `/status` 頁與 Task 10/11 新增路徑是否也遵守同一原則——`clear-credential`/`reauth` 等端點的 log 呼叫），因為 Task 9 的 caplog 測試只涵蓋它自己新增的程式碼路徑。

- [ ] **Step 3：交叉核對 spec §1-§8 需求對應**

在文件末尾附一個「需求覆蓋自查表」：逐條列 spec G1-G5（§1）、D1-D6（§2）、§4 device flow 全部子項、§5 憑證處理全部子項、§6 GUI 全部子項、§7 server 變更清單、§8 威脅模型每一列，各自標註對應到本計畫哪個 Task 編號——供撰寫完後核對「無遺漏」（見下方驗收條件的自查）。

- [ ] **Step 4：Commit**

```bash
git add docs/superpowers/reviews/2026-08-12-agent-setup-gui-manual-test-plan.md
git commit -m "docs: agent 設定精靈端到端人工測試計畫（spec §10 情境 1-19）"
```

---

## 附錄：spec 需求 → Task 對應自查（撰寫本計畫時的驗收自查，供 review 用）

| spec 章節 | 內容 | 對應 Task |
|---|---|---|
| §1 G1 | 零終端機輸入啟動 | 8/9/13/15 |
| §1 G2 | token 全自動（device-code） | 3/4/5/9 |
| §1 G3 | 永豐憑證 GUI 遮罩＋opt-in keychain | 9/11 |
| §1 G4 | 狀態儀表板＋停止按鈕 | 7/10 |
| §1 G5 | headless 完整保留 | 14（既有測試安全網） |
| §2 D1-D6 | 六項拍板決策 | D1→3/4；D2→11；D3→11（pyproject）；D4→（排程，本計畫本身）；D5→15；D6→10/11/13（WS 握手被拒引導 reauth 屬於 D6 的直接後果） |
| §3 URL 規則／不變量 | `--site` canonical／GUI 禁 `--server`／rotation 不中斷 | 14（`--site` canonical／GUI 禁 `--server`）／4（新增 WS revoke 不中斷連線的整合測試，Step 4b）／5 |
| §4.1 協定全部子項 | 發起/PoP/輪詢五步/claim 交易/slow_down/consumed | 3/4 |
| §4.2 新表 | `agent_device_codes` | 2 |
| §4.3 濫用防護＋信任鏈 | 限流／`request_ip`／XFF | 3/5/16 |
| §5.1 輸入路徑洩漏面 | 422 不回帶／no-store／log 掃描 | 9（Step 5b，正式 pytest；不再只是 Task 17 文件備忘） |
| §5.2 keychain | backend 檢查／兩 opt-in／先刪後寫／清除粒度 | 11／13（keyring entry 被外部刪除→回精靈重建，見 `resolve_gui_startup`） |
| §5.3 registry／GUI 決策樹／profile 選擇／fallback／CLI／buffer 隔離 | 全域鎖／決策樹／profile 選擇頁／fallback 規則／七層優先序／origin hash 路徑 | 12（registry 機制）／**13（決策樹＋profile 選擇頁＋fallback 規則，本輪覆核新增）**／14（CLI 優先序／canonical site） |
| §5.4 token 到期 | 倒數／opt-in 分流重新授權 | 10／13（direct-connect 探測失敗時的 reauth 導向） |
| §6.1 生命週期 | 單一協調器 | 8／13（`run_gui()` 串入決策樹） |
| §6.2 本機安全邊界 | bootstrap exchange／Host/Origin | 8 |
| §6.3 頁面 | `/setup`／`/status`／profile 選擇頁 | 9/10/13（profile 選擇頁，本輪覆核新增） |
| §6.4 Runner 快照 | immutable snapshot | 7／13（新增 `"rejected"` 連線態） |
| §7 server 變更清單 | 全部六項 | 1/2/4/5/6/16 |
| §8 威脅模型 | 十項對策 | 對應散落於 2/4/5/6/8/9/11/13/16（Task 17 需求覆蓋自查表逐列核對） |

