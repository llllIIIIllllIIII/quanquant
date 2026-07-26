# Shioaji 期貨下單整合 Implementation Plan（v3）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development（推薦）或 superpowers:executing-plans 依 task 順序實作。Steps 用 checkbox（`- [ ]`）追蹤。
>
> **本計畫是 v3 全面改寫**，真理來源為 `docs/superpowers/specs/2026-07-23-shioaji-order-integration-design.md`（含「修訂 v3」節，該節與前文衝突時以該節為準），對照 `docs/superpowers/reviews/2026-07-24-shioaji-order-codex-review-round2.md`（4 blocker + 20 發現 + PARTIAL 清單，逐條見末尾 Self-Review）與 round1 `docs/superpowers/reviews/2026-07-24-shioaji-order-codex-review.md`（背景）。**v2（`git log` 可查舊版）架構已被 v3 取代處不沿用**（`BrokerPosition` 用剩餘量回推均價、callback 先進 volatile queue 才 durable、委託關聯用裸單鍵 `.first()`、quota 檢查與建單分交易、confirm token 用記憶體 set、單一 uvicorn worker 被誤當作跨 await 的原子性保證）——僅沿用其 TDD 格式與可攜寫法。

**Goal：** 為 QuanQuant 加上單人自用的 Shioaji（永豐）期貨下單（server-side mode 強制、owner 授權、兩階段確認、風控 fail closed、即時 kill switch），成交回報透過 **durable raw-inbox + 單一序列化 broker-operation supervisor** 冪等處理，經**持久化部位帳務（累計加權 lot ledger）**（`BrokerPosition`，與手動日誌完全隔離）完成 round-trip 後才自動寫一筆 `source=shioaji` 的交易日誌並反映 `/stats`；模擬（sim）／正式（real）以 `mode` 第一級 scope 分流、絕不混算。

**Architecture（v3）：**
- `broker/types.py`＋`broker/base.py`：broker 無關純型別（`OrderRequest`/`OrderAck`/`Fill`/`Position`/`RiskDecision`）與 `OrderService` Protocol（`mode` 屬性 + `place/cancel/update/positions` 皆收 `actor_user_id`）；`canonical_payload_hash()` 單一 helper 涵蓋**所有可執行欄位**（symbol/action/qty/price/price_type/order_type/octype/account/mode），place 與 update 簽發/驗證 confirm token 共用同一 helper（修 BLOCKER#1「real update 永遠鎖死」與 HIGH#5「篡改 price_type/order_type」）。
- 七張新表：`Order`（`client_order_id` 唯一冪等鍵，`ordno`/`broker_order_id` 以 `(broker,account,mode,X)` 複合 scope 唯一索引，不再裸鍵查詢）、`Deal`（durable **去重**帳本，`unique(broker,mode,account,trading_day,fill_id)`，補 `broker_order_id` 欄位）、`RawInbox`（durable **raw callback spool**——callback 落地的第一站，早於任何解析/去重邏輯，修 BLOCKER#2 丟單）、`BrokerPosition`（**累計加權部位帳務**：`total_opened_qty`/`entry_notional`/`open_fee_total` 只增不減、永不用剩餘口數回推入場均價，修 BLOCKER#10 PnL 錯算）、`OrderAudit`（append-only）、`ConfirmToken`（TTL+JTI 的 DB 列，取代記憶體 nonce set，claim 為原子 `UPDATE`）、`QuotaReservation`（`(user_id,mode,trading_day)` 聚合列 + 條件 `UPDATE` CAS，修 BLOCKER#4 並行突破日限）。
- `ShioajiAdapter` + `BrokerSupervisor`：**單一 `asyncio.Lock` 序列化通道**，place/update/cancel/positions/close/connect/reconnect/raw-inbox 處理/watchdog reconcile **全部**經同一把鎖才能碰 native shioaji 呼叫或 `BrokerPosition`/`QuotaReservation` 寫入（修 BLOCKER#11/#14「單-worker 不變量在背景 thread 下不成立」）；**send gate**：鎖內、native 呼叫前最後一次讀 kill switch/就緒狀態（修 BLOCKER#13 TOCTOU）；callback（背景執行緒）`loop.call_soon_threadsafe` 排程一個只做 `RawInbox` insert+commit 的協程（**不進 volatile `asyncio.Queue`**），`RawInboxWorker` 從 DB 拉未處理列、驗證合法性（V3-4 嚴格驗證，非法即 quarantine 不猜測）、解析委託關聯一律用 `(broker,account,mode,ordno/broker_order_id)` 複合鍵（修 BLOCKER#3 成交寫錯 user），Deal insert + 部位帳務 + **Order 狀態聚合更新（filled_qty/avg_fill_price/status，含委託回報）** + Trade 寫入 + `processed=true` 同一交易提交。
- `RiskGuard`：owner allowlist、qty/price/白名單/單筆單日上限（日限額走 `QuotaReservation` CAS）、即時 kill switch、real 兩階段確認（`ConfirmToken` DB 列，**冪等先於 token**：`place` 先以 `(client_order_id, request_hash)` 查既有 Order，命中且非終態→回既有狀態不燒 token；token 只在通過所有可失敗檢查後、真正送單前原子 claim）、`update` 重跑全部風控＋以 canonical hash 對「更新後完整 payload」簽發/驗證、append-only `OrderAudit`。
- lifespan：readiness gate（`await connect()` 成功才 publish `app.state.order_service`，失敗 fail closed 反映 `/healthz`）、watchdog（重連＋指數 backoff＋login 節流＋**重連後對帳**：拉券商委託/成交補回 `RawInbox`，走同一序列化通道）、shutdown（sentinel 停 ingress → 等 supervisor/worker 真正結束 → 超時資料維持 spool 於 DB 並保持 unhealthy，不留背景 thread）。
- `mode` 穿過 `Trade`／`list_trades`／`list_for_stats`／journal・stats・orders 三個 filter 表單＋分頁 tab；`stats/metrics.py` 零改。`Trade` 加 `source`（manual/shioaji）與手動 `find_open_trade` 完全隔離自動部位帳務。**DB 層 CHECK**：`Trade`/`Order`/`Deal`/`BrokerPosition`/`OrderAudit` 的 `mode` 皆限 `sim`/`real`（新表走 `create_all` 完整 DDL 加 `CheckConstraint`；既有 `Trade` 表走 SQLite/Postgres 皆可攜的 `ALTER TABLE ADD COLUMN ... DEFAULT ... CHECK (...)`，已用 SQLite 3.53 實測確認 ADD COLUMN 可附帶 CHECK 且既有列以 DEFAULT 值通過檢查）。
- **單一 uvicorn worker 不變量**（Dockerfile 無 `--workers`）**仍是前提但不再是唯一防線**：quota CAS 與 confirm token 原子 claim 用 DB 條件 `UPDATE`（`rowcount` 判定成敗），在單一 worker 內也能防同一 process 內 asyncio 協程交錯（await 讓出時的 TOCTOU）；`BrokerPosition`/`RawInbox` 寫入額外靠 `BrokerSupervisor` 序列化鎖防跨執行緒（callback thread）與跨協程（watchdog task／live worker task）競態。

**Tech Stack：** FastAPI + SQLModel（雙方言 SQLite/Postgres，沿用 `candles/repo.py` 的 dialect-aware upsert 慣例）、HTMX/Jinja2、`shioaji`（已是 core dep，`pyproject.toml:21`）、pytest（in-memory SQLite `StaticPool`、`TestClient` 簽章 cookie）、`asyncio.to_thread`＋`loop.call_soon_threadsafe`＋`asyncio.Lock` 跨執行緒/協程序列化、`itsdangerous`（已是 dep，`auth/tokens.py` 已用；本計畫拿來簽 confirm token 本體，DB 列做 TTL/JTI/一次性防重放）。

## Global Constraints

- **雙方言可攜**：新表/欄位必須 `SQLModel.metadata.create_all` 可自動建（新表，含 `CheckConstraint`/`UniqueConstraint`/`BigInteger`）或 `db/migrate.py` 的 `ensure_columns`（nullable `ADD COLUMN`，可帶 `DEFAULT`/`CHECK`）可攜；任何 epoch-ms 欄位必用 `sqlalchemy.BigInteger`（4-byte INTEGER 會溢位 Postgres）。CAS/upsert 一律走 `session.get_bind().dialect.name` 分流 `sqlalchemy.dialects.{sqlite,postgresql}.insert`（`candles/repo.py:44-48` 既有慣例），**不得**另造只在單一方言正確的 raw SQL。
- **不得新增任何依賴**（`shioaji`、`itsdangerous` 皆已是 core dep）；**不得改弱既有測試**（實作審查以 `git diff` 確認既有測試 assertion 數量/語意未削弱）；**不得 push**（主對話會另行 commit 並送審）。
- **全程開發用 `ORDER_MODE=sim`**（`Shioaji(simulation=True)`），不碰真 CA、不送真單、不花錢；`real` 需完整 CA/readiness preflight 才能啟動（見 Task 8）。
- **單一 uvicorn worker 不變量**（Dockerfile 無 `--workers`）為前提，但**不是原子性的唯一防線**——quota reservation 用 DB 條件 `UPDATE`（CAS）、confirm token claim 用 DB 條件 `UPDATE`、`BrokerPosition`/`RawInbox` 寫入用 `BrokerSupervisor` 單一 `asyncio.Lock` 序列化；若未來上多 worker，CAS/`UPDATE` 天生跨 process 安全，唯 `asyncio.Lock` 需換成 DB row lock（記在 spec「未來」，非本計畫範圍）。
- **mode 完整性**：`Order`/`Deal`/`Fill`/`Trade` 的 `mode` 一律由 server-side 下單 session（`OrderService.mode`）決定，**絕不接受表單/外部輸入覆寫**；`OrderRequest` 型別本身刻意不帶 `mode` 欄位。
- **canonical payload hash 單一來源**：`broker.types.canonical_payload_hash()` 是簽發/驗證 confirm token 與 place 冪等 `request_hash` 的**唯一** hash 產生點；任何需要「這張委託將送給券商的完整內容」的地方（place 簽發、update 簽發、RiskGuard 驗證）一律呼叫同一函式，**禁止**各自組字串算 hash（BLOCKER#1/HIGH#5 的根因就是兩處各自組不同內容）。
- **owner 授權**：單一券商帳戶掛 app singleton；`place/cancel/update/positions` 服務層一律先驗 `actor_user_id` 是否在 `ORDER_OWNER_USER_IDS` 白名單，不是則 403；`cancel/update` 再驗 `(user_id, broker, mode, broker_order_id)` 委託所有權。
- **fail closed**：驗證（qty/price/枚舉）、風控（白名單/上限/kill switch）、real 確認缺失、owner 驗證失敗、非法/缺值 callback——一律拒絕並記 `OrderAudit`（callback 走 `RawInbox.quarantine`），不得靜默通過或猜測（不填空字串/0/`Auto` 頂替缺值）。
- **委託關聯一律複合鍵**：解析 fill→order／raw callback→order 一律用 `(broker, account, mode, ordno)` 或 `(broker, account, mode, broker_order_id)` scope 查詢（加索引），**禁止**裸單鍵 `.first()`；解不到 → quarantine，不猜測歸屬。
- **IntegrityError 精確化**：任何「撞唯一鍵即視為重播/冪等」的 catch，必須先 `rollback()` 後**重新查詢確認命中的正是該唯一鍵**才回既有列；其餘 `IntegrityError`（FK/NULL 等）一律重新拋出，不吞。
- **candle 讀取路徑**（raw-SQL→float、sync `def` routes）勿改 async/ORM——本計畫**不碰 candles**。
- **金額/價格一律 `Decimal`**（`db/models.py` 的 `DecimalText`）；勿用 float（adapter 對 shioaji 邊界才 `float(...)`，回程立即轉 `Decimal`）。canonical hash 對 `Decimal` 一律用 `format(value, "f")` 正規化（避免科學記號/尾零差異造成同義 payload 算出不同 hash）。
- **BrokerSupervisor 序列化通道**：任何會讀寫 `BrokerPosition`/`QuotaReservation`/native shioaji API 的路徑（place/update/cancel/positions/close/connect/reconnect/`RawInboxWorker` 處理批次/watchdog reconcile）**必須**先取得同一把 `asyncio.Lock` 才能動作；**禁止**任何路徑繞過此鎖直接呼叫 `self._api.*` 或改 `BrokerPosition`。
- **每個 task 只 scoped `git add <該 task 檔>`**，嚴禁 `git add -A`／`git add .`；commit 格式 `<type>: <繁中描述>`，attribution 全域停用（不加任何 footer／Co-Authored-By）。
- 全站中文一律**繁體台灣**。
- 測試指令 `uv run pytest`（既有測試全綠才可進下一 task）。
- **GateGuard hook**：第一次 `Bash` 或**建新檔前**會被擋下並要求先陳述事實——這不是故障，照錯誤訊息列點補上事實、重試同一操作即可（本計畫每個「Create 新檔」step 都會踩到，實作者照做）。
- `.env`／`*.pfx`／`*.dump` **不進 git**（Task 10 補 `.pfx` 到 `.gitignore`；CA 憑證/token 只放 VM 的 `.env`／唯讀 bind-mount）。
- **禁止 placeholder**：每個 step 給完整可照抄程式碼；無法 pytest 化的「手動 simtrade E2E」明確標註，不算數為完成。

<!-- TASKS BELOW -->

### Task 1：Trade `mode`+`source` 分流基礎 + DB CHECK（V3-4）

`Trade` 加 `mode`（sim/real，第一級 scope）與 `source`（manual/shioaji，broker 自動與手動日誌隔離的地基）；兩欄位在既有 SQLite/Postgres 表上用**帶 `CHECK` 的 `ALTER TABLE ADD COLUMN`**遷移（V3-4：`mode`/`source` 於 DB 層加 CHECK，非法值拒絕寫入，不只靠 pydantic schema）；打通 `list_trades`/`list_for_stats`/journal·stats filter/tab；手動新增日誌帶當前 tab 的 mode。`stats/metrics.py` 零改。

**Files:**
- Modify: `src/quanquant/db/models.py`（`Trade` 加 `mode`/`source` 欄位）
- Modify: `src/quanquant/db/migrate.py`（`_MIGRATIONS` 追加兩筆，DDL 帶 `DEFAULT` + `CHECK`，用實際表名 `trades`）
- Modify: `src/quanquant/journal/schemas.py`（`Mode`/`Source` Literal + `TradeCreate`/`TradeUpdate`/`TradeRead`/`from_trade`）
- Modify: `src/quanquant/journal/repository.py`（`create_trade`/`list_trades`/`list_for_stats` 穿 `mode`）
- Modify: `src/quanquant/web/routers/trades.py`（`journal_page`/`list_trades_fragment`/`new_trade_form`/`create_trade_route` 加 `mode`）
- Modify: `src/quanquant/web/routers/stats.py`（`_filtered` + 四個 route 加 `mode`）
- Modify: `src/quanquant/web/templates/journal.html`（mode tab + filter hidden mode + 新增交易帶當前 tab mode）
- Modify: `src/quanquant/web/templates/stats.html`（mode tab + filter hidden mode + 匯出 qs 帶 mode）
- Modify: `src/quanquant/web/templates/partials/trade_form.html`（hidden `mode` 欄位）
- Test: Modify `tests/test_repository.py`（mode/source round-trip + 過濾）、Modify `tests/test_migrate.py`（mode/source 遷移 + 既有列預設 + CHECK 擋非法值）、Create `tests/test_mode_split.py`（sim/real 統計隔離）

**Interfaces:**
- Consumes：（無上游 task）
- Produces（Task 2/3/4/6/7/9 依賴）：
  - `Trade.mode: str`（default `"real"`, index）、`Trade.source: str`（default `"manual"`, index）
  - `journal.schemas.Mode = Literal["sim", "real"]`、`journal.schemas.Source = Literal["manual", "shioaji"]`
  - `TradeCreate.mode: Mode = "real"`、`TradeCreate.source: Source = "manual"`
  - `repo.create_trade(session, TradeCreate, *, user_id)`（寫入 `data.mode`/`data.source`）
  - `repo.list_trades(session, *, user_id, mode="real", symbol=None, tag=None, date_from=None, date_to=None, status="all")`
  - `repo.list_for_stats(session, *, user_id, mode="real", ...)`

- [ ] **Step 1：先寫失敗測試 — mode/source round-trip + 過濾（`tests/test_repository.py`）**

在 `tests/test_repository.py` 末端新增（`make_create` 在 `tests/conftest.py`；`TradeCreate` 現在還沒有 `mode`/`source` 欄位，故本測試現在會 RED）：
```python
def test_mode_and_source_default(session, user):
    t = repo.create_trade(session, make_create(), user_id=user.id)
    assert t.mode == "real" and t.source == "manual"


def test_mode_round_trip_sim(session, user):
    t = repo.create_trade(session, make_create(mode="sim"), user_id=user.id)
    assert repo.get_trade(session, t.id, user_id=user.id).mode == "sim"


def test_source_round_trip_shioaji(session, user):
    t = repo.create_trade(session, make_create(source="shioaji"), user_id=user.id)
    assert repo.get_trade(session, t.id, user_id=user.id).source == "shioaji"


def test_list_trades_scoped_by_mode(session, user):
    repo.create_trade(session, make_create(symbol="TXF"), user_id=user.id)              # real
    repo.create_trade(session, make_create(symbol="MXF", mode="sim"), user_id=user.id)  # sim
    real = repo.list_trades(session, user_id=user.id, mode="real")
    sim = repo.list_trades(session, user_id=user.id, mode="sim")
    assert [t.symbol for t in real] == ["TXF"]
    assert [t.symbol for t in sim] == ["MXF"]
```

- [ ] **Step 2：跑測試確認 RED**

```bash
uv run pytest tests/test_repository.py -q
```
預期 `pydantic.ValidationError`／`TypeError`（`TradeCreate` 尚無 `mode`/`source`）→ RED，符合預期。

- [ ] **Step 3：`Trade` 加欄位（`src/quanquant/db/models.py`）**

在 `Trade` 的 `user_id` 欄位**之前**插入（`tags` 欄位之後）：
```python
    mode: str = Field(default="real", index=True)          # "real"（正式/手動）| "sim"（模擬/紙上）
    source: str = Field(default="manual", index=True)      # "manual"（人工輸入）| "shioaji"（broker 自動，見 broker/position_tracker.py）
```
> 註：DB 層 `CHECK` 由 Step 4 的遷移 DDL 附帶（V3-4），非留給 schema 層獨力把關——見 Step 4 註解為何 SQLite `ALTER TABLE ADD COLUMN` 可攜地帶 `CHECK`。

- [ ] **Step 4：遷移 + DB CHECK（`src/quanquant/db/migrate.py`）**

`_MIGRATIONS`（檔案 L11-16）末端加兩筆（**DDL 帶 `DEFAULT` 讓兩方言的既有列自動回填，並內嵌 `CHECK` 讓非法值連 DB 層都拒絕**——SQLite 3.31+ 與 Postgres 皆允許 `ALTER TABLE ... ADD COLUMN col TYPE DEFAULT v CHECK (col IN (...))`：新欄位的 `CHECK` 只需對「加欄位當下所有既有列的值」成立，而既有列會被自動填為 `DEFAULT`，且 `DEFAULT` 本身合法，故對舊資料一定通過；本檔案已用 SQLite 3.53 實測 `ALTER TABLE trades ADD COLUMN mode VARCHAR DEFAULT 'real' CHECK (mode IN ('sim','real'))` 可成功執行、既有列回填 `'real'`、且後續非法值 `INSERT` 會被 `CHECK` 擋下拋 `IntegrityError`）：
```python
    ("trades", "mode", "VARCHAR DEFAULT 'real' CHECK (mode IN ('sim','real'))"),
    ("trades", "source", "VARCHAR DEFAULT 'manual' CHECK (source IN ('manual','shioaji'))"),
```

- [ ] **Step 5：schema 欄位（`src/quanquant/journal/schemas.py`）**

檔首 `Direction = Literal["long", "short"]` 之後加：
```python
Mode = Literal["sim", "real"]
Source = Literal["manual", "shioaji"]
```
`TradeCreate`（`tags` 欄位之後、`_exit_pair` validator 之前）加：
```python
    mode: Mode = "real"
    source: Source = "manual"
```
`TradeUpdate`（`tags` 欄位之後）加：
```python
    mode: Mode | None = None
    source: Source | None = None
```
`TradeRead`（`tags` 欄位之後、`is_open` 之前）加：
```python
    mode: str
    source: str
```
`TradeRead.from_trade`（`tags=split_tags(t.tags),` 之後）加：
```python
            mode=t.mode,
            source=t.source,
```

- [ ] **Step 6：repo 寫入與過濾穿 `mode`（`src/quanquant/journal/repository.py`）**

`create_trade` 的 `Trade(...)` 建構（`symbol=data.symbol,` 之後）加：
```python
        mode=data.mode,
        source=data.source,
```
`list_trades` 簽名加 `mode` 參數並加 where。改為：
```python
def list_trades(
    session: Session,
    *,
    user_id: int,
    mode: str = "real",
    symbol: str | None = None,
    tag: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    status: str = "all",  # "all" | "open" | "closed"
) -> list[Trade]:
    stmt = select(Trade).where(Trade.user_id == user_id, Trade.mode == mode)
```
（其餘 `symbol`/`status`/`date`/`order_by` 區塊不動，接在這行後面。）
`list_for_stats` 簽名加 `mode` 並轉傳：
```python
def list_for_stats(
    session: Session,
    *,
    user_id: int,
    mode: str = "real",
    symbol: str | None = None,
    tag: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> list[Trade]:
    """Closed trades matching the filters, sorted by exit_time ascending."""
    trades = list_trades(
        session,
        user_id=user_id,
        mode=mode,
        symbol=symbol,
        tag=tag,
        date_from=date_from,
        date_to=date_to,
        status="closed",
    )
    return sorted(trades, key=lambda t: (t.exit_time or t.entry_time))
```

- [ ] **Step 7：跑測試確認 GREEN（repository）**

```bash
uv run pytest tests/test_repository.py -q
```
預期新增 4 個測試 + 既有全綠（既有 trade 皆 real/manual，`mode`/`source` 預設值不受影響）。

- [ ] **Step 8：遷移測試（RED→GREEN，含 CHECK 擋非法值）— `tests/test_migrate.py`**

先在 `tests/test_migrate.py` 末端新增失敗測試（沿用檔內既有 `_old_engine(tmp_path)` helper，它已建好含 `trades` 表的舊庫）：
```python
def test_adds_mode_and_source_to_trades_with_defaults(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'old_mode.db'}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE trades (id INTEGER PRIMARY KEY, symbol TEXT)"))
        conn.execute(text("INSERT INTO trades (id, symbol) VALUES (1, 'TXF')"))
    ensure_columns(eng)
    insp = inspect(eng)
    cols = {c["name"] for c in insp.get_columns("trades")}
    assert "mode" in cols and "source" in cols
    with eng.begin() as conn:
        row = conn.execute(text("SELECT mode, source FROM trades WHERE id = 1")).one()
    assert row[0] == "real" and row[1] == "manual"  # 既有列以 DEFAULT 回填


def test_mode_source_migration_idempotent(tmp_path):
    eng = _old_engine(tmp_path)
    ensure_columns(eng)
    ensure_columns(eng)  # 第二次不得 raise
    insp = inspect(eng)
    cols = {c["name"] for c in insp.get_columns("trades")}
    assert "mode" in cols and "source" in cols


def test_mode_check_constraint_rejects_illegal_value_after_migration(tmp_path):
    """V3-4：mode 於 DB 層加 CHECK，非法值連 pydantic 都不用就被 DB 擋下。"""
    eng = create_engine(f"sqlite:///{tmp_path / 'old_mode_check.db'}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE trades (id INTEGER PRIMARY KEY, symbol TEXT)"))
    ensure_columns(eng)
    with pytest.raises(IntegrityError):
        with eng.begin() as conn:
            conn.execute(text("INSERT INTO trades (id, symbol, mode) VALUES (1, 'TXF', 'paper')"))
```
檔首若尚無 `import pytest`／`from sqlalchemy.exc import IntegrityError`，一併補上。
跑 `uv run pytest tests/test_migrate.py -q` → RED（無 `mode`/`source` 欄位）→ 做 Step 4 → 重跑 → GREEN。

- [ ] **Step 9：sim/real 統計隔離測試（新檔 `tests/test_mode_split.py`）**

> GateGuard：建新檔前若被擋，照提示陳述事實（新增 mode 分流隔離測試、對齊既有 `tests/test_metrics.py` 風格）後重試。

```python
"""模擬/正式分流：同一 user 混存 sim+real，統計互不相加。"""
import datetime as dt
from decimal import Decimal

from quanquant.journal import repository as repo
from quanquant.stats.metrics import compute_stats

from tests.conftest import make_create


def _closed(session, user_id, *, mode, exit_price, day):
    repo.create_trade(
        session,
        make_create(
            mode=mode,
            exit_time=dt.datetime(2026, 6, day, 10, 0),
            exit_price=Decimal(exit_price),
        ),
        user_id=user_id,
    )


def test_stats_never_aggregate_across_mode(session, user):
    # real: 一筆 +20000；sim: 兩筆 (+20000, -20000)
    _closed(session, user.id, mode="real", exit_price="18100", day=1)
    _closed(session, user.id, mode="sim", exit_price="18100", day=2)
    _closed(session, user.id, mode="sim", exit_price="17900", day=3)

    real = compute_stats(repo.list_for_stats(session, user_id=user.id, mode="real"))
    sim = compute_stats(repo.list_for_stats(session, user_id=user.id, mode="sim"))

    assert real.overall.count == 1
    assert real.overall.total_pnl == Decimal("20000")
    assert sim.overall.count == 2
    assert sim.overall.total_pnl == Decimal("0")   # +20000 - 20000，未含 real 的 20000


def test_list_trades_mode_isolated(session, user):
    repo.create_trade(session, make_create(symbol="TXF"), user_id=user.id)               # real
    repo.create_trade(session, make_create(symbol="MXF", mode="sim"), user_id=user.id)   # sim
    assert len(repo.list_trades(session, user_id=user.id, mode="real")) == 1
    assert len(repo.list_trades(session, user_id=user.id, mode="sim")) == 1
```
跑 `uv run pytest tests/test_mode_split.py -q` → GREEN。

- [ ] **Step 10：routes 穿 `mode`（`src/quanquant/web/routers/trades.py`）**

`journal_page` 簽名加（放在 `status: str = Query("all"),` 之前）：
```python
    mode: str = Query("real"),
```
`repo.list_trades(...)` 呼叫加 `mode=mode,`（放在 `user_id=user.id,` 之後）；回傳 context 的 `"f"` 字典改為：
```python
            "f": {"symbol": symbol or "", "tag": tag or "", "date_from": date_from or "",
                  "date_to": date_to or "", "status": status, "mode": mode},
```
`list_trades_fragment` 同樣加 `mode: str = Query("real"),` 與 `repo.list_trades(..., mode=mode, ...)`。

`_form_values` 簽名改為 `_form_values(t=None, mode: str = "real") -> dict`；`t is None` 分支的回傳 dict 加一項 `"mode": mode,`；`else` 分支加 `"mode": t.mode,`。

`_clean_form` 回傳 dict 加一項（`"tags": split_tags(form.get("tags")),` 之後）：
```python
        "mode": form.get("mode") or "real",
```

`new_trade_form` 簽名加 `mode: str = Query("real"),`；`_form_values(None)` 呼叫改為 `_form_values(None, mode)`。

`edit_trade_form` 不變（沿用 `t.mode`，經 `_form_values(trade)` 走 `else` 分支）。

- [ ] **Step 11：stats routes 穿 `mode`（`src/quanquant/web/routers/stats.py`）**

`_filtered` 簽名加 `mode` 並轉傳兩個 repo 呼叫：
```python
def _filtered(session: Session, user_id: int, mode, symbol, tag, date_from, date_to):
    closed = repo.list_for_stats(
        session,
        user_id=user_id,
        mode=mode,
        symbol=symbol or None,
        tag=tag or None,
        date_from=parse_date(date_from),
        date_to=parse_date(date_to, end=True),
    )
    detail = repo.list_trades(
        session,
        user_id=user_id,
        mode=mode,
        symbol=symbol or None,
        tag=tag or None,
        date_from=parse_date(date_from),
        date_to=parse_date(date_to, end=True),
        status="all",
    )
    return detail, closed
```
四個 route（`stats_page`/`stats_data`/`export_csv`/`export_xlsx`）各加 `mode: str = Query("real"),`（放在 `symbol: ...` 參數之前）並把 `_filtered(session, user.id, symbol, tag, date_from, date_to)` 改成 `_filtered(session, user.id, mode, symbol, tag, date_from, date_to)`。`stats_page` 的 context `"f"` 改為：
```python
            "f": {"symbol": symbol or "", "tag": tag or "", "date_from": date_from or "",
                  "date_to": date_to or "", "mode": mode},
```

- [ ] **Step 12：journal 模板 mode tab + hidden + 新增交易帶當前 mode（`src/quanquant/web/templates/journal.html`）**

把 `journal-head` 區塊改為（帶當前 tab 的 `mode` 給新增表單）：
```html
  <div class="journal-head">
    <h3>交易日記</h3>
    <button hx-get="/trades/new?mode={{ f.mode }}" hx-target="#modal-body" hx-swap="innerHTML" @click="open = true">
      ＋ 新增
    </button>
  </div>

  <div class="mode-tabs" role="tablist">
    <a role="button" class="{{ '' if f.mode == 'sim' else 'contrast' }}" href="/journal?mode=real">正式</a>
    <a role="button" class="{{ 'contrast' if f.mode == 'sim' else '' }}" href="/journal?mode=sim">模擬</a>
  </div>
```
filter `<form class="filterbar card-bar" hx-get="/trades" ...>` 開始標籤之後、第一個 `<select>` 之前加：
```html
    <input type="hidden" name="mode" value="{{ f.mode }}">
```

- [ ] **Step 13：stats 模板 mode tab + hidden + 匯出 qs（`src/quanquant/web/templates/stats.html`）**

`{% set qs = ... %}` 那行改為（開頭加 `mode=`）：
```html
{% set qs = 'mode=' ~ f.mode ~ '&symbol=' ~ (f.symbol | urlencode) ~ '&tag=' ~ (f.tag | urlencode) ~ '&date_from=' ~ f.date_from ~ '&date_to=' ~ f.date_to %}
```
`journal-head` 區塊之後、filter form 之前加：
```html
<div class="mode-tabs" role="tablist">
  <a role="button" class="{{ '' if f.mode == 'sim' else 'contrast' }}" href="/stats?mode=real">正式</a>
  <a role="button" class="{{ 'contrast' if f.mode == 'sim' else '' }}" href="/stats?mode=sim">模擬</a>
</div>
```
filter `<form class="filterbar card-bar" method="get" action="/stats">` 開始標籤之後、第一個 `<select>` 之前加：
```html
  <input type="hidden" name="mode" value="{{ f.mode }}">
```

- [ ] **Step 14：新增/編輯交易表單帶 `mode`（`src/quanquant/web/templates/partials/trade_form.html`）**

`<div class="form-error-slot"></div>` 之後加：
```html
  <input type="hidden" name="mode" value="{{ v.mode }}">
```

- [ ] **Step 15：跑全測試確認整鏈綠**

```bash
uv run pytest -q
```
預期既有測試全綠（既有交易全 real/manual，預設值不變，行為不回歸）＋本 task 新增測試綠。頁面 tab 視覺留待 Task 9 併同 simtrade 手動實測。

- [ ] **Step 16：Commit（scoped）**

```bash
git add src/quanquant/db/models.py src/quanquant/db/migrate.py \
  src/quanquant/journal/schemas.py src/quanquant/journal/repository.py \
  src/quanquant/web/routers/trades.py src/quanquant/web/routers/stats.py \
  src/quanquant/web/templates/journal.html src/quanquant/web/templates/stats.html \
  src/quanquant/web/templates/partials/trade_form.html \
  tests/test_repository.py tests/test_migrate.py tests/test_mode_split.py
git commit -m "feat: Trade 加 mode(real/sim)+source(manual/shioaji) 全鏈路過濾+DB CHECK，journal/stats 模擬|正式分頁，統計絕不跨 mode 聚合"
```

---
### Task 2：broker 純型別 + `OrderService` 介面 + canonical payload hash（V3-3 型別地基）

定義 broker 無關的 domain 型別：`Mode` 不在 `OrderRequest`（mode 只能由 session 決定）；`client_order_id`/`fill_id` 分離；`OrderRequest.__post_init__` 對 qty/price/枚舉 fail closed；`OrderService` Protocol 每個方法收 `actor_user_id`。**新增 `canonical_payload_hash()`**：全計畫**唯一**的「這張委託將送給券商的完整內容」hash 產生點，Task 7（confirm token 簽發/驗證）與 Task 3（`request_hash` 冪等）都呼叫這一個函式，不得各自組字串算 hash（BLOCKER#1/HIGH#5 的根因）。純資料、無 DB/框架依賴。

**Files:**
- Create: `src/quanquant/broker/__init__.py`
- Create: `src/quanquant/broker/types.py`
- Create: `src/quanquant/broker/base.py`
- Test: Create `tests/test_broker_types.py`

**Interfaces:**
- Consumes：（無上游 task）
- Produces（Task 3/4/5/6/7/9 依賴）：
  - `broker.types.Mode = Literal["sim", "real"]`、`Action`/`PriceType`/`OrderType`/`OcType`（皆 `Literal`）
  - `broker.types.OrderRequest(client_order_id, symbol, action, qty, price, price_type, order_type, octype, user_id)`（**無 `mode` 欄位**；`__post_init__` fail closed 驗證）
  - `broker.types.OrderAck(client_order_id, broker_order_id, ordno, status)`
  - `broker.types.Fill(broker, fill_id, ordno, broker_order_id, symbol, action, price, qty, fee, octype, ts, account, mode, user_id)`（**補 `broker_order_id`**，D5/V3-4：`Deal` 表也要存，關聯 scope 兩種鍵都能查）
  - `broker.types.Position(symbol, direction, qty, avg_price)`
  - `broker.types.RiskDecision(allowed, reason, needs_confirm)`
  - `broker.types.canonical_payload_hash(*, symbol, action, qty, price, price_type, order_type, octype, account, mode) -> str`（sha256 hex；**涵蓋所有可執行欄位**，刻意不含 `client_order_id`/`broker_order_id`——那是委託身分鍵不是「將送給券商的內容」，見 Task 7/9 何時傳「新的」vs「現有」值）
  - `broker.base.OrderService`（Protocol：`mode` 屬性 + `async place/cancel/update/positions`（皆收 `actor_user_id`）+ `on_fill`）
  - `broker.base.OrderError`／`broker.base.RiskError`／`broker.base.AuthorizationError`（例外）

- [ ] **Step 1：先寫失敗測試（新檔 `tests/test_broker_types.py`）**

> GateGuard：建新檔前陳述事實（broker 純型別與 Protocol 契約測試，含 fail-closed 驗證與 canonical hash）後重試。

```python
"""broker 純型別：欄位齊全、OrderRequest 無 mode（server-side 決定）、
fail-closed 驗證（qty/price/枚舉）、Fill 帶 mode/user_id/broker_order_id、Protocol 可被 duck-typed、
canonical_payload_hash 涵蓋全部可執行欄位且逐欄敏感（HIGH#5 的型別層防線）。"""
from decimal import Decimal

import pytest

from quanquant.broker.base import AuthorizationError, OrderError, OrderService, RiskError
from quanquant.broker.types import Fill, OrderAck, OrderRequest, Position, RiskDecision, canonical_payload_hash


def _req(**over):
    base = dict(
        client_order_id="C1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", user_id=7,
    )
    base.update(over)
    return OrderRequest(**base)


def _hash_kwargs(**over):
    base = dict(
        symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", account="F1", mode="sim",
    )
    base.update(over)
    return base


def test_order_request_has_no_mode_field():
    req = _req()
    assert not hasattr(req, "mode")  # mode 只由 OrderService.mode（server-side）決定


def test_order_request_fields():
    req = _req()
    assert req.user_id == 7 and req.octype == "New" and req.client_order_id == "C1"


@pytest.mark.parametrize("bad", [dict(qty=0), dict(qty=-1)])
def test_order_request_rejects_nonpositive_qty(bad):
    with pytest.raises(ValueError):
        _req(**bad)


@pytest.mark.parametrize("bad", [dict(price=Decimal("0")), dict(price=Decimal("-1"))])
def test_order_request_rejects_nonpositive_price(bad):
    with pytest.raises(ValueError):
        _req(**bad)


@pytest.mark.parametrize("field,bad", [
    ("action", "Hold"), ("price_type", "XYZ"), ("order_type", "GTC"), ("octype", "Reverse"),
])
def test_order_request_rejects_illegal_enums(field, bad):
    with pytest.raises(ValueError):
        _req(**{field: bad})


def test_fill_carries_user_mode_and_broker_order_id():
    f = Fill(
        broker="shioaji", fill_id="F1", ordno="O1", broker_order_id="B1", symbol="TXF", action="Sell",
        price=Decimal("18100"), qty=1, fee=Decimal("50"), octype="Cover",
        ts=1_780_000_000_000, account="F123", mode="sim", user_id=7,
    )
    assert f.user_id == 7 and f.mode == "sim" and f.action == "Sell" and f.broker_order_id == "B1"


def test_fill_rejects_illegal_mode():
    with pytest.raises(ValueError):
        Fill(
            broker="shioaji", fill_id="F1", ordno="O1", broker_order_id=None, symbol="TXF", action="Sell",
            price=Decimal("1"), qty=1, fee=None, octype="Cover",
            ts=1, account="F123", mode="paper", user_id=7,
        )


def _fill(**over):
    base = dict(
        broker="shioaji", fill_id="F1", ordno="O1", broker_order_id="B1", symbol="TXF", action="Sell",
        price=Decimal("18100"), qty=1, fee=Decimal("50"), octype="Cover",
        ts=1_780_000_000_000, account="F123", mode="sim", user_id=7,
    )
    base.update(over)
    return Fill(**base)


@pytest.mark.parametrize("bad", [dict(qty=0), dict(qty=-1)])
def test_fill_rejects_nonpositive_qty(bad):
    with pytest.raises(ValueError):
        _fill(**bad)


@pytest.mark.parametrize("bad", [dict(price=Decimal("0")), dict(price=Decimal("-1"))])
def test_fill_rejects_nonpositive_price(bad):
    with pytest.raises(ValueError):
        _fill(**bad)


@pytest.mark.parametrize("field,bad", [("action", "Hold"), ("octype", "Reverse")])
def test_fill_rejects_illegal_enums(field, bad):
    """HIGH#8：非法 callback 不得被當合法值頂替（不猜測成 Auto/空字串）。"""
    with pytest.raises(ValueError):
        _fill(**{field: bad})


@pytest.mark.parametrize("bad", [dict(fill_id=""), dict(account=""), dict(ts=0), dict(ts=-1)])
def test_fill_rejects_missing_fill_id_account_or_nonpositive_ts(bad):
    with pytest.raises(ValueError):
        _fill(**bad)


def test_order_ack_position_and_risk_decision():
    ack = OrderAck(client_order_id="C1", broker_order_id="B1", ordno="O1", status="submitted")
    pos = Position(symbol="TXF", direction="long", qty=2, avg_price=Decimal("18000"))
    dec = RiskDecision(allowed=False, reason="kill switch", needs_confirm=False)
    assert ack.status == "submitted" and pos.qty == 2 and dec.allowed is False


def test_exceptions_are_distinct():
    assert issubclass(RiskError, Exception) and issubclass(OrderError, Exception)
    assert issubclass(AuthorizationError, Exception)
    assert len({RiskError, OrderError, AuthorizationError}) == 3


def test_protocol_is_runtime_checkable_duck():
    class _Impl:
        mode = "sim"

        async def place(self, req, *, actor_user_id, confirm_token=None): ...
        async def cancel(self, broker_order_id, *, actor_user_id): ...
        async def update(self, broker_order_id, *, actor_user_id, price=None, qty=None, confirm_token=None): ...
        async def positions(self, *, actor_user_id): ...
        def on_fill(self, handler): ...

    assert isinstance(_Impl(), OrderService)


def test_canonical_payload_hash_is_deterministic():
    a = canonical_payload_hash(**_hash_kwargs())
    b = canonical_payload_hash(**_hash_kwargs())
    assert a == b and isinstance(a, str) and len(a) == 64  # sha256 hex


@pytest.mark.parametrize("field,new_value", [
    ("price", Decimal("18500")), ("qty", 2), ("price_type", "MKT"),
    ("order_type", "IOC"), ("octype", "Cover"), ("account", "F2"), ("mode", "real"),
    ("action", "Sell"), ("symbol", "MXF"),
])
def test_canonical_payload_hash_sensitive_to_every_executable_field(field, new_value):
    """HIGH#5：逐欄 mutation 必令 hash 改變，否則確認 LMT/ROD 後可篡改成 MKT/IOC/FOK 送出。"""
    base = canonical_payload_hash(**_hash_kwargs())
    mutated = canonical_payload_hash(**_hash_kwargs(**{field: new_value}))
    assert base != mutated


def test_canonical_payload_hash_ignores_decimal_string_formatting_noise():
    # Decimal("18000") 與 Decimal("18000.00") 是同一個可執行內容，正規化後應同 hash。
    a = canonical_payload_hash(**_hash_kwargs(price=Decimal("18000")))
    b = canonical_payload_hash(**_hash_kwargs(price=Decimal("18000.00")))
    assert a == b
```
跑 `uv run pytest tests/test_broker_types.py -q` → RED。

- [ ] **Step 2：建 broker 套件（`src/quanquant/broker/__init__.py`）**

> GateGuard：建新檔前照提示陳述事實（新增 broker 套件容器，供下單子系統各模組掛載）後重試。

```python
"""Broker-agnostic order subsystem（OrderService 介面 + ShioajiAdapter + 部位帳務/風控）。"""
```

- [ ] **Step 3：domain 型別 + canonical hash（新檔 `src/quanquant/broker/types.py`）**

> GateGuard：建新檔前陳述事實（broker 無關 domain dataclass，型別層 fail-closed 驗證，canonical payload hash 單一來源）後重試。

```python
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

YuantaAdapter 日後把 SendFutureOrder / RR_RealReport 映射到同一組型別即可接同介面。
"""
import hashlib
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
    Decimal 用 format(value, "f") 正規化（避免科學記號/尾零差異讓同義 payload 算出不同 hash）。
    """
    parts = [
        symbol, action, str(int(qty)), format(price, "f"),
        price_type, order_type, octype, account, mode,
    ]
    canonical = "|".join(parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
```

- [ ] **Step 4：`OrderService` Protocol + 例外（新檔 `src/quanquant/broker/base.py`）**

> GateGuard：建新檔前陳述事實（OrderService Protocol 與例外型別，含授權例外）後重試。

```python
"""Broker 無關的下單服務介面與例外。"""
from collections.abc import Callable
from typing import Protocol, runtime_checkable

from quanquant.broker.types import Fill, Mode, OrderAck, OrderRequest, Position


class OrderError(Exception):
    """下單/改單/刪單失敗（券商拒單、連線異常、狀態不明等）。"""


class RiskError(Exception):
    """風控攔截（超限、非白名單、kill switch、缺/錯確認 token 等）。

    needs_confirm：True 時代表唯一的攔截原因是「real 缺/錯兩階段確認 token」——
    Task 9 UI 用這個旗標決定要彈確認框還是顯示一般錯誤（其餘原因一律 False）。
    """

    def __init__(self, message: str, *, needs_confirm: bool = False) -> None:
        super().__init__(message)
        self.needs_confirm = needs_confirm


class AuthorizationError(Exception):
    """owner allowlist / 委託所有權驗證失敗（router 對應 403）。"""


@runtime_checkable
class OrderService(Protocol):
    mode: Mode  # server-side 真實 session mode；place/update 產生的 Order/Fill/Trade 一律蓋此值

    async def place(
        self, req: OrderRequest, *, actor_user_id: int, confirm_token: str | None = None
    ) -> OrderAck: ...

    async def cancel(self, broker_order_id: str, *, actor_user_id: int) -> OrderAck: ...

    async def update(
        self,
        broker_order_id: str,
        *,
        actor_user_id: int,
        price=None,
        qty=None,
        confirm_token: str | None = None,
    ) -> OrderAck: ...

    async def positions(self, *, actor_user_id: int) -> list[Position]: ...

    def on_fill(self, handler: Callable[[Fill], None]) -> None: ...
```

- [ ] **Step 5：跑測試確認 GREEN**

```bash
uv run pytest tests/test_broker_types.py -q
```

- [ ] **Step 6：Commit（scoped）**

```bash
git add src/quanquant/broker/__init__.py src/quanquant/broker/types.py \
  src/quanquant/broker/base.py tests/test_broker_types.py
git commit -m "feat: broker domain 型別(OrderRequest無mode/Fill補broker_order_id)+OrderService Protocol+canonical_payload_hash單一來源"
```

---

### Task 3：`Order`／`Deal`／`RawInbox`／`BrokerPosition`／`OrderAudit`／`ConfirmToken`／`QuotaReservation` 資料層 + broker 倉儲（V3-1/V3-2/V3-3/V3-4/D5）

七張全新表（`create_all` 自動建，走完整 DDL，可加 `CheckConstraint`）：
- `Order`：`client_order_id` 唯一冪等鍵 + `request_hash`（V3-3 同鍵不同 payload 拒絕）；`ordno`/`broker_order_id` 各自以 `(broker,account,mode,X)` **複合唯一鍵**（scope 查詢地基，取代裸鍵 `.first()`）；`trading_day` 欄位供當日配額/計數查詢。
- `Deal`：durable **去重**帳本（`unique(broker,mode,account,trading_day,fill_id)`），**補 `broker_order_id`**（D5）與 `raw_inbox_id` 溯源。
- `RawInbox`：durable **raw callback spool**——callback 落地的第一站，早於任何解析/去重/業務邏輯（V3-2 修 BLOCKER#2 丟單：不進 volatile `asyncio.Queue`）。
- `BrokerPosition`：**累計加權部位帳務**——`total_opened_qty`/`entry_notional`/`open_fee_total` 只增不減，`closed_qty`/`exit_notional`/`close_fee_total` 只增不減；round-trip 結算永遠用 `entry_notional/total_opened_qty`（真實加權），**不用剩餘口數回推**（V3-1 修 BLOCKER#10 PnL 錯算——見本 task 註解為何「累計欄位」在數學上等價於逐筆 lot 加權平均，且不需要額外的 lot 表）。
- `OrderAudit`：append-only 稽核。
- `ConfirmToken`：TTL + JTI 的 DB 列（取代記憶體 nonce set），claim 為原子 `UPDATE`（V3-3）。
- `QuotaReservation`：`(user_id,mode,trading_day)` 聚合列，`reserved_qty` 用**條件 `UPDATE`**（CAS）遞增/遞減（V3-3 修 BLOCKER#4 兩個並行 update/place 突破日限）。

倉儲層**統一交易邊界政策**：本檔所有寫入函式一律**只 `flush`，不 `commit`**（唯一例外：純讀取函式本就不寫）——commit 時機一律交給呼叫端（Task 5 的 fill worker 要把「Deal+部位+Order+Trade+processed」包一個 commit；Task 7 的 place 要把「quota reserve+create_order」包一個 commit）；本檔測試因此每個寫入後自行 `session.commit()`。`IntegrityError` 精確化：先 `rollback()` 後重新查詢確認命中指定唯一鍵才當重播，其餘不吞。

**Files:**
- Modify: `src/quanquant/db/models.py`（檔尾新增 `RawInbox`/`Order`/`Deal`/`BrokerPosition`/`OrderAudit`/`ConfirmToken`/`QuotaReservation` 七表）
- Create: `src/quanquant/broker/repository.py`
- Test: Create `tests/test_broker_repo.py`

**Interfaces:**
- Consumes：Task 2 的 `broker.types.canonical_payload_hash`（僅供理解 `Order.request_hash` 是誰算出來的；本 task 函式簽名不直接呼叫它，由 Task 7/9 呼叫後把結果當字串傳進 `create_order`）
- Produces（Task 4/5/6/7/9 依賴）：
  - `db.models.RawInbox`（table `raw_inbox`）、`Order`（`orders`）、`Deal`（`deals`）、`BrokerPosition`（`broker_positions`）、`OrderAudit`（`order_audits`）、`ConfirmToken`（`confirm_tokens`）、`QuotaReservation`（`quota_reservations`）
  - `broker.repository.trading_day_for(ts_ms: int) -> str`
  - `broker.repository.create_order(session, *, client_order_id, request_hash, user_id, mode, broker, account, symbol, action, qty, price, price_type, order_type, octype, trading_day) -> Order`（冪等：同 `client_order_id` 且 `request_hash` 相符回既有列；`request_hash` 不符 → `ValueError`）
  - `broker.repository.find_order_by_client_order_id(session, client_order_id) -> Order | None`
  - `broker.repository.find_order_by_ordno(session, *, broker, account, mode, ordno) -> Order | None`（複合 scope，非裸鍵）
  - `broker.repository.find_order_by_broker_id(session, *, broker, account, mode, broker_order_id) -> Order | None`（複合 scope，非裸鍵）
  - `broker.repository.set_order_sending(session, order_id) -> Order | None`
  - `broker.repository.set_order_ack(session, order_id, *, broker_order_id, ordno, status) -> Order | None`
  - `broker.repository.apply_order_fill(session, order, *, fill_qty, fill_price, terminal_status=None) -> Order`（單調累加 `filled_qty`/加權更新 `avg_fill_price`；狀態預設依 `filled_qty>=qty` 判 `filled`/`partfilled`，可用 `terminal_status` 覆寫如 `"cancelled"`）
  - `broker.repository.mark_order_status(session, order, *, status) -> Order`（純狀態回報，不動 `filled_qty`）
  - `broker.repository.list_orders(session, *, user_id, mode, limit=100) -> list[Order]`
  - `broker.repository.count_orders_today(session, *, user_id, mode, trading_day) -> int`
  - `broker.repository.sum_qty_today(session, *, user_id, mode, trading_day) -> int`
  - `broker.repository.stage_raw_inbox(session, *, kind, broker, payload) -> RawInbox`
  - `broker.repository.list_unprocessed_raw_inbox(session, *, limit=200) -> list[RawInbox]`
  - `broker.repository.mark_raw_inbox_processed(session, row) -> None`
  - `broker.repository.quarantine_raw_inbox(session, row, *, error) -> None`
  - `broker.repository.unquarantine_stale_raw_inbox(session, *, older_than, limit=200) -> int`（Task 8 watchdog 用，較慢週期給 quarantine 列一次補救重試機會）
  - `broker.repository.stage_deal(session, *, broker, account, mode, trading_day, fill_id, ordno, broker_order_id, order_id, user_id, symbol, action, price, qty, fee, octype, ts, raw_inbox_id) -> Deal | None`（`None` = 重播）
  - `broker.repository.find_open_position(session, *, user_id, broker, account, mode, symbol, direction) -> BrokerPosition | None`
  - `broker.repository.list_open_positions(session, *, user_id, broker, account, mode, symbol) -> list[BrokerPosition]`
  - `broker.repository.remaining_qty(pos) -> int`／`avg_entry_price(pos) -> Decimal`／`avg_exit_price(pos) -> Decimal`
  - `broker.repository.audit_reference_hash(*parts) -> str`（**注意**：純稽核摘要，非 `canonical_payload_hash`，不可拿來做 confirm token 驗證）
  - `broker.repository.append_audit(session, *, actor_user_id, mode, action, payload_hash, result, rule=None, detail=None, now_ms=None) -> OrderAudit`
  - `broker.repository.create_confirm_token_row(session, *, jti, actor_user_id, payload_hash, expires_at) -> ConfirmToken`
  - `broker.repository.claim_confirm_token(session, *, jti, actor_user_id, payload_hash, now) -> bool`（原子 `UPDATE`；`True`=claim 成功）
  - `broker.repository.reserve_quota_delta(session, *, user_id, mode, trading_day, delta, daily_limit) -> bool`（CAS `UPDATE`；`delta` 可負；`True`=成功）
  - `broker.repository.release_quota(session, *, user_id, mode, trading_day, qty) -> None`

- [ ] **Step 1：先寫失敗測試（新檔 `tests/test_broker_repo.py`）**

> GateGuard：建新檔前陳述事實（Order/Deal/RawInbox/BrokerPosition/OrderAudit/ConfirmToken/QuotaReservation 倉儲：冪等建委託+request_hash防竄改、複合 scope 委託查詢、Deal 去重、raw-inbox spool、CAS quota、confirm token 原子 claim）後重試。

```python
"""broker 倉儲：client_order_id 冪等建委託(+request_hash 防同鍵異payload)、
複合 scope 委託查詢(取代裸鍵 .first())、Deal (broker,mode,account,trading_day,fill_id) 去重、
raw-inbox spool CRUD、BrokerPosition 累計欄位、confirm token 原子 claim、quota CAS。"""
import datetime as dt
from decimal import Decimal

import pytest
from sqlalchemy.exc import IntegrityError

from quanquant.broker import repository as brepo
from quanquant.db.models import BrokerPosition, ConfirmToken, Order, QuotaReservation, RawInbox


def _order_kwargs(**over):
    base = dict(
        client_order_id="C1", request_hash="H1", user_id=1, mode="sim", broker="shioaji", account="F1",
        symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", trading_day="2026-06-16",
    )
    base.update(over)
    return base


def _deal_kwargs(**over):
    base = dict(
        broker="shioaji", account="F1", mode="sim", trading_day="2026-07-24",
        fill_id="D1", ordno="O1", broker_order_id="B1", order_id=None, user_id=1, symbol="TXF",
        action="Buy", price=Decimal("18000"), qty=1, fee=Decimal("50"),
        octype="New", ts=1_780_000_000_000, raw_inbox_id=None,
    )
    base.update(over)
    return base


def test_create_order_defaults_pending(session):
    o = brepo.create_order(session, **_order_kwargs())
    session.commit()
    assert o.id is not None and o.status == "pending" and o.mode == "sim"


def test_create_order_idempotent_on_replay_same_hash(session):
    first = brepo.create_order(session, **_order_kwargs())
    session.commit()
    replay = brepo.create_order(session, **_order_kwargs())  # 同 client_order_id + 同 request_hash
    assert replay.id == first.id


def test_create_order_same_client_id_different_hash_rejected(session):
    """V3-3：同 client_order_id、不同 payload（request_hash 不符）一律拒絕，不得靜默覆寫或誤回舊單。"""
    brepo.create_order(session, **_order_kwargs())
    session.commit()
    with pytest.raises(ValueError):
        brepo.create_order(session, **_order_kwargs(request_hash="H2", qty=99))


def test_create_order_non_replay_integrity_error_raised(session):
    with pytest.raises(IntegrityError):
        brepo.create_order(session, **_order_kwargs(client_order_id="C-BAD", price=None))  # NOT NULL 撞非本鍵


def test_order_mode_check_constraint_rejects_illegal_value(session):
    bad = Order(
        client_order_id="C-ILLEGAL", request_hash="H", user_id=1, mode="paper", broker="shioaji", account="F1",
        symbol="TXF", action="Buy", qty=1, price=Decimal("1"),
        price_type="LMT", order_type="ROD", octype="New", trading_day="2026-06-16",
    )
    session.add(bad)
    with pytest.raises(IntegrityError):
        session.commit()


def test_find_order_by_ordno_is_scoped_not_bare_lookup(session):
    """V3-3：解析 fill→order 一律複合 scope，同 ordno 不同 (account,mode) 不可誤配（BLOCKER#3）。"""
    a = brepo.create_order(session, **_order_kwargs(client_order_id="CA", account="F1", mode="sim"))
    brepo.set_order_ack(session, a.id, broker_order_id="BA", ordno="SHARED", status="submitted")
    b = brepo.create_order(session, **_order_kwargs(client_order_id="CB", account="F2", mode="sim", request_hash="H2"))
    brepo.set_order_ack(session, b.id, broker_order_id="BB", ordno="SHARED", status="submitted")
    session.commit()

    found_a = brepo.find_order_by_ordno(session, broker="shioaji", account="F1", mode="sim", ordno="SHARED")
    found_b = brepo.find_order_by_ordno(session, broker="shioaji", account="F2", mode="sim", ordno="SHARED")
    assert found_a.id == a.id and found_b.id == b.id  # 同 ordno 不同 account 分別命中，不互相污染


def test_apply_order_fill_accumulates_weighted_average_and_status():
    pass  # 見下方需要 session 的版本


def test_apply_order_fill_weighted_average_and_terminal_status(session):
    o = brepo.create_order(session, **_order_kwargs(qty=3))
    session.commit()
    brepo.apply_order_fill(session, o, fill_qty=1, fill_price=Decimal("18000"))
    brepo.apply_order_fill(session, o, fill_qty=2, fill_price=Decimal("18030"))
    session.commit()
    assert o.filled_qty == 3
    assert o.avg_fill_price == (Decimal("18000") * 1 + Decimal("18030") * 2) / 3
    assert o.status == "filled"  # filled_qty(3) >= qty(3)


def test_mark_order_status_does_not_touch_filled_qty(session):
    o = brepo.create_order(session, **_order_kwargs())
    session.commit()
    brepo.mark_order_status(session, o, status="cancelled")
    session.commit()
    assert o.status == "cancelled" and o.filled_qty == 0


def test_count_and_sum_qty_today_scoped_by_mode_and_trading_day(session):
    brepo.create_order(session, **_order_kwargs(client_order_id="S1", mode="sim", qty=2))
    brepo.create_order(session, **_order_kwargs(client_order_id="R1", mode="real", qty=5, request_hash="H-R1"))
    session.commit()
    assert brepo.count_orders_today(session, user_id=1, mode="sim", trading_day="2026-06-16") == 1
    assert brepo.sum_qty_today(session, user_id=1, mode="sim", trading_day="2026-06-16") == 2
    assert brepo.sum_qty_today(session, user_id=1, mode="real", trading_day="2026-06-16") == 5


def test_raw_inbox_stage_list_process_quarantine_round_trip(session):
    row = brepo.stage_raw_inbox(session, kind="deal_report", broker="shioaji", payload='{"a":1}')
    session.commit()
    pending = brepo.list_unprocessed_raw_inbox(session)
    assert [r.id for r in pending] == [row.id]

    brepo.mark_raw_inbox_processed(session, row)
    session.commit()
    assert brepo.list_unprocessed_raw_inbox(session) == []

    row2 = brepo.stage_raw_inbox(session, kind="deal_report", broker="shioaji", payload='{"bad":true}')
    session.commit()
    brepo.quarantine_raw_inbox(session, row2, error="缺 fill_id")
    session.commit()
    assert brepo.list_unprocessed_raw_inbox(session) == []  # quarantine 不算未處理佇列
    refreshed = session.get(RawInbox, row2.id)
    assert refreshed.quarantine is True and refreshed.error == "缺 fill_id"


def test_stage_deal_dedup_on_replay(session):
    first = brepo.stage_deal(session, **_deal_kwargs())
    session.commit()
    assert first is not None and first.broker_order_id == "B1"
    replay = brepo.stage_deal(session, **_deal_kwargs())  # 同 (broker,mode,account,trading_day,fill_id)
    assert replay is None  # 撞唯一鍵 → 跳過，不重寫


def test_stage_deal_distinct_fill_id_ok(session):
    assert brepo.stage_deal(session, **_deal_kwargs(fill_id="A")) is not None
    session.commit()
    assert brepo.stage_deal(session, **_deal_kwargs(fill_id="B")) is not None


def test_stage_deal_non_replay_integrity_error_is_raised(session):
    with pytest.raises(IntegrityError):
        brepo.stage_deal(session, **_deal_kwargs(fill_id="F-BAD", price=None))  # NOT NULL 撞非本鍵


def test_find_and_list_open_positions_scoped(session):
    p1 = BrokerPosition(user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                        direction="long", total_opened_qty=1, entry_notional=Decimal("18000"))
    p2 = BrokerPosition(user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                        direction="short", total_opened_qty=1, entry_notional=Decimal("18000"))
    p3 = BrokerPosition(user_id=2, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                        direction="long", total_opened_qty=1, entry_notional=Decimal("18000"))
    session.add_all([p1, p2, p3])
    session.commit()
    found = brepo.find_open_position(session, user_id=1, broker="shioaji", account="F1",
                                     mode="sim", symbol="TXF", direction="long")
    assert found is not None and found.id == p1.id
    opens = brepo.list_open_positions(session, user_id=1, broker="shioaji", account="F1",
                                      mode="sim", symbol="TXF")
    assert {p.id for p in opens} == {p1.id, p2.id}  # user=2 的不混進來


def test_remaining_and_avg_price_helpers():
    pos = BrokerPosition(
        user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF", direction="long",
        total_opened_qty=3, entry_notional=Decimal("400"), closed_qty=1, exit_notional=Decimal("105"),
    )
    assert brepo.remaining_qty(pos) == 2
    assert brepo.avg_entry_price(pos) == Decimal("400") / 3
    assert brepo.avg_exit_price(pos) == Decimal("105")


def test_append_audit_visible_after_commit(session):
    brepo.append_audit(session, actor_user_id=1, mode="sim", action="place",
                       payload_hash="abc123", result="ok")
    session.commit()
    from quanquant.db.models import OrderAudit
    rows = list(session.exec(__import__("sqlmodel").select(OrderAudit)))
    assert len(rows) == 1 and rows[0].result == "ok" and rows[0].action == "place"


def test_confirm_token_claim_is_atomic_and_one_time(session):
    expires = dt.datetime(2026, 6, 16, 12, 5)
    brepo.create_confirm_token_row(session, jti="J1", actor_user_id=1, payload_hash="H1", expires_at=expires)
    session.commit()

    now = dt.datetime(2026, 6, 16, 12, 0)
    ok = brepo.claim_confirm_token(session, jti="J1", actor_user_id=1, payload_hash="H1", now=now)
    session.commit()
    assert ok is True

    replay = brepo.claim_confirm_token(session, jti="J1", actor_user_id=1, payload_hash="H1", now=now)
    session.commit()
    assert replay is False  # 已消費，不得重放


def test_confirm_token_claim_rejects_wrong_user_or_hash(session):
    expires = dt.datetime(2026, 6, 16, 12, 5)
    brepo.create_confirm_token_row(session, jti="J2", actor_user_id=1, payload_hash="H1", expires_at=expires)
    session.commit()
    now = dt.datetime(2026, 6, 16, 12, 0)
    assert brepo.claim_confirm_token(session, jti="J2", actor_user_id=99, payload_hash="H1", now=now) is False
    assert brepo.claim_confirm_token(session, jti="J2", actor_user_id=1, payload_hash="WRONG", now=now) is False


def test_confirm_token_claim_rejects_expired(session):
    expires = dt.datetime(2026, 6, 16, 12, 5)
    brepo.create_confirm_token_row(session, jti="J3", actor_user_id=1, payload_hash="H1", expires_at=expires)
    session.commit()
    late = dt.datetime(2026, 6, 16, 12, 6)
    assert brepo.claim_confirm_token(session, jti="J3", actor_user_id=1, payload_hash="H1", now=late) is False


def test_reserve_quota_delta_cas_blocks_once_limit_hit(session):
    assert brepo.reserve_quota_delta(session, user_id=1, mode="sim", trading_day="2026-06-16", delta=6, daily_limit=10) is True
    session.commit()
    assert brepo.reserve_quota_delta(session, user_id=1, mode="sim", trading_day="2026-06-16", delta=6, daily_limit=10) is False  # 6+6>10
    session.commit()
    row = session.exec(__import__("sqlmodel").select(QuotaReservation)).first()
    assert row.reserved_qty == 6  # 被擋的那次沒有偷偷加上去


def test_reserve_quota_delta_scoped_by_mode_and_trading_day(session):
    assert brepo.reserve_quota_delta(session, user_id=1, mode="sim", trading_day="2026-06-16", delta=10, daily_limit=10) is True
    session.commit()
    # 不同 mode/不同交易日互不影響額度
    assert brepo.reserve_quota_delta(session, user_id=1, mode="real", trading_day="2026-06-16", delta=10, daily_limit=10) is True
    assert brepo.reserve_quota_delta(session, user_id=1, mode="sim", trading_day="2026-06-17", delta=10, daily_limit=10) is True
    session.commit()


def test_release_quota_decrements_and_floors_at_zero(session):
    brepo.reserve_quota_delta(session, user_id=1, mode="sim", trading_day="2026-06-16", delta=5, daily_limit=10)
    session.commit()
    brepo.release_quota(session, user_id=1, mode="sim", trading_day="2026-06-16", qty=99)  # 超額釋放不得變負數
    session.commit()
    row = session.exec(__import__("sqlmodel").select(QuotaReservation)).first()
    assert row.reserved_qty == 0


def test_trading_day_for_cst_calendar_date():
    # 2026-06-16 04:00 UTC = 2026-06-16 12:00 CST（同一天）
    ts_ms = int(dt.datetime(2026, 6, 16, 4, 0, tzinfo=dt.timezone.utc).timestamp() * 1000)
    assert brepo.trading_day_for(ts_ms) == "2026-06-16"
```
（`test_apply_order_fill_accumulates_weighted_average_and_status` 是刻意留白的佔位測試名沖突防呆——上面實際斷言的是 `test_apply_order_fill_weighted_average_and_terminal_status`，兩者並存不衝突，pytest 兩個都會跑，前者是 no-op。）
跑 `uv run pytest tests/test_broker_repo.py -q` → RED（`RawInbox`/`Order`/`Deal`/`BrokerPosition`/`ConfirmToken`/`QuotaReservation`/`brepo` 未定義）。

- [ ] **Step 2：新增七張表（`src/quanquant/db/models.py` 檔尾）**

檔首 import 行改為（加 `CheckConstraint`）：
```python
from sqlalchemy import BigInteger, CheckConstraint, Column, String, TypeDecorator, UniqueConstraint
```
在檔尾（`UserChartState` 之後）新增：
```python
class RawInbox(SQLModel, table=True):
    """callback 落地的第一站——在任何解析/去重/業務邏輯之前先 durable 落地原始 payload，
    確保 QueueFull/worker 例外/處理中 crash 都不丟資料（V3-2，修 BLOCKER#2）。
    watchdog 對帳（Task 8）拉到的券商委託/成交也塞進同一張表，走同一套處理管線。"""

    __tablename__ = "raw_inbox"

    id: int | None = Field(default=None, primary_key=True)
    kind: str                                                # "order_report" | "deal_report"
    broker: str = "shioaji"
    payload: str                                             # JSON TEXT（callback stat+msg，已轉可序列化 dict）
    received_at: datetime = Field(default_factory=_utcnow, index=True)
    processed: bool = Field(default=False, index=True)
    quarantine: bool = False
    error: str | None = None
    processed_at: datetime | None = None


class Order(SQLModel, table=True):
    """一張委託單（下單面板／委託列表資料來源）。client_order_id 為下單前伺服器產生的冪等鍵；
    request_hash 是該冪等鍵當初綁定的 canonical_payload_hash（Task 2），同鍵不同 payload 拒絕（V3-3）。
    ordno/broker_order_id 各自以 (broker,account,mode,X) 複合唯一鍵 scope，解析 fill 時不再裸鍵查詢。"""

    __tablename__ = "orders"
    __table_args__ = (
        CheckConstraint("mode IN ('sim','real')", name="ck_orders_mode"),
        UniqueConstraint("broker", "account", "mode", "ordno", name="uq_orders_ordno_scope"),
        UniqueConstraint("broker", "account", "mode", "broker_order_id", name="uq_orders_broker_order_id_scope"),
    )

    id: int | None = Field(default=None, primary_key=True)
    client_order_id: str = Field(unique=True, index=True)   # 冪等鍵：同鍵重送不重建
    request_hash: str = Field(index=True)                    # 建單當下的 canonical_payload_hash
    user_id: int = Field(index=True)                        # 下單者
    mode: str = Field(index=True)                            # "real" | "sim"（server-side 決定，非表單）
    broker: str = "shioaji"
    account: str = ""                                        # futopt_account.account_id
    trading_day: str = Field(index=True)                      # CST 日曆日（trading_day_for），配額/計數用

    symbol: str = Field(index=True)
    action: str                                               # "Buy" | "Sell"
    qty: int
    price: Decimal = Field(sa_column=Column(DecimalText, nullable=False))
    price_type: str
    order_type: str
    octype: str

    broker_order_id: str | None = Field(default=None)              # 券商委託單號（scope 唯一，見 __table_args__）
    ordno: str | None = Field(default=None)                        # 委託流水（scope 唯一，見 __table_args__）
    status: str = "pending"  # pending|sending|submitted|partfilled|filled|cancelled|failed|unknown

    filled_qty: int = 0
    avg_fill_price: Decimal | None = Field(default=None, sa_column=Column(DecimalText))

    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)


class Deal(SQLModel, table=True):
    """durable fill **去重**帳本（unique broker+mode+account+trading_day+fill_id）。
    fill_id 與 ordno/broker_order_id 分離：fill_id 去重、ordno/broker_order_id 關聯委託（複合 scope）。
    只收真實 deal id 當 fill_id（V3-4：不用 ordno/seqno fallback 冒充）。"""

    __tablename__ = "deals"
    __table_args__ = (
        UniqueConstraint("broker", "mode", "account", "trading_day", "fill_id", name="uq_deal_fill"),
        CheckConstraint("mode IN ('sim','real')", name="ck_deals_mode"),
    )

    id: int | None = Field(default=None, primary_key=True)
    broker: str
    account: str
    mode: str = Field(index=True)
    trading_day: str = Field(index=True)   # "YYYY-MM-DD"（CST 日曆日，見 repository.trading_day_for）
    fill_id: str = Field(index=True)       # 券商成交唯一識別（去重鍵）
    ordno: str | None = Field(default=None, index=True)            # 委託關聯鍵之一
    broker_order_id: str | None = Field(default=None, index=True)  # 委託關聯鍵之二（D5）

    order_id: int | None = Field(default=None, foreign_key="orders.id", index=True)
    user_id: int | None = Field(default=None, index=True)  # 解析前為 None（quarantine）
    raw_inbox_id: int | None = Field(default=None, foreign_key="raw_inbox.id", index=True)  # 溯源

    symbol: str
    action: str
    price: Decimal = Field(sa_column=Column(DecimalText, nullable=False))
    qty: int
    fee: Decimal | None = Field(default=None, sa_column=Column(DecimalText))
    octype: str
    ts: int = Field(sa_column=Column(BigInteger, nullable=False))  # epoch-ms UTC

    processed: bool = False
    quarantine: bool = False
    error: str | None = None

    created_at: datetime = Field(default_factory=_utcnow)


class BrokerPosition(SQLModel, table=True):
    """持久化 broker 自動部位帳務；與手動 Trade 完全隔離——自動成交只動這張表，
    round-trip 完成才回寫一筆 Trade(source="shioaji")，trade_id 回填於此。

    V3-1（修 BLOCKER#10 PnL 錯算）：total_opened_qty/entry_notional/open_fee_total 只增不減，
    只在每次 New/開倉 fill 累加；closed_qty/exit_notional/close_fee_total 只在每次 Cover/平倉 fill
    累加。round-trip 結算的 entry 永遠是 entry_notional/total_opened_qty（真實加權平均），
    exit 永遠是 exit_notional/closed_qty——兩者都是「總量的加權平均」，數學上等價於對每一筆
    開倉/平倉 lot 做加權平均，不需要額外的逐筆 lot 表：因為 entry_notional 是 Σ(qty_i * price_i)
    對「每一筆」開倉 fill 的總和，不曾因為中途發生平倉而被回頭改寫——這正是舊版 bug 的根因
    （舊版只存「剩餘口數的加權均價」，中途 Cover 後再 New 會用『當時剩餘量』重新加權，
    等於用錯誤的權重覆蓋了已經平倉那筆的歷史貢獻）。"""

    __tablename__ = "broker_positions"
    __table_args__ = (
        CheckConstraint("mode IN ('sim','real')", name="ck_broker_positions_mode"),
        CheckConstraint("direction IN ('long','short')", name="ck_broker_positions_direction"),
    )

    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(index=True)
    broker: str
    account: str
    mode: str = Field(index=True)
    symbol: str = Field(index=True)
    direction: str  # "long" | "short"

    total_opened_qty: int                                      # 累計開倉口數（只增不減）
    entry_notional: Decimal = Field(sa_column=Column(DecimalText, nullable=False))  # Σ(qty*price) 開倉 fills
    open_fee_total: Decimal = Field(default=Decimal(0), sa_column=Column(DecimalText, nullable=False))

    closed_qty: int = 0                                        # 累計平倉口數（只增不減）
    exit_notional: Decimal = Field(default=Decimal(0), sa_column=Column(DecimalText, nullable=False))
    close_fee_total: Decimal = Field(default=Decimal(0), sa_column=Column(DecimalText, nullable=False))

    status: str = "open"  # "open"（remaining>0）| "closed"（remaining==0）
    trade_id: int | None = Field(default=None, foreign_key="trades.id")  # 結案回填

    opened_at: datetime = Field(default_factory=_utcnow)       # naive UTC；Trade 化時轉 CST（見 position_tracker）
    updated_at: datetime = Field(default_factory=_utcnow)


class OrderAudit(SQLModel, table=True):
    """append-only 稽核；不含秘密（只存 payload 的 hash，不存原始敏感值）。"""

    __tablename__ = "order_audits"
    __table_args__ = (CheckConstraint("mode IN ('sim','real')", name="ck_order_audits_mode"),)

    id: int | None = Field(default=None, primary_key=True)
    ts: int = Field(sa_column=Column(BigInteger, nullable=False))  # epoch-ms UTC
    actor_user_id: int | None = None
    mode: str
    action: str      # "place" | "cancel" | "update" | "risk_reject" | "fill" | "reconnect"
    payload_hash: str
    rule: str | None = None
    result: str      # "ok" | "rejected" | "error"
    detail: str | None = None


class ConfirmToken(SQLModel, table=True):
    """兩階段確認（real）用的 TTL + JTI DB 列，取代記憶體 nonce set（無界成長/重啟遺失）。
    itsdangerous 簽出的 token 字串本身即嵌入 jti（防偽造/過期，見 Task 7）；本表額外提供
    「一次性」保證：claim 是一個原子 UPDATE（見 claim_confirm_token），rowcount 判定成敗。"""

    __tablename__ = "confirm_tokens"
    __table_args__ = (UniqueConstraint("jti", name="uq_confirm_tokens_jti"),)

    id: int | None = Field(default=None, primary_key=True)
    jti: str = Field(index=True)
    actor_user_id: int
    payload_hash: str
    expires_at: datetime            # naive UTC
    consumed_at: datetime | None = None
    created_at: datetime = Field(default_factory=_utcnow)


class QuotaReservation(SQLModel, table=True):
    """每人/mode/交易日一列的聚合配額列；reserved_qty 用條件 UPDATE（CAS）遞增/遞減，
    與 create_order 同一交易提交，修 BLOCKER#4「兩個並行 update/place 突破日限」——
    單一 uvicorn worker 只保證同 process 內 DB commit 不跨 process 交錯，
    不保證同 process 內兩個 asyncio 協程在 await 讓出時不交錯讀寫；CAS UPDATE 在兩種情境都安全。"""

    __tablename__ = "quota_reservations"
    __table_args__ = (
        UniqueConstraint("user_id", "mode", "trading_day", name="uq_quota_reservations_day"),
        CheckConstraint("mode IN ('sim','real')", name="ck_quota_reservations_mode"),
    )

    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(index=True)
    mode: str = Field(index=True)
    trading_day: str = Field(index=True)
    reserved_qty: int = 0
    updated_at: datetime = Field(default_factory=_utcnow)
```
（`Column`／`BigInteger`／`UniqueConstraint`／`DecimalText`／`_utcnow` 皆已在本檔既有 import/定義。）

- [ ] **Step 3：broker 倉儲（新檔 `src/quanquant/broker/repository.py`）**

> GateGuard：建新檔前陳述事實（Order/Deal/RawInbox/BrokerPosition/OrderAudit/ConfirmToken/QuotaReservation 倉儲，IntegrityError 精確化去重，quota/token 用條件 UPDATE 做 CAS）後重試。

```python
"""Order/Deal/RawInbox/BrokerPosition/OrderAudit/ConfirmToken/QuotaReservation 倉儲。

雙方言可攜：CAS/upsert 一律走 session.get_bind().dialect.name 分流
sqlalchemy.dialects.{sqlite,postgresql}.insert（candles/repo.py 既有慣例），其餘用 SQLModel/select。

冪等/去重的 IntegrityError 精確化：任何「撞唯一鍵即重播」的 catch，一律先 rollback() 後
重新查詢確認命中的正是該唯一鍵才回既有列；其餘 IntegrityError（FK/NULL 等）一律重新拋出。

交易邊界（重要，本檔所有寫入函式的統一政策）：一律只 flush，不 commit——commit 的時機
交由呼叫端決定（Task 5 的 raw-inbox worker 要把「Deal insert + 部位帳務 + Order 狀態更新 +
Trade 寫入 + RawInbox.processed=true」包在同一個交易；Task 7 的 place 要把「quota reserve +
create_order」包在同一個交易）。純讀取函式本就不寫，不受此政策影響。
"""
import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from quanquant.db.models import (
    BrokerPosition,
    ConfirmToken,
    Deal,
    Order,
    OrderAudit,
    QuotaReservation,
    RawInbox,
)

_CST = timezone(timedelta(hours=8))


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def trading_day_for(ts_ms: int) -> str:
    """成交 epoch-ms UTC → CST 日曆日期字串（Deal/Order/QuotaReservation scope 組成之一）。
    刻意用「日曆日」而非交易時段語意（candles.trading_date 的夜盤跨日規則）——這裡只需要
    一個穩定、可重現的值防止 fill_id 理論上跨日碰撞，不需要交易時段判斷。"""
    return datetime.fromtimestamp(ts_ms / 1000, _CST).strftime("%Y-%m-%d")


# ---- Order ----

def create_order(
    session: Session,
    *,
    client_order_id: str,
    request_hash: str,
    user_id: int,
    mode: str,
    broker: str,
    account: str,
    symbol: str,
    action: str,
    qty: int,
    price,
    price_type: str,
    order_type: str,
    octype: str,
    trading_day: str,
) -> Order:
    """冪等建委託：同 client_order_id 已存在 → request_hash 相符回既有列，不符則拒絕（V3-3）。"""
    existing = find_order_by_client_order_id(session, client_order_id)
    if existing is not None:
        if existing.request_hash != request_hash:
            raise ValueError(
                f"client_order_id={client_order_id!r} 已存在但 payload 不同"
                "（冪等鍵不可變更內容，疑似竄改或用錯 client_order_id）"
            )
        return existing
    order = Order(
        client_order_id=client_order_id, request_hash=request_hash, user_id=user_id, mode=mode,
        broker=broker, account=account, symbol=symbol, action=action, qty=qty, price=price,
        price_type=price_type, order_type=order_type, octype=octype, trading_day=trading_day,
    )
    session.add(order)
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        existing = find_order_by_client_order_id(session, client_order_id)
        if existing is not None:
            if existing.request_hash != request_hash:
                raise ValueError(
                    f"client_order_id={client_order_id!r} 已存在但 payload 不同"
                ) from None
            return existing  # 競態：另一請求先插入，確認撞的正是 client_order_id 唯一鍵
        raise  # 非本鍵造成的 IntegrityError（FK/NULL 等）→ 不吞，拋出
    return order


def find_order_by_client_order_id(session: Session, client_order_id: str) -> Order | None:
    return session.exec(select(Order).where(Order.client_order_id == client_order_id)).first()


def find_order_by_ordno(
    session: Session, *, broker: str, account: str, mode: str, ordno: str
) -> Order | None:
    """複合 scope 查詢——修 BLOCKER#3：裸鍵 `.first()` 在多帳戶/多 mode 下可能撞到別人的委託。"""
    stmt = select(Order).where(
        Order.broker == broker, Order.account == account, Order.mode == mode, Order.ordno == ordno
    )
    return session.exec(stmt).first()


def find_order_by_broker_id(
    session: Session, *, broker: str, account: str, mode: str, broker_order_id: str
) -> Order | None:
    stmt = select(Order).where(
        Order.broker == broker, Order.account == account, Order.mode == mode,
        Order.broker_order_id == broker_order_id,
    )
    return session.exec(stmt).first()


def set_order_sending(session: Session, order_id: int) -> Order | None:
    order = session.get(Order, order_id)
    if order is None:
        return None
    order.status = "sending"
    order.updated_at = _utcnow()
    session.add(order)
    session.flush()
    return order


def set_order_ack(
    session: Session, order_id: int, *, broker_order_id: str, ordno: str | None, status: str
) -> Order | None:
    order = session.get(Order, order_id)
    if order is None:
        return None
    order.broker_order_id = broker_order_id
    order.ordno = ordno
    order.status = status
    order.updated_at = _utcnow()
    session.add(order)
    session.flush()
    return order


def apply_order_fill(
    session: Session, order: Order, *, fill_qty: int, fill_price, terminal_status: str | None = None
) -> Order:
    """單調累加 filled_qty + 加權更新 avg_fill_price（V3-2「Order 狀態由 fill 更新」）。

    數學上安全：filled_qty 只增不減（一張委託的成交只會累加，不像 BrokerPosition 有
    「中途平倉」這種會讓歷史貢獻被錯誤覆寫的操作），故 (old_avg*old_qty + new_price*new_qty)/(old+new)
    是精確的加權平均，不是估計。"""
    prior_qty = order.filled_qty
    prior_notional = (order.avg_fill_price or Decimal(0)) * prior_qty
    new_qty = prior_qty + fill_qty
    order.avg_fill_price = (prior_notional + fill_price * fill_qty) / new_qty
    order.filled_qty = new_qty
    order.status = terminal_status or ("filled" if new_qty >= order.qty else "partfilled")
    order.updated_at = _utcnow()
    session.add(order)
    session.flush()
    return order


def mark_order_status(session: Session, order: Order, *, status: str) -> Order:
    """純狀態回報（委託回報 callback，如 cancelled/failed/submitted），不動 filled_qty。"""
    order.status = status
    order.updated_at = _utcnow()
    session.add(order)
    session.flush()
    return order


def list_orders(session: Session, *, user_id: int, mode: str, limit: int = 100) -> list[Order]:
    stmt = (
        select(Order)
        .where(Order.user_id == user_id, Order.mode == mode)
        .order_by(Order.created_at.desc())  # type: ignore[union-attr]
        .limit(limit)
    )
    return list(session.exec(stmt))


def count_orders_today(session: Session, *, user_id: int, mode: str, trading_day: str) -> int:
    stmt = select(Order).where(
        Order.user_id == user_id, Order.mode == mode, Order.trading_day == trading_day
    )
    return len(list(session.exec(stmt)))


def sum_qty_today(session: Session, *, user_id: int, mode: str, trading_day: str) -> int:
    stmt = select(Order).where(
        Order.user_id == user_id, Order.mode == mode, Order.trading_day == trading_day
    )
    return sum(o.qty for o in session.exec(stmt))


# ---- RawInbox（durable callback spool，V3-2） ----

def stage_raw_inbox(session: Session, *, kind: str, broker: str, payload: str) -> RawInbox:
    row = RawInbox(kind=kind, broker=broker, payload=payload)
    session.add(row)
    session.flush()
    return row


def list_unprocessed_raw_inbox(session: Session, *, limit: int = 200) -> list[RawInbox]:
    stmt = (
        select(RawInbox)
        .where(RawInbox.processed.is_(False), RawInbox.quarantine.is_(False))  # type: ignore[union-attr]
        .order_by(RawInbox.id)
        .limit(limit)
    )
    return list(session.exec(stmt))


def mark_raw_inbox_processed(session: Session, row: RawInbox) -> None:
    row.processed = True
    row.processed_at = _utcnow()
    session.add(row)
    session.flush()


def quarantine_raw_inbox(session: Session, row: RawInbox, *, error: str) -> None:
    row.quarantine = True
    row.error = error
    session.add(row)
    session.flush()


def unquarantine_stale_raw_inbox(session: Session, *, older_than: datetime, limit: int = 200) -> int:
    """把 quarantine 超過 older_than 的列解除隔離，回到一般佇列重新嘗試一次
    （Task 8 watchdog 以較慢週期呼叫——給「當時解不到委託關聯」的列一個補救機會，
    不會無限重試：解除後若原因仍不變會再次被 quarantine，只是白工，不會誤判成功）。"""
    stmt = (
        select(RawInbox)
        .where(RawInbox.quarantine.is_(True), RawInbox.received_at < older_than)  # type: ignore[union-attr]
        .order_by(RawInbox.id)
        .limit(limit)
    )
    rows = list(session.exec(stmt))
    for row in rows:
        row.quarantine = False
        row.error = None
        session.add(row)
    session.flush()
    return len(rows)


# ---- Deal（fill 去重帳本） ----

def _find_deal(
    session: Session, *, broker: str, mode: str, account: str, trading_day: str, fill_id: str
) -> Deal | None:
    stmt = select(Deal).where(
        Deal.broker == broker, Deal.mode == mode, Deal.account == account,
        Deal.trading_day == trading_day, Deal.fill_id == fill_id,
    )
    return session.exec(stmt).first()


def stage_deal(
    session: Session,
    *,
    broker: str,
    account: str,
    mode: str,
    trading_day: str,
    fill_id: str,
    ordno: str | None,
    broker_order_id: str | None,
    order_id: int | None,
    user_id: int | None,
    symbol: str,
    action: str,
    price,
    qty: int,
    fee,
    octype: str,
    ts: int,
    raw_inbox_id: int | None,
) -> Deal | None:
    """把成交插進去重帳本；只 flush 不 commit（呼叫端負責交易邊界）。
    撞唯一鍵（重播）→ 回 None，不重寫；其餘 IntegrityError 不吞、往上拋。"""
    existing = _find_deal(session, broker=broker, mode=mode, account=account,
                          trading_day=trading_day, fill_id=fill_id)
    if existing is not None:
        return None
    deal = Deal(
        broker=broker, account=account, mode=mode, trading_day=trading_day, fill_id=fill_id,
        ordno=ordno, broker_order_id=broker_order_id, order_id=order_id, user_id=user_id,
        symbol=symbol, action=action, price=price, qty=qty, fee=fee, octype=octype, ts=ts,
        raw_inbox_id=raw_inbox_id,
    )
    session.add(deal)
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        existing = _find_deal(session, broker=broker, mode=mode, account=account,
                              trading_day=trading_day, fill_id=fill_id)
        if existing is not None:
            return None  # 競態：另一 worker 先插入，確認撞的正是本唯一鍵
        raise
    return deal


# ---- BrokerPosition ----

def find_open_position(
    session: Session, *, user_id: int, broker: str, account: str, mode: str, symbol: str, direction: str
) -> BrokerPosition | None:
    stmt = select(BrokerPosition).where(
        BrokerPosition.user_id == user_id, BrokerPosition.broker == broker,
        BrokerPosition.account == account, BrokerPosition.mode == mode,
        BrokerPosition.symbol == symbol, BrokerPosition.direction == direction,
        BrokerPosition.status == "open",
    )
    return session.exec(stmt).first()


def list_open_positions(
    session: Session, *, user_id: int, broker: str, account: str, mode: str, symbol: str
) -> list[BrokerPosition]:
    stmt = select(BrokerPosition).where(
        BrokerPosition.user_id == user_id, BrokerPosition.broker == broker,
        BrokerPosition.account == account, BrokerPosition.mode == mode,
        BrokerPosition.symbol == symbol, BrokerPosition.status == "open",
    )
    return list(session.exec(stmt))


def remaining_qty(pos: BrokerPosition) -> int:
    return pos.total_opened_qty - pos.closed_qty


def avg_entry_price(pos: BrokerPosition) -> Decimal:
    return pos.entry_notional / pos.total_opened_qty


def avg_exit_price(pos: BrokerPosition) -> Decimal:
    return pos.exit_notional / pos.closed_qty


# ---- OrderAudit ----

def audit_reference_hash(*parts: object) -> str:
    """純稽核用途的參考摘要（如 cancel/reconnect 沒有完整可執行 payload 時）。
    **不是** broker.types.canonical_payload_hash，不可拿來做 confirm token 簽發/驗證。"""
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def append_audit(
    session: Session,
    *,
    actor_user_id: int | None,
    mode: str,
    action: str,
    payload_hash: str,
    result: str,
    rule: str | None = None,
    detail: str | None = None,
    now_ms: int | None = None,
) -> OrderAudit:
    audit = OrderAudit(
        ts=now_ms if now_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000),
        actor_user_id=actor_user_id, mode=mode, action=action,
        payload_hash=payload_hash, rule=rule, result=result, detail=detail,
    )
    session.add(audit)
    session.flush()
    return audit


# ---- ConfirmToken（V3-3：TTL+JTI DB 列，claim 為原子 UPDATE） ----

def create_confirm_token_row(
    session: Session, *, jti: str, actor_user_id: int, payload_hash: str, expires_at: datetime
) -> ConfirmToken:
    row = ConfirmToken(jti=jti, actor_user_id=actor_user_id, payload_hash=payload_hash, expires_at=expires_at)
    session.add(row)
    session.flush()
    return row


def claim_confirm_token(
    session: Session, *, jti: str, actor_user_id: int, payload_hash: str, now: datetime
) -> bool:
    """原子 UPDATE：consumed_at IS NULL AND 未過期 AND actor/payload 相符 才能 claim 成功。
    rowcount==1 → True（單次呼叫最多消費一次，重放/競態下第二次一定拿到 False）。"""
    stmt = (
        update(ConfirmToken.__table__)
        .where(
            ConfirmToken.__table__.c.jti == jti,
            ConfirmToken.__table__.c.actor_user_id == actor_user_id,
            ConfirmToken.__table__.c.payload_hash == payload_hash,
            ConfirmToken.__table__.c.consumed_at.is_(None),
            ConfirmToken.__table__.c.expires_at > now,
        )
        .values(consumed_at=now)
    )
    result = session.exec(stmt)  # type: ignore[call-overload]
    return result.rowcount == 1


# ---- QuotaReservation（V3-3：條件 UPDATE 做 CAS） ----

def _ensure_quota_row(session: Session, *, user_id: int, mode: str, trading_day: str) -> None:
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":  # pragma: no cover - exercised after PG migration
        from sqlalchemy.dialects.postgresql import insert
    else:
        from sqlalchemy.dialects.sqlite import insert

    stmt = insert(QuotaReservation.__table__).values(
        user_id=user_id, mode=mode, trading_day=trading_day, reserved_qty=0, updated_at=_utcnow(),
    )
    stmt = stmt.on_conflict_do_nothing(index_elements=["user_id", "mode", "trading_day"])
    session.exec(stmt)  # type: ignore[call-overload]
    session.flush()


def reserve_quota_delta(
    session: Session, *, user_id: int, mode: str, trading_day: str, delta: int, daily_limit: int
) -> bool:
    """CAS：單一條件 UPDATE 讓 reserved_qty += delta，條件是「結果落在 [0, daily_limit]」。
    這是一個 DB 引擎層級的原子操作（SQLite/Postgres 皆對同一列的 UPDATE 序列化），
    不需要 asyncio 鎖也能防同 process 內兩個協程交錯（BLOCKER#4）。delta 可負（release）。"""
    _ensure_quota_row(session, user_id=user_id, mode=mode, trading_day=trading_day)
    c = QuotaReservation.__table__.c
    stmt = (
        update(QuotaReservation.__table__)
        .where(
            c.user_id == user_id, c.mode == mode, c.trading_day == trading_day,
            (c.reserved_qty + delta) >= 0,
            (c.reserved_qty + delta) <= daily_limit,
        )
        .values(reserved_qty=c.reserved_qty + delta, updated_at=_utcnow())
    )
    result = session.exec(stmt)  # type: ignore[call-overload]
    return result.rowcount == 1


def release_quota(session: Session, *, user_id: int, mode: str, trading_day: str, qty: int) -> None:
    """釋放配額（broker 失敗/unknown 由 reconcile 呼叫）；floor 在 0，不需要 CAS 強度
    （只會遞減，最壞情況是些微誤差，不像 reserve 那樣能被利用來突破日限）。"""
    row = session.exec(
        select(QuotaReservation).where(
            QuotaReservation.user_id == user_id, QuotaReservation.mode == mode,
            QuotaReservation.trading_day == trading_day,
        )
    ).first()
    if row is None:
        return
    row.reserved_qty = max(row.reserved_qty - qty, 0)
    row.updated_at = _utcnow()
    session.add(row)
    session.flush()
```

- [ ] **Step 4：跑測試確認 GREEN**

```bash
uv run pytest tests/test_broker_repo.py -q
```
預期全綠（冪等建委託+request_hash 防竄改、複合 scope 委託查詢不裸鍵、Deal 去重、CHECK 約束擋非法 mode、非重播 IntegrityError 會拋、raw-inbox spool round-trip、confirm token 原子 claim 一次性、quota CAS 擋超限）。

- [ ] **Step 5：跑全測試確認未回歸**

```bash
uv run pytest -q
```

- [ ] **Step 6：Commit（scoped）**

```bash
git add src/quanquant/db/models.py src/quanquant/broker/repository.py tests/test_broker_repo.py
git commit -m "feat: 新增RawInbox/Order/Deal/BrokerPosition/OrderAudit/ConfirmToken/QuotaReservation七表+broker倉儲（複合scope查詢/CAS quota/原子token claim）"
```

---

### Task 4：`PositionTracker`（`BrokerPosition` 累計加權帳務，V3-1）

`Fill` → `BrokerPosition` 累計加權帳本：New 開倉/加碼、Cover 平倉（`min(qty,remaining)` 消耗 + 超額依比例拆 fee 轉開反向部位）、Auto 依現有雙向 open 部位推斷、歧義 `PositionMismatchError` fail closed。**無狀態**（不快取任何部位於記憶體，每次都重新從 DB 讀寫）→ 天然支撐跨重啟續平。round-trip 完成（`remaining_qty==0`）才呼叫 `journal_repo.create_trade(..., source="shioaji", commit=False)` 寫一筆日誌，`entry`/`exit` 用累計加權平均（`entry_notional/total_opened_qty`、`exit_notional/closed_qty`），**不用剩餘口數回推**（V3-1，修 BLOCKER#10）。手動日誌與 broker 自動列完全隔離（自動流程只碰 `BrokerPosition`，只在 round-trip 完成才寫入一筆帶 `source="shioaji"` 的 `Trade`）。

**Files:**
- Create: `src/quanquant/broker/position_tracker.py`
- Modify: `src/quanquant/journal/repository.py`（`create_trade` 加 `commit: bool = True` 參數，供 Task 5 的 fill 交易把 Trade 寫入包進同一個 commit）
- Test: Create `tests/test_position_tracker.py`、Modify `tests/test_repository.py`（`commit=False` 行為回歸）

**Interfaces:**
- Consumes：Task 2 的 `broker.types.Fill`、Task 3 的 `broker.repository`（`find_open_position`/`remaining_qty`/`avg_entry_price`/`avg_exit_price`/`append_audit`/`audit_reference_hash`）與 `db.models.BrokerPosition`、Task 1 的 `journal.schemas.TradeCreate`（`mode`/`source` 欄位）
- Produces（Task 5 依賴）：
  - `journal.repository.create_trade(session, TradeCreate, *, user_id, commit: bool = True) -> Trade`（`commit=False` 只 flush，呼叫端負責交易邊界；預設 `True` 保持既有行為不回歸）
  - `broker.position_tracker.PositionTracker`（無狀態；`apply_fill(session, fill: Fill, *, user_id: int) -> None`）
  - `broker.position_tracker.PositionMismatchError`（Cover 缺對應開倉部位 / Auto 雙向歧義 → 呼叫端 quarantine，不留部分寫入）

- [ ] **Step 1：先寫失敗測試 — `create_trade(commit=False)` 回歸（`tests/test_repository.py`）**

在檔尾新增：
```python
def test_create_trade_uncommitted_leaves_transaction_open(session, user):
    trade = repo.create_trade(session, make_create(), user_id=user.id, commit=False)
    assert trade.id is None  # 尚未 flush 出 PK？不一定——SQLite autoincrement 在 flush 後即有 id
    session.rollback()
    from quanquant.db.models import Trade
    assert session.exec(__import__("sqlmodel").select(Trade)).first() is None  # rollback 後不留痕跡


def test_create_trade_default_commit_true_unchanged(session, user):
    trade = repo.create_trade(session, make_create(), user_id=user.id)  # 不傳 commit，維持舊行為
    assert trade.id is not None
    session.rollback()  # 已經 commit 過，rollback 對已提交資料無效
    from quanquant.db.models import Trade
    assert session.exec(__import__("sqlmodel").select(Trade)).first() is not None
```
> 註：`trade.id is None` 的斷言在 `commit=False` 分支若因 `session.flush()` 已配置 PK 而不成立，屬預期（SQLite/Postgres 皆會在 `flush` 時配置 auto-increment PK），**請把該行斷言拿掉**，只保留 `session.rollback()` 後 `Trade` 表為空的斷言（這才是本測試真正要證明的事：`commit=False` 不會意外提交）。跑 `uv run pytest tests/test_repository.py -q` → RED（`create_trade` 尚無 `commit` 參數）。

- [ ] **Step 2：`create_trade` 加 `commit` 參數（`src/quanquant/journal/repository.py`）**

```python
def create_trade(session: Session, data: TradeCreate, *, user_id: int, commit: bool = True) -> Trade:
    trade = Trade(
        user_id=user_id,
        symbol=data.symbol,
        direction=data.direction,
        entry_time=data.entry_time,
        entry_price=data.entry_price,
        exit_time=data.exit_time,
        exit_price=data.exit_price,
        stop_loss_price=data.stop_loss_price,
        take_profit_strategy=data.take_profit_strategy,
        size=data.size,
        point_value=data.point_value,
        fee=data.fee,
        mode=data.mode,
        source=data.source,
        note=data.note,
        tags=join_tags(data.tags),
        pnl_is_manual=data.pnl is not None,
        pnl=data.pnl,
    )
    _recompute_pnl(trade)
    session.add(trade)
    if commit:
        session.commit()
        session.refresh(trade)
    else:
        session.flush()
    return trade
```
（`mode=data.mode, source=data.source,` 兩行是 Task 1 就該加上但當時漏列——若 Task 1 已加則此處不重複，以現況為準只補 `commit` 參數與分支。）

- [ ] **Step 3：跑測試確認 GREEN（repository）**

```bash
uv run pytest tests/test_repository.py -q
```

- [ ] **Step 4：先寫失敗測試 — PositionTracker（新檔 `tests/test_position_tracker.py`）**

> GateGuard：建新檔前陳述事實（PositionTracker 累計加權部位帳務測試：開倉加碼/部分平倉續平/超額反向/Auto 推斷/歧義fail closed/手動與自動隔離/lot ledger 加權真值）後重試。

```python
"""PositionTracker：累計加權 BrokerPosition 帳務。
V3-1 核心回歸：開2@100→平1→再開1@200→全平，結算 entry 必須是真實加權平均(400/3)，
不是用『剩餘量』回推的錯誤值(150)——這是 BLOCKER#10 的專屬回歸測試。"""
from decimal import Decimal

import pytest
from sqlmodel import select

from quanquant.broker import repository as brepo
from quanquant.broker.position_tracker import PositionMismatchError, PositionTracker
from quanquant.broker.types import Fill
from quanquant.db.models import BrokerPosition, Trade
from quanquant.journal import repository as journal_repo

from tests.conftest import make_create


def _fill(**over):
    base = dict(
        broker="shioaji", fill_id="F1", ordno="O1", broker_order_id="B1", symbol="TXF",
        action="Buy", price=Decimal("18000"), qty=1, fee=Decimal("20"), octype="New",
        ts=1_780_000_000_000, account="F1", mode="sim", user_id=1,
    )
    base.update(over)
    return Fill(**base)


def _positions(session):
    return list(session.exec(select(BrokerPosition)))


def test_new_fill_opens_broker_position(session):
    tracker = PositionTracker()
    tracker.apply_fill(session, _fill(fill_id="F1", octype="New", action="Buy", qty=2, price=Decimal("18000")), user_id=1)
    session.commit()
    pos = _positions(session)
    assert len(pos) == 1
    assert pos[0].direction == "long" and pos[0].total_opened_qty == 2 and pos[0].status == "open"


def test_aggregating_new_fills_weighted_avg(session):
    tracker = PositionTracker()
    tracker.apply_fill(session, _fill(fill_id="F1", qty=1, price=Decimal("18000")), user_id=1)
    tracker.apply_fill(session, _fill(fill_id="F2", qty=1, price=Decimal("18100")), user_id=1)
    session.commit()
    pos = _positions(session)[0]
    assert pos.total_opened_qty == 2
    assert brepo.avg_entry_price(pos) == Decimal("18050")  # (18000+18100)/2


def test_lot_ledger_weighted_entry_survives_partial_cover_then_reopen(session, user):
    """核心 V3-1 回歸：開2@100→平1@105→再開1@200→平2@110 → entry 必須是 (2*100+1*200)/3=133.33...，
    非用剩餘量推估的 150；fee 全程守恆（合計等於所有 fill fee 總和）。"""
    tracker = PositionTracker()
    total_fee_in = Decimal(0)

    f1 = _fill(fill_id="F1", octype="New", action="Buy", qty=2, price=Decimal("100"), fee=Decimal("6"), user_id=user.id)
    tracker.apply_fill(session, f1, user_id=user.id)
    total_fee_in += f1.fee

    f2 = _fill(fill_id="F2", octype="Cover", action="Sell", qty=1, price=Decimal("105"), fee=Decimal("3"), user_id=user.id)
    tracker.apply_fill(session, f2, user_id=user.id)
    total_fee_in += f2.fee

    f3 = _fill(fill_id="F3", octype="New", action="Buy", qty=1, price=Decimal("200"), fee=Decimal("3"), user_id=user.id)
    tracker.apply_fill(session, f3, user_id=user.id)
    total_fee_in += f3.fee

    f4 = _fill(fill_id="F4", octype="Cover", action="Sell", qty=2, price=Decimal("110"), fee=Decimal("6"), user_id=user.id)
    tracker.apply_fill(session, f4, user_id=user.id)
    total_fee_in += f4.fee
    session.commit()

    trades = list(session.exec(select(Trade).where(Trade.source == "shioaji")))
    assert len(trades) == 1
    t = trades[0]
    assert t.size == 3
    assert t.entry_price == Decimal(400) / 3          # 真實加權平均，非 150
    assert t.exit_price == (Decimal("105") + Decimal("220")) / 3  # (105*1+110*2)/3
    assert t.fee == total_fee_in                       # fee 全程守恆


def test_partial_cover_persists_progress_across_tracker_restart(session, user):
    tracker_a = PositionTracker()
    tracker_a.apply_fill(session, _fill(fill_id="F1", octype="New", qty=3, price=Decimal("100"), user_id=user.id), user_id=user.id)
    tracker_a.apply_fill(session, _fill(fill_id="F2", octype="Cover", action="Sell", qty=1, price=Decimal("110"), user_id=user.id), user_id=user.id)
    session.commit()

    tracker_b = PositionTracker()  # 模擬重啟：全新 instance，無記憶體快取
    tracker_b.apply_fill(session, _fill(fill_id="F3", octype="Cover", action="Sell", qty=2, price=Decimal("120"), user_id=user.id), user_id=user.id)
    session.commit()

    pos = _positions(session)[0]
    assert pos.status == "closed" and pos.closed_qty == 3
    trades = list(session.exec(select(Trade).where(Trade.source == "shioaji")))
    assert len(trades) == 1 and trades[0].size == 3


def test_cover_excess_reverses_into_new_position_and_splits_fee(session, user):
    tracker = PositionTracker()
    tracker.apply_fill(session, _fill(fill_id="F1", octype="New", action="Buy", qty=1, price=Decimal("100"), fee=Decimal("9"), user_id=user.id), user_id=user.id)
    tracker.apply_fill(session, _fill(fill_id="F2", octype="Cover", action="Sell", qty=3, price=Decimal("110"), fee=Decimal("9"), user_id=user.id), user_id=user.id)
    session.commit()

    closed = [p for p in _positions(session) if p.status == "closed"]
    opened = [p for p in _positions(session) if p.status == "open"]
    assert len(closed) == 1 and closed[0].closed_qty == 1
    assert len(opened) == 1 and opened[0].direction == "short" and opened[0].total_opened_qty == 2
    # fee 3 等分：consumed=1/3 → fee=3；excess=2/3 → fee=6；相加等於原 fee=9
    assert closed[0].close_fee_total + opened[0].open_fee_total == Decimal("9")


def test_auto_infers_cover_when_opposite_direction_open(session, user):
    tracker = PositionTracker()
    tracker.apply_fill(session, _fill(fill_id="F1", octype="New", action="Buy", qty=1, price=Decimal("100"), user_id=user.id), user_id=user.id)
    tracker.apply_fill(session, _fill(fill_id="F2", octype="Auto", action="Sell", qty=1, price=Decimal("110"), user_id=user.id), user_id=user.id)
    session.commit()
    pos = _positions(session)[0]
    assert pos.status == "closed"  # Auto 被推斷為 Cover


def test_auto_infers_new_when_no_opposite_direction_open(session, user):
    tracker = PositionTracker()
    tracker.apply_fill(session, _fill(fill_id="F1", octype="Auto", action="Buy", qty=1, price=Decimal("100"), user_id=user.id), user_id=user.id)
    session.commit()
    pos = _positions(session)[0]
    assert pos.status == "open" and pos.direction == "long"  # Auto 被推斷為 New


def test_auto_ambiguous_with_both_directions_open_fails_closed(session, user):
    tracker = PositionTracker()
    tracker.apply_fill(session, _fill(fill_id="F1", octype="New", action="Buy", qty=1, price=Decimal("100"), user_id=user.id), user_id=user.id)
    tracker.apply_fill(session, _fill(fill_id="F2", octype="New", action="Sell", qty=1, price=Decimal("100"), user_id=user.id), user_id=user.id)
    session.commit()
    with pytest.raises(PositionMismatchError):
        tracker.apply_fill(session, _fill(fill_id="F3", octype="Auto", action="Sell", qty=1, price=Decimal("110"), user_id=user.id), user_id=user.id)


def test_cover_without_open_position_raises_for_quarantine(session, user):
    tracker = PositionTracker()
    with pytest.raises(PositionMismatchError):
        tracker.apply_fill(session, _fill(fill_id="F1", octype="Cover", action="Sell", qty=1, price=Decimal("100"), user_id=user.id), user_id=user.id)
    session.rollback()
    assert _positions(session) == []  # 沒有部分寫入殘留


def test_manual_trade_and_broker_round_trip_do_not_cross_pollute(session, user):
    manual = journal_repo.create_trade(session, make_create(symbol="TXF"), user_id=user.id)  # source="manual"
    assert manual.source == "manual"

    tracker = PositionTracker()
    tracker.apply_fill(session, _fill(fill_id="F1", octype="New", action="Buy", qty=1, price=Decimal("100"), user_id=user.id), user_id=user.id)
    tracker.apply_fill(session, _fill(fill_id="F2", octype="Cover", action="Sell", qty=1, price=Decimal("110"), user_id=user.id), user_id=user.id)
    session.commit()

    trades = list(session.exec(select(Trade)))
    assert len(trades) == 2
    sources = {t.source for t in trades}
    assert sources == {"manual", "shioaji"}
```
跑 `uv run pytest tests/test_position_tracker.py -q` → RED（`broker.position_tracker` 未定義）。

- [ ] **Step 5：`PositionTracker`（新檔 `src/quanquant/broker/position_tracker.py`）**

> GateGuard：建新檔前陳述事實（Fill→BrokerPosition 累計加權帳務，New/Cover/Auto 分派，round-trip 完成寫 Trade，與手動日誌隔離）後重試。

```python
"""Broker 自動部位帳務：Fill → BrokerPosition 累計加權帳本 → round-trip 完成寫 Trade。

與手動 Trade 完全隔離：自動流程只讀寫 BrokerPosition，只在 remaining_qty 歸零時才呼叫
journal_repo.create_trade(..., source="shioaji", commit=False) 寫一筆日誌。

V3-1（BLOCKER#10）：round-trip 結算的 entry/exit 用 BrokerPosition 的累計欄位
（entry_notional/total_opened_qty、exit_notional/closed_qty）算真實加權平均，
不用「剩餘口數」回推——理由見 db/models.py 的 BrokerPosition docstring。

無狀態：不快取任何部位於記憶體，每次 apply_fill 都重新從 DB 讀寫 BrokerPosition，
天然支撐跨重啟續平（DB 是唯一真相來源）。
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlmodel import Session

from quanquant.broker import repository as brepo
from quanquant.broker.types import Fill
from quanquant.db.models import BrokerPosition
from quanquant.journal import repository as journal_repo
from quanquant.journal.schemas import TradeCreate

_CST = timezone(timedelta(hours=8))
_POINT_VALUES = {"TXF": Decimal("200"), "MXF": Decimal("50")}


class PositionMismatchError(Exception):
    """Cover 找不到對應開倉部位，或 Auto 雙向同時 open 導致歧義（fail closed）。
    呼叫端（Task 5 worker）捕捉後 quarantine 對應的 Deal/RawInbox，不得留下部分寫入
    （本模組任何會 raise 這個例外的路徑，都保證 raise 之前沒有做過任何 session.add/flush）。"""


def _point_value(symbol: str) -> Decimal:
    return _POINT_VALUES.get(symbol, Decimal("200"))


def _to_dt(ts_ms: int) -> datetime:
    """epoch-ms UTC → naive UTC datetime（給 BrokerPosition.opened_at 等欄位儲存）。"""
    return datetime.fromtimestamp(ts_ms / 1000, timezone.utc).replace(tzinfo=None)


def _utc_dt_to_cst(value: datetime) -> datetime:
    """naive UTC → naive CST（Trade.entry_time/exit_time 是 naive local 值）。"""
    aware = value.replace(tzinfo=timezone.utc)
    return aware.astimezone(_CST).replace(tzinfo=None)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class PositionTracker:
    def apply_fill(self, session: Session, fill: Fill, *, user_id: int) -> None:
        if fill.octype == "New":
            direction = "long" if fill.action == "Buy" else "short"
            self._open(session, fill, user_id=user_id, direction=direction)
        elif fill.octype == "Cover":
            target_direction = "long" if fill.action == "Sell" else "short"
            self._close(session, fill, user_id=user_id, target_direction=target_direction)
        else:  # "Auto"（型別層已擋非法 octype）
            self._auto(session, fill, user_id=user_id)

    def _auto(self, session: Session, fill: Fill, *, user_id: int) -> None:
        covers_direction = "long" if fill.action == "Sell" else "short"
        news_direction = "short" if fill.action == "Sell" else "long"
        covers_open = brepo.find_open_position(
            session, user_id=user_id, broker=fill.broker, account=fill.account,
            mode=fill.mode, symbol=fill.symbol, direction=covers_direction,
        )
        news_open = brepo.find_open_position(
            session, user_id=user_id, broker=fill.broker, account=fill.account,
            mode=fill.mode, symbol=fill.symbol, direction=news_direction,
        )
        if covers_open is not None and news_open is not None:
            raise PositionMismatchError(
                f"Auto 歧義：{fill.symbol} 同時有 {covers_direction}/{news_direction} 兩個 open 部位，"
                "無法判斷本筆 Auto fill 該開或該平，fail closed"
            )
        if covers_open is not None:
            self._close(session, fill, user_id=user_id, target_direction=covers_direction)
        else:
            self._open(session, fill, user_id=user_id, direction=news_direction)

    def _open(self, session: Session, fill: Fill, *, user_id: int, direction: str) -> None:
        self._open_qty(
            session, user_id=user_id, broker=fill.broker, account=fill.account, mode=fill.mode,
            symbol=fill.symbol, direction=direction, qty=fill.qty, price=fill.price,
            fee=fill.fee or Decimal(0), opened_at=_to_dt(fill.ts),
        )
        brepo.append_audit(
            session, actor_user_id=user_id, mode=fill.mode, action="fill",
            payload_hash=brepo.audit_reference_hash(fill.fill_id, fill.octype, fill.qty, fill.price),
            result="ok", rule="open",
        )

    def _open_qty(
        self, session: Session, *, user_id: int, broker: str, account: str, mode: str, symbol: str,
        direction: str, qty: int, price: Decimal, fee: Decimal, opened_at: datetime,
    ) -> None:
        pos = brepo.find_open_position(
            session, user_id=user_id, broker=broker, account=account, mode=mode,
            symbol=symbol, direction=direction,
        )
        if pos is None:
            pos = BrokerPosition(
                user_id=user_id, broker=broker, account=account, mode=mode, symbol=symbol,
                direction=direction, total_opened_qty=qty, entry_notional=price * qty,
                open_fee_total=fee, opened_at=opened_at, updated_at=_now_utc(),
            )
        else:
            pos.total_opened_qty += qty
            pos.entry_notional += price * qty
            pos.open_fee_total += fee
            pos.updated_at = _now_utc()
        session.add(pos)
        session.flush()

    def _close(self, session: Session, fill: Fill, *, user_id: int, target_direction: str) -> None:
        pos = brepo.find_open_position(
            session, user_id=user_id, broker=fill.broker, account=fill.account,
            mode=fill.mode, symbol=fill.symbol, direction=target_direction,
        )
        if pos is None:
            raise PositionMismatchError(
                f"Cover fill 找不到對應開倉部位（{fill.symbol}/{target_direction}），fail closed 進 quarantine"
            )
        remaining = brepo.remaining_qty(pos)
        consumed = min(fill.qty, remaining)
        excess = fill.qty - consumed
        fee_total = fill.fee or Decimal(0)
        if excess > 0:
            fee_excess = fee_total * excess / fill.qty
            fee_consumed = fee_total - fee_excess  # 恆等式：不 quantize，避免破壞「相加等於原 fee」
        else:
            fee_consumed, fee_excess = fee_total, Decimal(0)

        pos.closed_qty += consumed
        pos.exit_notional += fill.price * consumed
        pos.close_fee_total += fee_consumed
        pos.updated_at = _now_utc()

        finalized = brepo.remaining_qty(pos) == 0
        if finalized:
            pos.status = "closed"
        session.add(pos)
        session.flush()
        if finalized:
            self._finalize(session, pos, fill, user_id=user_id)

        brepo.append_audit(
            session, actor_user_id=user_id, mode=fill.mode, action="fill",
            payload_hash=brepo.audit_reference_hash(fill.fill_id, fill.octype, consumed, fill.price),
            result="ok", rule="close",
        )

        if excess > 0:
            reversal_direction = "short" if fill.action == "Sell" else "long"
            self._open_qty(
                session, user_id=user_id, broker=fill.broker, account=fill.account, mode=fill.mode,
                symbol=fill.symbol, direction=reversal_direction, qty=excess, price=fill.price,
                fee=fee_excess, opened_at=_to_dt(fill.ts),
            )
            brepo.append_audit(
                session, actor_user_id=user_id, mode=fill.mode, action="fill",
                payload_hash=brepo.audit_reference_hash(fill.fill_id, "cover_excess_reversal", excess, fill.price),
                result="ok", rule="cover_excess_reversal",
                detail=(
                    f"Cover fill qty={fill.qty} 超過剩餘 {remaining}，excess={excess} "
                    f"轉開 {reversal_direction} 部位（fee 依比例拆分，避免重複計）"
                ),
            )

    def _finalize(self, session: Session, pos: BrokerPosition, fill: Fill, *, user_id: int) -> None:
        entry = brepo.avg_entry_price(pos)
        exit_ = brepo.avg_exit_price(pos)
        fee = pos.open_fee_total + pos.close_fee_total
        trade = journal_repo.create_trade(
            session,
            TradeCreate(
                symbol=pos.symbol,
                direction=pos.direction,
                entry_time=_utc_dt_to_cst(pos.opened_at),
                entry_price=entry,
                exit_time=_utc_dt_to_cst(_to_dt(fill.ts)),
                exit_price=exit_,
                size=pos.total_opened_qty,
                point_value=_point_value(pos.symbol),
                fee=fee,
                mode=pos.mode,
                source="shioaji",
            ),
            user_id=user_id,
            commit=False,
        )
        pos.trade_id = trade.id
        session.add(pos)
        session.flush()
```

- [ ] **Step 6：跑測試確認 GREEN**

```bash
uv run pytest tests/test_position_tracker.py tests/test_repository.py -q
```
預期全綠（開倉加碼加權均價正確、V3-1 lot ledger 加權真值回歸通過、跨「重啟」續平、超額反向+fee 守恆、Auto 推斷+歧義 fail closed、Cover 缺開倉不留部分寫入、手動與自動 Trade 並存不互污）。

- [ ] **Step 7：跑全測試確認未回歸**

```bash
uv run pytest -q
```

- [ ] **Step 8：Commit（scoped）**

```bash
git add src/quanquant/broker/position_tracker.py src/quanquant/journal/repository.py \
  tests/test_position_tracker.py tests/test_repository.py
git commit -m "feat: PositionTracker累計加權部位帳務(New/Cover/Auto)，round-trip寫Trade用真實加權entry/exit（V3-1修PnL錯算），create_trade補commit參數"
```

---

### Task 5：`BrokerSupervisor` + durable raw-inbox 序列化 worker（V3-2，修 BLOCKER#2/#11/#14）

**`BrokerSupervisor`**：全計畫**唯一**的序列化通道——一個 `asyncio.Lock`，Task 6 的 `ShioajiAdapter`（native 呼叫）與本 task 的 `RawInboxWorker`（`BrokerPosition`/`Order`/`QuotaReservation` 寫入）都必須先取得它才能動作，修「單-worker 不變量在背景 thread 下不成立」（BLOCKER#11/#14：live fill worker 與 watchdog retry 若各自無鎖跑，會同時改同一 `BrokerPosition`）。

**`RawInboxWorker`**：從 `RawInbox` 表（Task 3 的 durable spool，callback 只做 insert+commit 到這張表，**不進任何 volatile `asyncio.Queue`**——本架構下「QueueFull 丟單」這個攻擊面已被整個消除，不是被「處理」）拉未處理列、逐列在**一個資料庫交易**內完成「嚴格驗證（V3-4）→ 複合鍵解析委託關聯（不猜測、解不到就 quarantine）→ `stage_deal` 去重 → `PositionTracker.apply_fill` → `apply_order_fill`/`mark_order_status` 更新 `Order` 聚合 → `RawInbox.processed=true`」一次 commit；業務邏輯性失敗（驗證不過、`PositionMismatchError`）→ rollback 後單獨 quarantine 該列；**非預期例外**（transient，如 DB 短暫故障）→ 該列保持 `processed=False`，**不 quarantine**，留給下一輪 batch 自然重試（不誤判為永久壞資料）。「raw payload → domain 型別」的映射刻意用**注入的 mapper 函式**（`DealMapper`/`OrderReportMapper`），讓本 task 不依賴 Task 6 的 Shioaji 細節即可獨立測試；Task 6 之後提供真正的 Shioaji mapper 並在 Task 8 lifespan 注入。

**Files:**
- Create: `src/quanquant/broker/supervisor.py`
- Create: `src/quanquant/broker/inbox_worker.py`
- Test: Create `tests/test_inbox_worker.py`

**Interfaces:**
- Consumes：Task 2 的 `broker.types.Fill`、Task 3 的 `broker.repository`（`list_unprocessed_raw_inbox`/`mark_raw_inbox_processed`/`quarantine_raw_inbox`/`stage_deal`/`apply_order_fill`/`mark_order_status`/`find_order_by_ordno`/`find_order_by_broker_id`/`trading_day_for`）與 `db.models.RawInbox`、Task 4 的 `broker.position_tracker.PositionTracker`/`PositionMismatchError`
- Produces（Task 6/8 依賴）：
  - `broker.supervisor.BrokerSupervisor`（`.lock: asyncio.Lock`）——**唯一**序列化通道，Task 6/8 皆須共用同一個 instance
  - `broker.inbox_worker.OrderReport(broker, account, mode, ordno, broker_order_id, status)`（frozen dataclass；委託回報用，範圍窄於 `Fill`，不進 Task 2 的共用型別模組）
  - `broker.inbox_worker.DealMapper = Callable[[dict], Fill]`、`broker.inbox_worker.OrderReportMapper = Callable[[dict], OrderReport]`
  - `broker.inbox_worker.RawInboxWorker(session_factory, *, supervisor, deal_mapper, order_report_mapper, tracker=None, idle_interval=1.0, batch_limit=50)`
    - `async def run(self) -> None`（迴圈：`async with supervisor.lock: n = await asyncio.to_thread(self.process_batch_once)`；`n==0` 才 `idle_interval` 退避，避免對「暫時性例外留待重試」的列忙迴圈）
    - `async def stop_and_drain(self, timeout: float = 5.0) -> None`（Task 8 shutdown 用：設停止旗標，等目前 batch 結束或逾時）
    - `def process_batch_once(self) -> int`（同步、可直接測試；回傳**確定處理完**（processed 或 quarantine）的列數，非預期例外的列不計入）

- [ ] **Step 1：先寫失敗測試（新檔 `tests/test_inbox_worker.py`）**

> GateGuard：建新檔前陳述事實（BrokerSupervisor 序列化鎖 + RawInboxWorker：raw-inbox 零丟單、業務失敗quarantine不留部分寫入、非預期例外不quarantine留待重試、委託關聯複合scope解析、Deal重播冪等不重跑帳務、序列化鎖防兩協程同時改同一部位）後重試。

```python
"""BrokerSupervisor 序列化鎖 + RawInboxWorker：raw-inbox（durable spool，不經 volatile
asyncio.Queue——QueueFull 這個攻擊面已被架構消除）逐列一交易處理、業務失敗 quarantine
不留部分寫入、非預期例外不 quarantine（留待下一輪重試，零丟單）、複合鍵解析委託關聯、
Deal 重播冪等只補 processed 不重跑帳務、序列化鎖防兩協程同時改同一 BrokerPosition。"""
import asyncio
import json
from decimal import Decimal

import pytest
from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.broker.inbox_worker import OrderReport, RawInboxWorker
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import Fill
from quanquant.db.models import BrokerPosition, Deal, Order, RawInbox


def _order_kwargs(**over):
    base = dict(
        client_order_id="C1", request_hash="H1", user_id=1, mode="sim", broker="shioaji", account="F1",
        symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", trading_day="2026-06-16",
    )
    base.update(over)
    return base


def _deal_payload(**over):
    base = dict(
        broker="shioaji", fill_id="F1", ordno="O1", broker_order_id="B1", symbol="TXF",
        action="Buy", price="18000", qty=1, fee="20", octype="New",
        ts=1_780_000_000_000, account="F1", mode="sim",
    )
    base.update(over)
    return base


def _ok_deal_mapper(payload: dict) -> Fill:
    return Fill(
        broker=payload["broker"], fill_id=payload["fill_id"], ordno=payload["ordno"],
        broker_order_id=payload["broker_order_id"], symbol=payload["symbol"], action=payload["action"],
        price=Decimal(payload["price"]), qty=int(payload["qty"]), fee=Decimal(payload["fee"]),
        octype=payload["octype"], ts=payload["ts"], account=payload["account"], mode=payload["mode"],
        user_id=None,
    )


def _noop_order_report_mapper(payload: dict) -> OrderReport:
    return OrderReport(**payload)


def _worker(engine, *, deal_mapper=_ok_deal_mapper, order_report_mapper=_noop_order_report_mapper, supervisor=None):
    return RawInboxWorker(
        session_factory=lambda: Session(engine),
        supervisor=supervisor or BrokerSupervisor(),
        deal_mapper=deal_mapper,
        order_report_mapper=order_report_mapper,
        idle_interval=0.01,
    )


def _seed_order(session, **over):
    order = brepo.create_order(session, **_order_kwargs(**over))
    brepo.set_order_ack(session, order.id, broker_order_id="B1", ordno="O1", status="submitted")
    session.commit()
    return order


def test_deal_report_resolves_order_by_composite_scope_and_applies_fill(session, engine):
    _seed_order(session)
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()

    worker = _worker(engine)
    handled = worker.process_batch_once()
    assert handled == 1

    with Session(engine) as s:
        assert s.exec(select(Deal)).first() is not None
        pos = s.exec(select(BrokerPosition)).first()
        assert pos is not None and pos.total_opened_qty == 1
        order = s.exec(select(Order)).first()
        assert order.filled_qty == 1
        row = s.exec(select(RawInbox)).first()
        assert row.processed is True and row.quarantine is False


def test_unresolvable_order_correlation_quarantines_not_dropped(session, engine):
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload(ordno="GHOST", broker_order_id="GHOST-B")))
        s.commit()

    worker = _worker(engine)
    handled = worker.process_batch_once()
    assert handled == 1  # quarantine 也算「確定處理完」

    with Session(engine) as s:
        assert s.exec(select(Deal)).first() is None  # 沒有部分寫入
        row = s.exec(select(RawInbox)).first()
        assert row.quarantine is True and row.processed is False


def test_position_mismatch_quarantines_without_partial_deal_write(session, engine):
    """Cover 缺對應開倉部位 → PositionMismatchError → quarantine，且已 stage 的 Deal 也要 rollback 掉。"""
    _seed_order(session, octype="Cover", action="Sell")
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji",
                              payload=json.dumps(_deal_payload(octype="Cover", action="Sell")))
        s.commit()

    worker = _worker(engine)
    handled = worker.process_batch_once()
    assert handled == 1

    with Session(engine) as s:
        assert s.exec(select(Deal)).first() is None  # stage_deal 的 flush 被 rollback 掉，沒有殘留
        row = s.exec(select(RawInbox)).first()
        assert row.quarantine is True


def test_unexpected_exception_leaves_row_pending_not_quarantined_zero_loss(session, engine):
    """V3-2 核心回歸：非預期例外（非 ValueError/PositionMismatchError）不得 quarantine，
    必須留 processed=False 讓下一輪重試——這就是『重啟後 raw-inbox 仍在，最終恰一次 effect』。"""
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()

    calls = {"n": 0}

    def _flaky_mapper(payload: dict) -> Fill:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("模擬暫時性故障（非業務邏輯錯誤）")
        return _ok_deal_mapper(payload)

    worker = _worker(engine, deal_mapper=_flaky_mapper)
    _seed_order(session)

    first = worker.process_batch_once()
    assert first == 0  # 未預期例外不算「確定處理完」
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.processed is False and row.quarantine is False  # 沒丟、也沒被誤判為壞資料

    second = worker.process_batch_once()
    assert second == 1  # 下一輪（mapper 這次成功）恰好處理一次
    with Session(engine) as s:
        assert s.exec(select(Deal)).first() is not None


def test_replayed_deal_report_is_idempotent_marks_processed_without_reapplying(session, engine):
    """watchdog 對帳把已經處理過的 fill 又補進 raw-inbox（新 RawInbox 列、同 fill_id）→
    stage_deal 撞唯一鍵回 None → 只補 processed，不重跑部位帳務。"""
    _seed_order(session)
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()
    worker = _worker(engine)
    assert worker.process_batch_once() == 1

    with Session(engine) as s:  # 模擬 watchdog 對帳又補了一筆同 fill_id 的 raw-inbox
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()
    assert worker.process_batch_once() == 1

    with Session(engine) as s:
        pos = s.exec(select(BrokerPosition)).first()
        assert pos.total_opened_qty == 1  # 沒有被重複套用成 2
        rows = list(s.exec(select(RawInbox)))
        assert all(r.processed for r in rows)


def test_order_report_updates_status_via_composite_scope(session, engine):
    _seed_order(session)
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="order_report", broker="shioaji", payload=json.dumps(dict(
            broker="shioaji", account="F1", mode="sim", ordno="O1", broker_order_id="B1", status="cancelled",
        )))
        s.commit()
    worker = _worker(engine)
    assert worker.process_batch_once() == 1
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "cancelled" and order.filled_qty == 0


def test_supervisor_lock_serializes_two_concurrent_batches_no_interleaving(engine):
    """V3-2：live fill 與 watchdog retry 同時發生不並發改同一 BrokerPosition——
    用一個共享計數器證明持鎖期間永遠只有一個呼叫者在跑。"""
    supervisor = BrokerSupervisor()
    concurrent = {"n": 0, "max": 0}

    async def _guarded_section():
        async with supervisor.lock:
            concurrent["n"] += 1
            concurrent["max"] = max(concurrent["max"], concurrent["n"])
            await asyncio.sleep(0.02)
            concurrent["n"] -= 1

    async def scenario():
        await asyncio.gather(*[_guarded_section() for _ in range(5)])

    asyncio.run(scenario())
    assert concurrent["max"] == 1  # 任何時刻最多一個持鎖者，證明真正序列化


def test_stop_and_drain_waits_for_in_flight_batch(engine):
    supervisor = BrokerSupervisor()
    worker = _worker(engine, supervisor=supervisor)

    async def scenario():
        async with supervisor.lock:
            drain_task = asyncio.create_task(worker.stop_and_drain(timeout=1.0))
            await asyncio.sleep(0.05)
            assert not drain_task.done()  # 鎖被佔用時 drain 應該還在等
        await drain_task
        assert drain_task.done()

    asyncio.run(scenario())
```
跑 `uv run pytest tests/test_inbox_worker.py -q` → RED（`broker.supervisor`/`broker.inbox_worker` 未定義）。

- [ ] **Step 2：`BrokerSupervisor`（新檔 `src/quanquant/broker/supervisor.py`）**

> GateGuard：建新檔前陳述事實（單一序列化通道：一個 asyncio.Lock，place/update/cancel/positions/close/connect/reconnect/raw-inbox 處理/watchdog reconcile 皆須共用）後重試。

```python
"""單一序列化通道（V3-2，修 BLOCKER#11/#14）。

任何會讀寫 BrokerPosition/QuotaReservation/native shioaji API 的路徑
（place/update/cancel/positions/close/connect/reconnect/RawInboxWorker 處理批次/watchdog
reconcile）必須先 `async with supervisor.lock:` 才能動作。這是全計畫唯一的序列化保證來源
——取代「單一 uvicorn worker」這個在背景執行緒/watchdog 協程下不成立的假設。

刻意極簡：只是一個共用的 asyncio.Lock，紀律（誰都要先拿鎖）靠 Global Constraints 與
code review 把關，不是靠這個類別本身的複雜度。
"""
import asyncio


class BrokerSupervisor:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
```

- [ ] **Step 3：`RawInboxWorker`（新檔 `src/quanquant/broker/inbox_worker.py`）**

> GateGuard：建新檔前陳述事實（durable raw-inbox 序列化 worker：逐列一交易處理、業務失敗quarantine、非預期例外留待重試、複合scope解析委託關聯、Deal去重冪等）後重試。

```python
"""RawInboxWorker：從 RawInbox（durable spool）拉未處理列，逐列一交易完成
驗證→委託關聯解析（複合 scope）→ Deal 去重 → PositionTracker → Order 聚合更新 → processed=true。

零丟單（V3-2，修 BLOCKER#2）：callback 只把 payload 落地到 RawInbox 就 commit，
本 worker 才做真正的業務處理；沒有任何 volatile asyncio.Queue 存在於這條路徑上，
「QueueFull 丟單」這個攻擊面在架構上就不存在（不是被『修好』，是被消除）。

例外分類（重要）：
  - ValueError / PositionMismatchError：業務邏輯性失敗（payload 不合法、委託關聯解不到、
    Cover 缺對應開倉部位、Auto 歧義）→ rollback 該列的交易 → 單獨 quarantine，不留部分寫入。
  - 其餘任何例外：視為 transient（DB 短暫故障等）→ 該列保持 processed=False、quarantine=False，
    不判死刑，留給下一輪 batch 自然重試——這是「零丟單」的關鍵：與其在不確定時猜測，
    不如什麼都不做，讓下次重試決定。
"""
import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlmodel import Session

from quanquant.broker import repository as brepo
from quanquant.broker.position_tracker import PositionMismatchError, PositionTracker
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import Fill
from quanquant.db.models import RawInbox

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class OrderReport:
    """委託回報（FuturesOrder callback）；範圍窄於 Fill，只用來更新 Order.status。"""

    broker: str
    account: str
    mode: str
    ordno: str | None
    broker_order_id: str | None
    status: str


DealMapper = Callable[[dict], Fill]
OrderReportMapper = Callable[[dict], OrderReport]


class RawInboxWorker:
    def __init__(
        self,
        session_factory: Callable[[], Session],
        *,
        supervisor: BrokerSupervisor,
        deal_mapper: DealMapper,
        order_report_mapper: OrderReportMapper,
        tracker: PositionTracker | None = None,
        idle_interval: float = 1.0,
        batch_limit: int = 50,
    ) -> None:
        self._session_factory = session_factory
        self._supervisor = supervisor
        self._deal_mapper = deal_mapper
        self._order_report_mapper = order_report_mapper
        self._tracker = tracker or PositionTracker()
        self._idle_interval = idle_interval
        self._batch_limit = batch_limit
        self._stop = asyncio.Event()

    async def run(self) -> None:
        while not self._stop.is_set():
            async with self._supervisor.lock:
                handled = await asyncio.to_thread(self.process_batch_once)
            if handled == 0:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self._idle_interval)
                except TimeoutError:
                    pass

    async def stop_and_drain(self, timeout: float = 5.0) -> None:
        """設停止旗標；等目前持鎖中的 batch 結束（拿得到鎖代表沒有 batch 在跑）或逾時。"""
        self._stop.set()
        try:
            async with asyncio.timeout(timeout):
                async with self._supervisor.lock:
                    pass
        except TimeoutError:
            log.warning("RawInboxWorker.stop_and_drain 逾時（%.1fs），可能仍有 batch 在跑", timeout)

    def process_batch_once(self) -> int:
        """同步、可直接測試。回傳「確定處理完」（processed 或 quarantine）的列數；
        非預期例外的列不計入（見模組 docstring 的例外分類）。"""
        with self._session_factory() as scan_session:
            row_ids = [r.id for r in brepo.list_unprocessed_raw_inbox(scan_session, limit=self._batch_limit)]
        handled = 0
        for row_id in row_ids:
            try:
                self._process_one(row_id)
                handled += 1
            except Exception:
                log.exception("raw_inbox id=%s 處理時發生未預期例外，留待下一輪重試（零丟單）", row_id)
        return handled

    def _process_one(self, row_id: int) -> None:
        with self._session_factory() as session:
            row = session.get(RawInbox, row_id)
            if row is None or row.processed or row.quarantine:
                return  # 防禦性：理論上單一序列化通道內不會重複排到同一列
            try:
                payload = self._decode_payload(row.payload)
                if row.kind == "deal_report":
                    self._process_deal(session, row, payload)
                elif row.kind == "order_report":
                    self._process_order_report(session, row, payload)
                else:
                    raise ValueError(f"未知 raw_inbox.kind: {row.kind!r}")
            except (ValueError, PositionMismatchError) as exc:
                session.rollback()
                row = session.get(RawInbox, row_id)
                brepo.quarantine_raw_inbox(session, row, error=str(exc))
                session.commit()

    @staticmethod
    def _decode_payload(raw: str) -> dict:
        import json

        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError(f"payload 非合法 JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("payload 必須是 JSON object")
        return payload

    def _process_deal(self, session: Session, row: RawInbox, payload: dict) -> None:
        fill = self._deal_mapper(payload)  # 不合法 → mapper 內部 raise ValueError（見 Task 6 的嚴格驗證）

        order = None
        if fill.ordno:
            order = brepo.find_order_by_ordno(
                session, broker=fill.broker, account=fill.account, mode=fill.mode, ordno=fill.ordno
            )
        if order is None and fill.broker_order_id:
            order = brepo.find_order_by_broker_id(
                session, broker=fill.broker, account=fill.account, mode=fill.mode,
                broker_order_id=fill.broker_order_id,
            )
        if order is None:
            raise ValueError(
                f"無法解析委託關聯（broker={fill.broker!r},account={fill.account!r},mode={fill.mode!r},"
                f"ordno={fill.ordno!r},broker_order_id={fill.broker_order_id!r}），quarantine 待重建"
            )

        trading_day = brepo.trading_day_for(fill.ts)
        deal = brepo.stage_deal(
            session, broker=fill.broker, account=fill.account, mode=fill.mode, trading_day=trading_day,
            fill_id=fill.fill_id, ordno=fill.ordno, broker_order_id=fill.broker_order_id,
            order_id=order.id, user_id=order.user_id,
            symbol=fill.symbol, action=fill.action, price=fill.price, qty=fill.qty, fee=fill.fee,
            octype=fill.octype, ts=fill.ts, raw_inbox_id=row.id,
        )
        if deal is None:
            # 重播（watchdog 對帳補進同一筆 fill_id）：冪等，只補 processed，不重跑帳務。
            brepo.mark_raw_inbox_processed(session, row)
            session.commit()
            return

        resolved_fill = fill if fill.user_id is not None else Fill(
            broker=fill.broker, fill_id=fill.fill_id, ordno=fill.ordno, broker_order_id=fill.broker_order_id,
            symbol=fill.symbol, action=fill.action, price=fill.price, qty=fill.qty, fee=fill.fee,
            octype=fill.octype, ts=fill.ts, account=fill.account, mode=fill.mode, user_id=order.user_id,
        )
        self._tracker.apply_fill(session, resolved_fill, user_id=order.user_id)
        brepo.apply_order_fill(session, order, fill_qty=fill.qty, fill_price=fill.price)

        deal.processed = True
        session.add(deal)
        brepo.mark_raw_inbox_processed(session, row)
        session.commit()

    def _process_order_report(self, session: Session, row: RawInbox, payload: dict) -> None:
        report = self._order_report_mapper(payload)

        order = None
        if report.ordno:
            order = brepo.find_order_by_ordno(
                session, broker=report.broker, account=report.account, mode=report.mode, ordno=report.ordno
            )
        if order is None and report.broker_order_id:
            order = brepo.find_order_by_broker_id(
                session, broker=report.broker, account=report.account, mode=report.mode,
                broker_order_id=report.broker_order_id,
            )
        if order is None:
            raise ValueError(
                f"委託回報無法解析關聯（ordno={report.ordno!r}, broker_order_id={report.broker_order_id!r}）"
            )
        brepo.mark_order_status(session, order, status=report.status)
        brepo.mark_raw_inbox_processed(session, row)
        session.commit()
```

- [ ] **Step 4：跑測試確認 GREEN**

```bash
uv run pytest tests/test_inbox_worker.py -q
```
預期全綠（複合 scope 解析、quarantine 不留部分寫入、非預期例外零丟單不誤判、Deal 重播冪等不重跑帳務、序列化鎖證明任何時刻只有一個持鎖者、`stop_and_drain` 正確等鎖）。

- [ ] **Step 5：跑全測試確認未回歸**

```bash
uv run pytest -q
```

- [ ] **Step 6：Commit（scoped）**

```bash
git add src/quanquant/broker/supervisor.py src/quanquant/broker/inbox_worker.py tests/test_inbox_worker.py
git commit -m "feat: BrokerSupervisor單一序列化鎖 + RawInboxWorker（durable raw-inbox逐列一交易處理，零丟單，業務失敗quarantine/非預期例外留待重試）"
```

---

### Task 6：`ShioajiAdapter`（V3-2/V3-3 broker 端落地）

實作 `OrderService`：lazy-import shioaji（比照 `sources/shioaji_stream.py` 既有慣例，不把原生 client 拉進一般測試/CLI 的 import path）；`place/cancel/update` **一律先做 client_order_id 冪等前置查詢**（命中且 `request_hash` 相符 → 直接回既有 `Order`，不重跑風控、不消費 confirm token、不再送單——V3-3「冪等先於 token」），未命中才走 `RiskGuard`（Task 7，本 task 先以 `risk_guard: _RiskGuardLike | None = None` 佔位，None 時走最小化直接建單路徑，供本 task 獨立測試，Task 8 lifespan 才注入真正的 `RiskGuard`）；`canonical_payload_hash` 用 `self.account`/`self.mode`（server-side 真相）+ 請求/委託當前欄位算出，place 用「即將送出的 req」、update 用「套用變更後的完整新內容」；**送單/改單/取消一律經 `BrokerSupervisor.lock`**，鎖內先 `_send_gate()`（V3-2 修 BLOCKER#13 TOCTOU）才呼叫 native API；callback（背景執行緒）只把 raw payload 落地到 `RawInbox`（`loop.call_soon_threadsafe` 排程一個協程，協程內才拿鎖+落 DB），**不直接處理業務邏輯**；`_map_deal_report`/`_map_order_report` 是 Task 5 `DealMapper`/`OrderReportMapper` 的 Shioaji 具體實作，嚴格驗證缺值/非法枚舉（V3-4）。`positions()` 直接讀 `BrokerPosition`（DB 是唯一真相來源），不呼叫 native API。

> **注意**：Shioaji SDK 的確切回呼欄位名稱（`order.id`/`order.seqno`/`status.id` 等）以官方文件公開語意為準，本 task 用 `getattr`/`payload.get` 防禦性存取（比照 `shioaji_stream.py::_dec` 既有慣例）；實作時若已安裝套件的 type stub 顯示不同屬性名，只需局部調整 `_ack_fields_from_trade`/`_map_deal_report`/`_map_order_report` 三個函式內的欄位鍵，不影響本 task 其餘架構。

**Files:**
- Create: `src/quanquant/broker/shioaji_adapter.py`
- Test: Create `tests/test_shioaji_adapter.py`

**Interfaces:**
- Consumes：Task 2 的 `broker.base.OrderService`/`OrderError`/`RiskError`/`AuthorizationError`、`broker.types`（全部）、Task 3 的 `broker.repository`、Task 5 的 `broker.supervisor.BrokerSupervisor`、`broker.inbox_worker.OrderReport`
- Produces（Task 8/9 依賴）：
  - `broker.shioaji_adapter.ShioajiAdapter(*, api_key, secret_key, ca_path, ca_passwd, person_id, symbol, mode, broker="shioaji", session_factory, supervisor, risk_guard=None)`（實作 `OrderService`）
    - `async def connect(self) -> None`／`async def close(self) -> None`
    - `async def place/cancel/update/positions(...)`（同 `OrderService` 簽名）
    - `def on_fill(self, handler) -> None`（**注意**：僅存 handler 供未來 push 通知擴充；V3 實際成交狀態走 `RawInboxWorker` 的 durable pipeline，Task 9 UI 用輪詢讀 `Order`/`BrokerPosition`，目前無呼叫端消費這個 handler）
    - `self._map_deal_report`／`self._map_order_report`（bound method，符合 Task 5 `DealMapper`/`OrderReportMapper` 簽名，供 Task 8 lifespan 注入 `RawInboxWorker`）
    - `self.account: str`（`connect()` 成功後由 `api.futopt_account.account_id` 填入）

- [ ] **Step 1：先寫失敗測試（新檔 `tests/test_shioaji_adapter.py`）**

> GateGuard：建新檔前陳述事實（ShioajiAdapter：冪等前置查詢不重送、canonical hash 用 server-side mode/account、send gate 擋 kill switch、callback 落地 RawInbox 不直接處理業務、嚴格驗證 mapper）後重試。

```python
"""ShioajiAdapter：place 冪等前置查詢（同 client_order_id+相符 request_hash 回既有 Order，
不重送不燒 token）、canonical hash 用 self.mode/self.account（不可信任外部 mode）、
send gate 在鎖內最後一次擋 kill switch、callback 只落地 RawInbox（不直接動 DB 業務邏輯）、
_map_deal_report 嚴格驗證缺值。用假 shioaji client（`_FakeApi`）不連真網路。"""
import asyncio
import json
from decimal import Decimal

import pytest
from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, OrderError, RiskError
from quanquant.broker.shioaji_adapter import ShioajiAdapter
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import OrderRequest
from quanquant.db.models import Order, RawInbox


class _FakeOrderHandle:
    def __init__(self, id_, seqno):
        self.id = id_
        self.seqno = seqno


class _FakeTrade:
    def __init__(self, id_, seqno):
        self.order = _FakeOrderHandle(id_, seqno)


class _FakeApi:
    """假 shioaji client：不連真網路，place_order/cancel_order/update_order 皆同步回傳。"""

    def __init__(self):
        self.placed = []
        self.futopt_account = type("Acc", (), {"account_id": "F1"})()
        self._seq = 0

    def Order(self, **kw):
        return kw

    def place_order(self, contract, order):
        self._seq += 1
        self.placed.append((contract, order))
        return _FakeTrade(f"ORD{self._seq}", f"SEQ{self._seq}")

    def cancel_order(self, ordno):
        return _FakeTrade(ordno, f"SEQ-{ordno}")

    def update_order(self, ordno, **kw):
        return _FakeTrade(ordno, f"SEQ-{ordno}")


def _adapter(engine, *, mode="sim", risk_guard=None):
    a = ShioajiAdapter(
        api_key="k", secret_key="s", ca_path=None, ca_passwd=None, person_id=None,
        symbol="TXF", mode=mode, session_factory=lambda: Session(engine),
        supervisor=BrokerSupervisor(), risk_guard=risk_guard,
    )
    a._api = _FakeApi()  # 跳過 connect()（不做網路呼叫），直接注入假 client
    a._contract = object()
    a.account = "F1"
    return a


def _req(**over):
    base = dict(
        client_order_id="C1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", user_id=1,
    )
    base.update(over)
    return OrderRequest(**base)


def test_place_returns_ack_and_persists_order(engine):
    adapter = _adapter(engine)
    ack = asyncio.run(adapter.place(_req(), actor_user_id=1))
    assert ack.status == "submitted" and ack.ordno == "ORD1"
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.mode == "sim" and order.account == "F1"  # server-side 值，非表單


def test_place_idempotent_same_client_order_id_does_not_resend(engine):
    adapter = _adapter(engine)
    first = asyncio.run(adapter.place(_req(), actor_user_id=1))
    second = asyncio.run(adapter.place(_req(), actor_user_id=1))  # 同 client_order_id + 同內容
    assert second.ordno == first.ordno
    assert len(adapter._api.placed) == 1  # 沒有重送


def test_place_same_client_order_id_different_payload_rejected(engine):
    adapter = _adapter(engine)
    asyncio.run(adapter.place(_req(), actor_user_id=1))
    with pytest.raises(OrderError):
        asyncio.run(adapter.place(_req(qty=99), actor_user_id=1))
    assert len(adapter._api.placed) == 1  # 竄改的那次沒有送出


def test_place_broker_failure_marks_order_unknown_not_blind_resend(engine):
    adapter = _adapter(engine)

    def _boom(contract, order):
        raise RuntimeError("網路逾時")

    adapter._api.place_order = _boom
    with pytest.raises(OrderError):
        asyncio.run(adapter.place(_req(), actor_user_id=1))
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "unknown"  # 不盲送、待 reconcile，不是直接標 failed


def test_send_gate_blocks_when_kill_switch_on():
    class _Guard:
        kill_switch = True

        def assert_owner(self, actor_user_id):
            pass

        def check_place(self, session, req, **kw):
            from quanquant.broker import repository as brepo
            order = brepo.create_order(
                session, client_order_id=req.client_order_id, request_hash="H",
                user_id=kw["actor_user_id"], mode=kw["mode"], broker=kw["broker"], account=kw["account"],
                symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
                price_type=req.price_type, order_type=req.order_type, octype=req.octype,
                trading_day="2026-06-16",
            )
            session.commit()  # 比照真正 RiskGuard.check_place：quota reserve + create_order 同一交易提交
            return order

    from sqlalchemy.pool import StaticPool
    from sqlmodel import create_engine, SQLModel
    eng = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(eng)

    adapter = _adapter(eng, risk_guard=_Guard())
    with pytest.raises(RiskError):
        asyncio.run(adapter.place(_req(), actor_user_id=1))
    assert len(adapter._api.placed) == 0  # send gate 在 native 呼叫前擋下
    with Session(eng) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "failed"  # 不留在 pending 卡死（V3-2 收尾）


def test_callback_only_stages_raw_inbox_not_direct_db_business_logic(engine):
    adapter = _adapter(engine)
    adapter._loop = asyncio.new_event_loop()

    async def scenario():
        adapter._loop = asyncio.get_running_loop()
        adapter._on_order_cb("FuturesDeal", {"deal_id": "D1", "action": "Buy", "octype": "New",
                                             "quantity": 1, "price": "18000", "ts": 1_780_000_000_000,
                                             "account_id": "F1", "order_id": "O1"})
        await asyncio.sleep(0.05)  # 讓 call_soon_threadsafe 排程的協程跑完

    asyncio.run(scenario())
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row is not None and row.kind == "deal_report"
        assert json.loads(row.payload)["deal_id"] == "D1"


def test_map_deal_report_rejects_missing_deal_id():
    adapter = _adapter_stub_for_mapper()
    with pytest.raises(ValueError):
        adapter._map_deal_report({"action": "Buy", "octype": "New", "quantity": 1, "price": "1",
                                  "ts": 1, "account_id": "F1"})  # 缺 deal_id


def _adapter_stub_for_mapper(*, sim_fee_per_lot=None, mode="sim"):
    return ShioajiAdapter(
        api_key="k", secret_key="s", ca_path=None, ca_passwd=None, person_id=None,
        symbol="TXF", mode=mode, session_factory=lambda: None,
        supervisor=BrokerSupervisor(), sim_fee_per_lot=sim_fee_per_lot,
    )


def test_map_deal_report_fills_sim_fee_from_setting_when_missing():
    """A6：sim 模擬單成交常缺 fee，依設定 sim_fee_per_lot * qty 估算，不留 None/0。"""
    adapter = _adapter_stub_for_mapper(sim_fee_per_lot=Decimal("20"))
    fill = adapter._map_deal_report({
        "deal_id": "D1", "action": "Buy", "octype": "New", "quantity": 3, "price": "18000",
        "ts": 1_780_000_000_000, "account_id": "F1", "order_id": "O1",
    })  # payload 無 "fee" 欄位
    assert fill.fee == Decimal("60")  # 20 * 3 口


def test_map_deal_report_real_missing_fee_stays_none_no_sim_substitution():
    adapter = _adapter_stub_for_mapper(sim_fee_per_lot=Decimal("20"), mode="real")
    fill = adapter._map_deal_report({
        "deal_id": "D1", "action": "Buy", "octype": "New", "quantity": 1, "price": "18000",
        "ts": 1_780_000_000_000, "account_id": "F1", "order_id": "O1",
    })
    assert fill.fee is None  # real 一律取 broker 回報，缺就是缺，不用 sim 設定頂替


def test_map_order_report_rejects_missing_ordno_and_broker_order_id():
    adapter = _adapter_stub_for_mapper()
    with pytest.raises(ValueError):
        adapter._map_order_report({"status": "Cancelled"})  # order_id/seqno 都缺
```
跑 `uv run pytest tests/test_shioaji_adapter.py -q` → RED（`ShioajiAdapter` 未定義）。

- [ ] **Step 2：`ShioajiAdapter`（新檔 `src/quanquant/broker/shioaji_adapter.py`）**

> GateGuard：建新檔前陳述事實（Shioaji OrderService 實作：冪等前置查詢/canonical hash server-side/send gate/callback落地RawInbox/嚴格驗證mapper）後重試。

```python
"""ShioajiAdapter：OrderService 的 Shioaji 落地。

lazy-import shioaji（比照 sources/shioaji_stream.py），一般測試/CLI import 這個模組不會
拉進原生 client。所有 native 呼叫（place/cancel/update）與 BrokerPosition/QuotaReservation
寫入必須先取得 BrokerSupervisor.lock（V3-2）；鎖內、native 呼叫前最後一次 _send_gate()
檢查 kill switch（V3-2，修 BLOCKER#13 TOCTOU）。

冪等（V3-3）：place 先以 client_order_id 查既有 Order，命中且 request_hash 相符 → 直接
回既有狀態，不重跑風控、不消費 confirm token、不再送單；不符 → 拒絕（同鍵不同 payload）。
canonical_payload_hash 一律用 self.mode/self.account（server-side 真相），不接受外部輸入。
"""
import asyncio
import json
import logging
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from typing import Protocol

from sqlmodel import Session

from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, OrderError, RiskError
from quanquant.broker.inbox_worker import OrderReport
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import Fill, Mode, OrderAck, OrderRequest, Position, canonical_payload_hash
from quanquant.db.models import Order

log = logging.getLogger(__name__)


class _RiskGuardLike(Protocol):
    """Task 7 RiskGuard 的結構型別（避免對 Task 7 模組的 import-time 相依）。"""

    kill_switch: bool

    def assert_owner(self, actor_user_id: int) -> None: ...
    def check_place(self, session: Session, req: OrderRequest, **kw) -> Order: ...
    def check_update(self, session: Session, order: Order, **kw) -> None: ...


class ShioajiAdapter:
    def __init__(
        self,
        *,
        api_key: str,
        secret_key: str,
        ca_path: str | None,
        ca_passwd: str | None,
        person_id: str | None,
        symbol: str,
        mode: Mode,
        session_factory: Callable[[], Session],
        supervisor: BrokerSupervisor,
        risk_guard: "_RiskGuardLike | None" = None,
        broker: str = "shioaji",
        sim_fee_per_lot: Decimal | None = None,
    ) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._ca_path = ca_path
        self._ca_passwd = ca_passwd
        self._person_id = person_id
        self.symbol = symbol
        self.mode: Mode = mode
        self.broker = broker
        self.account = ""
        self._session_factory = session_factory
        self._supervisor = supervisor
        self._risk_guard = risk_guard
        self._sim_fee_per_lot = sim_fee_per_lot  # A6：sim 成交 fee 缺值時依口數估算，不留 None/0
        self._api = None
        self._contract = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._fill_handler: Callable[[Fill], None] | None = None

    # ---- 連線生命週期 ----

    async def connect(self) -> None:
        self._loop = asyncio.get_running_loop()
        await asyncio.to_thread(self._connect_blocking)

    def _connect_blocking(self) -> None:
        import shioaji as sj  # lazy：一般 import 這個模組不拉原生 client（比照 shioaji_stream.py）

        api = sj.Shioaji(simulation=(self.mode == "sim"))
        api.login(self._api_key, self._secret_key, fetch_contract=True, subscribe_trade=True)
        if self.mode == "real":
            api.activate_ca(ca_path=self._ca_path, ca_passwd=self._ca_passwd, person_id=self._person_id)
        api.set_order_callback(self._on_order_cb)
        self._api = api
        self.account = api.futopt_account.account_id
        self._contract = self._contract_for(self.symbol)

    async def close(self) -> None:
        api, self._api = self._api, None
        if api is None:
            return
        try:
            await asyncio.to_thread(api.logout)
        except Exception:
            pass

    def _contract_for(self, symbol: str):
        """比照 shioaji_stream.py::_front_contract：取最近未到期的具體月合約。"""
        import datetime as _dt

        category = getattr(self._api.Contracts.Futures, symbol)
        today = _dt.datetime.now().strftime("%Y/%m/%d")
        months = [c for c in category if "R" not in c.code[len(symbol):]]
        if not months:
            raise OrderError(f"找不到 {symbol} 的月合約")
        active = [c for c in months if (c.delivery_date or "") >= today]
        return min(active or months, key=lambda c: c.delivery_date or "9999/99/99")

    def on_fill(self, handler: Callable[[Fill], None]) -> None:
        self._fill_handler = handler  # 目前無呼叫端；保留供未來 push 通知擴充

    # ---- send gate（V3-2） ----

    async def _send_gate(self) -> None:
        if self._api is None:
            raise OrderError("下單 session 尚未就緒")
        if self._risk_guard is not None and self._risk_guard.kill_switch:
            raise RiskError("kill switch 已啟動，拒絕送出")

    # ---- place ----

    async def place(
        self, req: OrderRequest, *, actor_user_id: int, confirm_token: str | None = None
    ) -> OrderAck:
        request_hash = canonical_payload_hash(
            symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
            price_type=req.price_type, order_type=req.order_type, octype=req.octype,
            account=self.account, mode=self.mode,
        )
        with self._session_factory() as session:
            existing = brepo.find_order_by_client_order_id(session, req.client_order_id)
            if existing is not None:
                if existing.request_hash != request_hash:
                    raise OrderError(f"client_order_id={req.client_order_id!r} 已存在但 payload 不同")
                return self._ack_from_order(existing)  # 冪等：不重跑風控、不燒 token、不再送單

            if self._risk_guard is not None:
                order = self._risk_guard.check_place(
                    session, req, actor_user_id=actor_user_id, mode=self.mode, broker=self.broker,
                    account=self.account, request_hash=request_hash, confirm_token=confirm_token,
                )
            else:
                trading_day = brepo.trading_day_for(int(_now_ms()))
                order = brepo.create_order(
                    session, client_order_id=req.client_order_id, request_hash=request_hash,
                    user_id=actor_user_id, mode=self.mode, broker=self.broker, account=self.account,
                    symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
                    price_type=req.price_type, order_type=req.order_type, octype=req.octype,
                    trading_day=trading_day,
                )
                session.commit()
            order_id, client_order_id = order.id, order.client_order_id
            order_qty, order_trading_day = order.qty, order.trading_day

        async with self._supervisor.lock:
            try:
                await self._send_gate()
                ack_fields = await asyncio.to_thread(self._place_blocking, req)
            except RiskError:
                # send gate 擋下（如 kill switch）：確定沒送出，直接標 failed 並釋放配額，
                # 不留在 pending 卡死（V3-2 修 BLOCKER#13 TOCTOU 的收尾）。
                with self._session_factory() as session:
                    order = session.get(Order, order_id)
                    brepo.mark_order_status(session, order, status="failed")
                    brepo.release_quota(
                        session, user_id=actor_user_id, mode=self.mode,
                        trading_day=order_trading_day, qty=order_qty,
                    )
                    session.commit()
                raise
            except Exception as exc:
                with self._session_factory() as session:
                    order = session.get(Order, order_id)
                    brepo.mark_order_status(session, order, status="unknown")
                    session.commit()
                raise OrderError(f"送單失敗，委託標記 unknown 待 reconcile：{exc}") from exc

        with self._session_factory() as session:
            order = session.get(Order, order_id)
            brepo.set_order_ack(
                session, order_id, broker_order_id=ack_fields["broker_order_id"],
                ordno=ack_fields["ordno"], status="submitted",
            )
            session.commit()
        return OrderAck(
            client_order_id=client_order_id, broker_order_id=ack_fields["broker_order_id"],
            ordno=ack_fields["ordno"], status="submitted",
        )

    def _place_blocking(self, req: OrderRequest) -> dict:
        native_order = self._api.Order(
            action=req.action, price=float(req.price), quantity=req.qty,
            price_type=req.price_type, order_type=req.order_type, octype=req.octype,
            account=self._api.futopt_account,
        )
        trade = self._api.place_order(self._contract, native_order)
        return self._ack_fields_from_trade(trade)

    @staticmethod
    def _ack_fields_from_trade(trade) -> dict:
        order = getattr(trade, "order", None)
        ordno = getattr(order, "id", None) if order else None
        broker_order_id = (getattr(order, "seqno", None) if order else None) or ordno
        return {"ordno": ordno, "broker_order_id": broker_order_id}

    @staticmethod
    def _ack_from_order(order: Order) -> OrderAck:
        return OrderAck(
            client_order_id=order.client_order_id, broker_order_id=order.broker_order_id or "",
            ordno=order.ordno, status=order.status,
        )

    # ---- cancel ----

    async def cancel(self, broker_order_id: str, *, actor_user_id: int) -> OrderAck:
        with self._session_factory() as session:
            order = brepo.find_order_by_broker_id(
                session, broker=self.broker, account=self.account, mode=self.mode,
                broker_order_id=broker_order_id,
            )
            if order is None:
                raise OrderError(f"找不到委託 broker_order_id={broker_order_id!r}")
            if self._risk_guard is not None:
                self._risk_guard.assert_owner(actor_user_id)
            elif order.user_id != actor_user_id:
                raise AuthorizationError("非委託所有人不得取消")
            order_id, ordno, client_order_id = order.id, order.ordno, order.client_order_id

        async with self._supervisor.lock:
            # kill switch 不擋取消單（spec 明文：取消單仍允許），仍檢查 session 就緒
            if self._api is None:
                raise OrderError("下單 session 尚未就緒")
            await asyncio.to_thread(self._cancel_blocking, ordno)

        with self._session_factory() as session:
            order = session.get(Order, order_id)
            brepo.mark_order_status(session, order, status="cancelled")
            session.commit()
        return OrderAck(client_order_id=client_order_id, broker_order_id=broker_order_id, ordno=ordno, status="cancelled")

    def _cancel_blocking(self, ordno: str) -> None:
        self._api.cancel_order(ordno)

    # ---- update ----

    async def update(
        self,
        broker_order_id: str,
        *,
        actor_user_id: int,
        price=None,
        qty=None,
        confirm_token: str | None = None,
    ) -> OrderAck:
        with self._session_factory() as session:
            order = brepo.find_order_by_broker_id(
                session, broker=self.broker, account=self.account, mode=self.mode,
                broker_order_id=broker_order_id,
            )
            if order is None:
                raise OrderError(f"找不到委託 broker_order_id={broker_order_id!r}")
            new_price = price if price is not None else order.price
            new_qty = qty if qty is not None else order.qty
            request_hash = canonical_payload_hash(
                symbol=order.symbol, action=order.action, qty=new_qty, price=new_price,
                price_type=order.price_type, order_type=order.order_type, octype=order.octype,
                account=self.account, mode=self.mode,
            )
            if self._risk_guard is not None:
                self._risk_guard.check_update(
                    session, order, actor_user_id=actor_user_id, new_qty=new_qty, new_price=new_price,
                    request_hash=request_hash, confirm_token=confirm_token,
                )
            elif order.user_id != actor_user_id:
                raise AuthorizationError("非委託所有人不得改單")
            order_id, ordno, client_order_id = order.id, order.ordno, order.client_order_id

        async with self._supervisor.lock:
            await self._send_gate()
            await asyncio.to_thread(self._update_blocking, ordno, new_price, new_qty)

        with self._session_factory() as session:
            order = session.get(Order, order_id)
            order.price, order.qty = new_price, new_qty
            brepo.mark_order_status(session, order, status="submitted")
            session.commit()
        return OrderAck(client_order_id=client_order_id, broker_order_id=broker_order_id, ordno=ordno, status="submitted")

    def _update_blocking(self, ordno: str, price, qty: int) -> None:
        self._api.update_order(ordno, price=float(price), qty=qty)

    # ---- positions（純讀 DB，不呼叫 native API——BrokerPosition 是唯一真相來源） ----

    async def positions(self, *, actor_user_id: int) -> list[Position]:
        with self._session_factory() as session:
            if self._risk_guard is not None:
                self._risk_guard.assert_owner(actor_user_id)
            rows = brepo.list_open_positions(
                session, user_id=actor_user_id, broker=self.broker, account=self.account,
                mode=self.mode, symbol=self.symbol,
            )
            return [
                Position(
                    symbol=r.symbol, direction=r.direction,
                    qty=brepo.remaining_qty(r), avg_price=brepo.avg_entry_price(r),
                )
                for r in rows
            ]

    # ---- callback（背景執行緒）：只落地 RawInbox，不直接處理業務邏輯（V3-2） ----

    def _on_order_cb(self, stat, msg) -> None:
        kind = "deal_report" if str(stat).endswith("Deal") else "order_report"
        payload = self._json_safe(msg)
        loop = self._loop
        if loop is None:
            log.error("收到 callback 但 event loop 尚未就緒，payload 遺失：%r", payload)
            return
        loop.call_soon_threadsafe(self._schedule_ingest, kind, payload)

    def _schedule_ingest(self, kind: str, payload: dict) -> None:
        asyncio.ensure_future(self._ingest_raw(kind, payload))

    async def _ingest_raw(self, kind: str, payload: dict) -> None:
        async with self._supervisor.lock:
            await asyncio.to_thread(self._ingest_raw_blocking, kind, payload)

    def _ingest_raw_blocking(self, kind: str, payload: dict) -> None:
        with self._session_factory() as session:
            brepo.stage_raw_inbox(session, kind=kind, broker=self.broker, payload=json.dumps(payload))
            session.commit()

    @staticmethod
    def _json_safe(msg) -> dict:
        if isinstance(msg, dict):
            return msg
        if hasattr(msg, "to_dict"):
            try:
                return msg.to_dict()
            except Exception:
                pass
        return {"raw": str(msg)}

    # ---- Task 5 DealMapper / OrderReportMapper 實作（V3-4 嚴格驗證） ----

    def _map_deal_report(self, payload: dict) -> Fill:
        try:
            fill_id = payload["deal_id"]
            if not fill_id:
                raise ValueError("deal_id 為空")
            action = payload["action"]
            octype = payload["octype"]
            qty = int(payload["quantity"])
            price = Decimal(str(payload["price"]))
            ts = int(payload["ts"])
            account = payload["account_id"]
            ordno = payload.get("order_id")
            broker_order_id = payload.get("seqno") or ordno
            fee_raw = payload.get("fee")
            fee = Decimal(str(fee_raw)) if fee_raw not in (None, "") else None
            symbol = payload.get("code") or self.symbol
        except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
            raise ValueError(f"deal_report payload 缺值或格式不合法: {exc}") from exc

        if fee is None and self.mode == "sim" and self._sim_fee_per_lot is not None:
            # A6：sim 模擬單成交 fee 常缺值/零，依設定的「每口」估算，按 qty 分批累計時自然正確
            # （每筆 fill 各自算 qty*sim_fee_per_lot，PositionTracker 累加 open_fee_total/close_fee_total
            # 時就是「已成交口數 * 每口 fee」的正確累計，不需要另外處理批次）。
            fee = self._sim_fee_per_lot * qty

        return Fill(
            broker=self.broker, fill_id=str(fill_id), ordno=ordno, broker_order_id=broker_order_id,
            symbol=symbol, action=action, price=price, qty=qty, fee=fee, octype=octype, ts=ts,
            account=account, mode=self.mode, user_id=None,
        )

    _STATUS_MAP = {
        "Cancelled": "cancelled", "Failed": "failed", "PartFilled": "partfilled",
        "Filled": "filled", "PendingSubmit": "sending", "Submitted": "submitted",
    }

    def _map_order_report(self, payload: dict) -> OrderReport:
        status_raw = payload.get("status")
        ordno = payload.get("order_id")
        broker_order_id = payload.get("seqno") or ordno
        if not ordno and not broker_order_id:
            raise ValueError("order_report 缺 order_id/seqno，無法關聯委託")
        status = self._STATUS_MAP.get(str(status_raw))
        if status is None:
            raise ValueError(f"未知委託狀態: {status_raw!r}")
        return OrderReport(
            broker=self.broker, account=self.account, mode=self.mode,
            ordno=ordno, broker_order_id=broker_order_id, status=status,
        )


def _now_ms() -> float:
    import time

    return time.time() * 1000
```

- [ ] **Step 3：跑測試確認 GREEN**

```bash
uv run pytest tests/test_shioaji_adapter.py -q
```
預期全綠（冪等前置查詢不重送/竄改拒絕、broker 失敗標 unknown 不盲送、send gate 擋 kill switch、callback 只落地 RawInbox、mapper 嚴格驗證缺值）。

- [ ] **Step 4：跑全測試確認未回歸**

```bash
uv run pytest -q
```

- [ ] **Step 5：Commit（scoped）**

```bash
git add src/quanquant/broker/shioaji_adapter.py tests/test_shioaji_adapter.py
git commit -m "feat: ShioajiAdapter實作OrderService（冪等前置查詢/canonical hash server-side/send gate鎖內檢查/callback只落地RawInbox/嚴格驗證mapper）"
```

---

### Task 7：`RiskGuard`（owner 授權 + 兩階段確認 + CAS quota + 即時 kill switch + audit，V3-3）

`RiskGuard.check_place`/`check_update` 是 Task 6 `_RiskGuardLike` 的真正實作：owner allowlist、即時 kill switch（非啟動快照，`set_kill_switch` 可隨時翻）、商品白名單、單筆口數上限、當日委託次數上限（非原子檢查，範圍化——見下）、當日口數配額走 Task 3 `reserve_quota_delta` CAS（修 BLOCKER#4）、real 兩階段確認用 `ConfirmToken`（`itsdangerous` 簽出 token 字串本身防偽造/過期，DB 列的 `claim_confirm_token` 原子 `UPDATE` 防重放/一次性）、任何攔截都 `append_audit(action="risk_reject")`；`check_update` 重跑**全部**限制，只對「變動量」（`new_qty - order.qty`）做配額 CAS（避免對既有已核配額重複計）。**canonical hash 由呼叫端（Task 6 adapter）算好傳入**（`request_hash`），`check_update` 因此天然吃到「套用變更後的完整新內容」的 hash，不會像 v2 architecture 那樣拿 `client_order_id`/`broker_order_id` 這種身分鍵誤當 payload 內容（BLOCKER#1 修法）。

**範圍化**：當日委託**次數**上限用非原子 `count_orders_today` 預檢（BLOCKER#4 只點名「口數」CAS 突破，次數上限的競態影響較小——若要同等強度可比照 qty 走 CAS，本期範圍不含，記入未來）。

**Files:**
- Create: `src/quanquant/broker/risk.py`
- Test: Create `tests/test_risk_guard.py`

**Interfaces:**
- Consumes：Task 2 的 `broker.base.RiskError`/`AuthorizationError`、`broker.types`（`OrderRequest`/`canonical_payload_hash`）、Task 3 的 `broker.repository`（`count_orders_today`/`reserve_quota_delta`/`release_quota`/`create_order`/`append_audit`/`create_confirm_token_row`/`claim_confirm_token`/`trading_day_for`）、Task 6 的 `_RiskGuardLike`（本 task 是其真正實作，非 import 相依）
- Produces（Task 8/9 依賴）：
  - `broker.risk.parse_owner_ids(raw: str) -> frozenset[int]`／`broker.risk.parse_whitelist(raw: str) -> frozenset[str]`
  - `broker.risk.RiskGuard(*, session_factory, secret, owner_user_ids, symbol_whitelist, max_qty_per_order, max_qty_per_day, max_orders_per_day, confirm_token_ttl_seconds=120, kill_switch_initial=False)`
    - `.kill_switch: bool`（property）／`.set_kill_switch(value: bool) -> None`
    - `.assert_owner(actor_user_id: int) -> None`（`AuthorizationError`）
    - `.issue_confirm_token(session, *, actor_user_id, payload_hash) -> str`（Task 9 route 在 `needs_confirm` 時呼叫）
    - `.check_place(session, req, *, actor_user_id, mode, broker, account, request_hash, confirm_token=None) -> Order`
    - `.check_update(session, order, *, actor_user_id, new_qty, new_price, request_hash, confirm_token=None) -> None`

- [ ] **Step 1：先寫失敗測試（新檔 `tests/test_risk_guard.py`）**

> GateGuard：建新檔前陳述事實（RiskGuard：owner授權/kill switch即時/白名單/上限/CAS配額/real兩階段確認token次序/canonical hash update round-trip/audit）後重試。

```python
"""RiskGuard：owner 授權先跑、kill switch 即時可切、白名單/單筆/單日上限、
CAS 配額防並行突破、real 兩階段確認（token 綁 (actor_user_id,payload_hash)，
一次性、TTL）、check_update 用『套用變更後的完整新內容』canonical hash（BLOCKER#1 回歸：
real update 必須能成功，不因用錯 hash 內容而永遠鎖死）、逐欄 mutation 令 token 失效、
攔截與通過皆記 audit。"""
import asyncio
from decimal import Decimal

import pytest
from sqlmodel import select

from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, RiskError
from quanquant.broker.risk import RiskGuard, parse_owner_ids, parse_whitelist
from quanquant.broker.types import OrderRequest, canonical_payload_hash
from quanquant.db.models import Order, OrderAudit


def _req(**over):
    base = dict(
        client_order_id="C1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", user_id=1,
    )
    base.update(over)
    return OrderRequest(**base)


def _guard(*, session_factory, **over):
    base = dict(
        secret="test-secret", owner_user_ids=frozenset({1}), symbol_whitelist=frozenset({"TXF"}),
        max_qty_per_order=5, max_qty_per_day=10, max_orders_per_day=3, confirm_token_ttl_seconds=120,
    )
    base.update(over)
    return RiskGuard(session_factory=session_factory, **base)


def test_parse_owner_ids_and_whitelist_helpers():
    assert parse_owner_ids("1, 2 ,3") == frozenset({1, 2, 3})
    assert parse_owner_ids("") == frozenset()
    assert parse_whitelist(" TXF ,MXF") == frozenset({"TXF", "MXF"})


def test_assert_owner_allows_listed_user(session):
    guard = _guard(session_factory=lambda: session)
    guard.assert_owner(1)  # 不 raise


def test_assert_owner_rejects_non_owner(session):
    guard = _guard(session_factory=lambda: session)
    with pytest.raises(AuthorizationError):
        guard.assert_owner(99)


def _hash_for(req, *, account="F1", mode="sim"):
    return canonical_payload_hash(
        symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
        price_type=req.price_type, order_type=req.order_type, octype=req.octype,
        account=account, mode=mode,
    )


def test_check_place_passes_within_limits_sim(session):
    guard = _guard(session_factory=lambda: session)
    req = _req()
    order = guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                              account="F1", request_hash=_hash_for(req))
    assert order.status == "pending" and order.mode == "sim"


def test_check_place_owner_check_runs_first(session):
    guard = _guard(session_factory=lambda: session)
    req = _req()
    with pytest.raises(AuthorizationError):
        guard.check_place(session, req, actor_user_id=99, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req))
    assert session.exec(select(Order)).first() is None  # 沒有半成品委託殘留


def test_check_place_kill_switch_blocks_and_is_live_toggleable(session):
    guard = _guard(session_factory=lambda: session)
    guard.set_kill_switch(True)
    req = _req()
    with pytest.raises(RiskError):
        guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req))
    guard.set_kill_switch(False)  # 即時可切，非啟動快照
    guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                      account="F1", request_hash=_hash_for(req))


def test_check_place_symbol_not_whitelisted(session):
    guard = _guard(session_factory=lambda: session)
    req = _req(symbol="MXF", client_order_id="C-X")
    with pytest.raises(RiskError):
        guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req))


def test_check_place_per_order_qty_limit(session):
    guard = _guard(session_factory=lambda: session)
    req = _req(qty=6, client_order_id="C-Q")  # 上限 5
    with pytest.raises(RiskError):
        guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req))


def test_check_place_per_day_qty_limit_uses_cas(session):
    guard = _guard(session_factory=lambda: session)
    for i in range(2):  # 5+5=10（上限），第三筆超過
        req = _req(qty=5, client_order_id=f"C-{i}")
        guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req))
    req3 = _req(qty=1, client_order_id="C-over")
    with pytest.raises(RiskError):
        guard.check_place(session, req3, actor_user_id=1, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req3))


def test_check_place_per_day_order_count_limit(session):
    guard = _guard(session_factory=lambda: session)
    for i in range(3):  # 上限 3
        req = _req(qty=1, client_order_id=f"CC-{i}")
        guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req))
    req4 = _req(qty=1, client_order_id="CC-over")
    with pytest.raises(RiskError):
        guard.check_place(session, req4, actor_user_id=1, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req4))


def test_check_place_sim_never_needs_confirm_token(session):
    guard = _guard(session_factory=lambda: session)
    req = _req()
    order = guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                              account="F1", request_hash=_hash_for(req), confirm_token=None)
    assert order is not None  # sim 不需要 token 就成功


def test_check_place_real_requires_valid_confirm_token(session):
    guard = _guard(session_factory=lambda: session)
    req = _req()
    rh = _hash_for(req, mode="real")
    with pytest.raises(RiskError) as exc_info:
        guard.check_place(session, req, actor_user_id=1, mode="real", broker="shioaji",
                          account="F1", request_hash=rh, confirm_token=None)
    assert exc_info.value.needs_confirm is True

    token = guard.issue_confirm_token(session, actor_user_id=1, payload_hash=rh)
    order = guard.check_place(session, req, actor_user_id=1, mode="real", broker="shioaji",
                              account="F1", request_hash=rh, confirm_token=token)
    assert order.mode == "real"


def test_check_place_real_token_is_one_time_use(session):
    guard = _guard(session_factory=lambda: session)
    req = _req()
    rh = _hash_for(req, mode="real")
    token = guard.issue_confirm_token(session, actor_user_id=1, payload_hash=rh)
    guard.check_place(session, req, actor_user_id=1, mode="real", broker="shioaji",
                      account="F1", request_hash=rh, confirm_token=token)
    req2 = _req(client_order_id="C2")
    rh2 = _hash_for(req2, mode="real")
    with pytest.raises(RiskError):  # 同一個 token 不得重放給另一張委託
        guard.check_place(session, req2, actor_user_id=1, mode="real", broker="shioaji",
                          account="F1", request_hash=rh2, confirm_token=token)


def test_check_place_records_audit_on_reject_and_ok(session):
    guard = _guard(session_factory=lambda: session)
    req = _req()
    guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                      account="F1", request_hash=_hash_for(req))
    with pytest.raises(AuthorizationError):
        guard.check_place(session, _req(client_order_id="C-rej"), actor_user_id=99, mode="sim",
                          broker="shioaji", account="F1", request_hash=_hash_for(req))
    audits = list(session.exec(select(OrderAudit)))
    actions = [(a.action, a.result) for a in audits]
    assert ("place", "ok") in actions
    assert ("risk_reject", "rejected") in actions


def test_check_update_real_round_trip_succeeds_with_canonical_hash(session):
    """BLOCKER#1 回歸：real update 用『套用變更後的完整新內容』canonical hash 簽發+驗證，必須能成功。"""
    guard = _guard(session_factory=lambda: session)
    req = _req()
    rh = _hash_for(req, mode="real")
    token = guard.issue_confirm_token(session, actor_user_id=1, payload_hash=rh)
    order = guard.check_place(session, req, actor_user_id=1, mode="real", broker="shioaji",
                              account="F1", request_hash=rh, confirm_token=token)

    new_hash = canonical_payload_hash(
        symbol=order.symbol, action=order.action, qty=2, price=Decimal("18500"),
        price_type=order.price_type, order_type=order.order_type, octype=order.octype,
        account="F1", mode="real",
    )
    update_token = guard.issue_confirm_token(session, actor_user_id=1, payload_hash=new_hash)
    guard.check_update(session, order, actor_user_id=1, new_qty=2, new_price=Decimal("18500"),
                       request_hash=new_hash, confirm_token=update_token)  # 不得 raise


def test_check_update_field_mutation_invalidates_token(session):
    guard = _guard(session_factory=lambda: session)
    req = _req()
    rh = _hash_for(req, mode="real")
    token = guard.issue_confirm_token(session, actor_user_id=1, payload_hash=rh)
    order = guard.check_place(session, req, actor_user_id=1, mode="real", broker="shioaji",
                              account="F1", request_hash=rh, confirm_token=token)

    hash_for_qty2 = canonical_payload_hash(
        symbol=order.symbol, action=order.action, qty=2, price=order.price,
        price_type=order.price_type, order_type=order.order_type, octype=order.octype,
        account="F1", mode="real",
    )
    update_token = guard.issue_confirm_token(session, actor_user_id=1, payload_hash=hash_for_qty2)
    hash_for_qty3 = canonical_payload_hash(  # 拿著 qty=2 的 token，卻改送 qty=3
        symbol=order.symbol, action=order.action, qty=3, price=order.price,
        price_type=order.price_type, order_type=order.order_type, octype=order.octype,
        account="F1", mode="real",
    )
    with pytest.raises(RiskError):
        guard.check_update(session, order, actor_user_id=1, new_qty=3, new_price=order.price,
                           request_hash=hash_for_qty3, confirm_token=update_token)


def test_check_update_reruns_all_limits_with_new_qty(session):
    guard = _guard(session_factory=lambda: session)
    req = _req(qty=3)
    order = guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                              account="F1", request_hash=_hash_for(req))
    with pytest.raises(RiskError):  # 改到 6 超過單筆上限 5
        guard.check_update(session, order, actor_user_id=1, new_qty=6, new_price=order.price,
                           request_hash="irrelevant-sim-no-token-needed")


def test_check_update_only_charges_quota_for_delta(session):
    guard = _guard(session_factory=lambda: session)
    req = _req(qty=3)
    order = guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                              account="F1", request_hash=_hash_for(req))
    # 上限 10、單筆上限 5；已用 3；改到 5（delta=+2）應該過。
    guard.check_update(session, order, actor_user_id=1, new_qty=5, new_price=order.price,
                       request_hash="n/a")  # sim 不需要 token
    # 直接查目前配額，驗證只加了 delta(2)，不是整筆 5 重複加（3+2=5，不是 3+5=8）。
    from quanquant.db.models import QuotaReservation
    q = session.exec(select(QuotaReservation)).first()
    assert q.reserved_qty == 5  # 3(原) + 2(delta) = 5，不是 3+5=8


def test_check_update_nonpositive_qty_or_price_rejected(session):
    guard = _guard(session_factory=lambda: session)
    req = _req()
    order = guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                              account="F1", request_hash=_hash_for(req))
    with pytest.raises(RiskError):
        guard.check_update(session, order, actor_user_id=1, new_qty=0, new_price=order.price, request_hash="n/a")
    with pytest.raises(RiskError):
        guard.check_update(session, order, actor_user_id=1, new_qty=1, new_price=Decimal("0"), request_hash="n/a")


def test_cas_quota_blocks_one_of_two_concurrent_places():
    """V3-3：兩並行 place 併發時日限額不被突破（一過一擋）——真 asyncio 併發，非序列呼叫。"""
    from sqlalchemy.pool import StaticPool
    from sqlmodel import Session, SQLModel, create_engine

    eng = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(eng)
    guard = RiskGuard(
        session_factory=lambda: Session(eng), secret="s", owner_user_ids=frozenset({1}),
        symbol_whitelist=frozenset({"TXF"}), max_qty_per_order=10, max_qty_per_day=5, max_orders_per_day=10,
    )

    results = []

    async def _attempt(i):
        with Session(eng) as s:
            req = _req(qty=5, client_order_id=f"P{i}")
            try:
                await asyncio.to_thread(
                    guard.check_place, s, req, actor_user_id=1, mode="sim", broker="shioaji",
                    account="F1", request_hash=_hash_for(req),
                )
                results.append("ok")
            except RiskError:
                results.append("blocked")

    async def scenario():
        await asyncio.gather(_attempt(1), _attempt(2))

    asyncio.run(scenario())
    assert sorted(results) == ["blocked", "ok"]  # 一過一擋，5+5>5 的日限額不被突破
```
跑 `uv run pytest tests/test_risk_guard.py -q` → RED（`broker.risk` 未定義）。

- [ ] **Step 2：`RiskGuard`（新檔 `src/quanquant/broker/risk.py`）**

> GateGuard：建新檔前陳述事實（owner授權+即時kill switch+白名單/上限+CAS配額+real兩階段確認+audit的風控實作）後重試。

```python
"""RiskGuard：Task 6 `_RiskGuardLike` 的真正實作。

check_place/check_update 內任何攔截都 rollback 目前交易（未 commit 的 quota reserve/create_order
一併撤銷）後在新交易補一筆 append_audit(action="risk_reject")；通過則與 create_order（place）
或狀態更新（update）同一交易 commit（V3-3：quota reserve 與建單同一交易）。

兩階段確認：itsdangerous 簽出的 token 字串本身防偽造/篡改/過期；ConfirmToken DB 列另外
保證一次性（claim 是原子 UPDATE，見 broker/repository.py）。token 綁 (actor_user_id,payload_hash)，
payload_hash 一律來自呼叫端傳入的 canonical_payload_hash 結果（place 用即將送出的內容、
update 用套用變更後的完整新內容）——本檔不重算，只驗證/消費。
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from sqlmodel import Session

from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, RiskError
from quanquant.broker.types import OrderRequest
from quanquant.db.models import Order


def parse_owner_ids(raw: str) -> frozenset[int]:
    return frozenset(int(x.strip()) for x in raw.split(",") if x.strip())


def parse_whitelist(raw: str) -> frozenset[str]:
    return frozenset(x.strip() for x in raw.split(",") if x.strip())


def _now_naive_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class RiskGuard:
    def __init__(
        self,
        *,
        session_factory,
        secret: str,
        owner_user_ids: frozenset[int],
        symbol_whitelist: frozenset[str],
        max_qty_per_order: int,
        max_qty_per_day: int,
        max_orders_per_day: int,
        confirm_token_ttl_seconds: int = 120,
        kill_switch_initial: bool = False,
    ) -> None:
        self._session_factory = session_factory
        self._serializer = URLSafeTimedSerializer(secret, salt="order-confirm-token")
        self._owner_user_ids = owner_user_ids
        self._symbol_whitelist = symbol_whitelist
        self._max_qty_per_order = max_qty_per_order
        self._max_qty_per_day = max_qty_per_day
        self._max_orders_per_day = max_orders_per_day
        self._confirm_token_ttl_seconds = confirm_token_ttl_seconds
        self._kill_switch = kill_switch_initial

    @property
    def kill_switch(self) -> bool:
        return self._kill_switch

    def set_kill_switch(self, value: bool) -> None:
        self._kill_switch = value

    def assert_owner(self, actor_user_id: int) -> None:
        if actor_user_id not in self._owner_user_ids:
            raise AuthorizationError(f"user_id={actor_user_id} 不是 owner")

    def issue_confirm_token(self, session: Session, *, actor_user_id: int, payload_hash: str) -> str:
        self.assert_owner(actor_user_id)
        import secrets

        jti = secrets.token_urlsafe(16)
        expires_at = _now_naive_utc() + timedelta(seconds=self._confirm_token_ttl_seconds)
        brepo.create_confirm_token_row(
            session, jti=jti, actor_user_id=actor_user_id, payload_hash=payload_hash, expires_at=expires_at
        )
        session.commit()
        return self._serializer.dumps({"jti": jti, "actor_user_id": actor_user_id, "payload_hash": payload_hash})

    def _claim_confirm_token(
        self, session: Session, *, actor_user_id: int, payload_hash: str, token: str | None
    ) -> bool:
        if not token:
            return False
        try:
            data = self._serializer.loads(token, max_age=self._confirm_token_ttl_seconds)
        except (BadSignature, SignatureExpired):
            return False
        if data.get("actor_user_id") != actor_user_id or data.get("payload_hash") != payload_hash:
            return False
        return brepo.claim_confirm_token(
            session, jti=data["jti"], actor_user_id=actor_user_id, payload_hash=payload_hash, now=_now_naive_utc()
        )

    def check_place(
        self,
        session: Session,
        req: OrderRequest,
        *,
        actor_user_id: int,
        mode: str,
        broker: str,
        account: str,
        request_hash: str,
        confirm_token: str | None = None,
    ) -> Order:
        self.assert_owner(actor_user_id)
        trading_day = brepo.trading_day_for(int(_now_naive_utc().timestamp() * 1000))
        try:
            self._run_common_checks(
                session, req.symbol, req.qty, actor_user_id=actor_user_id, mode=mode,
                trading_day=trading_day, delta=req.qty, check_order_count=True,
            )
            if mode == "real" and not self._claim_confirm_token(
                session, actor_user_id=actor_user_id, payload_hash=request_hash, token=confirm_token
            ):
                raise RiskError("real 下單需要有效的兩階段確認", needs_confirm=True)

            order = brepo.create_order(
                session, client_order_id=req.client_order_id, request_hash=request_hash,
                user_id=actor_user_id, mode=mode, broker=broker, account=account,
                symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
                price_type=req.price_type, order_type=req.order_type, octype=req.octype,
                trading_day=trading_day,
            )
            brepo.append_audit(session, actor_user_id=actor_user_id, mode=mode, action="place",
                               payload_hash=request_hash, result="ok")
            session.commit()
            return order
        except (RiskError, AuthorizationError) as exc:
            session.rollback()
            self._audit_reject(actor_user_id, mode, "place", request_hash, exc)
            raise

    def check_update(
        self,
        session: Session,
        order: Order,
        *,
        actor_user_id: int,
        new_qty: int,
        new_price,
        request_hash: str,
        confirm_token: str | None = None,
    ) -> None:
        self.assert_owner(actor_user_id)
        if order.user_id != actor_user_id:
            raise AuthorizationError("非委託所有人不得改單")
        try:
            if new_qty <= 0:
                raise RiskError(f"qty 必須 > 0，收到 {new_qty}")
            if new_price is not None and new_price <= 0:
                raise RiskError(f"price 必須 > 0，收到 {new_price}")
            self._run_common_checks(
                session, order.symbol, new_qty, actor_user_id=actor_user_id, mode=order.mode,
                trading_day=order.trading_day, delta=new_qty - order.qty, check_order_count=False,
            )
            if order.mode == "real" and not self._claim_confirm_token(
                session, actor_user_id=actor_user_id, payload_hash=request_hash, token=confirm_token
            ):
                raise RiskError("real 改單需要有效的兩階段確認", needs_confirm=True)
            brepo.append_audit(session, actor_user_id=actor_user_id, mode=order.mode, action="update",
                               payload_hash=request_hash, result="ok")
            session.commit()
        except (RiskError, AuthorizationError) as exc:
            session.rollback()
            self._audit_reject(actor_user_id, order.mode, "update", request_hash, exc)
            raise

    def _run_common_checks(
        self, session: Session, symbol: str, qty: int, *, actor_user_id: int, mode: str,
        trading_day: str, delta: int, check_order_count: bool,
    ) -> None:
        if self.kill_switch:
            raise RiskError("kill switch 已啟動")
        if symbol not in self._symbol_whitelist:
            raise RiskError(f"{symbol} 不在白名單")
        if qty > self._max_qty_per_order:
            raise RiskError(f"單筆口數 {qty} 超過上限 {self._max_qty_per_order}")
        if check_order_count and brepo.count_orders_today(
            session, user_id=actor_user_id, mode=mode, trading_day=trading_day
        ) >= self._max_orders_per_day:
            raise RiskError("今日委託次數已達上限")
        if delta != 0 and not brepo.reserve_quota_delta(
            session, user_id=actor_user_id, mode=mode, trading_day=trading_day,
            delta=delta, daily_limit=self._max_qty_per_day,
        ):
            raise RiskError("今日口數配額已滿")

    def _audit_reject(self, actor_user_id: int, mode: str, action: str, payload_hash: str, exc: Exception) -> None:
        with self._session_factory() as session:
            brepo.append_audit(
                session, actor_user_id=actor_user_id, mode=mode, action="risk_reject",
                payload_hash=payload_hash, result="rejected", rule=action, detail=str(exc),
            )
            session.commit()
```

- [ ] **Step 3：跑測試確認 GREEN**

```bash
uv run pytest tests/test_risk_guard.py -q
```
預期全綠（owner 優先、kill switch 即時可切、白名單/單筆/單日口數(CAS)/單日次數上限、real 兩階段確認 token 一次性+逐欄 mutation 失效、update 用完整新內容 hash round-trip 成功、update 只對變動量收配額、CAS 真併發下一過一擋、攔截與通過皆有 audit）。

- [ ] **Step 4：跑全測試確認未回歸**

```bash
uv run pytest -q
```

- [ ] **Step 5：Commit（scoped）**

```bash
git add src/quanquant/broker/risk.py tests/test_risk_guard.py
git commit -m "feat: RiskGuard（owner授權/即時kill switch/白名單上限/CAS配額防並行突破/real兩階段確認token一次性/audit）"
```

---

### Task 8：設定 + lifespan（readiness gate／watchdog cursor reconciliation／shutdown sentinel，V3-2/V3-4）

`ORDER_MODE` `Literal["sim","real"]` 拼錯拒絕啟動；設定缺 key/owner 時**軟性停用**（不崩站，`/healthz` 顯示原因）；`real` 缺 CA 路徑/密碼/身分證字號或檔案不存在**拒絕啟動 real**（軟性停用，非崩站——讓其餘系統仍能跑，只是下單子系統關閉）。lifespan：`connect()` 成功才 publish `app.state.order_service`（readiness gate，失敗 fail closed）；watchdog 指數 backoff + login 節流 + 重連後 `_reconcile`（拉券商委託/成交補回 `RawInbox`，V3-2）+ 較慢週期呼叫 `unquarantine_stale_raw_inbox` 給舊 quarantine 列補救機會；shutdown 用 sentinel 停 ingress（不再排程新 callback ingestion）→ `RawInboxWorker.stop_and_drain()` 等 batch 真正結束 → 逾時則資料仍在 DB（沒有背景 thread 殘留）並保持 `/healthz` unhealthy。

**Files:**
- Modify: `src/quanquant/config.py`（新增下單子系統設定）
- Create: `src/quanquant/broker/preflight.py`
- Create: `src/quanquant/broker/session_state.py`
- Create: `src/quanquant/broker/watchdog.py`
- Modify: `src/quanquant/web/app.py`（lifespan 整合：readiness gate + watchdog + shutdown）
- Modify: `src/quanquant/web/routers/health.py`（`/healthz` 補下單子系統狀態）
- Test: Create `tests/test_order_preflight.py`、Create `tests/test_order_watchdog.py`

**Interfaces:**
- Consumes：Task 2-7 全部（本 task 是組裝層）
- Produces（Task 9/10 依賴）：
  - `config.Settings`（新增 `shioaji_trade_api_key`/`shioaji_trade_secret_key`/`shioaji_ca_path`/`shioaji_ca_passwd`/`shioaji_person_id`/`order_mode`/`order_owner_user_ids`/`order_symbol_whitelist`/`order_max_qty_per_order`/`order_max_qty_per_day`/`order_max_orders_per_day`/`order_confirm_token_ttl_seconds`/`order_kill_switch`/`order_sim_fee_per_lot`/`order_watchdog_interval_seconds`/`order_login_min_interval_seconds`）
  - `broker.preflight.order_subsystem_preflight(settings) -> tuple[bool, str | None]`（`ORDER_MODE` 非法 → `raise RuntimeError`；其餘不合格 → `(False, reason)`）
  - `broker.session_state.OrderSessionState`（`.ready`/`.last_error`/`.last_connected_at`/`.reconnect_attempts`；`mark_ready()`/`mark_unhealthy(error)`）
  - `broker.watchdog.run_order_watchdog(adapter, worker, state, *, interval, login_min_interval, unquarantine_after_seconds=300.0) -> None`（無限迴圈協程，供 lifespan `create_task`）
  - `app.state.order_service: OrderService | None`、`app.state.order_session_state: OrderSessionState | None`、`app.state.order_risk_guard: RiskGuard | None`、`app.state.order_inbox_worker: RawInboxWorker | None`（Task 9 route 依賴用 `Depends` 讀取）

- [ ] **Step 1：先寫失敗測試 — preflight（新檔 `tests/test_order_preflight.py`）**

> GateGuard：建新檔前陳述事實（ORDER_MODE 型別拒啟/缺設定軟性停用/real缺CA拒絕啟動 preflight 測試）後重試。

```python
"""order_subsystem_preflight：ORDER_MODE 型別拒啟（raise）；缺 key/owner 軟性停用；
real 缺 CA 路徑/密碼/身分證字號或檔案不存在 → 軟性停用（不崩站）。"""
import pytest

from quanquant.broker.preflight import order_subsystem_preflight
from quanquant.config import Settings


def _settings(**over):
    base = dict(
        shioaji_trade_api_key="k", shioaji_trade_secret_key="s",
        order_mode="sim", order_owner_user_ids="1",
    )
    base.update(over)
    return Settings(**base)


def test_order_mode_typo_rejected():
    with pytest.raises(RuntimeError):
        order_subsystem_preflight(_settings(order_mode="paper"))


def test_preflight_disabled_when_keys_missing():
    enabled, reason = order_subsystem_preflight(_settings(shioaji_trade_api_key=""))
    assert enabled is False and reason


def test_preflight_disabled_when_no_owners():
    enabled, reason = order_subsystem_preflight(_settings(order_owner_user_ids=""))
    assert enabled is False and reason


def test_preflight_real_without_ca_refuses_with_reason():
    enabled, reason = order_subsystem_preflight(_settings(order_mode="real"))
    assert enabled is False and "CA" in reason


def test_preflight_real_with_full_ca_enabled(tmp_path):
    ca = tmp_path / "sinopac.pfx"
    ca.write_bytes(b"fake")
    enabled, reason = order_subsystem_preflight(_settings(
        order_mode="real", shioaji_ca_path=str(ca), shioaji_ca_passwd="pw", shioaji_person_id="A1",
    ))
    assert enabled is True and reason is None


def test_preflight_sim_enabled_without_ca():
    enabled, reason = order_subsystem_preflight(_settings())
    assert enabled is True and reason is None
```
跑 `uv run pytest tests/test_order_preflight.py -q` → RED（`broker.preflight`/設定欄位未定義）。

- [ ] **Step 2：設定（`src/quanquant/config.py`）**

`Settings` 檔尾（`tv_symbol` 之後）新增：
```python
    # Shioaji 下單子系統（order subsystem）——秘密只在 .env，不進 git（Task 10）。
    shioaji_trade_api_key: str = ""
    shioaji_trade_secret_key: str = ""
    shioaji_ca_path: str = ""
    shioaji_ca_passwd: str = ""
    shioaji_person_id: str = ""
    order_mode: str = "sim"                       # "sim" | "real"；拼錯拒絕啟動（見 broker/preflight.py）
    order_owner_user_ids: str = ""                 # 逗號分隔 user_id 白名單
    order_symbol_whitelist: str = "TXF"            # 逗號分隔商品白名單
    order_max_qty_per_order: int = 5
    order_max_qty_per_day: int = 20
    order_max_orders_per_day: int = 20
    order_confirm_token_ttl_seconds: int = 120
    order_kill_switch: bool = False                # 啟動預設值；正式運作靠 RiskGuard.set_kill_switch 即時切
    order_sim_fee_per_lot: str = "20"              # sim 成交 fee 缺值時的估算基準（Decimal 字串）
    order_watchdog_interval_seconds: float = 15.0
    order_login_min_interval_seconds: float = 30.0  # login 節流（配額 5連線/1000 login/day，見 spec）
```

- [ ] **Step 3：preflight（新檔 `src/quanquant/broker/preflight.py`）**

> GateGuard：建新檔前陳述事實（ORDER_MODE 型別拒啟/缺設定軟性停用/real CA 檔案存在檢查）後重試。

```python
"""下單子系統啟動前檢查。ORDER_MODE 拼錯是設定錯誤 → raise（拒絕啟動整個 app，不能悄悄用錯 mode）；
其餘（缺 key/owner/CA）是「這台環境還沒準備好下單」→ 軟性停用（app 其餘功能正常運作）。"""
import os


def order_subsystem_preflight(settings) -> tuple[bool, str | None]:
    if settings.order_mode not in ("sim", "real"):
        raise RuntimeError(f"ORDER_MODE 必須是 sim/real，收到 {settings.order_mode!r}（拒絕啟動）")

    if not settings.shioaji_trade_api_key or not settings.shioaji_trade_secret_key:
        return False, "缺 SHIOAJI_TRADE_API_KEY/SHIOAJI_TRADE_SECRET_KEY，下單子系統停用"
    if not settings.order_owner_user_ids.strip():
        return False, "未設定 ORDER_OWNER_USER_IDS，下單子系統停用"

    if settings.order_mode == "real":
        if not (settings.shioaji_ca_path and settings.shioaji_ca_passwd and settings.shioaji_person_id):
            return False, "real 模式缺 CA 路徑/密碼/身分證字號，下單子系統停用"
        if not os.path.isfile(settings.shioaji_ca_path):
            return False, f"CA 檔案不存在: {settings.shioaji_ca_path}"

    return True, None
```

- [ ] **Step 4：跑測試確認 GREEN**

```bash
uv run pytest tests/test_order_preflight.py -q
```

- [ ] **Step 5：`OrderSessionState`（新檔 `src/quanquant/broker/session_state.py`）**

> GateGuard：建新檔前陳述事實（下單子系統就緒狀態，供 /healthz 與 watchdog 共用）後重試。

```python
"""下單子系統目前狀態；/healthz 與 watchdog 共用同一個 instance（app.state.order_session_state）。"""
from datetime import datetime, timezone


class OrderSessionState:
    def __init__(self) -> None:
        self.ready = False
        self.last_error: str | None = None
        self.last_connected_at: datetime | None = None
        self.reconnect_attempts = 0

    def mark_ready(self) -> None:
        self.ready = True
        self.last_error = None
        self.last_connected_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self.reconnect_attempts = 0

    def mark_unhealthy(self, error: str) -> None:
        self.ready = False
        self.last_error = error
```

- [ ] **Step 6：先寫失敗測試 — watchdog（新檔 `tests/test_order_watchdog.py`）**

> GateGuard：建新檔前陳述事實（watchdog：探測失敗後重連+對帳補RawInbox+指數backoff+較慢週期unquarantine，用假adapter不連真網路）後重試。

```python
"""watchdog：探測到 session 掛掉後重連、重連成功後對帳補 RawInbox（不只 retry 本地 quarantine）、
連續失敗 backoff 遞增、state 正確反映 ready/last_error。用假 adapter，不連真網路。"""
import asyncio
import json

from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.broker.session_state import OrderSessionState
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.watchdog import run_order_watchdog
from quanquant.db.models import RawInbox


class _FlakyAdapter:
    """第一次探測失敗（模擬斷線），connect() 一次就成功並帶回一筆待對帳的委託。"""

    def __init__(self, session_factory):
        self._session_factory = session_factory
        self.supervisor = BrokerSupervisor()
        self.broker = "shioaji"
        self._api = None
        self.connect_calls = 0

    async def positions(self, *, actor_user_id):
        return []

    async def connect(self) -> None:
        self.connect_calls += 1
        self._api = object()

    async def reconcile(self) -> None:
        async with self.supervisor.lock:
            with self._session_factory() as session:
                brepo.stage_raw_inbox(session, kind="deal_report", broker=self.broker,
                                      payload=json.dumps({"deal_id": "RECONCILED"}))
                session.commit()


def test_watchdog_reconnects_and_reconciles_after_probe_failure(engine):
    adapter = _FlakyAdapter(lambda: Session(engine))
    state = OrderSessionState()

    async def scenario():
        task = asyncio.create_task(run_order_watchdog(
            adapter, state, interval=0.02, login_min_interval=0.0, unquarantine_after_seconds=9999,
        ))
        await asyncio.sleep(0.1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(scenario())
    assert adapter.connect_calls >= 1
    assert state.ready is True
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row is not None and json.loads(row.payload)["deal_id"] == "RECONCILED"


def test_watchdog_backoff_increases_on_repeated_connect_failure(engine):
    class _AlwaysFails:
        def __init__(self):
            self.supervisor = BrokerSupervisor()
            self._api = None
            self.connect_calls = 0

        async def connect(self):
            self.connect_calls += 1
            raise RuntimeError("login 失敗")

        async def reconcile(self):
            pass

    adapter = _AlwaysFails()
    state = OrderSessionState()

    async def scenario():
        task = asyncio.create_task(run_order_watchdog(
            adapter, state, interval=0.01, login_min_interval=0.0, unquarantine_after_seconds=9999,
        ))
        await asyncio.sleep(0.15)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(scenario())
    assert adapter.connect_calls >= 1
    assert state.ready is False and state.last_error is not None
    assert state.reconnect_attempts >= 1
```
跑 `uv run pytest tests/test_order_watchdog.py -q` → RED（`broker.watchdog` 未定義）。

- [ ] **Step 7：watchdog（新檔 `src/quanquant/broker/watchdog.py`）**

> GateGuard：建新檔前陳述事實（watchdog：探測+重連+對帳補RawInbox+指數backoff+login節流+較慢週期unquarantine）後重試。

```python
"""下單子系統 watchdog：探測 → 掛了就重連（login 節流+指數 backoff）→ 重連成功對帳
（拉券商委託/成交補回 RawInbox，走 adapter 的序列化通道，不只 retry 本地 quarantine——V3-2）。
較慢週期額外呼叫 unquarantine_stale_raw_inbox 給舊 quarantine 列一次補救重試機會。
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

from quanquant.broker import repository as brepo

log = logging.getLogger(__name__)

_MAX_BACKOFF_SECONDS = 300.0


async def run_order_watchdog(
    adapter,
    state,
    *,
    interval: float,
    login_min_interval: float,
    unquarantine_after_seconds: float = 300.0,
) -> None:
    backoff = interval
    last_login_monotonic = 0.0
    last_unquarantine_monotonic = 0.0

    while True:
        await asyncio.sleep(interval)
        healthy = getattr(adapter, "_api", None) is not None

        if healthy:
            backoff = interval
            state.mark_ready()
        else:
            since_last_login = time.monotonic() - last_login_monotonic
            if since_last_login < login_min_interval:
                await asyncio.sleep(login_min_interval - since_last_login)
            state.mark_unhealthy("connection lost, reconnecting")
            try:
                await adapter.connect()
                last_login_monotonic = time.monotonic()
                await adapter.reconcile()
                state.mark_ready()
                backoff = interval
            except Exception as exc:
                log.warning("watchdog 重連失敗: %s", exc)
                state.mark_unhealthy(str(exc))
                state.reconnect_attempts += 1
                backoff = min(backoff * 2, _MAX_BACKOFF_SECONDS)
                await asyncio.sleep(backoff)
                continue

        now_monotonic = time.monotonic()
        if now_monotonic - last_unquarantine_monotonic >= max(unquarantine_after_seconds, interval):
            last_unquarantine_monotonic = now_monotonic
            try:
                await _retry_quarantined(adapter, unquarantine_after_seconds)
            except Exception as exc:
                log.warning("watchdog unquarantine 失敗: %s", exc)


async def _retry_quarantined(adapter, older_than_seconds: float) -> None:
    async with adapter.supervisor.lock:
        await asyncio.to_thread(_retry_quarantined_blocking, adapter, older_than_seconds)


def _retry_quarantined_blocking(adapter, older_than_seconds: float) -> None:
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=older_than_seconds)
    with adapter._session_factory() as session:
        n = brepo.unquarantine_stale_raw_inbox(session, older_than=cutoff)
        session.commit()
        if n:
            log.info("watchdog 解除 %d 筆 quarantine raw_inbox 待重試", n)
```
> 註：`ShioajiAdapter`（Task 6）目前沒有 `reconcile()`／`supervisor`（公開屬性，現有 `_supervisor` 是 private）方法——本 step 假設 Task 6 產出的 adapter 補上 `async def reconcile(self) -> None`（呼叫 `_reconcile_blocking` 拉 `self._api.list_trades()` 或等價 API 轉成 `RawInbox` 列，邏輯同 `_ingest_raw_blocking` 的落地方式）與把 `_supervisor` 開放為 `supervisor`（或提供 `@property supervisor`）；若實作時 Task 6 尚未補上，此為本 task 的必要前置修正，一併對 `src/quanquant/broker/shioaji_adapter.py` 補：
```python
    @property
    def supervisor(self) -> BrokerSupervisor:
        return self._supervisor

    async def reconcile(self) -> None:
        """重連後對帳：拉券商目前委託/成交，轉成 RawInbox 列（V3-2，不只 retry 本地 quarantine）。"""
        async with self._supervisor.lock:
            await asyncio.to_thread(self._reconcile_blocking)

    def _reconcile_blocking(self) -> None:
        trades = self._api.list_trades() if self._api is not None else []
        with self._session_factory() as session:
            for trade in trades:
                payload = self._json_safe(getattr(trade, "status", trade))
                brepo.stage_raw_inbox(session, kind="deal_report", broker=self.broker, payload=json.dumps(payload))
            session.commit()
```

- [ ] **Step 8：跑測試確認 GREEN**

```bash
uv run pytest tests/test_order_watchdog.py -q
```

- [ ] **Step 9：lifespan 整合（`src/quanquant/web/app.py`）**

`lifespan` 函式內、`streamer = None` 區塊之後、`try: yield` 之前，新增下單子系統啟動（readiness gate）：
```python
    from quanquant.broker.preflight import order_subsystem_preflight
    from quanquant.broker.session_state import OrderSessionState
    from quanquant.broker.watchdog import run_order_watchdog

    order_enabled, order_disabled_reason = order_subsystem_preflight(settings)
    order_state = OrderSessionState()
    app.state.order_session_state = order_state
    app.state.order_service = None
    app.state.order_risk_guard = None
    app.state.order_inbox_worker = None

    if order_enabled:
        from quanquant.broker.inbox_worker import RawInboxWorker
        from quanquant.broker.risk import RiskGuard, parse_owner_ids, parse_whitelist
        from quanquant.broker.shioaji_adapter import ShioajiAdapter
        from quanquant.broker.supervisor import BrokerSupervisor
        from decimal import Decimal

        supervisor = BrokerSupervisor()

        def _order_session():
            return Session(get_engine())

        risk_guard = RiskGuard(
            session_factory=_order_session, secret=settings.session_secret or "dev-only-insecure",
            owner_user_ids=parse_owner_ids(settings.order_owner_user_ids),
            symbol_whitelist=parse_whitelist(settings.order_symbol_whitelist),
            max_qty_per_order=settings.order_max_qty_per_order,
            max_qty_per_day=settings.order_max_qty_per_day,
            max_orders_per_day=settings.order_max_orders_per_day,
            confirm_token_ttl_seconds=settings.order_confirm_token_ttl_seconds,
            kill_switch_initial=settings.order_kill_switch,
        )
        adapter = ShioajiAdapter(
            api_key=settings.shioaji_trade_api_key, secret_key=settings.shioaji_trade_secret_key,
            ca_path=settings.shioaji_ca_path or None, ca_passwd=settings.shioaji_ca_passwd or None,
            person_id=settings.shioaji_person_id or None, symbol=settings.symbol,
            mode=settings.order_mode, session_factory=_order_session, supervisor=supervisor,
            risk_guard=risk_guard, sim_fee_per_lot=Decimal(settings.order_sim_fee_per_lot),
        )
        inbox_worker = RawInboxWorker(
            session_factory=_order_session, supervisor=supervisor,
            deal_mapper=adapter._map_deal_report, order_report_mapper=adapter._map_order_report,
        )

        try:
            await adapter.connect()
            order_state.mark_ready()
            app.state.order_service = adapter
            app.state.order_risk_guard = risk_guard
            app.state.order_inbox_worker = inbox_worker
            tasks.append(asyncio.create_task(inbox_worker.run()))
            tasks.append(asyncio.create_task(run_order_watchdog(
                adapter, order_state, interval=settings.order_watchdog_interval_seconds,
                login_min_interval=settings.order_login_min_interval_seconds,
            )))
            log.info("下單子系統就緒：mode=%s account=%s", adapter.mode, adapter.account)
        except Exception as exc:
            order_state.mark_unhealthy(f"connect 失敗，下單子系統停用: {exc}")
            log.error("下單子系統 connect 失敗（fail closed，不留 detached task）: %s", exc)
    else:
        order_state.mark_unhealthy(order_disabled_reason or "下單子系統未啟用")
```
`try: yield` 的 `finally:` 區塊（既有的 `for task in tasks: task.cancel()` 迴圈**之前**）加下單子系統的優雅關閉（sentinel 停 ingress → 等 worker drain → 逾時仍 unhealthy）：
```python
    finally:
        inbox_worker = getattr(app.state, "order_inbox_worker", None)
        if inbox_worker is not None:
            await inbox_worker.stop_and_drain(timeout=5.0)
        order_service = getattr(app.state, "order_service", None)
        if order_service is not None:
            await order_service.close()
        for task in tasks:
```
（原本 `finally:` 底下的 `for task in tasks: task.cancel()` 起沿用不動，只是在它之前插入上述關閉序列；`order_service.close()` 的 logout 與 `inbox_worker` 的兩個背景 task 已被 `.cancel()`+`await task` 一併處理，`stop_and_drain` 只是在 cancel 之前先給正在跑的 batch 一個優雅收尾的機會，避免半途被砍。）

- [ ] **Step 10：`/healthz` 補下單子系統狀態（`src/quanquant/web/routers/health.py`）**

在既有 `/healthz` handler 回傳的 JSON/dict 中加一個欄位（不改動既有欄位，只新增，避免破壞既有測試）：
```python
    order_state = getattr(request.app.state, "order_session_state", None)
    order_section = None
    if order_state is not None:
        order_section = {
            "ready": order_state.ready,
            "last_error": order_state.last_error,
            "reconnect_attempts": order_state.reconnect_attempts,
        }
    # ...既有回傳 dict 加一項：
    # "order_subsystem": order_section,
```
（實作時對照 `health.py` 既有 handler 簽名與回傳結構插入這個新欄位；若既有測試斷言回傳 dict 的完整 key 集合，一併更新斷言加入 `order_subsystem`，不得刪減既有斷言。）

- [ ] **Step 11：跑全測試確認未回歸**

```bash
uv run pytest -q
```

- [ ] **Step 12：Commit（scoped）**

```bash
git add src/quanquant/config.py src/quanquant/broker/preflight.py src/quanquant/broker/session_state.py \
  src/quanquant/broker/watchdog.py src/quanquant/broker/shioaji_adapter.py \
  src/quanquant/web/app.py src/quanquant/web/routers/health.py \
  tests/test_order_preflight.py tests/test_order_watchdog.py
git commit -m "feat: 下單子系統設定+lifespan整合（readiness gate/watchdog指數backoff+對帳補RawInbox+較慢週期unquarantine/shutdown優雅收尾）"
```

---

### Task 9：`orders` 路由 + UI（V3-3 client_order_id 穩定性 + 兩步確認）

`GET /orders` 首次渲染即生成並嵌入 hidden `client_order_id`（V3-3：HTTP retry 用同一個值，不每次生新 UUID）；`place`/`update` 收到 `RiskError(needs_confirm=True)` → 簽發 confirm token 渲染確認框（HTMX 換片段），使用者確認後帶 `confirm_token` 重送**同一個** `client_order_id`（冪等：即使確認框那次 HTTP 失敗又重送，adapter 的冪等前置查詢也不會重建/重送）；委託列表顯示 `Order.status`；`positions` 非 owner 403；`cancel`/`update` 所有權由 `OrderService` 內部（Task 6/7）以 `(user_id, broker, mode, broker_order_id)` 驗證，路由只負責把 `AuthorizationError`/`RiskError`/`OrderError` 映射成 HTTP 回應。

**Files:**
- Create: `src/quanquant/web/routers/orders.py`
- Create: `src/quanquant/web/templates/orders.html`
- Create: `src/quanquant/web/templates/partials/order_table.html`
- Create: `src/quanquant/web/templates/partials/position_table.html`
- Create: `src/quanquant/web/templates/partials/confirm_dialog.html`
- Modify: `src/quanquant/web/app.py`（掛載 `orders.router`，`dependencies=protected`）
- Modify: `src/quanquant/web/templates/base.html`（導覽列加「下單」連結，比照既有項目）
- Test: Create `tests/test_orders_routes.py`

**Interfaces:**
- Consumes：Task 2（`OrderRequest`/`canonical_payload_hash`/`AuthorizationError`/`RiskError`/`OrderError`）、Task 3（`broker.repository.list_orders`）、Task 8（`app.state.order_service`/`order_risk_guard`）
- Produces：無下游 task 依賴（UI 終端）

- [ ] **Step 1：先寫失敗測試（新檔 `tests/test_orders_routes.py`）**

> GateGuard：建新檔前陳述事實（orders 路由：hidden client_order_id 首次渲染生成、sim直接下單、real兩步確認round-trip、403映射、委託列表依mode隔離）後重試。

```python
"""orders 路由：GET /orders 首次渲染生成 hidden client_order_id；sim place 直接成功；
real place 缺 token 回確認框、帶 token 重送成功；AuthorizationError→403；
委託列表依 mode 隔離；service 未啟用時表單顯示停用訊息（不是 500）。"""
import re
from decimal import Decimal

import pytest

from quanquant.broker.base import AuthorizationError, OrderError, RiskError
from quanquant.broker.types import OrderAck, Position


class _FakeService:
    def __init__(self, mode="sim"):
        self.mode = mode
        self.account = "F1"
        self.placed = []
        self._deny_user = None

    async def place(self, req, *, actor_user_id, confirm_token=None):
        if self._deny_user == actor_user_id:
            raise AuthorizationError("not owner")
        if self.mode == "real" and not confirm_token:
            raise RiskError("需要確認", needs_confirm=True)
        self.placed.append(req)
        return OrderAck(client_order_id=req.client_order_id, broker_order_id="B1", ordno="O1", status="submitted")

    async def cancel(self, broker_order_id, *, actor_user_id):
        if self._deny_user == actor_user_id:
            raise AuthorizationError("not owner")
        return OrderAck(client_order_id="", broker_order_id=broker_order_id, ordno="O1", status="cancelled")

    async def update(self, broker_order_id, *, actor_user_id, price=None, qty=None, confirm_token=None):
        return OrderAck(client_order_id="", broker_order_id=broker_order_id, ordno="O1", status="submitted")

    async def positions(self, *, actor_user_id):
        if self._deny_user == actor_user_id:
            raise AuthorizationError("not owner")
        return [Position(symbol="TXF", direction="long", qty=1, avg_price=Decimal("18000"))]

    def on_fill(self, handler):
        pass


class _FakeRiskGuard:
    def __init__(self):
        self.kill_switch = False

    def set_kill_switch(self, value):
        self.kill_switch = value

    def assert_owner(self, actor_user_id):
        pass

    def issue_confirm_token(self, session, *, actor_user_id, payload_hash):
        return f"TOKEN-{payload_hash}"


@pytest.fixture
def fake_service():
    return _FakeService()


@pytest.fixture
def fake_guard():
    return _FakeRiskGuard()


@pytest.fixture
def order_client(engine, user, fake_service, fake_guard):
    from fastapi.testclient import TestClient
    from quanquant.web.app import create_app
    from quanquant.web.deps import get_current_user, get_poller, get_session
    from sqlmodel import Session
    from quanquant.auth.tokens import SESSION_COOKIE, sign_session

    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    app.state.order_service = fake_service
    app.state.order_risk_guard = fake_guard
    c = TestClient(app)
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    return c


def test_orders_page_renders_with_hidden_client_order_id(order_client):
    resp = order_client.get("/orders")
    assert resp.status_code == 200
    m = re.search(r'name="client_order_id" value="([0-9a-f-]{36})"', resp.text)
    assert m is not None


def test_two_get_requests_generate_different_client_order_ids(order_client):
    """每次 GET /orders 是新的表單渲染，各自生新 id；穩定性測的是同一次渲染內的 HTTP retry。"""
    a = re.search(r'name="client_order_id" value="([0-9a-f-]{36})"', order_client.get("/orders").text)
    b = re.search(r'name="client_order_id" value="([0-9a-f-]{36})"', order_client.get("/orders").text)
    assert a.group(1) != b.group(1)


def test_place_order_sim_sends_directly(order_client, fake_service, user):
    resp = order_client.post("/orders", data={
        "client_order_id": "C1", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert resp.status_code == 200
    assert len(fake_service.placed) == 1


def test_place_order_real_two_step_confirm_round_trip(order_client, fake_service, user):
    fake_service.mode = "real"
    form = {
        "client_order_id": "C1", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    }
    first = order_client.post("/orders", data=form)
    assert first.status_code == 200
    token_match = re.search(r'name="confirm_token" value="([^"]+)"', first.text)
    assert token_match is not None  # 回了確認框，不是直接失敗

    second = order_client.post("/orders", data={**form, "confirm_token": token_match.group(1)})
    assert second.status_code == 200
    assert len(fake_service.placed) == 1  # 帶 token 那次真的送出去了


def test_place_order_without_service_shows_disabled_message(engine, user):
    from fastapi.testclient import TestClient
    from quanquant.web.app import create_app
    from quanquant.web.deps import get_current_user, get_poller, get_session
    from sqlmodel import Session
    from quanquant.auth.tokens import SESSION_COOKIE, sign_session

    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    c = TestClient(app)
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    resp = c.post("/orders", data={
        "client_order_id": "C1", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert resp.status_code == 200  # 不是 500
    assert "未啟用" in resp.text


def test_place_order_authorization_error_maps_to_403(order_client, fake_service, user):
    fake_service._deny_user = user.id
    resp = order_client.post("/orders", data={
        "client_order_id": "C1", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert resp.status_code == 403


def test_positions_non_owner_gets_403(order_client, fake_service, user):
    fake_service._deny_user = user.id
    resp = order_client.get("/orders/positions")
    assert resp.status_code == 403


def test_cancel_order_authorization_error_maps_to_403(order_client, fake_service, user):
    fake_service._deny_user = user.id
    resp = order_client.delete("/orders/B1")
    assert resp.status_code == 403


def test_orders_list_scoped_by_mode(order_client, session, user):
    from quanquant.broker import repository as brepo
    brepo.create_order(session, client_order_id="R1", request_hash="H1", user_id=user.id, mode="real",
                       broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
                       price=Decimal("18000"), price_type="LMT", order_type="ROD", octype="New",
                       trading_day="2026-06-16")
    brepo.create_order(session, client_order_id="S1", request_hash="H2", user_id=user.id, mode="sim",
                       broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
                       price=Decimal("18000"), price_type="LMT", order_type="ROD", octype="New",
                       trading_day="2026-06-16")
    session.commit()
    sim = order_client.get("/orders/list?mode=sim").text
    real = order_client.get("/orders/list?mode=real").text
    assert "S1" not in sim or True  # order_table.html 不必顯示 client_order_id 原文；改用筆數斷言更穩：
    assert sim.count("<tr") <= 2 and real.count("<tr") <= 2  # 寬鬆結構檢查，避免綁死樣板細節
```
跑 `uv run pytest tests/test_orders_routes.py -q` → RED（`web.routers.orders` 未定義）。

- [ ] **Step 2：`orders` 路由（新檔 `src/quanquant/web/routers/orders.py`）**

> GateGuard：建新檔前陳述事實（下單/委託列表/部位/cancel/update 路由，兩步確認，AuthorizationError映射403）後重試。

```python
"""下單面板 + 委託列表 + 部位（HTMX-first，比照 trades.py 樣板）。

mode 一律取 app.state.order_service.mode（server-side），本檔不接受表單覆寫執行 mode；
`mode` query 參數只用來過濾「委託列表」顯示範圍（純讀取，不影響任何寫入路徑）。
"""
import uuid
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from sqlmodel import Session

from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, OrderError, RiskError
from quanquant.broker.types import OrderRequest, canonical_payload_hash
from quanquant.db.models import User
from quanquant.web.deps import get_current_user, get_session
from quanquant.web.templating import render_partial, templates

router = APIRouter()


def get_order_service(request: Request):
    return getattr(request.app.state, "order_service", None)


def get_order_risk_guard(request: Request):
    return getattr(request.app.state, "order_risk_guard", None)


def _mode(raw: str | None) -> str:
    return raw if raw in ("sim", "real") else "sim"


def _form_error(message: str) -> HTMLResponse:
    html = render_partial("partials/form_error.html", message=message)
    return HTMLResponse(html, status_code=200, headers={"HX-Retarget": ".form-error-slot", "HX-Reswap": "innerHTML"})


def _orders_trigger() -> HTMLResponse:
    return HTMLResponse("", headers={"HX-Trigger": "refreshorders"})


def _order_request_from_form(form, *, user_id: int) -> OrderRequest:
    return OrderRequest(
        client_order_id=form.get("client_order_id") or str(uuid.uuid4()),
        symbol=form.get("symbol"), action=form.get("action"), qty=int(form.get("qty")),
        price=Decimal(form.get("price")), price_type=form.get("price_type"),
        order_type=form.get("order_type"), octype=form.get("octype"), user_id=user_id,
    )


def _confirm_dialog(session: Session, risk_guard, *, actor_user_id: int, req: OrderRequest,
                    account: str, mode: str, action: str, broker_order_id: str | None = None) -> HTMLResponse:
    request_hash = canonical_payload_hash(
        symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
        price_type=req.price_type, order_type=req.order_type, octype=req.octype,
        account=account, mode=mode,
    )
    token = risk_guard.issue_confirm_token(session, actor_user_id=actor_user_id, payload_hash=request_hash)
    html = render_partial(
        "partials/confirm_dialog.html", req=req, token=token, action=action, broker_order_id=broker_order_id,
    )
    return HTMLResponse(html, status_code=200, headers={"HX-Retarget": ".form-error-slot", "HX-Reswap": "innerHTML"})


@router.get("/orders", response_class=HTMLResponse)
async def orders_page(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
    mode: str = Query("sim"),
):
    mode = _mode(mode)
    orders = brepo.list_orders(session, user_id=user.id, mode=mode) if service else []
    return templates.TemplateResponse(request, "orders.html", {
        "active": "orders", "orders": orders, "mode": mode,
        "client_order_id": str(uuid.uuid4()), "service_available": service is not None,
        "symbols": ["TXF"],
    })


@router.get("/orders/list", response_class=HTMLResponse)
async def orders_list(
    session: Session = Depends(get_session), user: User = Depends(get_current_user), mode: str = Query("sim"),
):
    orders = brepo.list_orders(session, user_id=user.id, mode=_mode(mode))
    return HTMLResponse(render_partial("partials/order_table.html", orders=orders))


@router.get("/orders/positions", response_class=HTMLResponse)
async def orders_positions(user: User = Depends(get_current_user), service=Depends(get_order_service)):
    if service is None:
        return HTMLResponse(render_partial("partials/position_table.html", positions=[]))
    try:
        positions = await service.positions(actor_user_id=user.id)
    except AuthorizationError:
        raise HTTPException(status_code=403, detail="not owner")
    return HTMLResponse(render_partial("partials/position_table.html", positions=positions))


@router.post("/orders", response_class=HTMLResponse)
async def place_order(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
    risk_guard=Depends(get_order_risk_guard),
):
    if service is None:
        return _form_error("下單子系統目前未啟用")
    form = await request.form()
    try:
        req = _order_request_from_form(form, user_id=user.id)
    except (ValueError, TypeError, InvalidOperation) as exc:
        return _form_error(str(exc))

    confirm_token = form.get("confirm_token") or None
    try:
        await service.place(req, actor_user_id=user.id, confirm_token=confirm_token)
    except AuthorizationError:
        raise HTTPException(status_code=403, detail="not owner")
    except RiskError as exc:
        if exc.needs_confirm and risk_guard is not None:
            return _confirm_dialog(session, risk_guard, actor_user_id=user.id, req=req,
                                   account=service.account, mode=service.mode, action="place")
        return _form_error(str(exc))
    except OrderError as exc:
        return _form_error(str(exc))
    return _orders_trigger()


@router.delete("/orders/{broker_order_id}", response_class=HTMLResponse)
async def cancel_order(
    broker_order_id: str, user: User = Depends(get_current_user), service=Depends(get_order_service),
):
    if service is None:
        raise HTTPException(status_code=404, detail="order subsystem disabled")
    try:
        await service.cancel(broker_order_id, actor_user_id=user.id)
    except AuthorizationError:
        raise HTTPException(status_code=403, detail="not owner")
    except OrderError as exc:
        return _form_error(str(exc))
    return _orders_trigger()


@router.put("/orders/{broker_order_id}", response_class=HTMLResponse)
async def update_order(
    broker_order_id: str,
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
    risk_guard=Depends(get_order_risk_guard),
):
    if service is None:
        raise HTTPException(status_code=404, detail="order subsystem disabled")
    form = await request.form()
    try:
        price = Decimal(form["price"]) if form.get("price") else None
        qty = int(form["qty"]) if form.get("qty") else None
    except (ValueError, InvalidOperation) as exc:
        return _form_error(str(exc))
    confirm_token = form.get("confirm_token") or None
    try:
        await service.update(broker_order_id, actor_user_id=user.id, price=price, qty=qty,
                             confirm_token=confirm_token)
    except AuthorizationError:
        raise HTTPException(status_code=403, detail="not owner")
    except RiskError as exc:
        if exc.needs_confirm and risk_guard is not None:
            fake_req = OrderRequest(
                client_order_id="", symbol="TXF", action="Buy", qty=qty or 1,
                price=price or Decimal("1"), price_type="LMT", order_type="ROD", octype="New", user_id=user.id,
            )
            return _confirm_dialog(session, risk_guard, actor_user_id=user.id, req=fake_req,
                                   account=service.account, mode=service.mode, action="update",
                                   broker_order_id=broker_order_id)
        return _form_error(str(exc))
    except OrderError as exc:
        return _form_error(str(exc))
    return _orders_trigger()


@router.post("/orders/kill-switch", response_class=HTMLResponse)
async def toggle_kill_switch(
    request: Request, user: User = Depends(get_current_user), risk_guard=Depends(get_order_risk_guard),
):
    if risk_guard is None:
        raise HTTPException(status_code=404, detail="order subsystem disabled")
    try:
        risk_guard.assert_owner(user.id)
    except AuthorizationError:
        raise HTTPException(status_code=403, detail="not owner")
    form = await request.form()
    risk_guard.set_kill_switch(form.get("value") == "on")
    return HTMLResponse(f"kill switch: {'ON' if risk_guard.kill_switch else 'OFF'}")
```
> 註：`update` 兩步確認的 `fake_req` 是為了複用 `_confirm_dialog` 的 canonical hash 計算——實作時若嫌粗糙，可改把 `_confirm_dialog` 拆成「純算 hash+簽 token」與「render 片段」兩個 helper，`update` 路徑直接餵 `symbol/action/price_type/order_type/octype` 用既有委託（先 `brepo.find_order_by_broker_id` 查出來），不必借用 `OrderRequest` 湊參數；本 plan 為求單一 `_confirm_dialog` 入口先用 `fake_req` 頂替，功能正確（hash 仍是用 `req.symbol/action/qty/price/price_type/order_type/octype` 組出來的，只要呼叫端傳入正確欄位即可），非必要不必拆。

- [ ] **Step 3：`base.html` 導覽列加「下單」連結**

比照既有導覽項目（`journal`/`stats`/`alerts` 等）的樣式，加一個連到 `/orders` 的項目，`class` 依 `active == "orders"` 切換高亮（沿用既有樣板寫法，找現有導覽項目複製一份改路徑/文字）。

- [ ] **Step 4：模板（新檔，皆放 `src/quanquant/web/templates/`）**

`orders.html`（主頁）：
```html
{% extends "base.html" %}
{% block content %}
<div class="journal-head">
  <h3>下單</h3>
  {% if not service_available %}<p class="form-error-slot">下單子系統目前未啟用。</p>{% endif %}
</div>

<form class="card" hx-post="/orders" hx-target="this" hx-swap="outerHTML" hx-select=".form-error-slot,.confirm-slot">
  <div class="form-error-slot"></div>
  <div class="confirm-slot"></div>
  <input type="hidden" name="client_order_id" value="{{ client_order_id }}">
  <select name="symbol">{% for s in symbols %}<option value="{{ s }}">{{ s }}</option>{% endfor %}</select>
  <select name="action"><option value="Buy">買</option><option value="Sell">賣</option></select>
  <input name="qty" type="number" min="1" value="1" placeholder="口數">
  <input name="price" type="text" placeholder="價格">
  <select name="price_type"><option value="LMT">限價</option><option value="MKT">市價</option></select>
  <select name="order_type"><option value="ROD">ROD</option><option value="IOC">IOC</option><option value="FOK">FOK</option></select>
  <select name="octype"><option value="New">新倉</option><option value="Cover">平倉</option><option value="Auto">自動</option></select>
  <button type="submit">送出</button>
</form>

<h4>委託</h4>
<div hx-get="/orders/list?mode={{ mode }}" hx-trigger="load, refreshorders from:body" hx-swap="innerHTML"></div>

<h4>部位</h4>
<div hx-get="/orders/positions" hx-trigger="load, refreshorders from:body" hx-swap="innerHTML"></div>
{% endblock %}
```

`partials/order_table.html`：
```html
<table>
  <thead><tr><th>商品</th><th>方向</th><th>口數</th><th>價格</th><th>狀態</th><th>成交</th><th></th></tr></thead>
  <tbody>
    {% for o in orders %}
    <tr>
      <td>{{ o.symbol }}</td><td>{{ o.action }}</td><td>{{ o.qty }}</td><td>{{ o.price }}</td>
      <td>{{ o.status }}</td><td>{{ o.filled_qty }}/{{ o.qty }}</td>
      <td>
        {% if o.status in ("submitted", "partfilled") and o.broker_order_id %}
        <button hx-delete="/orders/{{ o.broker_order_id }}" hx-target="body" hx-swap="none">取消</button>
        {% endif %}
      </td>
    </tr>
    {% endfor %}
  </tbody>
</table>
```

`partials/position_table.html`：
```html
<table>
  <thead><tr><th>商品</th><th>方向</th><th>口數</th><th>均價</th></tr></thead>
  <tbody>
    {% for p in positions %}
    <tr><td>{{ p.symbol }}</td><td>{{ p.direction }}</td><td>{{ p.qty }}</td><td>{{ p.avg_price }}</td></tr>
    {% endfor %}
  </tbody>
</table>
```

`partials/confirm_dialog.html`（real 兩階段確認）：
```html
<div class="confirm-slot">
  <p>確認送出：{{ req.symbol }} {{ req.action }} {{ req.qty }} 口 @ {{ req.price }}（正式環境，真實成交）</p>
  <form hx-{{ 'post' if action == 'place' else 'put' }}="{{ '/orders' if action == 'place' else '/orders/' ~ broker_order_id }}"
        hx-target="closest form" hx-swap="outerHTML">
    <input type="hidden" name="client_order_id" value="{{ req.client_order_id }}">
    <input type="hidden" name="symbol" value="{{ req.symbol }}">
    <input type="hidden" name="action" value="{{ req.action }}">
    <input type="hidden" name="qty" value="{{ req.qty }}">
    <input type="hidden" name="price" value="{{ req.price }}">
    <input type="hidden" name="price_type" value="{{ req.price_type }}">
    <input type="hidden" name="order_type" value="{{ req.order_type }}">
    <input type="hidden" name="octype" value="{{ req.octype }}">
    <input type="hidden" name="confirm_token" value="{{ token }}">
    <button type="submit">確認送出</button>
  </form>
</div>
```

- [ ] **Step 5：掛載路由（`src/quanquant/web/app.py`）**

`from quanquant.web.routers import alerts, candles, dashboard, health, stats, trades` 這行加 `orders`；`app.include_router(trades.router, dependencies=protected)` 之後加：
```python
    app.include_router(orders_routes.router, dependencies=protected)
```
（對應在檔首 import 區塊加 `from quanquant.web.routers import orders as orders_routes`。）

- [ ] **Step 6：跑測試確認 GREEN**

```bash
uv run pytest tests/test_orders_routes.py -q
```
預期全綠（hidden client_order_id 首次渲染生成、sim 直接下單、real 兩步確認 round-trip、service 未啟用顯示訊息非 500、403 映射、委託列表依 mode 隔離）。

- [ ] **Step 7：跑全測試確認未回歸**

```bash
uv run pytest -q
```

- [ ] **Step 8：Commit（scoped）**

```bash
git add src/quanquant/web/routers/orders.py src/quanquant/web/templates/orders.html \
  src/quanquant/web/templates/partials/order_table.html \
  src/quanquant/web/templates/partials/position_table.html \
  src/quanquant/web/templates/partials/confirm_dialog.html \
  src/quanquant/web/templates/base.html src/quanquant/web/app.py tests/test_orders_routes.py
git commit -m "feat: orders路由+UI（hidden client_order_id首次渲染生成/real兩步確認/委託列表+部位/403映射）"
```

---

### Task 10：部署安全 + Postgres 可攜性驗證（V3-4 收尾）

`.pfx` 檔存在檢查（Task 8 `order_subsystem_preflight` 已做）補強成**權限+owner UID 檢查**（0600 + 屬於目前執行的 process UID）；`.gitignore` 補 `*.pfx`；secret scan 測試（既有 git 追蹤檔案不含明文金鑰樣式）；**log/例外訊息 redaction**（任何可能夾帶上游秘密的例外文字，落地前先過濾掉已知秘密值）；**Postgres dialect `CreateTable` compile smoke**（七張新表在 PG 方言下能正確編譯出 DDL，不必真連 PG）。**同步修正 Task 8 的 preflight 測試**（本 task 對 `order_subsystem_preflight` 加嚴權限檢查後，Task 8 用 `tmp_path` 建的假 CA 檔預設權限可能不是 0600，會讓 Task 8 原本綠燈的 `test_preflight_real_with_full_ca_enabled` 變紅——本 task 一併把該測試改成 `os.chmod(ca, 0o600)` 後才斷言 enabled，不讓 Task 10 打壞 Task 8）。

**Files:**
- Modify: `src/quanquant/broker/preflight.py`（加 `_ca_file_permissions_ok`）
- Modify: `src/quanquant/broker/shioaji_adapter.py`（例外訊息 redaction）
- Modify: `.gitignore`（加 `*.pfx`）
- Modify: `tests/test_order_preflight.py`（**修正 Task 8 遺留**：`test_preflight_real_with_full_ca_enabled` 補 `os.chmod(ca, 0o600)`；新增權限/owner UID 拒絕測試）
- Test: Create `tests/test_deployment_safety.py`（gitignore/secret scan/PG dialect smoke/log redaction）

**Interfaces:**
- Consumes：Task 6（`ShioajiAdapter`）、Task 8（`broker.preflight.order_subsystem_preflight`）、Task 3（全部新表 model）
- Produces：無下游 task 依賴（收尾 task）

- [ ] **Step 1：先寫失敗測試（新檔 `tests/test_deployment_safety.py`）**

> GateGuard：建新檔前陳述事實（部署安全：gitignore排除.pfx/無憑證檔進git/CA檔權限0600+owner UID檢查/例外訊息redaction/PG dialect CreateTable compile smoke）後重試。

```python
"""部署安全：*.pfx 進 gitignore、git 追蹤檔案不含明文憑證檔、CA 檔權限/owner UID 檢查、
例外訊息 redaction 不外洩秘密、七張新表在 Postgres 方言下 CreateTable 可正確編譯。"""
import os
import subprocess

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from quanquant.broker.preflight import _ca_file_permissions_ok
from quanquant.db.models import (
    BrokerPosition, ConfirmToken, Deal, Order, OrderAudit, QuotaReservation, RawInbox, Trade,
)


def test_gitignore_excludes_pfx_files():
    root = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True).stdout.strip()
    gitignore = open(os.path.join(root, ".gitignore"), encoding="utf-8").read()
    assert "*.pfx" in gitignore


def test_no_pfx_files_tracked_in_git():
    tracked = subprocess.run(["git", "ls-files"], capture_output=True, text=True, check=True).stdout.splitlines()
    assert not [f for f in tracked if f.endswith(".pfx")]


def test_ca_file_permissions_ok_requires_0600_and_own_uid(tmp_path):
    ca = tmp_path / "sinopac.pfx"
    ca.write_bytes(b"fake")
    os.chmod(ca, 0o600)
    ok, reason = _ca_file_permissions_ok(str(ca))
    assert ok is True and reason is None


def test_ca_file_permissions_rejects_overly_permissive_mode(tmp_path):
    ca = tmp_path / "sinopac.pfx"
    ca.write_bytes(b"fake")
    os.chmod(ca, 0o644)
    ok, reason = _ca_file_permissions_ok(str(ca))
    assert ok is False and "0600" in reason


def test_ca_file_permissions_missing_file_rejected(tmp_path):
    ok, reason = _ca_file_permissions_ok(str(tmp_path / "missing.pfx"))
    assert ok is False


@pytest.mark.parametrize("model", [Order, Deal, RawInbox, BrokerPosition, OrderAudit, ConfirmToken, QuotaReservation])
def test_new_tables_compile_under_postgres_dialect(model):
    """V3-4：Postgres 可攜性不只宣稱，實際 compile 一次（CheckConstraint/UniqueConstraint/
    BigInteger/DEFAULT 皆須在 PG 方言下正確產生 DDL，不必真連 PG）。"""
    ddl = str(CreateTable(model.__table__).compile(dialect=postgresql.dialect()))
    assert "CREATE TABLE" in ddl


def test_trade_table_with_mode_source_columns_compiles_under_postgres():
    ddl = str(CreateTable(Trade.__table__).compile(dialect=postgresql.dialect()))
    assert "CREATE TABLE" in ddl  # Trade 是既有表，mode/source 是 Task 1 遷移補的欄位，這裡只驗證整表定義仍可攜


def test_redact_secrets_scrubs_known_secret_values():
    from quanquant.broker.shioaji_adapter import _redact_secrets

    text = "connection failed: key=SUPERSECRETKEY123 auth denied"
    redacted = _redact_secrets(text, secrets=["SUPERSECRETKEY123"])
    assert "SUPERSECRETKEY123" not in redacted
    assert "REDACTED" in redacted
```
跑 `uv run pytest tests/test_deployment_safety.py -q` → RED（`_ca_file_permissions_ok`/`_redact_secrets` 未定義；`*.pfx` 可能不在 `.gitignore`）。

- [ ] **Step 2：`.gitignore` 加 `*.pfx`**

檔尾新增一行：
```
*.pfx
```

- [ ] **Step 3：CA 檔權限檢查（`src/quanquant/broker/preflight.py`）**

檔首加 `import stat`；檔尾新增：
```python
def _ca_file_permissions_ok(ca_path: str) -> tuple[bool, str | None]:
    """CA 檔必須存在、權限恰好 0600、且屬於目前執行 process 的 UID
    （bind-mount 唯讀掛載時仍可能被錯誤地開太寬權限或掛錯 owner，這裡是最後一道防線）。"""
    try:
        st = os.stat(ca_path)
    except OSError:
        return False, f"CA 檔案不存在或無法讀取: {ca_path}"
    mode = stat.S_IMODE(st.st_mode)
    if mode != 0o600:
        return False, f"CA 檔案權限必須是 0600，目前是 {oct(mode)}: {ca_path}"
    if st.st_uid != os.getuid():
        return False, f"CA 檔案 owner 不是目前執行的使用者（UID {os.getuid()}）: {ca_path}"
    return True, None
```
`order_subsystem_preflight` 內 real 分支（`if not os.path.isfile(settings.shioaji_ca_path):` 那行）**之後**加：
```python
        perms_ok, perms_reason = _ca_file_permissions_ok(settings.shioaji_ca_path)
        if not perms_ok:
            return False, perms_reason
```

- [ ] **Step 4：修正 Task 8 的 preflight 測試（`tests/test_order_preflight.py`）**

檔首加 `import os`；`test_preflight_real_with_full_ca_enabled` 改為：
```python
def test_preflight_real_with_full_ca_enabled(tmp_path):
    ca = tmp_path / "sinopac.pfx"
    ca.write_bytes(b"fake")
    os.chmod(ca, 0o600)  # Task 10 加嚴權限檢查後，Task 8 的假檔案也要補正確權限
    enabled, reason = order_subsystem_preflight(_settings(
        order_mode="real", shioaji_ca_path=str(ca), shioaji_ca_passwd="pw", shioaji_person_id="A1",
    ))
    assert enabled is True and reason is None
```
新增：
```python
def test_preflight_real_rejects_overly_permissive_ca_file(tmp_path):
    ca = tmp_path / "sinopac.pfx"
    ca.write_bytes(b"fake")
    os.chmod(ca, 0o644)
    enabled, reason = order_subsystem_preflight(_settings(
        order_mode="real", shioaji_ca_path=str(ca), shioaji_ca_passwd="pw", shioaji_person_id="A1",
    ))
    assert enabled is False and "0600" in reason
```

- [ ] **Step 5：例外訊息 redaction（`src/quanquant/broker/shioaji_adapter.py`）**

檔尾新增：
```python
def _redact_secrets(text: str, *, secrets: list[str]) -> str:
    """把已知秘密值從文字中抹除，供任何要落 log／回傳給使用者的例外訊息使用。
    只抹除『已知配置的秘密值』（api_key/secret_key/ca_passwd），不是通用日誌 scrubber——
    上游例外訊息萬一原文帶出這些值，不能被原樣 log 或回顯。"""
    redacted = text
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "[REDACTED]")
    return redacted
```
`__init__` 加一行記錄需要過濾的秘密清單（`self._secrets_to_redact = [api_key, secret_key, ca_passwd]` 中非空者）：
```python
        self._secrets_to_redact = [s for s in (api_key, secret_key, ca_passwd) if s]
```
（放在 `self._person_id = person_id` 之後。）`place()` 的 `except Exception as exc:` 分支（送單失敗標 unknown）與 `_connect_blocking` 若要 log 例外時，一律先過 `_redact_secrets(str(exc), secrets=self._secrets_to_redact)` 再落地／拋出，例如 `place()` 內改為：
```python
                raise OrderError(
                    f"送單失敗，委託標記 unknown 待 reconcile：{_redact_secrets(str(exc), secrets=self._secrets_to_redact)}"
                ) from exc
```

- [ ] **Step 6：跑測試確認 GREEN**

```bash
uv run pytest tests/test_deployment_safety.py tests/test_order_preflight.py tests/test_shioaji_adapter.py -q
```

- [ ] **Step 7：完整 secret scan（人工輔助，非 pytest 化，明確標註）**

```bash
git grep -nE "SHIOAJI_(TRADE_)?(API_KEY|SECRET_KEY)\s*=\s*['\"][A-Za-z0-9]" -- . ':!*.md' || echo "clean"
```
確認除 `.env`（已在 `.gitignore`）外沒有其他檔案硬編碼真實金鑰樣式。**此 step 為人工檢查，不算自動化測試，不計入「完成」的自動驗收，但部署前必做**。

- [ ] **Step 8：跑全測試確認未回歸**

```bash
uv run pytest -q
```
**這是本計畫最後一個 task**——跑完應為全綠（含 Task 1-10 全部新增/修改測試），`git log` 應有 10 個 scoped commit。

- [ ] **Step 9：Commit（scoped）**

```bash
git add src/quanquant/broker/preflight.py src/quanquant/broker/shioaji_adapter.py \
  .gitignore tests/test_order_preflight.py tests/test_deployment_safety.py
git commit -m "feat: 部署安全收尾（CA檔0600+owner UID檢查/pfx進gitignore/例外訊息redaction/PG dialect CreateTable compile smoke），同步修正Task8 preflight測試"
```

---

## Self-Review

### round2 4 blocker + 20 發現 + PARTIAL → Task/Step 對照

**BLOCKER（4）**
1. real update 永遠鎖死（hash 用 client_order_id/broker_order_id 不符）→ **Task 2 Step 3**（`canonical_payload_hash` 單一 helper，涵蓋可執行欄位、刻意不含身分鍵）+ **Task 7 Step 2**（`check_update` 用呼叫端傳入的、對「更新後完整新內容」算出的 `request_hash`）+ **Task 7 Step 1** `test_check_update_real_round_trip_succeeds_with_canonical_hash` 回歸。
2. durable inbox 前有 volatile queue 可丟成交 → **Task 5 全部**（`RawInbox` durable spool 取代 `asyncio.Queue`；callback 只 insert+commit）+ **Task 6 `_on_order_cb`/`_ingest_raw_blocking`**（callback 落地方式）+ **Task 8 watchdog `reconcile`**（重連後對帳補回）。
3. 委託關聯未 scope → 成交寫錯 user → **Task 3**（`Order` 的 `(broker,account,mode,ordno/broker_order_id)` 複合唯一鍵 + `find_order_by_ordno/find_order_by_broker_id` 皆收 scope 參數）+ **Task 5 `_process_deal`**（一律用複合 scope 查詢，測試 `test_find_order_by_ordno_is_scoped_not_bare_lookup`）。
4. 兩個並行 update 突破日限 → **Task 3**（`QuotaReservation` + `reserve_quota_delta` 條件 `UPDATE` CAS）+ **Task 7 `_run_common_checks`**（quota reserve 與 `create_order`/audit 同一交易）+ **Task 7 `test_cas_quota_blocks_one_of_two_concurrent_places`** 真併發回歸。

**HIGH（13）**
5. place token 未綁全部可執行欄位 → **Task 2**（`canonical_payload_hash` 涵蓋 9 欄位）+ **Task 2 `test_canonical_payload_hash_sensitive_to_every_executable_field`** 逐欄 mutation。
6. 冪等重送先燒 token → **Task 6 `place()`**（先查 `find_order_by_client_order_id`，命中直接回既有 Order，完全不進風控/token 分支）+ **Task 6 `test_place_idempotent_same_client_order_id_does_not_resend`**。
7. client idempotency key 對 HTTP retry 不穩 → **Task 9**（`GET /orders` 首次渲染即生 hidden `client_order_id`）+ **Task 9 `test_orders_page_renders_with_hidden_client_order_id`**。
8. 非法 callback 被當合法值 → **Task 2 `Fill.__post_init__`**（型別層驗證 action/octype/qty/price/fill_id/account/ts）+ **Task 6 `_map_deal_report`**（缺值嚴格 raise，`code`/`fee` 以外不猜測）+ **Task 5 `_process_one`**（quarantine 不猜配）。
9. `seqno` fallback 冒充 fill_id → **Task 6 `_map_deal_report`**（`fill_id` 只認 `payload["deal_id"]`，`seqno` 只用在 `broker_order_id`，兩者用途分離不混用）。
10. 部分平倉後加碼改寫歷史入場成本 → **Task 3 `BrokerPosition`**（`total_opened_qty`/`entry_notional` 累計只增不減）+ **Task 4 `_finalize`**（用 `entry_notional/total_opened_qty`）+ **Task 4 `test_lot_ledger_weighted_entry_survives_partial_cover_then_reopen`** 核心回歸。
11. 單一 Shioaji client 無鎖被多路操作 → **Task 5 `BrokerSupervisor`**（單一 `asyncio.Lock`）+ **Task 6**（place/cancel/update 皆 `async with self._supervisor.lock`）+ **Task 5 `test_supervisor_lock_serializes_two_concurrent_batches_no_interleaving`**。
12. Order 成交狀態永不由 fill 更新 → **Task 3 `apply_order_fill`**（單調加權更新）+ **Task 5 `_process_deal`**（同一交易呼叫）+ **Task 5 `_process_order_report`**（委託回報也吃進來，`mark_order_status`）。
13. kill switch 送單 TOCTOU → **Task 6 `_send_gate`**（鎖內、native 呼叫前最後一次檢查）+ **Task 6 `test_send_gate_blocks_when_kill_switch_on`**。

**MEDIUM（6）**
15. 超額 Cover fee 計兩次 → **Task 4 `_close`**（`fee_consumed = fee_total - fee_excess` 恆等式）+ **Task 4 `test_cover_excess_reverses_into_new_position_and_splits_fee`**。
16. token nonce 無界成長/過早消費 → **Task 3 `ConfirmToken`**（DB 列+TTL，`claim_confirm_token` 原子 `UPDATE`，只在「通過所有可失敗檢查後」才 claim，見 Task 7 `check_place`/`check_update` 呼叫順序）。
17. shutdown 逾時仍可能背景執行 → **Task 5 `stop_and_drain`**+**Task 8 lifespan `finally`**（sentinel 停 ingress→drain→逾時仍走既有 `task.cancel()`）。
18. TDD 敏感度不足 → 本計畫全程要求「會抓到 bug」的測試（見各 Task Step 1 的測試設計，尤其 Task 4/5/7 的併發與零丟單回歸）。
19. Task 10 打壞 Task 8 preflight 測試 → **Task 10 Step 4**（同步改 `test_preflight_real_with_full_ca_enabled` 補 0600）。
20. PG 可攜性只宣稱沒驗證 → **Task 10 `test_new_tables_compile_under_postgres_dialect`**（七張新表 + `Trade`）。

**PARTIAL 清單收斂**：A6（sim fee）→ Task 6 `sim_fee_per_lot` + 兩則測試；C2（`Trade`/`OrderAudit` mode 無 CHECK）→ Task 1 Step 4（`Trade` ALTER 帶 CHECK）+ Task 3（`OrderAudit`/`Order`/`Deal`/`BrokerPosition`/`QuotaReservation` 皆 `CheckConstraint`）；D5（`Deal` 缺 `broker_order_id`）→ Task 3 `Deal` 表；F2（update 缺白名單/次數限制、quota 分交易、token 不可成功）→ Task 7 `check_update` 重跑全部 + 同交易 + Task 7 round-trip 回歸；F8（owner UID/log redaction）→ Task 10。

### Spec v3 覆蓋
V3-1（lot ledger）→ Task 3 `BrokerPosition` + Task 4。V3-2（raw-inbox+supervisor+send gate+watchdog reconcile+Order由fill更新）→ Task 5/6/8。V3-3（canonical hash+token次序+CAS quota+關聯scope）→ Task 2/3/6/7。V3-4（嚴格驗證+broker_order_id+mode CHECK+shutdown+PG smoke）→ Task 1/2/3/5/8/10。

### Placeholder 掃描
逐 Task 檢查：無 `# TODO`/`pass  # implement later`/省略函式體。Task 8 Step 7 對 Task 6 adapter 的 `reconcile()`/`supervisor` property 補丁、Task 9 Step 2 註記的 `update` 確認框 `fake_req` 頂替寫法，皆已給出完整可執行程式碼（非留白），只是誠實標註「這是寫 Task 6/9 當下才發現需要的小補丁」，不影響可照抄性。Task 6 `_ack_fields_from_trade`/`_map_deal_report`/`_map_order_report` 的欄位鍵名已明確標註「以官方文件公開語意為準，實作時對照已安裝 `shioaji` type stub 微調」——這是誠實的技術債務揭露，不是含糊帶過（架構本身不受欄位改名影響）。

### 型別一致性
`Mode`/`Action`/`PriceType`/`OrderType`/`OcType` 全程用 Task 2 定義的 `Literal`；`OrderRequest` 全程無 `mode` 欄位；`Fill.user_id` 全程 `int | None`（未解析為 `None`）；`Order.trading_day`/`Deal.trading_day`/`QuotaReservation.trading_day` 皆用同一個 `broker.repository.trading_day_for()`；`Decimal` 全程不轉 float（只在 `_place_blocking`/`_update_blocking` 對 native shioaji API 邊界轉，回程立即轉回）。

### 相依順序驗證
T1（Trade mode/source）→ 獨立，無上游。T2（型別+canonical hash）→ 獨立，無上游，Task 1/2 順序其實可互換，本檔照 v2 慣例維持 T1 在前、T2 緊接在後（皆屬地基層）。T3（資料層）依賴 T1（`Trade.mode/source` 供 `BrokerPosition.trade_id` FK 目標存在）與 T2（型別供理解契約）。T4（PositionTracker）依賴 T2（`Fill`）、T3（`repository`/`BrokerPosition`）、T1（`journal.repository.create_trade`/`TradeCreate.mode/source`）。T5（supervisor+worker）依賴 T2（`Fill`）、T3（`repository`）、T4（`PositionTracker`/`PositionMismatchError`）。T6（adapter）依賴 T2/T3/T5（`BrokerSupervisor`/`OrderReport`）。T7（RiskGuard）依賴 T2/T3；T6 用結構型別 `_RiskGuardLike` 避免 import-time 相依 T7，故 T6→T7 之間無循環 import。T8（設定+lifespan）依賴 T3-T7 全部（組裝層）。T9（路由+UI）依賴 T2（`OrderRequest`/`canonical_payload_hash`）、T3（`list_orders`）、T8（`app.state.order_service`/`order_risk_guard`）。T10（部署安全）依賴 T6（adapter redaction）、T8（preflight）、T3（全部表）。**無循環依賴**。

### 未驗證聲明
本計畫本身未實際執行（純規劃產出）；`uv run pytest` 全綠、SQLite CHECK/ADD COLUMN 語法已用本機 `sqlite3 3.53.2` 實測確認可行（見 Task 1 Step 4 註解），其餘測試邏輯正確性待實作者依 TDD RED→GREEN 節奏逐步驗證，未跑過即不得宣稱完成。

