# Shioaji 期貨下單整合 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development（推薦）或 superpowers:executing-plans 依 task 順序實作。Steps 用 checkbox（`- [ ]`）追蹤。
>
> **本計畫已依 codex 外部審查全面改寫**（見 `docs/superpowers/reviews/2026-07-24-shioaji-order-codex-review.md` 發現 A–H）。真理來源為 `docs/superpowers/specs/2026-07-23-shioaji-order-integration-design.md`（2026-07-24 修訂版，233 行）。**舊版計畫（`git log` 可查）架構已過時（誤把 broker 成交直接寫進手動 `Trade` 表、`Deal` 用 `seqno` 單鍵去重、mode 信任表單、無 owner 授權、無兩階段確認）——本檔為全新改寫，不沿用其架構，僅借用其 TDD 格式。**

**Goal：** 為 QuanQuant 加上單人自用的 Shioaji（永豐）期貨下單（server-side mode 強制、owner 授權、兩階段確認、風控 fail closed、即時 kill switch），成交回報透過 durable inbox 冪等處理，經**持久化 broker 部位帳務**（`BrokerPosition`，與手動日誌完全隔離）完成 round-trip 後才自動寫一筆 `source=shioaji` 的交易日誌並反映 `/stats`；模擬（sim）／正式（real）以 `mode` 第一級 scope 分流、絕不混算。

**Architecture：**
- `broker/types.py`＋`broker/base.py`：broker 無關純型別（`OrderRequest`/`OrderAck`/`Fill`/`Position`/`RiskDecision`）與 `OrderService` Protocol（`mode` 屬性 + `place/cancel/update/positions` 皆收 `actor_user_id`）。
- 四張新表：`Order`（`client_order_id` 唯一冪等鍵）、`Deal`（durable fill inbox，`unique(broker,mode,account,trading_day,fill_id)`，`processed`/`quarantine` 追蹤）、`BrokerPosition`（持久化部位帳務，與 `Trade` 完全分表、只在 round-trip 完成時回寫一筆 `Trade(source="shioaji")`）、`OrderAudit`（append-only 稽核）。
- `ShioajiAdapter` 實作 `OrderService`：lazy-import shioaji、`activate_ca`(real)／`simulation=True`(sim)、送單前先落地 pending correlation（`client_order_id`→user/mode 佔位）防 callback 早於 ack、`set_order_callback`→`loop.call_soon_threadsafe`→只 enqueue 進 `FillWorker`（單一有序＋backpressure，每筆開新 Session，Deal insert＋部位帳務＋Trade 寫入＋`processed=true` 同一交易提交）。
- `RiskGuard`：owner allowlist、qty/price/白名單/單筆單日上限、**即時**（非啟動快照）kill switch、real 兩階段確認 token（短效一次性綁 `(actor_user_id, payload_hash)`）、`update` 重跑全部風控、append-only `OrderAudit`。
- lifespan：readiness gate（`await connect()` 成功才 publish `app.state.order_service`，失敗 fail closed 反映 `/healthz`）、watchdog（重連＋backoff＋重連後對帳）、shutdown（停 callback→drain inbox→關 loop）。
- `mode` 穿過 `Trade`／`list_trades`／`list_for_stats`／journal・stats・orders 三個 filter 表單＋分頁 tab；`stats/metrics.py` 零改。`Trade` 加 `source`（manual/shioaji）與手動 `find_open_trade` 完全隔離自動部位帳務（A1 修正）。
- **明訂單一 uvicorn worker**（Dockerfile 無 `--workers`）：quota reservation＋建單、Deal 去重＋部位更新＋Trade 寫入的原子性皆建立在此不變量上，不額外加 DB row lock。

**Tech Stack：** FastAPI + SQLModel（雙方言 SQLite/Postgres）、HTMX/Jinja2、`shioaji`（已是 core dep，`pyproject.toml:21`）、pytest（in-memory SQLite `StaticPool`、`TestClient` 簽章 cookie）、`asyncio.to_thread`＋`loop.call_soon_threadsafe` 跨執行緒、`itsdangerous`（已是 dep，`auth/tokens.py` 已用；本計畫拿來做確認 token）。

## Global Constraints

- **雙方言可攜**：新表/欄位必須 `SQLModel.metadata.create_all` 可自動建（新表）或 `db/migrate.py` 的 `ensure_columns`（nullable ADD COLUMN，可帶 `DEFAULT`）可攜；任何 epoch-ms 欄位必用 `sqlalchemy.BigInteger`（4-byte INTEGER 會溢位 Postgres）。
- **不得新增任何依賴**（`shioaji`、`itsdangerous` 皆已是 core dep）；**不得改弱既有測試**（實作審查以 `git diff` 確認既有測試 assertion 數量/語意未削弱）；**不得 push**（主對話會另行 commit 並送審）。
- **全程開發用 `ORDER_MODE=sim`**（`Shioaji(simulation=True)`），不碰真 CA、不送真單、不花錢；`real` 需完整 CA/readiness preflight 才能啟動（見 Task 8）。
- **單一 uvicorn worker 不變量**（Dockerfile 無 `--workers`）：quota reservation＋建單、Deal 去重＋部位帳務＋Trade 寫入的原子性，建立在單一 worker（同一 process 內所有 DB 交易依序序列化提交）之上，**不額外加 DB row lock**；若未來上多 worker 需回頭補（記在 spec「未來」，非本計畫範圍）。
- **mode 完整性**：`Order`/`Deal`/`Fill`/`Trade` 的 `mode` 一律由 server-side 下單 session（`OrderService.mode`）決定，**絕不接受表單/外部輸入覆寫**；`OrderRequest` 型別本身刻意不帶 `mode` 欄位。
- **owner 授權**：單一券商帳戶掛 app singleton；`place/cancel/update/positions` 服務層一律先驗 `actor_user_id` 是否在 `ORDER_OWNER_USER_IDS` 白名單，不是則 403；`cancel/update` 再驗 `(user_id, broker, mode, broker_order_id)` 委託所有權。
- **fail closed**：驗證（qty/price/枚舉）、風控（白名單/上限/kill switch）、real 確認缺失、owner 驗證失敗——一律拒絕並記 `OrderAudit`，不得靜默通過或猜測。
- **IntegrityError 精確化**：任何「撞唯一鍵即視為重播/冪等」的 catch，必須先 `rollback()` 後**重新查詢確認命中的正是該唯一鍵**才回既有列；其餘 `IntegrityError`（FK/NULL 等）一律重新拋出，不吞。
- **candle 讀取路徑**（raw-SQL→float、sync `def` routes）勿改 async/ORM——本計畫**不碰 candles**。
- **金額/價格一律 `Decimal`**（`db/models.py` 的 `DecimalText`）；勿用 float（adapter 對 shioaji 邊界才 `float(...)`，回程立即轉 `Decimal`）。
- **每個 task 只 scoped `git add <該 task 檔>`**，嚴禁 `git add -A`／`git add .`；commit 格式 `<type>: <繁中描述>`，attribution 全域停用（不加任何 footer／Co-Authored-By）。
- 全站中文一律**繁體台灣**。
- 測試指令 `uv run pytest`（既有測試全綠才可進下一 task）。
- **GateGuard hook**：第一次 `Bash` 或**建新檔前**會被擋下並要求先陳述事實——這不是故障，照錯誤訊息列點補上事實、重試同一操作即可（本計畫每個「Create 新檔」step 都會踩到，實作者照做）。
- `.env`／`*.pfx`／`*.dump` **不進 git**（Task 10 補 `.pfx` 到 `.gitignore`；CA 憑證/token 只放 VM 的 `.env`／唯讀 bind-mount）。
- **禁止 placeholder**：每個 step 給完整可照抄程式碼；無法 pytest 化的「手動 simtrade E2E」明確標註，不算數為完成。

<!-- TASKS BELOW -->

### Task 1：Trade `mode`+`source` 分流基礎（codex C1/C2/E）

`Trade` 加 `mode`（sim/real，第一級 scope）與 `source`（manual/shioaji，broker 自動與手動日誌隔離的地基——A1 修正的前提）；打通 `list_trades`/`list_for_stats`/journal·stats filter/tab；手動新增日誌帶當前 tab 的 mode。`stats/metrics.py` 零改。

**Files:**
- Modify: `src/quanquant/db/models.py`（`Trade` 加 `mode`/`source` 欄位）
- Modify: `src/quanquant/db/migrate.py`（`_MIGRATIONS` 追加兩筆，用實際表名 `trades`）
- Modify: `src/quanquant/journal/schemas.py`（`Mode`/`Source` Literal + `TradeCreate`/`TradeUpdate`/`TradeRead`/`from_trade`）
- Modify: `src/quanquant/journal/repository.py`（`create_trade`/`list_trades`/`list_for_stats` 穿 `mode`）
- Modify: `src/quanquant/web/routers/trades.py`（`journal_page`/`list_trades_fragment`/`new_trade_form`/`create_trade_route` 加 `mode`）
- Modify: `src/quanquant/web/routers/stats.py`（`_filtered` + 四個 route 加 `mode`）
- Modify: `src/quanquant/web/templates/journal.html`（mode tab + filter hidden mode + 新增交易帶當前 tab mode）
- Modify: `src/quanquant/web/templates/stats.html`（mode tab + filter hidden mode + 匯出 qs 帶 mode）
- Modify: `src/quanquant/web/templates/partials/trade_form.html`（hidden `mode` 欄位）
- Test: Modify `tests/test_repository.py`（mode/source round-trip + 過濾）、Modify `tests/test_migrate.py`（mode/source 遷移 + 既有列預設）、Create `tests/test_mode_split.py`（sim/real 統計隔離）

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
> 註：`Trade` 是既有表，遷移靠 `ensure_columns` 的 nullable `ADD COLUMN`（見 Step 4），SQLite 無法對既有表portably補 `CHECK` 約束；`mode`/`source` 的枚舉限制在**這張表**上只到 pydantic schema 這層（Step 5），DB 層 `CheckConstraint` 留給 Task 3 的**全新**表（`create_all` 走完整 DDL 可攜）。此為刻意取捨，非疏漏。

- [ ] **Step 4：遷移（`src/quanquant/db/migrate.py`）**

`_MIGRATIONS`（檔案 L11-16）末端加兩筆（**DDL 帶 `DEFAULT`** 讓兩方言的既有列自動回填）：
```python
    ("trades", "mode", "VARCHAR DEFAULT 'real'"),
    ("trades", "source", "VARCHAR DEFAULT 'manual'"),
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

- [ ] **Step 8：遷移測試（RED→GREEN）— `tests/test_migrate.py`**

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
```
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
      + 新增交易
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
git commit -m "feat: Trade 加 mode(real/sim)+source(manual/shioaji) 全鏈路過濾＋journal/stats 模擬|正式分頁，統計絕不跨 mode 聚合"
```

---

### Task 2：broker 純型別 + `OrderService` 介面（codex A/C/D/F 的型別地基）

定義 broker 無關的 domain 型別：`Mode` 不在 `OrderRequest`（C1 修正的型別層防線——mode 只能由 session 決定）；`client_order_id`/`fill_id` 分離（B2 修正）；`OrderRequest.__post_init__` 對 qty/price/枚舉 fail closed（F3 型別層防線）；`OrderService` Protocol 每個方法收 `actor_user_id`（D1/D3/D4 授權地基）。純資料、無 DB/框架依賴。

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
  - `broker.types.Fill(broker, fill_id, ordno, symbol, action, price, qty, fee, octype, ts, account, mode, user_id)`
  - `broker.types.Position(symbol, direction, qty, avg_price)`
  - `broker.types.RiskDecision(allowed, reason, needs_confirm)`
  - `broker.base.OrderService`（Protocol：`mode` 屬性 + `async place/cancel/update/positions`（皆收 `actor_user_id`）+ `on_fill`）
  - `broker.base.OrderError`／`broker.base.RiskError`／`broker.base.AuthorizationError`（例外）

- [ ] **Step 1：先寫失敗測試（新檔 `tests/test_broker_types.py`）**

> GateGuard：建新檔前陳述事實（broker 純型別與 Protocol 契約測試，含 fail-closed 驗證）後重試。

```python
"""broker 純型別：欄位齊全、OrderRequest 無 mode（server-side 決定）、
fail-closed 驗證（qty/price/枚舉）、Fill 帶 mode/user_id、Protocol 可被 duck-typed。"""
from decimal import Decimal

import pytest

from quanquant.broker.base import AuthorizationError, OrderError, OrderService, RiskError
from quanquant.broker.types import Fill, OrderAck, OrderRequest, Position, RiskDecision


def _req(**over):
    base = dict(
        client_order_id="C1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", user_id=7,
    )
    base.update(over)
    return OrderRequest(**base)


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


def test_fill_carries_user_and_mode():
    f = Fill(
        broker="shioaji", fill_id="F1", ordno="O1", symbol="TXF", action="Sell",
        price=Decimal("18100"), qty=1, fee=Decimal("50"), octype="Cover",
        ts=1_780_000_000_000, account="F123", mode="sim", user_id=7,
    )
    assert f.user_id == 7 and f.mode == "sim" and f.action == "Sell"


def test_fill_rejects_illegal_mode():
    with pytest.raises(ValueError):
        Fill(
            broker="shioaji", fill_id="F1", ordno="O1", symbol="TXF", action="Sell",
            price=Decimal("1"), qty=1, fee=None, octype="Cover",
            ts=1, account="F123", mode="paper", user_id=7,
        )


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
```
跑 `uv run pytest tests/test_broker_types.py -q` → RED。

- [ ] **Step 2：建 broker 套件（`src/quanquant/broker/__init__.py`）**

> GateGuard：建新檔前照提示陳述事實（新增 broker 套件容器，供下單子系統各模組掛載）後重試。

```python
"""Broker-agnostic order subsystem（OrderService 介面 + ShioajiAdapter + 部位帳務/風控）。"""
```

- [ ] **Step 3：domain 型別（新檔 `src/quanquant/broker/types.py`）**

> GateGuard：建新檔前陳述事實（broker 無關 domain dataclass，型別層 fail-closed 驗證）後重試。

```python
"""Broker 無關的 domain 型別（純資料、無 DB/框架依賴）。

mode 完整性（codex C1）：OrderRequest 刻意不帶 mode 欄位——執行 mode 只由
OrderService.mode（server-side session）決定；Fill.mode 由 adapter 依 self.mode 蓋入，
不接受外部輸入覆寫。fill_id 與 ordno 分離（codex B2）：fill_id 是成交去重鍵，
ordno 是委託關聯鍵，兩者用途不同、不可共用。

YuantaAdapter 日後把 SendFutureOrder / RR_RealReport 映射到同一組型別即可接同介面。
"""
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

    client_order_id: str      # 伺服器產生的冪等鍵（UUID）；同鍵重送不重建委託（codex B4）
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
    fill_id: str          # 券商成交唯一識別（去重鍵，與 ordno 分離——codex B2）
    ordno: str | None     # 委託關聯鍵（對應 Order.ordno）
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
        if self.mode not in _MODES:
            raise ValueError(f"非法 mode: {self.mode!r}")


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
    """風控攔截（超限、非白名單、kill switch、缺/錯確認 token 等）。"""


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
git commit -m "feat: broker domain 型別（OrderRequest 無mode/Fill分離fill_id與ordno）＋OrderService Protocol(actor_user_id)＋例外"
```

---

### Task 3：`Order`／`Deal`／`BrokerPosition`／`OrderAudit` 資料層 + broker 倉儲（codex A1/A2/B1-B5/C2/D5）

四張全新表（`create_all` 自動建，走完整 DDL，可加 `CheckConstraint`）：`Order`（`client_order_id` 唯一冪等鍵）、`Deal`（durable fill inbox，`unique(broker,mode,account,trading_day,fill_id)`）、`BrokerPosition`（持久化部位帳務，與 `Trade` 完全分表——A1/A2 修正的地基）、`OrderAudit`（append-only）。倉儲層精確化 `IntegrityError`（B5）：先 rollback 後重查確認命中指定唯一鍵才當重播，其餘不吞。

**Files:**
- Modify: `src/quanquant/db/models.py`（檔尾新增 `Order`/`Deal`/`BrokerPosition`/`OrderAudit` 四表）
- Create: `src/quanquant/broker/repository.py`
- Test: Create `tests/test_broker_repo.py`

**Interfaces:**
- Consumes：（無上游 task；`Fill`/`OrderRequest` 型別來自 Task 2，僅供理解契約，本 task 的函式簽名不直接吃這兩型別，接的是拆開的關鍵字參數）
- Produces（Task 4/5/6/7/9 依賴）：
  - `db.models.Order`（table `orders`）、`Deal`（`deals`）、`BrokerPosition`（`broker_positions`）、`OrderAudit`（`order_audits`）
  - `broker.repository.create_order(session, *, client_order_id, user_id, mode, broker, account, symbol, action, qty, price, price_type, order_type, octype) -> Order`（冪等：同 `client_order_id` 回既有列）
  - `broker.repository.find_order_by_client_order_id/find_order_by_ordno/find_order_by_broker_id(session, key) -> Order | None`
  - `broker.repository.set_order_sending(session, order_id) -> Order | None`
  - `broker.repository.set_order_ack(session, order_id, *, broker_order_id, ordno, status) -> Order | None`
  - `broker.repository.mark_order_status(session, order_id, *, status, filled_qty=None, avg_fill_price=None) -> Order | None`
  - `broker.repository.list_orders(session, *, user_id, mode, limit=100) -> list[Order]`
  - `broker.repository.count_orders_today/sum_qty_today(session, *, user_id, mode, now=None) -> int`
  - `broker.repository.sum_qty_today_excluding(session, *, user_id, mode, exclude_order_id, now=None) -> int`
  - `broker.repository.trading_day_for(ts_ms: int) -> str`
  - `broker.repository.stage_deal(session, *, broker, account, mode, trading_day, fill_id, ordno, order_id, user_id, symbol, action, price, qty, fee, octype, ts) -> Deal | None`（**只 flush 不 commit**；`None`=重播）
  - `broker.repository.find_open_position(session, *, user_id, broker, account, mode, symbol, direction) -> BrokerPosition | None`
  - `broker.repository.list_open_positions(session, *, user_id, broker, account, mode, symbol) -> list[BrokerPosition]`
  - `broker.repository.hash_payload(*parts) -> str`
  - `broker.repository.append_audit(session, *, actor_user_id, mode, action, payload_hash, result, rule=None, detail=None, now_ms=None) -> OrderAudit`（**只 flush 不 commit**）

- [ ] **Step 1：先寫失敗測試 — 冪等/去重/計數/部位/稽核（新檔 `tests/test_broker_repo.py`）**

> GateGuard：建新檔前陳述事實（Order/Deal/BrokerPosition/OrderAudit 倉儲：冪等建委託、Deal 去重、當日配額、部位查詢、稽核）後重試。

```python
"""broker 倉儲：client_order_id 冪等建委託、Deal (broker,mode,account,trading_day,fill_id)
去重、非重播 IntegrityError 不吞、當日配額、部位查詢、audit+hash。"""
import datetime as dt
from decimal import Decimal

import pytest
from sqlalchemy.exc import IntegrityError

from quanquant.broker import repository as brepo
from quanquant.db.models import BrokerPosition, Order


def _order_kwargs(**over):
    base = dict(
        client_order_id="C1", user_id=1, mode="sim", broker="shioaji", account="F1",
        symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New",
    )
    base.update(over)
    return base


def _deal_kwargs(**over):
    base = dict(
        broker="shioaji", account="F1", mode="sim", trading_day="2026-07-24",
        fill_id="D1", ordno="O1", order_id=None, user_id=1, symbol="TXF",
        action="Buy", price=Decimal("18000"), qty=1, fee=Decimal("50"),
        octype="New", ts=1_780_000_000_000,
    )
    base.update(over)
    return base


def test_create_order_defaults_pending(session):
    o = brepo.create_order(session, **_order_kwargs())
    assert o.id is not None and o.status == "pending" and o.mode == "sim"


def test_create_order_idempotent_on_replay(session):
    first = brepo.create_order(session, **_order_kwargs())
    replay = brepo.create_order(session, **_order_kwargs(qty=99))  # 同 client_order_id，qty 竄改也忽略
    assert replay.id == first.id and replay.qty == first.qty == 1  # 不重建，回既有列


def test_create_order_non_replay_integrity_error_raised(session):
    with pytest.raises(IntegrityError):
        brepo.create_order(session, **_order_kwargs(client_order_id="C-BAD", price=None))  # NOT NULL 撞非本鍵


def test_order_mode_check_constraint_rejects_illegal_value(session):
    bad = Order(
        client_order_id="C-ILLEGAL", user_id=1, mode="paper", broker="shioaji", account="F1",
        symbol="TXF", action="Buy", qty=1, price=Decimal("1"),
        price_type="LMT", order_type="ROD", octype="New",
    )
    session.add(bad)
    with pytest.raises(IntegrityError):
        session.commit()


def test_stage_deal_dedup_on_replay(session):
    first = brepo.stage_deal(session, **_deal_kwargs())
    session.commit()
    assert first is not None
    replay = brepo.stage_deal(session, **_deal_kwargs())  # 同 (broker,mode,account,trading_day,fill_id)
    assert replay is None  # 撞唯一鍵 → 跳過，不重寫


def test_stage_deal_distinct_fill_id_ok(session):
    assert brepo.stage_deal(session, **_deal_kwargs(fill_id="A")) is not None
    session.commit()
    assert brepo.stage_deal(session, **_deal_kwargs(fill_id="B")) is not None


def test_stage_deal_non_replay_integrity_error_is_raised(session):
    with pytest.raises(IntegrityError):
        brepo.stage_deal(session, **_deal_kwargs(fill_id="F-BAD", price=None))  # NOT NULL 撞非本鍵


def test_count_and_sum_qty_today_scoped_by_mode(session):
    now = dt.datetime(2026, 6, 16, 12, 0)  # naive UTC 中午 → CST 同一天
    brepo.create_order(session, **_order_kwargs(client_order_id="S1", mode="sim", qty=2))
    brepo.create_order(session, **_order_kwargs(client_order_id="R1", mode="real", qty=5))
    assert brepo.count_orders_today(session, user_id=1, mode="sim", now=now) == 1
    assert brepo.sum_qty_today(session, user_id=1, mode="sim", now=now) == 2
    assert brepo.sum_qty_today(session, user_id=1, mode="real", now=now) == 5


def test_sum_qty_today_excluding_leaves_out_target_order(session):
    now = dt.datetime(2026, 6, 16, 12, 0)
    a = brepo.create_order(session, **_order_kwargs(client_order_id="E1", mode="sim", qty=2))
    brepo.create_order(session, **_order_kwargs(client_order_id="E2", mode="sim", qty=3))
    total_excl = brepo.sum_qty_today_excluding(session, user_id=1, mode="sim", exclude_order_id=a.id, now=now)
    assert total_excl == 3  # 排除 a（qty=2），只剩 3


def test_find_and_list_open_positions_scoped(session):
    p1 = BrokerPosition(user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                        direction="long", open_qty=1, avg_entry=Decimal("18000"))
    p2 = BrokerPosition(user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                        direction="short", open_qty=1, avg_entry=Decimal("18000"))
    p3 = BrokerPosition(user_id=2, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                        direction="long", open_qty=1, avg_entry=Decimal("18000"))
    session.add_all([p1, p2, p3])
    session.commit()
    found = brepo.find_open_position(session, user_id=1, broker="shioaji", account="F1",
                                     mode="sim", symbol="TXF", direction="long")
    assert found is not None and found.id == p1.id
    opens = brepo.list_open_positions(session, user_id=1, broker="shioaji", account="F1",
                                      mode="sim", symbol="TXF")
    assert {p.id for p in opens} == {p1.id, p2.id}  # user=2 的不混進來


def test_hash_payload_is_deterministic():
    a = brepo.hash_payload("TXF", "Buy", 1, Decimal("18000"))
    b = brepo.hash_payload("TXF", "Buy", 1, Decimal("18000"))
    c = brepo.hash_payload("TXF", "Sell", 1, Decimal("18000"))
    assert a == b and a != c


def test_append_audit_visible_after_commit(session):
    brepo.append_audit(session, actor_user_id=1, mode="sim", action="place",
                       payload_hash="abc123", result="ok")
    session.commit()
    from quanquant.db.models import OrderAudit
    rows = list(session.exec(__import__("sqlmodel").select(OrderAudit)))
    assert len(rows) == 1 and rows[0].result == "ok" and rows[0].action == "place"


def test_trading_day_for_cst_calendar_date():
    # 2026-06-16 04:00 UTC = 2026-06-16 12:00 CST（同一天）
    ts_ms = int(dt.datetime(2026, 6, 16, 4, 0, tzinfo=dt.timezone.utc).timestamp() * 1000)
    assert brepo.trading_day_for(ts_ms) == "2026-06-16"
```
跑 `uv run pytest tests/test_broker_repo.py -q` → RED（`Order`/`Deal`/`BrokerPosition`/`OrderAudit`/`brepo` 未定義）。

- [ ] **Step 2：新增四張表（`src/quanquant/db/models.py` 檔尾）**

檔首 import 行改為（加 `CheckConstraint`）：
```python
from sqlalchemy import BigInteger, CheckConstraint, Column, String, TypeDecorator, UniqueConstraint
```
在檔尾（`UserChartState` 之後）新增：
```python
class Order(SQLModel, table=True):
    """一張委託單（下單面板／委託列表資料來源）。client_order_id 為下單前伺服器產生的冪等鍵。"""

    __tablename__ = "orders"
    __table_args__ = (CheckConstraint("mode IN ('sim','real')", name="ck_orders_mode"),)

    id: int | None = Field(default=None, primary_key=True)
    client_order_id: str = Field(unique=True, index=True)   # 冪等鍵：同鍵重送不重建（codex B4）
    user_id: int = Field(index=True)                        # 下單者
    mode: str = Field(index=True)                            # "real" | "sim"（server-side 決定，非表單）
    broker: str = "shioaji"
    account: str = ""                                        # futopt_account.account_id

    symbol: str = Field(index=True)
    action: str                                               # "Buy" | "Sell"
    qty: int
    price: Decimal = Field(sa_column=Column(DecimalText, nullable=False))
    price_type: str
    order_type: str
    octype: str

    broker_order_id: str | None = Field(default=None, index=True)  # 券商委託單號
    ordno: str | None = Field(default=None, index=True)            # 委託流水（fill 關聯鍵）
    status: str = "pending"  # pending|sending|submitted|partfilled|filled|cancelled|failed|unknown

    filled_qty: int = 0
    avg_fill_price: Decimal | None = Field(default=None, sa_column=Column(DecimalText))

    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)


class Deal(SQLModel, table=True):
    """durable fill inbox：去重（unique broker+mode+account+trading_day+fill_id）+ 待處理佇列
    （processed/quarantine）。fill_id 與 ordno 分離（codex B2）：fill_id 去重、ordno 關聯委託。"""

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
    ordno: str | None = Field(default=None, index=True)   # 委託關聯鍵

    order_id: int | None = Field(default=None, foreign_key="orders.id", index=True)
    user_id: int | None = Field(default=None, index=True)  # 解析前為 None（quarantine）

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
    """持久化 broker 自動部位帳務；與手動 Trade 完全隔離（codex A1）——自動成交只動這張表，
    round-trip 完成（open_qty 歸零）才回寫一筆 Trade(source="shioaji")，trade_id 回填於此。"""

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

    open_qty: int                                             # 剩餘未平口數（0 = 已收單）
    avg_entry: Decimal = Field(sa_column=Column(DecimalText, nullable=False))
    closed_qty: int = 0
    exit_notional: Decimal = Field(default=Decimal(0), sa_column=Column(DecimalText, nullable=False))
    fee_total: Decimal = Field(default=Decimal(0), sa_column=Column(DecimalText, nullable=False))

    status: str = "open"  # "open" | "closed"
    trade_id: int | None = Field(default=None, foreign_key="trades.id")  # 結案回填

    opened_at: datetime = Field(default_factory=_utcnow)       # naive UTC；Trade 化時轉 CST（見 position_tracker）
    updated_at: datetime = Field(default_factory=_utcnow)


class OrderAudit(SQLModel, table=True):
    """append-only 稽核；不含秘密（只存 payload 的 hash，不存原始敏感值）。"""

    __tablename__ = "order_audits"

    id: int | None = Field(default=None, primary_key=True)
    ts: int = Field(sa_column=Column(BigInteger, nullable=False))  # epoch-ms UTC
    actor_user_id: int | None = None
    mode: str
    action: str      # "place" | "cancel" | "update" | "risk_reject" | "fill" | "reconnect"
    payload_hash: str
    rule: str | None = None
    result: str      # "ok" | "rejected" | "error"
    detail: str | None = None
```
（`Column`／`BigInteger`／`UniqueConstraint`／`DecimalText`／`_utcnow` 皆已在本檔既有 import/定義。）

- [ ] **Step 3：broker 倉儲（新檔 `src/quanquant/broker/repository.py`）**

> GateGuard：建新檔前陳述事實（Order/Deal/BrokerPosition/OrderAudit 倉儲，IntegrityError 精確化去重）後重試。

```python
"""Order/Deal/BrokerPosition/OrderAudit 倉儲。

雙方言可攜：只用 SQLModel/select，無 raw SQL。

冪等/去重的 IntegrityError 精確化（codex B5）：任何「撞唯一鍵即重播」的 catch，
一律先 rollback() 後重新查詢確認命中的正是該唯一鍵才回既有列；其餘 IntegrityError
（FK/NULL 等）一律重新拋出，不吞——呼叫端會看到例外，不會被靜默吃掉。

commit 邊界（重要）：本檔函式分兩類——
  * 「委託」CRUD（create_order/set_order_*/mark_order_status）是獨立單步操作，內部自行 commit。
  * 「Deal 進 durable inbox」（stage_deal）與「稽核」（append_audit）刻意只 flush 不 commit，
    因為 Task 5 的 fill worker 需要把「Deal insert + 部位帳務 + Trade 寫入 + processed=true」
    包在同一個交易、同一次 commit 內（codex B1）——commit 的時機交由呼叫端（fill worker）決定。
"""
import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from quanquant.db.models import BrokerPosition, Deal, Order, OrderAudit

_CST = timezone(timedelta(hours=8))


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _today_utc_bounds(now: datetime | None) -> tuple[datetime, datetime]:
    """以 CST 日界回傳 [start, end) 的 naive-UTC 邊界（created_at 存 naive UTC）。"""
    now_utc = now or _utcnow()
    cst_day = now_utc.replace(tzinfo=timezone.utc).astimezone(_CST).date()
    start_cst = datetime(cst_day.year, cst_day.month, cst_day.day, tzinfo=_CST)
    start_utc = start_cst.astimezone(timezone.utc).replace(tzinfo=None)
    return start_utc, start_utc + timedelta(days=1)


def trading_day_for(ts_ms: int) -> str:
    """成交 epoch-ms UTC → CST 日曆日期字串（Deal 唯一鍵組成之一，codex B3）。
    刻意用「日曆日」而非交易時段語意（candles.trading_date 的夜盤跨日規則）——這裡只需要
    一個穩定、可重現的值防止 fill_id 理論上跨日碰撞，不需要交易時段判斷。"""
    return datetime.fromtimestamp(ts_ms / 1000, _CST).strftime("%Y-%m-%d")


# ---- Order ----

def create_order(
    session: Session,
    *,
    client_order_id: str,
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
) -> Order:
    """冪等建委託：同 client_order_id 已存在 → 不重建，回既有列（codex B4）。"""
    existing = find_order_by_client_order_id(session, client_order_id)
    if existing is not None:
        return existing
    order = Order(
        client_order_id=client_order_id, user_id=user_id, mode=mode, broker=broker,
        account=account, symbol=symbol, action=action, qty=qty, price=price,
        price_type=price_type, order_type=order_type, octype=octype,
    )
    session.add(order)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        existing = find_order_by_client_order_id(session, client_order_id)
        if existing is not None:
            return existing  # 競態：另一請求先插入，確認撞的正是 client_order_id 唯一鍵
        raise  # 非本鍵造成的 IntegrityError（FK/NULL 等）→ 不吞，拋出
    session.refresh(order)
    return order


def find_order_by_client_order_id(session: Session, client_order_id: str) -> Order | None:
    return session.exec(select(Order).where(Order.client_order_id == client_order_id)).first()


def find_order_by_ordno(session: Session, ordno: str) -> Order | None:
    return session.exec(select(Order).where(Order.ordno == ordno)).first()


def find_order_by_broker_id(session: Session, broker_order_id: str) -> Order | None:
    return session.exec(select(Order).where(Order.broker_order_id == broker_order_id)).first()


def set_order_sending(session: Session, order_id: int) -> Order | None:
    order = session.get(Order, order_id)
    if order is None:
        return None
    order.status = "sending"
    order.updated_at = _utcnow()
    session.add(order)
    session.commit()
    session.refresh(order)
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
    session.commit()
    session.refresh(order)
    return order


def mark_order_status(
    session: Session,
    order_id: int,
    *,
    status: str,
    filled_qty: int | None = None,
    avg_fill_price=None,
) -> Order | None:
    order = session.get(Order, order_id)
    if order is None:
        return None
    order.status = status
    if filled_qty is not None:
        order.filled_qty = filled_qty
    if avg_fill_price is not None:
        order.avg_fill_price = avg_fill_price
    order.updated_at = _utcnow()
    session.add(order)
    session.commit()
    session.refresh(order)
    return order


def list_orders(session: Session, *, user_id: int, mode: str, limit: int = 100) -> list[Order]:
    stmt = (
        select(Order)
        .where(Order.user_id == user_id, Order.mode == mode)
        .order_by(Order.created_at.desc())  # type: ignore[union-attr]
        .limit(limit)
    )
    return list(session.exec(stmt))


def count_orders_today(session: Session, *, user_id: int, mode: str, now: datetime | None = None) -> int:
    start, end = _today_utc_bounds(now)
    stmt = select(Order).where(
        Order.user_id == user_id, Order.mode == mode,
        Order.created_at >= start, Order.created_at < end,
    )
    return len(list(session.exec(stmt)))


def sum_qty_today(session: Session, *, user_id: int, mode: str, now: datetime | None = None) -> int:
    start, end = _today_utc_bounds(now)
    stmt = select(Order).where(
        Order.user_id == user_id, Order.mode == mode,
        Order.created_at >= start, Order.created_at < end,
    )
    return sum(o.qty for o in session.exec(stmt))


def sum_qty_today_excluding(
    session: Session, *, user_id: int, mode: str, exclude_order_id: int, now: datetime | None = None
) -> int:
    start, end = _today_utc_bounds(now)
    stmt = select(Order).where(
        Order.user_id == user_id, Order.mode == mode,
        Order.created_at >= start, Order.created_at < end,
        Order.id != exclude_order_id,
    )
    return sum(o.qty for o in session.exec(stmt))


# ---- Deal（durable fill inbox） ----

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
    order_id: int | None,
    user_id: int | None,
    symbol: str,
    action: str,
    price,
    qty: int,
    fee,
    octype: str,
    ts: int,
) -> Deal | None:
    """把成交插進 durable inbox；只 flush 不 commit（呼叫端負責交易邊界）。
    撞唯一鍵（重播）→ 回 None，不重寫；其餘 IntegrityError 不吞、往上拋（codex B5）。"""
    existing = _find_deal(session, broker=broker, mode=mode, account=account,
                          trading_day=trading_day, fill_id=fill_id)
    if existing is not None:
        return None
    deal = Deal(
        broker=broker, account=account, mode=mode, trading_day=trading_day, fill_id=fill_id,
        ordno=ordno, order_id=order_id, user_id=user_id, symbol=symbol, action=action,
        price=price, qty=qty, fee=fee, octype=octype, ts=ts,
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


# ---- OrderAudit ----

def hash_payload(*parts: object) -> str:
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
    """append-only 稽核；只 flush 不 commit（呼叫端負責交易邊界，常與其他寫入同一交易提交）。"""
    audit = OrderAudit(
        ts=now_ms if now_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000),
        actor_user_id=actor_user_id, mode=mode, action=action,
        payload_hash=payload_hash, rule=rule, result=result, detail=detail,
    )
    session.add(audit)
    session.flush()
    return audit
```

- [ ] **Step 4：跑測試確認 GREEN**

```bash
uv run pytest tests/test_broker_repo.py -q
```
預期全綠（冪等建委託、Deal 去重、CHECK 約束擋非法 mode、非重播 IntegrityError 會拋、當日配額依 mode 隔離、部位查詢 scope 正確、audit+hash 可用）。

- [ ] **Step 5：跑全測試確認未回歸**

```bash
uv run pytest -q
```

- [ ] **Step 6：Commit（scoped）**

```bash
git add src/quanquant/db/models.py src/quanquant/broker/repository.py tests/test_broker_repo.py
git commit -m "feat: 新增 Order/Deal/BrokerPosition/OrderAudit 四表＋broker 倉儲（client_order_id冪等/Deal複合唯一鍵去重/IntegrityError精確化）"
```

---

### Task 4：`PositionTracker`（`BrokerPosition` 帳務，codex A1-A5）

成交 `Fill` → 持久化 `BrokerPosition`（**不是** `Trade`）帳務：New 開倉聚合加權均價、Cover 平倉 `min(qty,remaining)` 消耗＋跨重啟續平（進度全在 DB，不在記憶體）、超額不吞（轉記反向新倉＋audit）、Auto 依現有雙向 open 集合推斷（歧義 fail closed）；round-trip 完成（`open_qty=0`）才寫**一筆新的** `Trade(source="shioaji")`，與手動日誌（`source="manual"`）完全隔離。

**Files:**
- Modify: `src/quanquant/journal/repository.py`（`create_trade` 加 `commit: bool = True` 參數；新增 `find_open_trade`）
- Create: `src/quanquant/broker/position_tracker.py`
- Test: Modify `tests/test_repository.py`（`find_open_trade` source 隔離）、Create `tests/test_position_tracker.py`

**Interfaces:**
- Consumes：
  - Task 1：`repo.create_trade`、`TradeCreate.mode/source/entry_*/exit_*/size/fee/point_value`
  - Task 2：`broker.types.Fill`
  - Task 3：`brepo.find_open_position`/`list_open_positions`/`hash_payload`/`append_audit`、`db.models.BrokerPosition`
- Produces（Task 5 依賴）：
  - `repo.create_trade(session, TradeCreate, *, user_id, commit=True) -> Trade`（`commit=False` 時只 `flush`，`trade.id` 仍可用，交易邊界交呼叫端）
  - `repo.find_open_trade(session, symbol, *, user_id, mode, source="shioaji", direction=None) -> Trade | None`
  - `broker.position_tracker.PositionTracker()`；`tracker.apply_fill(session, fill: Fill, *, user_id: int) -> None`（**呼叫端已開好交易**，本函式只 `add`/`flush`，不 `commit`）
  - `broker.position_tracker.PositionMismatchError`（Cover 缺對應開倉 / Auto 歧義；呼叫端捕捉後把 `Deal` 標 quarantine）

- [ ] **Step 1：先寫失敗測試 — `find_open_trade` 限 `source`（`tests/test_repository.py`）**

在 `tests/test_repository.py` 末端新增：
```python
def test_find_open_trade_scoped_by_source(session, user):
    manual = repo.create_trade(
        session,
        TradeCreate(symbol="TXF", direction="long", entry_time=dt.datetime(2026, 6, 1, 9, 0),
                    entry_price=Decimal("18000"), size=1, mode="sim", source="manual"),
        user_id=user.id,
    )
    # 手動列存在，但 find_open_trade(source="shioaji") 絕不撈到它（codex A1）
    assert repo.find_open_trade(session, "TXF", user_id=user.id, mode="sim", source="shioaji") is None
    found_manual = repo.find_open_trade(session, "TXF", user_id=user.id, mode="sim", source="manual")
    assert found_manual is not None and found_manual.id == manual.id


def test_create_trade_uncommitted_leaves_transaction_open(session, user):
    t = repo.create_trade(session, make_create(), user_id=user.id, commit=False)
    assert t.id is not None  # flush 已配 PK，不需 commit 也可用
    session.rollback()  # 呼叫端主動放棄 → 這筆不該留存
    assert repo.get_trade(session, t.id, user_id=user.id) is None
```
> 本檔頂端若尚未 `import datetime as dt`／`from decimal import Decimal`／`from quanquant.journal.schemas import TradeCreate`，先補上（既有檔案多半已有 `Decimal`；`dt`/`TradeCreate` 若缺就加）。

跑 `uv run pytest tests/test_repository.py -q` → RED（`find_open_trade` 未定義、`create_trade` 無 `commit` 參數）。

- [ ] **Step 2：`find_open_trade` + `create_trade` 加 `commit` 參數（`src/quanquant/journal/repository.py`）**

`create_trade` 改為：
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
        note=data.note,
        tags=join_tags(data.tags),
        pnl_is_manual=data.pnl is not None,
        pnl=data.pnl,
        mode=data.mode,
        source=data.source,
    )
    _recompute_pnl(trade)
    session.add(trade)
    if commit:
        session.commit()
        session.refresh(trade)
    else:
        session.flush()  # 呼叫端（fill worker）要把這筆併進更大的交易，稍後自行 commit
    return trade
```
（`commit=True` 是既有所有呼叫點的行為，零回歸；`commit=False` 只給 Task 5 的 fill worker 用。）

在 `list_for_stats` 之後新增：
```python
def find_open_trade(
    session: Session,
    symbol: str,
    *,
    user_id: int,
    mode: str,
    source: str = "shioaji",
    direction: str | None = None,
) -> Trade | None:
    """(user_id, symbol, mode, source) scope 下最舊（FIFO）的未平倉列；預設限 source="shioaji"，
    永不觸及手動日誌（codex A1）。"""
    stmt = select(Trade).where(
        Trade.user_id == user_id,
        Trade.symbol == symbol,
        Trade.mode == mode,
        Trade.source == source,
        Trade.exit_time.is_(None),  # type: ignore[union-attr]
    )
    if direction is not None:
        stmt = stmt.where(Trade.direction == direction)
    stmt = stmt.order_by(Trade.entry_time.asc())  # type: ignore[union-attr]
    return session.exec(stmt).first()
```

- [ ] **Step 3：跑測試確認 GREEN（repository）**

```bash
uv run pytest tests/test_repository.py -q
```

- [ ] **Step 4：先寫失敗測試 — PositionTracker（新檔 `tests/test_position_tracker.py`）**

> GateGuard：建新檔前陳述事實（BrokerPosition 帳務：開倉聚合/跨重啟續平/超額不吞/Auto歧義/缺開倉/手動隔離）後重試。

```python
"""PositionTracker：BrokerPosition 帳務（不是 Trade）。開倉聚合加權均價、Cover 消耗
min(qty,remaining) 且跨重啟續平（進度在 DB）、超額轉反向新倉不吞口數、Auto 依雙向 open
集合推斷（歧義 fail closed）、缺對應開倉 raise（呼叫端 quarantine）、與手動日誌完全隔離。"""
import datetime as dt
from decimal import Decimal

import pytest
from sqlmodel import select

from quanquant.broker.position_tracker import PositionMismatchError, PositionTracker
from quanquant.broker.types import Fill
from quanquant.db.models import BrokerPosition
from quanquant.journal import repository as repo
from quanquant.journal.schemas import TradeCreate


def _fill(**over):
    base = dict(
        broker="shioaji", fill_id="F1", ordno="O1", symbol="TXF", action="Buy",
        price=Decimal("18000"), qty=1, fee=Decimal("50"), octype="New",
        ts=1_780_000_000_000, account="F1", mode="sim", user_id=None,
    )
    base.update(over)
    return Fill(**base)


def _positions(session):
    return list(session.exec(select(BrokerPosition)))


def test_new_fill_opens_broker_position(session):
    PositionTracker().apply_fill(session, _fill(), user_id=1)
    session.commit()
    pos = _positions(session)
    assert len(pos) == 1
    assert pos[0].direction == "long" and pos[0].open_qty == 1 and pos[0].avg_entry == Decimal("18000")
    assert pos[0].status == "open"


def test_aggregating_new_fills_weighted_avg(session):
    tr = PositionTracker()
    tr.apply_fill(session, _fill(fill_id="A", price=Decimal("18000"), qty=1), user_id=1)
    tr.apply_fill(session, _fill(fill_id="B", price=Decimal("18100"), qty=1), user_id=1)
    session.commit()
    pos = _positions(session)
    assert len(pos) == 1 and pos[0].open_qty == 2 and pos[0].avg_entry == Decimal("18050")


def test_partial_cover_persists_progress_across_tracker_restart(session):
    PositionTracker().apply_fill(session, _fill(fill_id="A", qty=3, price=Decimal("18000")), user_id=1)
    session.commit()
    # 「行程重啟」模擬：全新 PositionTracker 實例分批平倉；進度靠 BrokerPosition，不靠記憶體
    PositionTracker().apply_fill(
        session, _fill(fill_id="B", action="Sell", octype="Cover", qty=1, price=Decimal("18100")), user_id=1,
    )
    session.commit()
    pos = _positions(session)[0]
    assert pos.status == "open" and pos.open_qty == 2 and pos.closed_qty == 1
    assert repo.list_trades(session, user_id=1, mode="sim", status="closed") == []  # 未收單

    PositionTracker().apply_fill(
        session, _fill(fill_id="C", action="Sell", octype="Cover", qty=2, price=Decimal("18200")), user_id=1,
    )
    session.commit()
    pos = _positions(session)[0]
    assert pos.status == "closed" and pos.open_qty == 0 and pos.closed_qty == 3
    closed = repo.list_trades(session, user_id=1, mode="sim", status="closed")
    assert len(closed) == 1
    expected_exit = (Decimal("18100") * 1 + Decimal("18200") * 2) / Decimal(3)
    assert closed[0].exit_price == expected_exit
    assert closed[0].source == "shioaji" and pos.trade_id == closed[0].id


def test_cover_excess_reverses_into_new_position_not_dropped(session):
    tr = PositionTracker()
    tr.apply_fill(session, _fill(fill_id="A", qty=1, price=Decimal("18000")), user_id=1)  # long 1
    tr.apply_fill(session, _fill(
        fill_id="B", action="Sell", octype="Cover", qty=3, price=Decimal("18100"),
    ), user_id=1)  # 平 1 收單 + 超額 2 反向開空
    session.commit()
    positions = _positions(session)
    closed = [p for p in positions if p.status == "closed"]
    opened = [p for p in positions if p.status == "open"]
    assert len(closed) == 1 and closed[0].closed_qty == 1  # 口數守恆：只消耗剩餘的 1 口
    assert len(opened) == 1 and opened[0].direction == "short" and opened[0].open_qty == 2  # 超額轉反向新倉


def test_auto_infers_cover_when_opposite_direction_open(session):
    tr = PositionTracker()
    tr.apply_fill(session, _fill(fill_id="A", octype="New", qty=1), user_id=1)  # long 1
    tr.apply_fill(session, _fill(fill_id="B", action="Sell", octype="Auto", qty=1), user_id=1)
    session.commit()
    pos = _positions(session)[0]
    assert pos.status == "closed" and pos.closed_qty == 1


def test_auto_infers_new_when_same_direction_open(session):
    tr = PositionTracker()
    tr.apply_fill(session, _fill(fill_id="A", octype="New", qty=1), user_id=1)  # long 1
    tr.apply_fill(
        session, _fill(fill_id="B", action="Buy", octype="Auto", qty=1, price=Decimal("18100")), user_id=1,
    )
    session.commit()
    pos = _positions(session)[0]
    assert pos.status == "open" and pos.open_qty == 2  # Auto 判為加碼，非平倉


def test_auto_ambiguous_with_both_directions_open_fails_closed(session):
    tr = PositionTracker()
    tr.apply_fill(session, _fill(fill_id="A", action="Buy", octype="New", qty=1), user_id=1)   # long
    tr.apply_fill(session, _fill(fill_id="B", action="Sell", octype="New", qty=1), user_id=1)  # short（雙向並存）
    session.commit()
    with pytest.raises(PositionMismatchError):
        tr.apply_fill(session, _fill(fill_id="C", action="Buy", octype="Auto", qty=1), user_id=1)


def test_cover_without_open_position_raises_for_quarantine(session):
    with pytest.raises(PositionMismatchError):
        PositionTracker().apply_fill(
            session, _fill(fill_id="A", action="Sell", octype="Cover", qty=1), user_id=1,
        )
    assert _positions(session) == []  # 沒有任何 BrokerPosition 被建立/污染


def test_manual_trade_and_broker_round_trip_do_not_cross_pollute(session, user):
    manual = repo.create_trade(
        session,
        TradeCreate(symbol="TXF", direction="long", entry_time=dt.datetime(2026, 6, 1, 9, 0),
                    entry_price=Decimal("18000"), size=1, mode="sim", source="manual"),
        user_id=user.id,
    )
    session.commit()

    tr = PositionTracker()
    tr.apply_fill(session, _fill(fill_id="A", qty=1, mode="sim"), user_id=user.id)
    tr.apply_fill(session, _fill(
        fill_id="B", action="Sell", octype="Cover", qty=1, price=Decimal("18100"), mode="sim",
    ), user_id=user.id)
    session.commit()

    still_open = repo.get_trade(session, manual.id, user_id=user.id)
    assert still_open.exit_time is None and still_open.source == "manual"  # 手動列完全沒被動過
    broker_closed = repo.list_trades(session, user_id=user.id, mode="sim", status="closed")
    assert len(broker_closed) == 1 and broker_closed[0].source == "shioaji"
    assert broker_closed[0].id != manual.id  # 是「新建」的一筆，不是同一列
```
跑 `uv run pytest tests/test_position_tracker.py -q` → RED（`PositionTracker` 未定義）。

- [ ] **Step 5：PositionTracker（新檔 `src/quanquant/broker/position_tracker.py`）**

> GateGuard：建新檔前陳述事實（成交→BrokerPosition 帳務映射，聚合/跨重啟續平/超額反向/Auto歧義fail closed）後重試。

```python
"""成交 Fill → 部位帳務（BrokerPosition，持久化）→ round-trip 完成才寫一筆 Trade(source="shioaji")。

與手動日誌完全隔離（codex A1）：本模組只讀寫 BrokerPosition，從不直接碰 Trade（除了
round-trip 完成時呼叫 repo.create_trade 建立「新的一筆」）。所有部位進度（open_qty/
avg_entry/closed_qty/exit_notional/fee_total）持久化在 BrokerPosition，支援跨重啟續平
（codex A2）——不依賴任何行程內記憶體狀態；PositionTracker 本身無實例狀態，可任意重建。

Auto 判斷（codex A4）：依 (user,broker,account,mode,symbol) 下「目前 OPEN 的方向集合」推斷：
  - 集合為空，或只有一個方向且與這筆 fill 隱含方向相同 → 開倉（加碼）。
  - 集合只有一個方向且與這筆 fill 隱含方向相反 → 平倉。
  - 集合同時有多空兩個方向 → 歧義，fail closed（raise PositionMismatchError），
    交由呼叫端（Task 5 fill worker）把 Deal 標 quarantine，不猜測。

超額 Cover（codex A3）：消耗 min(fill.qty, remaining)；超過剩餘的口數，因為真實部位在
券商端已經反向（賣超就是空、買超就是多），若直接丟棄會讓帳務與券商真實部位脫勾——
故超額部分轉記一筆反向新倉（並寫 OrderAudit 供覆核），口數不憑空消失。

呼叫慣例：apply_fill(session, fill, user_id=...) 假設呼叫端已開好交易（session 尚未
commit），本函式內部只 add()/flush()，commit 的時機由呼叫端決定（Task 5：Deal insert +
本函式 + processed=true 同一次 commit）。
"""
import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlmodel import Session

from quanquant.broker import repository as brepo
from quanquant.broker.types import Fill
from quanquant.db.models import BrokerPosition
from quanquant.journal import repository as repo
from quanquant.journal.schemas import TradeCreate

log = logging.getLogger(__name__)
_CST = timezone(timedelta(hours=8))
_POINT_VALUES = {"TXF": Decimal("200"), "MXF": Decimal("50")}


class PositionMismatchError(Exception):
    """Cover 找不到對應開倉部位，或 Auto 判斷開平歧義——fail closed，交由呼叫端 quarantine。"""


def _point_value(symbol: str) -> Decimal:
    return _POINT_VALUES.get(symbol, Decimal("200"))


def _to_dt(ts_ms: int) -> datetime:
    """epoch-ms UTC → naive CST（對齊 Trade.entry_time/exit_time 的 naive 本地慣例）。"""
    return datetime.fromtimestamp(ts_ms / 1000, _CST).replace(tzinfo=None)


def _utc_dt_to_cst(value: datetime) -> datetime:
    """naive UTC datetime（BrokerPosition.opened_at）→ naive CST（Trade.entry_time 慣例）。"""
    return value.replace(tzinfo=timezone.utc).astimezone(_CST).replace(tzinfo=None)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class PositionTracker:
    """無實例狀態；所有進度都在 DB，可任意重建（跨行程重啟安全）。"""

    def apply_fill(self, session: Session, fill: Fill, *, user_id: int) -> None:
        if self._is_opening(session, fill, user_id=user_id):
            self._open(session, fill, user_id=user_id)
        else:
            self._close(session, fill, user_id=user_id)

    def _is_opening(self, session: Session, fill: Fill, *, user_id: int) -> bool:
        if fill.octype == "New":
            return True
        if fill.octype == "Cover":
            return False
        opens = brepo.list_open_positions(session, user_id=user_id, broker=fill.broker,
                                          account=fill.account, mode=fill.mode, symbol=fill.symbol)
        directions = {p.direction for p in opens}
        if len(directions) >= 2:
            raise PositionMismatchError(
                f"Auto fill 無法判斷開平：{fill.symbol} 同時有多頭與空頭未平倉部位，歧義 fail closed"
            )
        covered_dir = "long" if fill.action == "Sell" else "short"
        return covered_dir not in directions

    def _open(self, session: Session, fill: Fill, *, user_id: int) -> None:
        direction = "long" if fill.action == "Buy" else "short"
        pos = brepo.find_open_position(session, user_id=user_id, broker=fill.broker,
                                       account=fill.account, mode=fill.mode,
                                       symbol=fill.symbol, direction=direction)
        if pos is None:
            pos = BrokerPosition(
                user_id=user_id, broker=fill.broker, account=fill.account, mode=fill.mode,
                symbol=fill.symbol, direction=direction,
                open_qty=fill.qty, avg_entry=fill.price,
                fee_total=(fill.fee or Decimal(0)),
            )
        else:
            new_qty = pos.open_qty + fill.qty
            pos.avg_entry = (pos.avg_entry * pos.open_qty + fill.price * fill.qty) / Decimal(new_qty)
            pos.open_qty = new_qty
            pos.fee_total += (fill.fee or Decimal(0))
            pos.updated_at = _now_utc()
        session.add(pos)
        session.flush()

    def _close(self, session: Session, fill: Fill, *, user_id: int) -> None:
        target_dir = "long" if fill.action == "Sell" else "short"
        pos = brepo.find_open_position(session, user_id=user_id, broker=fill.broker,
                                       account=fill.account, mode=fill.mode,
                                       symbol=fill.symbol, direction=target_dir)
        if pos is None:
            raise PositionMismatchError(
                f"Cover fill 無對應未平倉部位：user={user_id} symbol={fill.symbol} "
                f"mode={fill.mode} direction={target_dir}"
            )
        consumed = min(fill.qty, pos.open_qty)
        pos.closed_qty += consumed
        pos.exit_notional += fill.price * Decimal(consumed)
        pos.fee_total += (fill.fee or Decimal(0))
        pos.open_qty -= consumed
        pos.updated_at = _now_utc()

        excess = fill.qty - consumed
        if pos.open_qty == 0:
            self._finalize(session, pos, fill, user_id=user_id)
        else:
            session.add(pos)
            session.flush()

        if excess > 0:
            # 超額：真實部位已反向，不吞口數——轉記一筆反向新倉，並寫 OrderAudit 供覆核（codex A3）。
            reversed_fill = replace(fill, qty=excess, octype="New")
            self._open(session, reversed_fill, user_id=user_id)
            brepo.append_audit(
                session, actor_user_id=user_id, mode=fill.mode, action="fill",
                payload_hash=brepo.hash_payload(fill.fill_id, fill.symbol, fill.qty),
                rule="cover_excess_reversal", result="ok",
                detail=(
                    f"Cover fill qty={fill.qty} 超過剩餘 {consumed}，"
                    f"多出 {excess} 口轉記反向新倉（fill_id={fill.fill_id}）"
                ),
            )

    def _finalize(self, session: Session, pos: BrokerPosition, fill: Fill, *, user_id: int) -> None:
        """round-trip 完成（open_qty 歸零）→ 寫一筆 Trade(source="shioaji")，trade_id 回填。"""
        pos.status = "closed"
        guard = repo.find_open_trade(session, pos.symbol, user_id=user_id, mode=pos.mode, source="shioaji")
        if guard is not None:
            log.error(
                "不變量違反：source=shioaji 已有開放 Trade（id=%s），round-trip 完成前不應存在", guard.id
            )
        trade = repo.create_trade(
            session,
            TradeCreate(
                symbol=pos.symbol,
                direction=pos.direction,
                entry_time=_utc_dt_to_cst(pos.opened_at),
                entry_price=pos.avg_entry,
                exit_time=_to_dt(fill.ts),
                exit_price=pos.exit_notional / Decimal(pos.closed_qty),
                size=pos.closed_qty,
                point_value=_point_value(pos.symbol),
                fee=pos.fee_total,
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
預期全綠（開倉聚合、跨重啟續平、超額反向不吞、Auto 推斷/歧義 fail closed、缺開倉 raise、手動與 broker 完全隔離）。

- [ ] **Step 7：跑全測試確認未回歸**

```bash
uv run pytest -q
```

- [ ] **Step 8：Commit（scoped）**

```bash
git add src/quanquant/journal/repository.py src/quanquant/broker/position_tracker.py \
  tests/test_position_tracker.py tests/test_repository.py
git commit -m "feat: PositionTracker 持久化 BrokerPosition 帳務（聚合/跨重啟續平/超額反向不吞/Auto歧義fail closed），round-trip完成才寫Trade(source=shioaji)"
```

---

### Task 5：Durable fill worker（codex A5/B1/D2/E1/E4）

`FillWorker`：enqueue-only（`loop.call_soon_threadsafe` 呼叫端安全）→ 單一有序＋backpressure 的 asyncio Queue → 每筆 `asyncio.to_thread` 開新 Session → **`stage_deal`＋`PositionTracker.apply_fill`＋`processed=true` 同一次 `commit()`**（codex B1 的核心不變量：要嘛全部生效、要嘛全部沒發生，重播天然安全）。解不到 order context 或部位歧義 → quarantine 不 drop（A5/D2）；`retry_quarantined()` 供重連對帳（Task 8）與測試直接呼叫。

**Files:**
- Create: `src/quanquant/broker/fill_worker.py`
- Test: Create `tests/test_fill_worker.py`

**Interfaces:**
- Consumes：
  - Task 2：`broker.types.Fill`
  - Task 3：`brepo.find_order_by_ordno`/`stage_deal`/`trading_day_for`、`db.models.Deal`
  - Task 4：`PositionTracker`/`PositionMismatchError`
- Produces（Task 6/8 依賴）：
  - `broker.fill_worker.FillWorker(session_factory, *, tracker=None, maxsize=1000)`
  - `worker.enqueue(fill: Fill) -> None`（**只能透過 `loop.call_soon_threadsafe(worker.enqueue, fill)` 呼叫**）
  - `async worker.run() -> None`（forever 迴圈；單一 consumer）
  - `async worker.drain(timeout: float = 5.0) -> None`
  - `worker.qsize() -> int`
  - `worker.apply_fill_transaction(session, fill) -> None`（同步；核心交易，`run()` 與 `retry_quarantined()` 共用）
  - `worker.retry_quarantined(session, *, limit: int = 200) -> int`（回傳這次解決的筆數）

- [ ] **Step 1：先寫失敗測試（新檔 `tests/test_fill_worker.py`）**

> GateGuard：建新檔前陳述事實（durable fill worker：Deal+部位+Trade 同交易、quarantine 重試、callback-before-ack 真執行緒競態測試）後重試。

```python
"""FillWorker：Deal+部位+processed 同一交易（重播安全）、解不到 context 進 quarantine 不 drop、
retry_quarantined 可重建、失敗不留痕跡（重播只一次 effect）、callback-before-ack 真執行緒競態。"""
import asyncio
import threading
from decimal import Decimal

from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.broker.fill_worker import FillWorker
from quanquant.broker.position_tracker import PositionTracker
from quanquant.broker.types import Fill
from quanquant.db.models import BrokerPosition, Deal


def _fill(**over):
    base = dict(
        broker="shioaji", fill_id="F1", ordno="O1", symbol="TXF", action="Buy",
        price=Decimal("18000"), qty=1, fee=Decimal("50"), octype="New",
        ts=1_780_000_000_000, account="F1", mode="sim", user_id=None,
    )
    base.update(over)
    return Fill(**base)


def _make_order(session, *, client_order_id, user_id, ordno, broker_order_id, action="Buy", octype="New"):
    order = brepo.create_order(
        session, client_order_id=client_order_id, user_id=user_id, mode="sim", broker="shioaji",
        account="F1", symbol="TXF", action=action, qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype=octype,
    )
    brepo.set_order_ack(session, order.id, broker_order_id=broker_order_id, ordno=ordno, status="submitted")
    return order


def test_unresolvable_ordno_quarantines_not_dropped(engine):
    worker = FillWorker(lambda: Session(engine))
    with Session(engine) as s:
        worker.apply_fill_transaction(s, _fill(fill_id="F1", ordno="UNKNOWN"))
    with Session(engine) as s:
        deals = list(s.exec(select(Deal).where(Deal.fill_id == "F1")))
        assert len(deals) == 1 and deals[0].quarantine is True and deals[0].processed is False
        assert list(s.exec(select(BrokerPosition))) == []  # 沒有半個部位被誤建


def test_retry_quarantined_resolves_once_order_context_appears(engine):
    worker = FillWorker(lambda: Session(engine))
    with Session(engine) as s:
        worker.apply_fill_transaction(s, _fill(fill_id="F2", ordno="O2"))  # 這時還沒有對應委託 → quarantine

    with Session(engine) as s:
        _make_order(s, client_order_id="C2", user_id=9, ordno="O2", broker_order_id="B2")

    with Session(engine) as s:
        assert worker.retry_quarantined(s) == 1

    with Session(engine) as s:
        deal = s.exec(select(Deal).where(Deal.fill_id == "F2")).first()
        assert deal.processed is True and deal.quarantine is False and deal.user_id == 9
        pos = s.exec(select(BrokerPosition).where(BrokerPosition.user_id == 9)).first()
        assert pos is not None and pos.open_qty == 1


def test_position_mismatch_quarantines_without_partial_effect(engine):
    with Session(engine) as s:
        _make_order(s, client_order_id="C3", user_id=5, ordno="O3", broker_order_id="B3",
                   action="Sell", octype="Cover")
    worker = FillWorker(lambda: Session(engine))
    with Session(engine) as s:
        worker.apply_fill_transaction(s, _fill(fill_id="F3", ordno="O3", action="Sell", octype="Cover"))
    with Session(engine) as s:
        deal = s.exec(select(Deal).where(Deal.fill_id == "F3")).first()
        assert deal.quarantine is True and deal.processed is False and deal.error is not None
        assert list(s.exec(select(BrokerPosition).where(BrokerPosition.user_id == 5))) == []


def test_replayed_fill_id_after_success_is_idempotent(engine):
    with Session(engine) as s:
        _make_order(s, client_order_id="C5", user_id=3, ordno="O5", broker_order_id="B5")
    worker = FillWorker(lambda: Session(engine))
    with Session(engine) as s:
        worker.apply_fill_transaction(s, _fill(fill_id="F5", ordno="O5"))
    with Session(engine) as s:
        worker.apply_fill_transaction(s, _fill(fill_id="F5", ordno="O5"))  # 重播同一 fill_id
    with Session(engine) as s:
        assert len(list(s.exec(select(Deal).where(Deal.fill_id == "F5")))) == 1
        pos = s.exec(select(BrokerPosition).where(BrokerPosition.user_id == 3)).first()
        assert pos.open_qty == 1  # 沒有因重播而變成 2


def test_transaction_failure_before_commit_leaves_no_trace_then_replay_succeeds_once(engine):
    """codex B1：任何失敗都發生在 commit 之前（stage_deal 只 flush），所以失敗那次完全不留痕跡；
    重播（第二次呼叫）在乾淨的狀態上重來，最終恰好一次 effect——不是「重播時去重」，
    而是「失敗根本沒有東西可重播」。"""
    import pytest

    class _BoomOnceTracker(PositionTracker):
        def __init__(self):
            self.calls = 0

        def apply_fill(self, session, fill, *, user_id):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("模擬暫時性錯誤（bug/DB 故障）")
            return super().apply_fill(session, fill, user_id=user_id)

    with Session(engine) as s:
        _make_order(s, client_order_id="C6", user_id=7, ordno="O6", broker_order_id="B6")

    boom = _BoomOnceTracker()
    worker = FillWorker(lambda: Session(engine), tracker=boom)
    fill = _fill(fill_id="F6", ordno="O6")

    with Session(engine) as s1:
        with pytest.raises(RuntimeError):
            worker.apply_fill_transaction(s1, fill)
        s1.rollback()

    with Session(engine) as verify:
        assert list(verify.exec(select(Deal))) == []
        assert list(verify.exec(select(BrokerPosition))) == []

    with Session(engine) as s2:
        worker.apply_fill_transaction(s2, fill)  # 重播

    with Session(engine) as verify:
        deals = list(verify.exec(select(Deal)))
        positions = list(verify.exec(select(BrokerPosition)))
        assert len(deals) == 1 and deals[0].processed is True
        assert len(positions) == 1 and positions[0].open_qty == 1


def test_enqueue_and_run_processes_fill_end_to_end(engine):
    async def scenario():
        with Session(engine) as s:
            _make_order(s, client_order_id="C4", user_id=3, ordno="O4", broker_order_id="B4")

        worker = FillWorker(lambda: Session(engine))
        run_task = asyncio.create_task(worker.run())
        worker.enqueue(_fill(fill_id="F4", ordno="O4"))
        await worker.drain(timeout=5)
        run_task.cancel()
        try:
            await run_task
        except asyncio.CancelledError:
            pass

        with Session(engine) as s:
            pos = s.exec(select(BrokerPosition).where(BrokerPosition.user_id == 3)).first()
            assert pos is not None and pos.open_qty == 1

    asyncio.run(scenario())


def test_enqueue_backpressure_drops_without_raising(engine):
    worker = FillWorker(lambda: Session(engine), maxsize=1)
    worker.enqueue(_fill(fill_id="A"))
    worker.enqueue(_fill(fill_id="B"))  # 佇列已滿 → 不 raise，只記 log 丟棄
    assert worker.qsize() == 1


def test_callback_before_ack_barrier_real_thread_and_loop(engine):
    """codex E1：callback 執行緒送出 fill 的時間點，可能早於「委託 ordno 已持久化」——用真
    asyncio loop + 背景 worker 執行緒 + barrier 重現這個競態，驗證不論競態誰先誰後，
    quarantine→retry_quarantined 最終都能正確歸屬（不 drop、不誤配）。"""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()

    worker = FillWorker(lambda: Session(engine))
    run_task = asyncio.run_coroutine_threadsafe(worker.run(), loop)
    try:
        barrier = threading.Barrier(2)

        def callback_thread():
            barrier.wait(timeout=5)
            loop.call_soon_threadsafe(worker.enqueue, _fill(fill_id="F7", ordno="O-RACE"))

        cb = threading.Thread(target=callback_thread)
        cb.start()
        barrier.wait(timeout=5)  # 讓 callback 執行緒與「委託落地」幾乎同時起跑，製造競態窗口
        with Session(engine) as s:
            _make_order(s, client_order_id="C7", user_id=42, ordno="O-RACE", broker_order_id="B-RACE")
        cb.join(timeout=5)

        fut = asyncio.run_coroutine_threadsafe(worker.drain(timeout=5), loop)
        fut.result(timeout=10)

        with Session(engine) as s:
            worker.retry_quarantined(s)  # 若這次競態輸了（quarantine），靠這行救回來

        with Session(engine) as s:
            positions = list(s.exec(select(BrokerPosition).where(BrokerPosition.user_id == 42)))
            deals = list(s.exec(select(Deal).where(Deal.fill_id == "F7")))
            assert len(positions) == 1 and positions[0].open_qty == 1
            assert len(deals) == 1 and deals[0].processed is True and deals[0].user_id == 42
    finally:
        run_task.cancel()
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
```
跑 `uv run pytest tests/test_fill_worker.py -q` → RED（`FillWorker` 未定義）。

- [ ] **Step 2：FillWorker（新檔 `src/quanquant/broker/fill_worker.py`）**

> GateGuard：建新檔前陳述事實（durable fill worker：enqueue-only、單一有序＋backpressure、Deal+部位+processed 同一交易）後重試。

```python
"""Durable fill 處理：enqueue-only（callback 執行緒安全）→ 單一有序＋backpressure 的
asyncio Queue → 每筆 asyncio.to_thread 開新 Session（codex E4：不佔用 event loop 做同步
DB I/O）→「Deal insert + 部位帳務 + Trade 寫入 + processed=true」同一交易提交（codex B1）。

核心不變量（apply_fill_transaction）：
  1. 先依 fill.ordno 解析 Order（取得 user_id）。
  2. stage_deal()（flush，不 commit）：撞唯一鍵 → 重播，直接 return（前次已完整處理過，冪等跳過）。
  3a. 解不到 order → Deal.quarantine=True/processed=False，commit（只有 Deal 落地）。
  3b. 解到 order 但 PositionTracker 判斷部位歧義/缺對應開倉 → 同 3a，quarantine（codex A5/D2）。
  3c. 成功 → PositionTracker.apply_fill()（可能連帶建立 Trade）→ Deal.processed=True → commit。
  任何步驟中途拋出未預期例外，因為 Deal 只 flush 未 commit，session 關閉時整個交易連 Deal
  一起回滾——不會有「Deal 已提交但部位/Trade 沒寫」的中間態，重播天然安全。
"""
import asyncio
import logging

from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.broker.position_tracker import PositionMismatchError, PositionTracker
from quanquant.broker.types import Fill
from quanquant.db.models import Deal

log = logging.getLogger(__name__)


class FillWorker:
    def __init__(self, session_factory, *, tracker: PositionTracker | None = None, maxsize: int = 1000):
        self._session_factory = session_factory
        self._tracker = tracker or PositionTracker()
        self._queue: asyncio.Queue[Fill] = asyncio.Queue(maxsize=maxsize)

    def enqueue(self, fill: Fill) -> None:
        """只能透過 `loop.call_soon_threadsafe(worker.enqueue, fill)` 呼叫——讓 asyncio.Queue
        的存取落在 event loop 執行緒上（asyncio.Queue 本身非跨執行緒安全）。佇列滿了就丟棄
        並記 error（backpressure 顯性失敗，不無界成長吃記憶體）；資料復原靠重連對帳
        （Task 8 的 watchdog），不是這裡硬塞等待。"""
        try:
            self._queue.put_nowait(fill)
        except asyncio.QueueFull:
            log.error(
                "fill queue 已滿（backpressure），丟棄 fill_id=%s ordno=%s — worker 可能卡住，"
                "需檢查；資料復原靠下次重連對帳", fill.fill_id, fill.ordno,
            )

    async def run(self) -> None:
        """單一有序 worker：逐筆處理，每筆開新 Session。"""
        while True:
            fill = await self._queue.get()
            try:
                await asyncio.to_thread(self._process_one, fill)
            except Exception:
                log.exception(
                    "fill 處理失敗（本筆交易已回滾、未 processed；靠 retry_quarantined/重連對帳補救）："
                    "fill_id=%s", fill.fill_id,
                )
            finally:
                self._queue.task_done()

    async def drain(self, timeout: float = 5.0) -> None:
        """shutdown／測試同步點：等佇列清空（有上限，避免無限卡住）。"""
        try:
            await asyncio.wait_for(self._queue.join(), timeout=timeout)
        except asyncio.TimeoutError:
            log.warning(
                "fill worker drain timeout：仍有 %d 筆未處理，交由下次啟動時的重連對帳補救",
                self._queue.qsize(),
            )

    def qsize(self) -> int:
        return self._queue.qsize()

    def _process_one(self, fill: Fill) -> None:
        with self._session_factory() as session:
            self.apply_fill_transaction(session, fill)

    def apply_fill_transaction(self, session: Session, fill: Fill) -> None:
        """核心不變量：見模組 docstring。同步函式，供 worker（to_thread）與 retry_quarantined 共用。"""
        order = brepo.find_order_by_ordno(session, fill.ordno) if fill.ordno else None
        deal = brepo.stage_deal(
            session, broker=fill.broker, account=fill.account, mode=fill.mode,
            trading_day=brepo.trading_day_for(fill.ts), fill_id=fill.fill_id, ordno=fill.ordno,
            order_id=order.id if order else None, user_id=order.user_id if order else None,
            symbol=fill.symbol, action=fill.action, price=fill.price, qty=fill.qty,
            fee=fill.fee, octype=fill.octype, ts=fill.ts,
        )
        if deal is None:
            return  # 重播（同一 fill_id 已完整處理過）→ 冪等跳過，不重寫任何東西

        if order is None:
            self._quarantine(deal, f"無法解析 ordno={fill.ordno!r} 對應委託（callback 早於 ack 或未重建）")
            session.add(deal)
            session.commit()
            return

        try:
            self._tracker.apply_fill(session, fill, user_id=order.user_id)
        except PositionMismatchError as exc:
            self._quarantine(deal, str(exc))
            session.add(deal)
            session.commit()
            return

        deal.processed = True
        session.add(deal)
        session.commit()

    @staticmethod
    def _quarantine(deal: Deal, error: str) -> None:
        deal.quarantine = True
        deal.error = error
        log.warning("fill 進 quarantine：fill_id=%s error=%s", deal.fill_id, error)

    def retry_quarantined(self, session: Session, *, limit: int = 200) -> int:
        """重連對帳／定期重試（Task 8 的 watchdog 呼叫）：撿回 quarantine=True 且
        processed=False 的 Deal，重新嘗試解析 order context + 套用部位帳務。成功則
        processed=True/quarantine=False；仍失敗則保持 quarantine（error 更新）。
        回傳這次成功解決的筆數。"""
        stmt = (
            select(Deal)
            .where(Deal.quarantine.is_(True), Deal.processed.is_(False))  # type: ignore[union-attr]
            .order_by(Deal.created_at.asc())  # type: ignore[union-attr]
            .limit(limit)
        )
        resolved = 0
        for deal in list(session.exec(stmt)):
            order = brepo.find_order_by_ordno(session, deal.ordno) if deal.ordno else None
            if order is None:
                continue  # 依然解不到，留在 quarantine
            fill = Fill(
                broker=deal.broker, fill_id=deal.fill_id, ordno=deal.ordno, symbol=deal.symbol,
                action=deal.action, price=deal.price, qty=deal.qty, fee=deal.fee,
                octype=deal.octype, ts=deal.ts, account=deal.account, mode=deal.mode,
                user_id=order.user_id,
            )
            try:
                self._tracker.apply_fill(session, fill, user_id=order.user_id)
            except PositionMismatchError as exc:
                deal.error = str(exc)
                session.add(deal)
                session.commit()
                continue
            deal.order_id = order.id
            deal.user_id = order.user_id
            deal.processed = True
            deal.quarantine = False
            deal.error = None
            session.add(deal)
            session.commit()
            resolved += 1
        return resolved
```

- [ ] **Step 3：跑測試確認 GREEN**

```bash
uv run pytest tests/test_fill_worker.py -q
```
預期全綠（quarantine 不 drop、retry 可重建、失敗不留痕跡+重播恰一次 effect、backpressure 不 raise、callback-before-ack 真執行緒競態最終正確歸屬）。此檔含執行緒/loop 測試，若偶發逾時，先確認不是機器負載問題再重跑；**不得**把 timeout 加大到掩蓋真正的死結。

- [ ] **Step 4：跑全測試確認未回歸**

```bash
uv run pytest -q
```

- [ ] **Step 5：Commit（scoped）**

```bash
git add src/quanquant/broker/fill_worker.py tests/test_fill_worker.py
git commit -m "feat: durable FillWorker（enqueue-only+單一有序worker，Deal+部位+processed同交易，quarantine不drop+retry_quarantined重建）"
```

---

### Task 6：`ShioajiAdapter`（codex B4/C1/D3/E1/F6 的 broker 端落地）

實作 `OrderService`：lazy-import shioaji；`activate_ca`(real)／`simulation=True`(sim)；`place` 前先落地 pending correlation（`client_order_id` 冪等鍵，`status=pending→sending`，防重送真單）；`cancel`/`update` 驗 `(user_id, broker, mode, broker_order_id)` 委託所有權（D3）；`_on_order_cb` 只做「映射 Fill＋丟給 handlers」不碰 DB（E4，真正的 DB 交易在 Task 5 的 fill worker）；`Fill.mode` 一律蓋 `self.mode`，絕不信任 callback payload（C1/F6）。`risk_guard` 參數本 task 只接受並在該呼叫點調用（duck-typed，可為 `None`）——Task 7 才實作真正的 `RiskGuard`，屆時無需再改本檔的呼叫點（codex H 的「前移」處理）。

**Files:**
- Create: `src/quanquant/broker/shioaji_adapter.py`
- Test: Create `tests/test_shioaji_adapter.py`

**Interfaces:**
- Consumes：
  - Task 2：`OrderRequest`/`OrderAck`/`Fill`/`Position`、`OrderError`/`RiskError`/`AuthorizationError`
  - Task 3：`brepo.create_order`/`find_order_by_client_order_id`/`find_order_by_broker_id`/`set_order_sending`/`set_order_ack`/`mark_order_status`
- Produces（Task 7/8/9 依賴）：
  - `broker.shioaji_adapter.ShioajiAdapter(session_factory, loop, *, mode, broker="shioaji", api_key="", secret_key="", ca_path="", ca_passwd="", person_id="", risk_guard=None)`
  - `adapter.mode: str`（public 屬性，`OrderService` Protocol 要求）
  - `async connect() -> None`、`async close() -> None`
  - `async place(req, *, actor_user_id, confirm_token=None) -> OrderAck`
  - `async cancel(broker_order_id, *, actor_user_id) -> OrderAck`
  - `async update(broker_order_id, *, actor_user_id, price=None, qty=None, confirm_token=None) -> OrderAck`
  - `async positions(*, actor_user_id) -> list[Position]`
  - `on_fill(handler) -> None`
  - **`risk_guard` 期待的介面**（Task 7 實作，本 task 只呼叫）：`assert_owner(actor_user_id)`、`check_place(req, *, actor_user_id, mode, confirm_token, session)`、`check_update(order, *, new_qty, new_price, actor_user_id, mode, confirm_token, session)`（皆違規時 raise `AuthorizationError`／`RiskError`）

- [ ] **Step 1：先寫失敗測試（新檔 `tests/test_shioaji_adapter.py`，用 fake shioaji，不 import 原生 client）**

> GateGuard：建新檔前陳述事實（adapter 下單/回報以 fake 驗證，不碰原生 shioaji；含 client_order_id 冪等/委託所有權/風控透傳）後重試。

```python
"""ShioajiAdapter：以 fake api + recording loop 驗 place/cancel/update 回 OrderAck、
client_order_id 冪等不重送、cancel/update 委託所有權、風控透傳、成交 callback 映射 Fill
（mode 一律等於 adapter.mode，不受 msg 內容影響）。"""
import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlmodel import Session

from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, OrderError, RiskError
from quanquant.broker.shioaji_adapter import ShioajiAdapter
from quanquant.broker.types import OrderRequest


class _RecordingLoop:
    def call_soon_threadsafe(self, fn, *args):
        fn(*args)  # inline，方便斷言


class _FakeTrade:
    def __init__(self, ordno, broker_order_id):
        self.order = SimpleNamespace(id=broker_order_id, ordno=ordno)
        self.status = SimpleNamespace(id=broker_order_id, status="Submitted")


class _FakeShioaji:
    """只實作 adapter 會呼叫的最小面。"""
    def __init__(self):
        self.placed = []
        self.cancelled = []
        self.updated = []
        self.futopt_account = SimpleNamespace(account_id="F123")

    def Order(self, **kw):
        return SimpleNamespace(**kw)

    def place_order(self, contract, order):
        self.placed.append((contract, order))
        n = len(self.placed)
        return _FakeTrade(ordno=f"O{n}", broker_order_id=f"B{n}")

    def cancel_order(self, trade):
        self.cancelled.append(trade)
        return _FakeTrade(ordno="O1", broker_order_id="B1")

    def update_order(self, trade, **kw):
        self.updated.append((trade, kw))
        return _FakeTrade(ordno="O1", broker_order_id="B1")

    def list_trades(self):
        return [_FakeTrade(ordno="O1", broker_order_id="B1")]


def _adapter(engine, *, mode="sim", risk_guard=None):
    a = ShioajiAdapter(lambda: Session(engine), _RecordingLoop(), mode=mode, risk_guard=risk_guard)
    a._api = _FakeShioaji()
    a._account = "F123"
    a._contract_for = lambda symbol: SimpleNamespace(code="TXF202607")  # 免查合約
    return a


def _req(**over):
    base = dict(client_order_id="C1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
               price_type="LMT", order_type="ROD", octype="New", user_id=7)
    base.update(over)
    return OrderRequest(**base)


def test_place_returns_ack_and_persists_order(engine, session):
    a = _adapter(engine)
    ack = asyncio.run(a.place(_req(), actor_user_id=7))
    assert ack.broker_order_id == "B1" and ack.status == "submitted" and ack.ordno == "O1"
    orders = brepo.list_orders(session, user_id=7, mode="sim")
    assert len(orders) == 1 and orders[0].broker_order_id == "B1" and orders[0].status == "submitted"


def test_place_idempotent_on_same_client_order_id_does_not_resend(engine):
    a = _adapter(engine)
    first = asyncio.run(a.place(_req(client_order_id="DUP"), actor_user_id=7))
    second = asyncio.run(a.place(_req(client_order_id="DUP", qty=99), actor_user_id=7))
    assert second.broker_order_id == first.broker_order_id
    assert len(a._api.placed) == 1  # 沒有真的送第二次單給券商（codex B4）


def test_place_broker_failure_marks_order_unknown_not_blind_resend(engine, session):
    class _BoomShioaji(_FakeShioaji):
        def place_order(self, contract, order):
            raise RuntimeError("network glitch")

    a = _adapter(engine)
    a._api = _BoomShioaji()
    with pytest.raises(OrderError):
        asyncio.run(a.place(_req(client_order_id="BOOM"), actor_user_id=7))
    order = brepo.find_order_by_client_order_id(session, "BOOM")
    assert order.status == "unknown"  # 中途失敗 → unknown，不是 failed（禁止假設沒送出就盲送重試）


def test_on_fill_mode_always_matches_adapter_mode_not_msg(engine):
    a = _adapter(engine, mode="sim")
    fills = []
    a.on_fill(fills.append)
    msg = {"code": "TXF", "action": "Sell", "price": 18100, "quantity": 1,
          "trade_id": "D1", "ordno": "O1", "ts": 1_780_000_000, "order_cond": "Cover"}
    a._on_order_cb(SimpleNamespace(value="FuturesDeal"), msg)
    assert len(fills) == 1
    assert fills[0].mode == "sim" and fills[0].fill_id == "D1" and fills[0].ordno == "O1"
    assert fills[0].user_id is None  # 由 fill_worker 依 ordno 解析 order context 補上，這裡不猜


def test_cancel_and_update_return_ack(engine):
    a = _adapter(engine)
    ack = asyncio.run(a.place(_req(), actor_user_id=7))
    c = asyncio.run(a.cancel(ack.broker_order_id, actor_user_id=7))
    assert c.status == "cancelled"
    u = asyncio.run(a.update(ack.broker_order_id, actor_user_id=7, price=Decimal("18010")))
    assert u.status == "updated"


def test_cancel_by_non_owner_of_order_raises_authorization_error(engine):
    a = _adapter(engine)
    ack = asyncio.run(a.place(_req(), actor_user_id=7))
    with pytest.raises(AuthorizationError):
        asyncio.run(a.cancel(ack.broker_order_id, actor_user_id=999))  # 別人不能刪這張委託（codex D3）


def test_cancel_unknown_broker_order_id_raises_order_error(engine):
    a = _adapter(engine)
    with pytest.raises(OrderError):
        asyncio.run(a.cancel("NOPE", actor_user_id=7))


def test_place_propagates_risk_guard_rejection_without_sending(engine):
    class _RejectGuard:
        def assert_owner(self, actor_user_id):
            pass

        def check_place(self, req, **kw):
            raise RiskError("blocked")

    a = _adapter(engine, risk_guard=_RejectGuard())
    with pytest.raises(RiskError):
        asyncio.run(a.place(_req(), actor_user_id=7))
    assert a._api.placed == []  # 風控擋下，根本沒送到券商


def test_positions_checks_owner(engine):
    class _RejectOwner:
        def assert_owner(self, actor_user_id):
            raise AuthorizationError("not owner")

    a = _adapter(engine, risk_guard=_RejectOwner())
    with pytest.raises(AuthorizationError):
        asyncio.run(a.positions(actor_user_id=999))
```
跑 `uv run pytest tests/test_shioaji_adapter.py -q` → RED。

- [ ] **Step 2：ShioajiAdapter（新檔 `src/quanquant/broker/shioaji_adapter.py`）**

> GateGuard：建新檔前陳述事實（Shioaji 下單 adapter：lazy-import、client_order_id 冪等、委託所有權、跨執行緒 callback 橋接）後重試。

```python
"""Shioaji 下單 adapter（實作 OrderService）。

- lazy-import shioaji（同 sources/shioaji_stream.py，避免測試/非下單情境載入原生 client）。
- sim：Shioaji(simulation=True) 免 CA；real：login + activate_ca(person_id)。
- place 前先落地 pending correlation（create_order 狀態 pending→sending，client_order_id
  冪等鍵）：同 client_order_id 重送 → 不重送真單，回既有委託狀態（codex B4）。
- callback-before-ack（codex E1）：_on_order_cb 只做「映射 Fill + 丟給 handlers」，
  不碰 DB；真正解析 order context（依 ordno）與寫入是 Task 5 fill worker 的職責。
  「pending correlation」在這裡的實際作用是縮短 quarantine 窗口——place() 一旦拿到
  broker 回覆的 ordno 並寫回 Order.ordno，Task 8 的 watchdog 週期性 retry_quarantined()
  很快就能把在這之前進 quarantine 的 fill 救回來；不是「送單前就能預先比對」的魔法。
- cancel/update 先驗 (user_id, broker, mode, broker_order_id) 委託所有權（codex D3），
  不符一律 AuthorizationError；kill switch 刻意不擋 cancel（緊急時仍應能撤單）。
- Fill.mode 一律蓋 self.mode，絕不信任 callback payload 內容（codex C1/F6）。
"""
import asyncio
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from sqlmodel import Session

from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, OrderError
from quanquant.broker.types import Fill, OrderAck, OrderRequest, Position
from quanquant.db.models import Order

log = logging.getLogger(__name__)


def _dec(value, fallback: str = "0") -> Decimal:
    try:
        return Decimal(str(value)) if value not in (None, "") else Decimal(fallback)
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(fallback)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _pick_front_contract(api, symbol: str):
    """近月非連續月 TXF/MXF 合約；獨立一份邏輯（同 sources/shioaji_stream.py 的近月挑選），
    避免下單 adapter 耦合只服務行情的 ShioajiStreamer。"""
    category = getattr(api.Contracts.Futures, symbol)
    today = datetime.now(timezone.utc).astimezone().strftime("%Y/%m/%d")
    months = [c for c in category if "R" not in c.code[len(symbol):]]
    if not months:
        raise OrderError(f"no month contracts for {symbol}")
    active = [c for c in months if (c.delivery_date or "") >= today]
    return min(active or months, key=lambda c: c.delivery_date or "9999/99/99")


class ShioajiAdapter:
    def __init__(
        self,
        session_factory: Callable[[], Session],
        loop,
        *,
        mode: str,
        broker: str = "shioaji",
        api_key: str = "",
        secret_key: str = "",
        ca_path: str = "",
        ca_passwd: str = "",
        person_id: str = "",
        risk_guard=None,
    ) -> None:
        self.mode = mode  # OrderService Protocol 要求的 public 屬性；一路蓋 Order/Fill/Trade
        self._session_factory = session_factory
        self._loop = loop
        self._broker = broker
        self._api_key = api_key
        self._secret_key = secret_key
        self._ca_path = ca_path
        self._ca_passwd = ca_passwd
        self._person_id = person_id
        self._risk_guard = risk_guard
        self._api = None
        self._account = ""
        self._fill_handlers: list[Callable[[Fill], None]] = []

    # ---- 生命週期 ----

    async def connect(self) -> None:
        await asyncio.to_thread(self._connect_blocking)

    def _connect_blocking(self) -> None:
        import shioaji as sj  # lazy：keep native client off the import path

        api = sj.Shioaji(simulation=(self.mode == "sim"))
        api.login(self._api_key, self._secret_key, subscribe_trade=True, fetch_contract=True)
        if self.mode == "real":
            api.activate_ca(ca_path=self._ca_path, ca_passwd=self._ca_passwd,
                            person_id=self._person_id)
        api.set_order_callback(self._on_order_cb)
        self._api = api
        self._account = str(getattr(api.futopt_account, "account_id", ""))
        log.info("Shioaji order session connected (mode=%s)", self.mode)

    async def close(self) -> None:
        api, self._api = self._api, None
        if api is None:
            return
        try:
            await asyncio.to_thread(api.logout)
        except Exception:
            log.warning("Shioaji order session logout 失敗（忽略，程序即將結束）", exc_info=True)

    def _contract_for(self, symbol: str):
        return _pick_front_contract(self._api, symbol)

    # ---- OrderService 介面 ----

    def on_fill(self, handler: Callable[[Fill], None]) -> None:
        self._fill_handlers.append(handler)

    async def place(
        self, req: OrderRequest, *, actor_user_id: int, confirm_token: str | None = None
    ) -> OrderAck:
        with self._session_factory() as session:
            if self._risk_guard is not None:
                self._risk_guard.assert_owner(actor_user_id)
                self._risk_guard.check_place(
                    req, actor_user_id=actor_user_id, mode=self.mode,
                    confirm_token=confirm_token, session=session,
                )
            existing = brepo.find_order_by_client_order_id(session, req.client_order_id)
            if existing is not None:
                return self._ack_from_order(existing)  # 冪等：同 client_order_id 不重送（codex B4）
            order = brepo.create_order(
                session, client_order_id=req.client_order_id, user_id=actor_user_id, mode=self.mode,
                broker=self._broker, account=self._account, symbol=req.symbol, action=req.action,
                qty=req.qty, price=req.price, price_type=req.price_type, order_type=req.order_type,
                octype=req.octype,
            )
            order = brepo.set_order_sending(session, order.id)

        try:
            ack = await asyncio.to_thread(self._place_blocking, req)
        except Exception as exc:
            with self._session_factory() as session:
                brepo.mark_order_status(session, order.id, status="unknown")
            raise OrderError(f"place 失敗，狀態轉 unknown（待 reconcile，禁止盲送重試）：{exc}") from exc

        with self._session_factory() as session:
            brepo.set_order_ack(session, order.id, broker_order_id=ack.broker_order_id,
                                ordno=ack.ordno, status="submitted")
        return ack

    @staticmethod
    def _ack_from_order(order: Order) -> OrderAck:
        return OrderAck(client_order_id=order.client_order_id,
                        broker_order_id=order.broker_order_id or "",
                        ordno=order.ordno, status=order.status)

    def _place_blocking(self, req: OrderRequest) -> OrderAck:
        if self._api is None:
            raise OrderError("order session not connected")
        contract = self._contract_for(req.symbol)
        order = self._api.Order(
            action=req.action, price=float(req.price), quantity=req.qty,
            price_type=req.price_type, order_type=req.order_type, octype=req.octype,
            account=self._api.futopt_account,
        )
        trade = self._api.place_order(contract, order)
        return OrderAck(
            client_order_id=req.client_order_id,
            broker_order_id=str(trade.order.id), ordno=str(trade.order.ordno),
            status="submitted",
        )

    async def cancel(self, broker_order_id: str, *, actor_user_id: int) -> OrderAck:
        with self._session_factory() as session:
            order = brepo.find_order_by_broker_id(session, broker_order_id)
            if order is None:
                raise OrderError(f"unknown broker_order_id: {broker_order_id}")
            if order.user_id != actor_user_id or order.mode != self.mode or order.broker != self._broker:
                raise AuthorizationError("無權操作此委託")  # codex D3：委託所有權驗證
            if self._risk_guard is not None:
                self._risk_guard.assert_owner(actor_user_id)
            order_id = order.id
        # kill switch 刻意不擋 cancel（緊急時仍應能撤單，見 spec F）
        ack = await asyncio.to_thread(self._cancel_blocking, broker_order_id)
        with self._session_factory() as session:
            brepo.mark_order_status(session, order_id, status="cancelled")
        return ack

    def _cancel_blocking(self, broker_order_id: str) -> OrderAck:
        if self._api is None:
            raise OrderError("order session not connected")
        trade = self._resolve_trade(broker_order_id)
        result = self._api.cancel_order(trade)
        return OrderAck(client_order_id="", broker_order_id=broker_order_id,
                        ordno=str(getattr(result.order, "ordno", "")) or None, status="cancelled")

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
            order = brepo.find_order_by_broker_id(session, broker_order_id)
            if order is None:
                raise OrderError(f"unknown broker_order_id: {broker_order_id}")
            if order.user_id != actor_user_id or order.mode != self.mode or order.broker != self._broker:
                raise AuthorizationError("無權操作此委託")
            if self._risk_guard is not None:
                self._risk_guard.assert_owner(actor_user_id)
                self._risk_guard.check_update(
                    order, new_qty=qty, new_price=price, actor_user_id=actor_user_id,
                    mode=self.mode, confirm_token=confirm_token, session=session,
                )
            order_id = order.id

        ack = await asyncio.to_thread(self._update_blocking, broker_order_id, price, qty)

        with self._session_factory() as session:
            fresh = session.get(Order, order_id)
            if qty is not None:
                fresh.qty = qty
            if price is not None:
                fresh.price = price
            fresh.status = "submitted"
            fresh.updated_at = _utcnow()
            session.add(fresh)
            session.commit()
        return ack

    def _update_blocking(self, broker_order_id: str, price, qty) -> OrderAck:
        if self._api is None:
            raise OrderError("order session not connected")
        trade = self._resolve_trade(broker_order_id)
        kw = {}
        if price is not None:
            kw["price"] = float(price)
        if qty is not None:
            kw["qty"] = qty
        result = self._api.update_order(trade, **kw)
        return OrderAck(client_order_id="", broker_order_id=broker_order_id,
                        ordno=str(getattr(result.order, "ordno", "")) or None, status="updated")

    def _resolve_trade(self, broker_order_id: str):
        """以 broker_order_id 從 api 現有委託找回 trade 物件（真整合時 api.list_trades()）。"""
        for trade in (self._api.list_trades() if hasattr(self._api, "list_trades") else []):
            if str(getattr(trade.order, "id", "")) == broker_order_id:
                return trade
        raise OrderError(f"unknown broker_order_id: {broker_order_id}")

    async def positions(self, *, actor_user_id: int) -> list[Position]:
        if self._risk_guard is not None:
            self._risk_guard.assert_owner(actor_user_id)
        return await asyncio.to_thread(self._positions_blocking)

    def _positions_blocking(self) -> list[Position]:
        if self._api is None:
            return []
        out: list[Position] = []
        for p in self._api.list_positions(self._api.futopt_account):
            out.append(Position(
                symbol=str(getattr(p, "code", "")),
                direction="long" if str(getattr(p, "direction", "")).lower() == "buy" else "short",
                qty=int(getattr(p, "quantity", 0) or 0),
                avg_price=_dec(getattr(p, "price", 0)),
            ))
        return out

    # ---- 成交/委託 callback（solace 執行緒；只映射+丟給 handlers，不碰 DB——codex E4）----

    def _on_order_cb(self, stat, msg) -> None:
        state = getattr(stat, "value", stat)
        if state != "FuturesDeal":
            return  # 委託狀態變更（FuturesOrder）由 place/cancel/update 的回傳值處理，此處不重複
        try:
            fill = self._deal_to_fill(msg)
        except Exception:
            log.exception("成交回報映射失敗，丟棄本筆（結構異常，非 durable inbox 涵蓋範圍）")
            return
        for handler in self._fill_handlers:
            self._loop.call_soon_threadsafe(handler, fill)

    def _deal_to_fill(self, msg: dict) -> Fill:
        """OrderState.FuturesDeal payload → Fill。欄位鍵名為官方文件最佳猜測；真整合
        （Task 9 的 simtrade 手動 E2E）需對照實際 dict 微調——見該 task 註記。mode/account
        一律用 self.mode/self._account，不讀 msg（codex C1/F6：不信任外部輸入覆寫 mode）。"""
        return Fill(
            broker=self._broker,
            fill_id=str(msg.get("trade_id") or msg.get("seqno") or msg.get("id") or ""),
            ordno=str(msg.get("ordno") or msg.get("seqno") or "") or None,
            symbol=str(msg.get("code", "")),
            action=str(msg.get("action", "")),
            price=_dec(msg.get("price")),
            qty=int(msg.get("quantity", 0) or 0),
            fee=_dec(msg.get("fee")) if msg.get("fee") is not None else None,
            octype=str(msg.get("order_cond") or msg.get("octype") or msg.get("order_type") or "Auto"),
            ts=int(float(msg.get("ts", 0)) * 1000),  # 秒 → 毫秒
            account=self._account,
            mode=self.mode,
            user_id=None,  # 由 fill_worker 依 ordno 解析 order context 補上，這裡不猜
        )
```

- [ ] **Step 3：跑測試確認 GREEN**

```bash
uv run pytest tests/test_shioaji_adapter.py -q
```

- [ ] **Step 4：跑全測試確認未回歸**

```bash
uv run pytest -q
```

- [ ] **Step 5：Commit（scoped）**

```bash
git add src/quanquant/broker/shioaji_adapter.py tests/test_shioaji_adapter.py
git commit -m "feat: ShioajiAdapter（client_order_id冪等place/委託所有權cancel-update/callback只映射不碰DB/Fill.mode恆等於self.mode）"
```

---

### Task 7：`RiskGuard`＋驗證＋授權＋兩階段確認＋即時 kill switch＋audit（codex D1/F1-F5/F7）

`RiskGuard` 建構式**刻意不吃 `Settings`**，改吃拆開的關鍵字參數（`owner_user_ids`/`symbol_whitelist`/上限/`kill_switch`）——這樣 Task 7 完全不依賴 Task 8 才會新增的 config 欄位，徹底解掉 codex H 指出的「Task6→Task7 RiskGuard 相依」問題（不需要「前移」或 `risk_guard=None` 佔位回填；Task 8 只需讀 `Settings` 組出這些參數餵進來）。owner allowlist（D1/D4）、即時可切換 kill switch（F4，非啟動快照）、real 兩階段確認 token（F1，短效一次性綁 `(actor_user_id, payload_hash)`）、`update` 重跑全部風控＋排除自身舊 qty 算配額（F2）、成功與拒絕都寫 `OrderAudit`（F5）。

**Files:**
- Create: `src/quanquant/broker/confirm.py`
- Create: `src/quanquant/broker/risk.py`
- Test: Create `tests/test_confirm.py`、Create `tests/test_risk_guard.py`

**Interfaces:**
- Consumes：
  - Task 2：`OrderRequest`、`RiskError`/`AuthorizationError`
  - Task 3：`brepo.hash_payload`/`append_audit`/`sum_qty_today`/`count_orders_today`/`sum_qty_today_excluding`、`db.models.OrderAudit`
  - Task 6：`ShioajiAdapter` 已在 `place`/`cancel`/`update`/`positions` 呼叫 `risk_guard.assert_owner`/`check_place`/`check_update`（duck-typed，本 task 補上真正實作，呼叫點零修改）
- Produces（Task 8/9 依賴）：
  - `broker.confirm.issue(actor_user_id, payload_hash) -> str`、`broker.confirm.verify(token, *, actor_user_id, payload_hash) -> bool`（一次性，驗證成功即消費）
  - `broker.confirm.TTL_SECONDS`、`broker.confirm.clear_consumed()`（測試輔助）
  - `broker.risk.parse_owner_ids(raw: str) -> frozenset[int]`、`broker.risk.parse_whitelist(raw: str) -> frozenset[str]`
  - `broker.risk.RiskGuard(*, owner_user_ids, symbol_whitelist, max_qty_per_order, max_qty_per_day, max_orders_per_day, kill_switch=False)`
  - `guard.kill_switch`（property）、`guard.set_kill_switch(value: bool)`（即時切換，供 Task 9 的 UI kill switch 開關使用）
  - `guard.assert_owner(actor_user_id)`、`guard.check_place(req, *, actor_user_id, mode, confirm_token, session)`、`guard.check_update(order, *, new_qty, new_price, actor_user_id, mode, confirm_token, session)`

- [ ] **Step 1：先寫失敗測試 — 兩階段確認 token（新檔 `tests/test_confirm.py`）**

> GateGuard：建新檔前陳述事實（短效一次性確認 token：簽章/綁定/一次性消費/過期）後重試。

```python
"""兩階段確認 token：短效、一次性、綁定 (actor_user_id, payload_hash)（codex F1）。"""
import time

from quanquant.broker import confirm


def setup_function():
    confirm.clear_consumed()


def test_issue_and_verify_round_trip():
    token = confirm.issue(7, "hash-abc")
    assert confirm.verify(token, actor_user_id=7, payload_hash="hash-abc") is True


def test_verify_rejects_wrong_user():
    token = confirm.issue(7, "hash-abc")
    assert confirm.verify(token, actor_user_id=999, payload_hash="hash-abc") is False


def test_verify_rejects_wrong_payload_hash():
    token = confirm.issue(7, "hash-abc")
    assert confirm.verify(token, actor_user_id=7, payload_hash="different") is False


def test_verify_is_one_time_use():
    token = confirm.issue(7, "hash-abc")
    assert confirm.verify(token, actor_user_id=7, payload_hash="hash-abc") is True
    assert confirm.verify(token, actor_user_id=7, payload_hash="hash-abc") is False  # 用過即失效


def test_verify_rejects_expired_token(monkeypatch):
    monkeypatch.setattr(confirm, "TTL_SECONDS", 0)
    token = confirm.issue(7, "hash-abc")
    time.sleep(0.05)
    assert confirm.verify(token, actor_user_id=7, payload_hash="hash-abc") is False


def test_verify_rejects_garbage_token():
    assert confirm.verify("not-a-real-token", actor_user_id=7, payload_hash="hash-abc") is False
```
跑 `uv run pytest tests/test_confirm.py -q` → RED。

- [ ] **Step 2：確認 token（新檔 `src/quanquant/broker/confirm.py`）**

> GateGuard：建新檔前陳述事實（短效一次性確認 token，仿 auth/tokens.py 簽章模式）後重試。

```python
"""兩階段確認 token：real 下單/改單前，伺服器產生短效、一次性、綁定 (actor_user_id,
payload_hash) 的 token（codex F1）。以 itsdangerous 簽章（防偽造/竄改，仿 auth/tokens.py）
+ 短 TTL（防重放過久）+ 記憶體一次性消費集合（防同 token 用兩次）。單一 uvicorn worker
不變量下，記憶體狀態全站一致，同 auth/service.py 的登入失敗鎖定模式（單行程記憶體狀態，
重啟自然清空——token 本就短效，可接受）。
"""
import logging
import secrets

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from quanquant.config import get_settings

TTL_SECONDS = 120
log = logging.getLogger(__name__)
_fallback_secret: str | None = None  # 同 auth/tokens.py：行程內穩定，僅供 dev/測試無 SESSION_SECRET 時用
_consumed: set[str] = set()


def _secret() -> str:
    global _fallback_secret
    configured = get_settings().session_secret
    if configured:
        return configured
    if _fallback_secret is None:
        _fallback_secret = secrets.token_urlsafe(32)
        log.warning("SESSION_SECRET 未設，下單確認 token 用行程內暫時金鑰（僅供 dev/測試）")
    return _fallback_secret


def _serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(_secret(), salt="qq-order-confirm")


def issue(actor_user_id: int, payload_hash: str) -> str:
    nonce = secrets.token_urlsafe(16)
    return _serializer().dumps({"uid": actor_user_id, "hash": payload_hash, "nonce": nonce})


def verify(token: str, *, actor_user_id: int, payload_hash: str) -> bool:
    """驗證成功即消費（一次性）；任何不符（過期/竄改/user 不符/payload 不符/已用過）皆回 False。"""
    try:
        data = _serializer().loads(token, max_age=TTL_SECONDS)
    except (BadSignature, SignatureExpired):
        return False
    if not isinstance(data, dict):
        return False
    if data.get("uid") != actor_user_id or data.get("hash") != payload_hash:
        return False
    nonce = data.get("nonce")
    if not nonce or nonce in _consumed:
        return False
    _consumed.add(nonce)
    return True


def clear_consumed() -> None:
    """測試輔助：重置一次性消費集合。"""
    _consumed.clear()
```

- [ ] **Step 3：跑測試確認 GREEN（confirm）**

```bash
uv run pytest tests/test_confirm.py -q
```

- [ ] **Step 4：先寫失敗測試 — RiskGuard（新檔 `tests/test_risk_guard.py`）**

> GateGuard：建新檔前陳述事實（owner allowlist/即時kill switch/白名單/單筆單日口數/單日次數/real二次確認/update重跑風控/audit落地）後重試。

```python
"""RiskGuard：owner allowlist、kill switch（即時可讀，非啟動快照）、白名單、單筆/單日口數、
單日次數、real 兩階段確認、update 重跑全部風控＋排除自身舊 qty、audit 落地（成功與拒絕都記）。"""
from decimal import Decimal

import pytest
from sqlmodel import select

from quanquant.broker import confirm
from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, RiskError
from quanquant.broker.risk import RiskGuard, parse_owner_ids, parse_whitelist
from quanquant.broker.types import OrderRequest
from quanquant.db.models import OrderAudit


def setup_function():
    confirm.clear_consumed()


def _req(**over):
    base = dict(client_order_id="C1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
               price_type="LMT", order_type="ROD", octype="New", user_id=1)
    base.update(over)
    return OrderRequest(**base)


def _guard(**over):
    base = dict(owner_user_ids=frozenset({1}), symbol_whitelist=frozenset({"TXF", "MXF"}),
               max_qty_per_order=2, max_qty_per_day=5, max_orders_per_day=3, kill_switch=False)
    base.update(over)
    return RiskGuard(**base)


def _confirm_for(req):
    payload_hash = brepo.hash_payload(req.client_order_id, req.symbol, req.action, req.qty, req.price, req.octype)
    return confirm.issue(req.user_id, payload_hash)


def test_assert_owner_allows_listed_user():
    _guard().assert_owner(1)  # 不 raise


def test_assert_owner_rejects_non_owner():
    with pytest.raises(AuthorizationError):
        _guard().assert_owner(999)


def test_check_place_passes_within_limits_sim(session):
    _guard().check_place(_req(), actor_user_id=1, mode="sim", confirm_token=None, session=session)


def test_check_place_owner_check_runs_first(session):
    with pytest.raises(AuthorizationError):
        _guard().check_place(_req(), actor_user_id=999, mode="sim", confirm_token=None, session=session)


def test_check_place_kill_switch_blocks_and_is_live_toggleable(session):
    g = _guard()
    g.check_place(_req(), actor_user_id=1, mode="sim", confirm_token=None, session=session)  # 一開始放行
    g.set_kill_switch(True)  # 執行中即時切換（codex F4：非啟動快照）
    with pytest.raises(RiskError):
        g.check_place(_req(client_order_id="C2"), actor_user_id=1, mode="sim",
                      confirm_token=None, session=session)
    g.set_kill_switch(False)
    g.check_place(_req(client_order_id="C3"), actor_user_id=1, mode="sim", confirm_token=None, session=session)


def test_check_place_symbol_not_whitelisted(session):
    with pytest.raises(RiskError):
        _guard().check_place(_req(symbol="ZZZ"), actor_user_id=1, mode="sim",
                             confirm_token=None, session=session)


def test_check_place_per_order_qty_limit(session):
    with pytest.raises(RiskError):
        _guard().check_place(_req(qty=3), actor_user_id=1, mode="sim", confirm_token=None, session=session)


def test_check_place_per_day_qty_limit(session):
    g = _guard(max_qty_per_day=3)
    brepo.create_order(session, client_order_id="P1", user_id=1, mode="sim", broker="shioaji",
                       account="F1", symbol="TXF", action="Buy", qty=2, price=Decimal("1"),
                       price_type="MKT", order_type="IOC", octype="New")
    with pytest.raises(RiskError):
        g.check_place(_req(client_order_id="P2", qty=2), actor_user_id=1, mode="sim",
                      confirm_token=None, session=session)  # 已 2 口 + 這筆 2 = 4 > 3


def test_check_place_per_day_order_count_limit(session):
    g = _guard(max_orders_per_day=1)
    brepo.create_order(session, client_order_id="Q1", user_id=1, mode="sim", broker="shioaji",
                       account="F1", symbol="TXF", action="Buy", qty=1, price=Decimal("1"),
                       price_type="MKT", order_type="IOC", octype="New")
    with pytest.raises(RiskError):
        g.check_place(_req(client_order_id="Q2"), actor_user_id=1, mode="sim",
                      confirm_token=None, session=session)  # 已 1 筆 + 這筆 = 2 > 1


def test_check_place_real_requires_valid_confirm_token(session):
    g = _guard()
    req = _req()
    with pytest.raises(RiskError):
        g.check_place(req, actor_user_id=1, mode="real", confirm_token=None, session=session)
    token = _confirm_for(req)
    g.check_place(req, actor_user_id=1, mode="real", confirm_token=token, session=session)  # 有效 token 放行


def test_check_place_real_rejects_token_for_different_payload(session):
    g = _guard()
    token = confirm.issue(1, "wrong-hash")
    with pytest.raises(RiskError):
        g.check_place(_req(), actor_user_id=1, mode="real", confirm_token=token, session=session)


def test_check_place_sim_never_needs_confirm_token(session):
    _guard().check_place(_req(), actor_user_id=1, mode="sim", confirm_token=None, session=session)


def test_check_place_records_audit_on_reject_and_ok(session):
    g = _guard()
    g.check_place(_req(client_order_id="A1"), actor_user_id=1, mode="sim", confirm_token=None, session=session)
    with pytest.raises(RiskError):
        g.check_place(_req(client_order_id="A2", symbol="ZZZ"), actor_user_id=1, mode="sim",
                      confirm_token=None, session=session)
    rows = list(session.exec(select(OrderAudit)))
    assert {r.result for r in rows} == {"ok", "rejected"}
    rejected = next(r for r in rows if r.result == "rejected")
    assert rejected.rule == "whitelist" and rejected.payload_hash


def test_check_update_reruns_all_limits_with_new_qty(session):
    g = _guard()
    order = brepo.create_order(session, client_order_id="U1", user_id=1, mode="sim", broker="shioaji",
                               account="F1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
                               price_type="LMT", order_type="ROD", octype="New")
    with pytest.raises(RiskError):
        g.check_update(order, new_qty=5, new_price=None, actor_user_id=1, mode="sim",
                       confirm_token=None, session=session)  # 改到 5 口，超過單筆上限 2


def test_check_update_excludes_own_prior_qty_from_daily_sum(session):
    g = _guard(max_qty_per_day=3, max_qty_per_order=3)
    order = brepo.create_order(session, client_order_id="U2", user_id=1, mode="sim", broker="shioaji",
                               account="F1", symbol="TXF", action="Buy", qty=2, price=Decimal("18000"),
                               price_type="LMT", order_type="ROD", octype="New")
    g.check_update(order, new_qty=3, new_price=None, actor_user_id=1, mode="sim",
                   confirm_token=None, session=session)  # 排除自己原本的 2 口，改到 3 口不超過上限 3


def test_check_update_real_requires_confirm_token(session):
    g = _guard()
    order = brepo.create_order(session, client_order_id="U3", user_id=1, mode="real", broker="shioaji",
                               account="F1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
                               price_type="LMT", order_type="ROD", octype="New")
    with pytest.raises(RiskError):
        g.check_update(order, new_qty=2, new_price=None, actor_user_id=1, mode="real",
                       confirm_token=None, session=session)


def test_parse_owner_ids_and_whitelist_helpers():
    assert parse_owner_ids("1, 2 ,3") == frozenset({1, 2, 3})
    assert parse_owner_ids("") == frozenset()
    assert parse_whitelist("TXF, MXF ,") == frozenset({"TXF", "MXF"})
```
跑 `uv run pytest tests/test_risk_guard.py -q` → RED（`RiskGuard`/`parse_owner_ids`/`parse_whitelist` 未定義）。

- [ ] **Step 5：RiskGuard（新檔 `src/quanquant/broker/risk.py`）**

> GateGuard：建新檔前陳述事實（owner授權+風控上限+即時kill switch+兩階段確認+audit）後重試。

```python
"""下單前風控閘 + owner 授權 + 兩階段確認 + append-only 稽核（codex D1/F1-F5/F7）。

owner 授權（D1/D4）：assert_owner() 先驗 actor_user_id 是否在設定的擁有者白名單，不是
→ AuthorizationError（router 對應 403）。單一券商帳戶掛 app singleton，第二個 app 使用者
一律被擋在這一層，不可能碰到下面的風控/送單邏輯。

即時 kill switch（F4）：_kill_switch 是 instance 屬性（不是建構時快照的常數），
set_kill_switch() 可在執行中即時切換；每次 check_place/check_update 都讀當下的值，
緊貼券商呼叫前再查一次。cancel 刻意不受 kill switch 影響（見 shioaji_adapter.cancel）。

兩階段確認（F1）：real 下單/改單缺 confirm_token，或 token 與 (actor_user_id, payload_hash)
不符/過期/用過 → RiskError。sim 完全不需要。

update 重跑全部風控（F2）：check_update 用「改後的有效 qty」重新過一次所有限制，並用
sum_qty_today_excluding（排除這張單自己原本的 qty）算單日累計，避免「先下小單再增量超限」。
quota 檢查與後續寫入 qty（在 adapter.update 的下一個交易）之間的視窗——本專案明訂單一
uvicorn worker（F7 範圍化決議），不另加 DB row lock；多 worker 需回頭補。

audit（F5）：每次 check_place/check_update 不論通過或拒絕，都寫一筆 OrderAudit（append-only、
只存 payload 的 hash，不存原始價格/口數等敏感欄位）。
"""
from quanquant.broker import confirm
from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, RiskError
from quanquant.broker.types import OrderRequest


def parse_owner_ids(raw: str) -> frozenset[int]:
    return frozenset(int(x.strip()) for x in raw.split(",") if x.strip())


def parse_whitelist(raw: str) -> frozenset[str]:
    return frozenset(x.strip() for x in raw.split(",") if x.strip())


class RiskGuard:
    def __init__(
        self,
        *,
        owner_user_ids: frozenset[int],
        symbol_whitelist: frozenset[str],
        max_qty_per_order: int,
        max_qty_per_day: int,
        max_orders_per_day: int,
        kill_switch: bool = False,
    ) -> None:
        self._owner_user_ids = owner_user_ids
        self._symbol_whitelist = symbol_whitelist
        self._max_qty_per_order = max_qty_per_order
        self._max_qty_per_day = max_qty_per_day
        self._max_orders_per_day = max_orders_per_day
        self._kill_switch = kill_switch

    @property
    def kill_switch(self) -> bool:
        return self._kill_switch

    def set_kill_switch(self, value: bool) -> None:
        self._kill_switch = value

    def assert_owner(self, actor_user_id: int) -> None:
        if actor_user_id not in self._owner_user_ids:
            raise AuthorizationError(f"user {actor_user_id} 非下單白名單擁有者")

    def check_place(
        self, req: OrderRequest, *, actor_user_id: int, mode: str, confirm_token: str | None, session,
    ) -> None:
        self.assert_owner(actor_user_id)
        payload_hash = brepo.hash_payload(
            req.client_order_id, req.symbol, req.action, req.qty, req.price, req.octype,
        )
        if self._kill_switch:
            self._reject(session, actor_user_id, mode, "kill_switch",
                        "全域 kill switch 已啟動，暫停所有下單", payload_hash)
        if req.symbol not in self._symbol_whitelist:
            self._reject(session, actor_user_id, mode, "whitelist",
                        f"商品 {req.symbol} 不在下單白名單", payload_hash)
        if req.qty > self._max_qty_per_order:
            self._reject(session, actor_user_id, mode, "max_qty_per_order",
                        f"單筆 {req.qty} 口超過上限 {self._max_qty_per_order}", payload_hash)
        if mode == "real" and not (
            confirm_token and confirm.verify(confirm_token, actor_user_id=actor_user_id, payload_hash=payload_hash)
        ):
            self._reject(session, actor_user_id, mode, "real_confirm",
                        "正式下單需有效二次確認 token", payload_hash)
        qty_today = brepo.sum_qty_today(session, user_id=actor_user_id, mode=mode)
        if qty_today + req.qty > self._max_qty_per_day:
            self._reject(session, actor_user_id, mode, "max_qty_per_day",
                        f"單日累計 {qty_today + req.qty} 口超過上限 {self._max_qty_per_day}", payload_hash)
        orders_today = brepo.count_orders_today(session, user_id=actor_user_id, mode=mode)
        if orders_today + 1 > self._max_orders_per_day:
            self._reject(session, actor_user_id, mode, "max_orders_per_day",
                        f"單日委託 {orders_today + 1} 次超過上限 {self._max_orders_per_day}", payload_hash)
        brepo.append_audit(session, actor_user_id=actor_user_id, mode=mode, action="place",
                           payload_hash=payload_hash, result="ok")
        session.commit()

    def check_update(
        self,
        order,
        *,
        new_qty: int | None,
        new_price,
        actor_user_id: int,
        mode: str,
        confirm_token: str | None,
        session,
    ) -> None:
        self.assert_owner(actor_user_id)
        payload_hash = brepo.hash_payload(order.client_order_id, new_qty, new_price)
        if self._kill_switch:
            self._reject(session, actor_user_id, mode, "kill_switch",
                        "全域 kill switch 已啟動，暫停改單", payload_hash)
        effective_qty = new_qty if new_qty is not None else order.qty
        if effective_qty <= 0:
            self._reject(session, actor_user_id, mode, "qty_invalid", "qty 必須 > 0", payload_hash)
        if effective_qty > self._max_qty_per_order:
            self._reject(session, actor_user_id, mode, "max_qty_per_order",
                        f"單筆 {effective_qty} 口超過上限 {self._max_qty_per_order}", payload_hash)
        if new_price is not None and new_price <= 0:
            self._reject(session, actor_user_id, mode, "price_invalid", "price 必須 > 0", payload_hash)
        if mode == "real" and not (
            confirm_token and confirm.verify(confirm_token, actor_user_id=actor_user_id, payload_hash=payload_hash)
        ):
            self._reject(session, actor_user_id, mode, "real_confirm",
                        "正式改單需有效二次確認 token", payload_hash)
        qty_today_others = brepo.sum_qty_today_excluding(
            session, user_id=actor_user_id, mode=mode, exclude_order_id=order.id,
        )
        if qty_today_others + effective_qty > self._max_qty_per_day:
            self._reject(session, actor_user_id, mode, "max_qty_per_day",
                        f"改單後單日累計 {qty_today_others + effective_qty} 口超過上限 {self._max_qty_per_day}",
                        payload_hash)
        brepo.append_audit(session, actor_user_id=actor_user_id, mode=mode, action="update",
                           payload_hash=payload_hash, result="ok")
        session.commit()

    @staticmethod
    def _reject(session, actor_user_id: int, mode: str, rule: str, message: str, payload_hash: str) -> None:
        brepo.append_audit(session, actor_user_id=actor_user_id, mode=mode, action="risk_reject",
                           payload_hash=payload_hash, rule=rule, result="rejected", detail=message)
        session.commit()  # 違規要真的落地，即便馬上 raise（呼叫端 session 於例外傳播時會關閉/回滾未提交項）
        raise RiskError(message)
```

- [ ] **Step 6：跑測試確認 GREEN**

```bash
uv run pytest tests/test_risk_guard.py tests/test_confirm.py -q
```

- [ ] **Step 7：跑全測試確認未回歸**

```bash
uv run pytest -q
```

- [ ] **Step 8：Commit（scoped）**

```bash
git add src/quanquant/broker/confirm.py src/quanquant/broker/risk.py \
  tests/test_confirm.py tests/test_risk_guard.py
git commit -m "feat: RiskGuard（owner allowlist+即時kill switch+白名單+單筆單日上限+real兩階段確認+audit）不依賴Settings"
```

---

### Task 8：設定 + lifespan（readiness gate／watchdog／shutdown，codex E2/E3/F6/A6）

`config.py` 加下單設定（`ORDER_MODE` 用 `Literal["sim","real"]`，拼錯直接 pydantic 驗證失敗拒絕啟動——F6）；lifespan 起下單子系統：**readiness gate**（`await adapter.connect()` 成功才 publish `app.state.order_service`，失敗 fail closed、`/healthz` 反映——E2）、**watchdog**（週期性用一次 `positions()` 探活，失敗則 `close()+connect()` 重連，每輪都順手 `retry_quarantined()` 對帳——E3）、**shutdown**（先 `adapter.close()` 停 callback → `fill_worker.drain()` → 才 cancel 背景任務）。`real` 模式缺 CA 設定 → 下單子系統不啟動（只擋下單，不擋整個監控 app）。`ShioajiAdapter` 補 `order_sim_fee`：sim 模式的 fee 一律伺服器依設定按口計費，不信任（常缺失的）broker sim payload（codex A6）。

**Files:**
- Modify: `src/quanquant/config.py`（`Settings` 加下單設定欄位）
- Modify: `src/quanquant/broker/base.py`（加 `OrderSessionState` dataclass）
- Modify: `src/quanquant/broker/shioaji_adapter.py`（`__init__` 加 `order_sim_fee` 參數；`_deal_to_fill` 依 `mode` 決定 fee 來源）
- Modify: `src/quanquant/web/deps.py`（`get_order_service`/`get_order_state`/`get_order_risk_guard`）
- Modify: `src/quanquant/web/routers/health.py`（`/healthz` 回傳 `order_session` 狀態）
- Modify: `src/quanquant/web/app.py`（import＋lifespan 起下單子系統：`_order_subsystem_preflight`/`_order_watchdog`＋readiness gate＋shutdown 排序）
- Test: Create `tests/test_order_settings.py`、Create `tests/test_order_watchdog.py`、Modify `tests/test_shioaji_adapter.py`（sim fee 測試）

**Interfaces:**
- Consumes：Task 6 `ShioajiAdapter`、Task 5 `FillWorker`、Task 4 `PositionTracker`、Task 7 `RiskGuard`/`parse_owner_ids`/`parse_whitelist`
- Produces（Task 9/10 依賴）：
  - `Settings.order_mode: Literal["sim","real"]`、`shioaji_trade_api_key/secret_key/ca_path/ca_passwd/person_id`、`order_owner_user_ids`、`order_symbol_whitelist`、`order_max_qty_per_order/day`、`order_max_orders_per_day`、`order_kill_switch`、`order_sim_fee: Decimal`
  - `broker.base.OrderSessionState(enabled=False, connected=False, last_error=None)`
  - `app.state.order_service`（`OrderService | None`）、`app.state.order_risk_guard`（`RiskGuard | None`）、`app.state.order_state`（`OrderSessionState`）
  - `web.deps.get_order_service(request) -> OrderService | None`、`get_order_state(request) -> OrderSessionState | None`、`get_order_risk_guard(request) -> RiskGuard | None`
  - `/healthz` 回傳新增 `"order_session": {"enabled", "connected", "last_error"}`（下單子系統未啟用時不含此鍵）

- [ ] **Step 1：先寫失敗測試（新檔 `tests/test_order_settings.py`）**

> GateGuard：建新檔前陳述事實（下單設定安全預設、ORDER_MODE 拼錯拒啟、preflight 決策函式）後重試。

```python
"""下單設定：安全預設（sim/kill switch off/上限保守）＋ env 覆寫＋ORDER_MODE 拼錯拒絕啟動；
_order_subsystem_preflight 決策（真正需不需要啟動下單子系統/real缺CA不啟動）；healthz 回傳
order_session 狀態。"""
from decimal import Decimal

import pytest
from pydantic import ValidationError

from quanquant.config import Settings


def test_order_defaults_are_safe():
    s = Settings(_env_file=None)
    assert s.order_mode == "sim"               # 預設模擬，不碰真錢
    assert s.order_kill_switch is False
    assert s.order_owner_user_ids == ""         # 預設沒有 owner（fail closed：沒設定就沒人能下單）
    assert "TXF" in s.order_symbol_whitelist
    assert s.order_max_qty_per_order >= 1
    assert s.order_max_qty_per_day >= 1
    assert s.order_max_orders_per_day >= 1
    assert s.order_sim_fee == Decimal("50")


def test_order_mode_typo_rejected():
    with pytest.raises(ValidationError):
        Settings(_env_file=None, order_mode="smi")  # 拼錯 → 驗證失敗，拒絕啟動（codex F6）


def test_order_env_override(monkeypatch):
    monkeypatch.setenv("ORDER_MODE", "real")
    monkeypatch.setenv("ORDER_KILL_SWITCH", "true")
    monkeypatch.setenv("ORDER_MAX_QTY_PER_ORDER", "3")
    s = Settings(_env_file=None)
    assert s.order_mode == "real" and s.order_kill_switch is True
    assert s.order_max_qty_per_order == 3


def test_preflight_disabled_when_keys_missing():
    from quanquant.web.app import _order_subsystem_preflight
    s = Settings(_env_file=None, order_owner_user_ids="1")
    enabled, reason = _order_subsystem_preflight(s)
    assert enabled is False and reason is None  # 單純沒設金鑰，非錯誤


def test_preflight_disabled_when_no_owners():
    from quanquant.web.app import _order_subsystem_preflight
    s = Settings(_env_file=None, shioaji_trade_api_key="k", shioaji_trade_secret_key="s")
    enabled, reason = _order_subsystem_preflight(s)
    assert enabled is False and reason is None


def test_preflight_real_without_ca_refuses_with_reason():
    from quanquant.web.app import _order_subsystem_preflight
    s = Settings(_env_file=None, shioaji_trade_api_key="k", shioaji_trade_secret_key="s",
                order_owner_user_ids="1", order_mode="real")
    enabled, reason = _order_subsystem_preflight(s)
    assert enabled is False and reason is not None and "CA" in reason  # real 缺 CA 不啟動（codex F6）


def test_preflight_real_with_full_ca_enabled():
    from quanquant.web.app import _order_subsystem_preflight
    s = Settings(_env_file=None, shioaji_trade_api_key="k", shioaji_trade_secret_key="s",
                order_owner_user_ids="1", order_mode="real",
                shioaji_ca_path="/x.pfx", shioaji_ca_passwd="pw", shioaji_person_id="A1")
    enabled, reason = _order_subsystem_preflight(s)
    assert enabled is True and reason is None


def test_preflight_sim_enabled_without_ca():
    from quanquant.web.app import _order_subsystem_preflight
    s = Settings(_env_file=None, shioaji_trade_api_key="k", shioaji_trade_secret_key="s",
                order_owner_user_ids="1", order_mode="sim")
    enabled, reason = _order_subsystem_preflight(s)
    assert enabled is True and reason is None  # sim 免 CA


def test_healthz_reports_order_session_state(engine):
    from fastapi.testclient import TestClient

    from quanquant.broker.base import OrderSessionState
    from quanquant.web.deps import get_order_state
    from tests.conftest import _build_app

    app = _build_app(engine)
    app.dependency_overrides[get_order_state] = lambda: OrderSessionState(
        enabled=True, connected=False, last_error="boom",
    )
    body = TestClient(app).get("/healthz").json()
    assert body["order_session"] == {"enabled": True, "connected": False, "last_error": "boom"}
```
跑 `uv run pytest tests/test_order_settings.py -q` → RED。

- [ ] **Step 2：設定欄位（`src/quanquant/config.py`）**

檔首改為（加 `Decimal`/`Literal`）：
```python
from decimal import Decimal
from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict
```
在 `Settings` 的 `tv_symbol`（最後一個既有欄位）之後、`class` 結束前新增：
```python
    # ---- 下單子系統（Shioaji 期貨；預設 sim，不碰真 CA/真錢）----
    order_mode: Literal["sim", "real"] = "sim"  # 拼錯值 → pydantic 驗證失敗，直接拒絕啟動（codex F6）
    shioaji_trade_api_key: str = ""             # 下單專用 token（可與行情 key 不同帳戶）
    shioaji_trade_secret_key: str = ""
    shioaji_ca_path: str = ""                   # .pfx 路徑（real；read-only bind-mount，不進 git）
    shioaji_ca_passwd: str = ""
    shioaji_person_id: str = ""                 # CA 綁定身分
    order_owner_user_ids: str = ""              # 逗號分隔 user id；空字串＝沒有人被授權（fail closed）
    order_symbol_whitelist: str = "TXF,MXF"     # 逗號分隔可下單商品白名單
    order_max_qty_per_order: int = 2            # 單筆口數上限
    order_max_qty_per_day: int = 10             # 單日累計口數上限
    order_max_orders_per_day: int = 20          # 單日委託次數上限
    order_kill_switch: bool = False             # 全域急停啟動預設值（執行中可由 RiskGuard 即時切換）
    order_sim_fee: Decimal = Decimal("50")      # sim 成交手續費（伺服器按口計費，不信任 broker sim payload）
```

- [ ] **Step 3：跑設定測試（部分 GREEN，preflight/healthz 仍 RED）**

```bash
uv run pytest tests/test_order_settings.py -q
```

- [ ] **Step 4：`OrderSessionState`（`src/quanquant/broker/base.py`）**

檔首 import 行加 `from dataclasses import dataclass`：
```python
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, runtime_checkable
```
在 `AuthorizationError` 之後、`OrderService` Protocol 之前新增：
```python
@dataclass
class OrderSessionState:
    """下單 session 就緒狀態；/healthz 用來反映 readiness gate 結果（codex E2）。"""

    enabled: bool = False
    connected: bool = False
    last_error: str | None = None
```

- [ ] **Step 5：`ShioajiAdapter` 加 `order_sim_fee`（`src/quanquant/broker/shioaji_adapter.py`）**

`__init__` 簽名（`risk_guard=None,` 之後）加一個參數：
```python
        risk_guard=None,
        order_sim_fee=None,
    ) -> None:
```
（`Decimal` 已經 import；在檔首 `from decimal import Decimal, InvalidOperation` 保持不動即可，型別註記略過以維持與既有參數一致的簡潔風格。）建構式內（`self._risk_guard = risk_guard` 之後）加：
```python
        self._order_sim_fee = order_sim_fee if order_sim_fee is not None else Decimal("50")
```
`_deal_to_fill` 改為（fee 計算依 `self.mode` 分流）：
```python
    def _deal_to_fill(self, msg: dict) -> Fill:
        """OrderState.FuturesDeal payload → Fill。欄位鍵名為官方文件最佳猜測；真整合
        （Task 9 的 simtrade 手動 E2E）需對照實際 dict 微調——見該 task 註記。mode/account
        一律用 self.mode/self._account，不讀 msg（codex C1/F6）。sim 的 fee 一律伺服器按口
        計費，不信任（常缺失/不可靠的）broker sim payload（codex A6）。"""
        qty = int(msg.get("quantity", 0) or 0)
        if self.mode == "sim":
            fee = self._order_sim_fee * qty
        else:
            fee = _dec(msg.get("fee")) if msg.get("fee") is not None else None
        return Fill(
            broker=self._broker,
            fill_id=str(msg.get("trade_id") or msg.get("seqno") or msg.get("id") or ""),
            ordno=str(msg.get("ordno") or msg.get("seqno") or "") or None,
            symbol=str(msg.get("code", "")),
            action=str(msg.get("action", "")),
            price=_dec(msg.get("price")),
            qty=qty,
            fee=fee,
            octype=str(msg.get("order_cond") or msg.get("octype") or msg.get("order_type") or "Auto"),
            ts=int(float(msg.get("ts", 0)) * 1000),  # 秒 → 毫秒
            account=self._account,
            mode=self.mode,
            user_id=None,  # 由 fill_worker 依 ordno 解析 order context 補上，這裡不猜
        )
```

在 `tests/test_shioaji_adapter.py` 末端追加（沿用檔內既有 `_RecordingLoop`/`_FakeShioaji`/`_adapter` helper 與 import）：
```python
def test_sim_fee_computed_by_qty_not_trusted_from_msg(engine):
    a = ShioajiAdapter(lambda: Session(engine), _RecordingLoop(), mode="sim", order_sim_fee=Decimal("50"))
    a._api = _FakeShioaji()
    a._account = "F123"
    fills = []
    a.on_fill(fills.append)
    msg = {"code": "TXF", "action": "Buy", "price": 18000, "quantity": 3,
          "trade_id": "D1", "ordno": "O1", "ts": 1, "order_cond": "New", "fee": 1}  # msg 的 fee 刻意給假值
    a._on_order_cb(SimpleNamespace(value="FuturesDeal"), msg)
    assert fills[0].fee == Decimal("150")  # 50 * 3 口，不信任 msg 的 fee=1


def test_real_fee_taken_from_broker_msg(engine):
    a = ShioajiAdapter(lambda: Session(engine), _RecordingLoop(), mode="real", order_sim_fee=Decimal("50"))
    a._api = _FakeShioaji()
    a._account = "F123"
    fills = []
    a.on_fill(fills.append)
    msg = {"code": "TXF", "action": "Buy", "price": 18000, "quantity": 1,
          "trade_id": "D2", "ordno": "O2", "ts": 1, "order_cond": "New", "fee": 47}
    a._on_order_cb(SimpleNamespace(value="FuturesDeal"), msg)
    assert fills[0].fee == Decimal("47")  # real：照券商回報
```

- [ ] **Step 6：`get_order_service`/`get_order_state`/`get_order_risk_guard`（`src/quanquant/web/deps.py`）**

`__all__` 加三個名字：
```python
__all__ = ["get_session", "get_poller", "get_pulse", "parse_date",
           "get_current_user", "require_admin",
           "get_order_service", "get_order_state", "get_order_risk_guard"]
```
在 `get_pulse` 之後新增：
```python
def get_order_service(request: Request):
    """下單服務單例；未啟用/連線失敗/測試未接線時為 None（readiness gate fail closed）。"""
    return getattr(request.app.state, "order_service", None)


def get_order_state(request: Request):
    """下單 session 就緒狀態；供 /healthz 與 UI 顯示。"""
    return getattr(request.app.state, "order_state", None)


def get_order_risk_guard(request: Request):
    """RiskGuard 單例；未啟用時為 None（供 kill switch 開關路由使用）。"""
    return getattr(request.app.state, "order_risk_guard", None)
```

- [ ] **Step 7：`/healthz` 回傳 order_session（`src/quanquant/web/routers/health.py`）**

改為：
```python
"""Unauthenticated health check (GCP uptime check hits this without credentials)."""
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from quanquant.broker.base import OrderSessionState
from quanquant.poller import QuotePoller
from quanquant.web.deps import get_order_state, get_poller

router = APIRouter()


@router.get("/healthz")
async def healthz(
    poller: QuotePoller | None = Depends(get_poller),
    order_state: OrderSessionState | None = Depends(get_order_state),
):
    age: float | None = None
    if poller is not None and poller.last is not None:
        age = (datetime.now(timezone.utc) - poller.last.fetched_at).total_seconds()
    payload = {"status": "ok", "last_quote_age_s": age}
    if order_state is not None:
        payload["order_session"] = {
            "enabled": order_state.enabled,
            "connected": order_state.connected,
            "last_error": order_state.last_error,
        }
    return JSONResponse(payload)
```

- [ ] **Step 8：跑測試確認 GREEN（settings/preflight/healthz）**

```bash
uv run pytest tests/test_order_settings.py tests/test_shioaji_adapter.py -q
```

- [ ] **Step 9：先寫失敗測試 — watchdog（新檔 `tests/test_order_watchdog.py`）**

> GateGuard：建新檔前陳述事實（下單 session watchdog：探活失敗重連＋每輪對帳）後重試。

```python
"""_order_watchdog：探活失敗 → close+connect 重連並更新 state；不論探活結果每輪都
retry_quarantined 對帳（codex E3：重連+backoff+重連後對帳）。"""
import asyncio

from sqlmodel import Session

from quanquant.broker.base import OrderSessionState
from quanquant.web.app import _order_watchdog


class _FakeFillWorker:
    def __init__(self):
        self.retry_calls = 0

    def retry_quarantined(self, session):
        self.retry_calls += 1
        return 0


class _FlakyAdapter:
    """第一次探活失敗（模擬斷線），close+connect 後第二次起探活成功。"""

    def __init__(self):
        self.positions_calls = 0
        self.closed = False
        self.connected = False

    async def positions(self, *, actor_user_id):
        self.positions_calls += 1
        if self.positions_calls == 1:
            raise RuntimeError("connection lost")
        return []

    async def close(self):
        self.closed = True

    async def connect(self):
        self.connected = True


def test_watchdog_reconnects_after_probe_failure_and_reconciles(engine):
    async def scenario():
        adapter = _FlakyAdapter()
        worker = _FakeFillWorker()
        state = OrderSessionState(enabled=True, connected=True)
        task = asyncio.create_task(
            _order_watchdog(adapter, worker, state, lambda: Session(engine), frozenset({1}), interval=0.01)
        )
        await asyncio.sleep(0.05)  # 讓它跑過至少一輪（探活失敗→重連→對帳）
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert adapter.closed is True and adapter.connected is True
        assert state.connected is True and state.last_error is None
        assert worker.retry_calls >= 1

    asyncio.run(scenario())


def test_watchdog_without_owner_still_reconciles(engine):
    async def scenario():
        worker = _FakeFillWorker()
        state = OrderSessionState(enabled=True, connected=True)

        class _NoProbeAdapter:
            async def positions(self, *, actor_user_id):
                raise AssertionError("owner_ids 為空時不該呼叫 positions 探活")

        task = asyncio.create_task(
            _order_watchdog(_NoProbeAdapter(), worker, state, lambda: Session(engine), frozenset(), interval=0.01)
        )
        await asyncio.sleep(0.03)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert worker.retry_calls >= 1  # 沒 owner 也照樣定期對帳

    asyncio.run(scenario())
```
跑 `uv run pytest tests/test_order_watchdog.py -q` → RED（`_order_watchdog` 未定義）。

- [ ] **Step 10：lifespan 接線（`src/quanquant/web/app.py`）**

import 區（`from quanquant.alerts.engine import run_alert_engine` 之後）加：
```python
from quanquant.broker.fill_worker import FillWorker
from quanquant.broker.position_tracker import PositionTracker
from quanquant.broker.risk import RiskGuard, parse_owner_ids, parse_whitelist
from quanquant.broker.shioaji_adapter import ShioajiAdapter
```

在 `_prune_quotes_loop` 之後、`lifespan` 之前新增兩個模組層函式：
```python
def _order_subsystem_preflight(settings) -> tuple[bool, str | None]:
    """回傳 (enabled, refuse_reason)。refuse_reason 非 None 時 enabled 必為 False；
    enabled 為 False 但 refuse_reason 為 None，代表單純沒設定金鑰/owner（非錯誤，靜默不啟用）。"""
    owner_ids = parse_owner_ids(settings.order_owner_user_ids)
    if not (settings.shioaji_trade_api_key and settings.shioaji_trade_secret_key and owner_ids):
        return False, None
    if settings.order_mode == "real":
        ca_ready = bool(settings.shioaji_ca_path and settings.shioaji_ca_passwd and settings.shioaji_person_id)
        if not ca_ready:
            return False, (
                "ORDER_MODE=real 但 CA 設定不完整（ca_path/ca_passwd/person_id），下單子系統不啟動"
            )
    return True, None


async def _order_watchdog(
    adapter, fill_worker, state, session_factory, owner_ids: frozenset, *, interval: float = 60.0,
) -> None:
    """下單 session 監看（codex E3）：週期性用一次輕量 positions() 探活；失敗視為斷線 →
    close()+connect() 重連（backoff＝interval，節流避免撞 5連線/1000login 額度）；
    不論探活結果，每輪都掃一次 retry_quarantined（重連後對帳，也順手清掉殘留 quarantine）。"""
    probe_user = next(iter(owner_ids)) if owner_ids else None
    while True:
        await asyncio.sleep(interval)
        if probe_user is not None:
            try:
                await adapter.positions(actor_user_id=probe_user)
            except Exception as exc:
                state.connected = False
                state.last_error = f"探活失敗，嘗試重連：{exc}"
                log.warning("order session 探活失敗，重連中：%s", exc)
                try:
                    await adapter.close()
                    await adapter.connect()
                except Exception as reconnect_exc:
                    state.last_error = str(reconnect_exc)
                    log.warning("order session 重連失敗，%.0fs 後再試：%s", interval, reconnect_exc)
                    continue
                state.connected = True
                state.last_error = None
                log.info("order session 重連成功")
        with session_factory() as session:
            resolved = fill_worker.retry_quarantined(session)
        if resolved:
            log.info("watchdog 對帳：解決 %d 筆 quarantine fill", resolved)
```

`lifespan` 內，`else: tasks.append(asyncio.create_task(poller.run()))` 之後、`try:\n        yield` 之前插入下單子系統接線：
```python
    order_state = OrderSessionState()
    order_service = None
    fill_worker = None
    guard = None
    owner_ids = parse_owner_ids(settings.order_owner_user_ids)
    order_enabled, refuse_reason = _order_subsystem_preflight(settings)
    if refuse_reason is not None:
        order_state.last_error = refuse_reason
        log.error(refuse_reason)
    if order_enabled:
        order_state.enabled = True
        guard = RiskGuard(
            owner_user_ids=owner_ids,
            symbol_whitelist=parse_whitelist(settings.order_symbol_whitelist),
            max_qty_per_order=settings.order_max_qty_per_order,
            max_qty_per_day=settings.order_max_qty_per_day,
            max_orders_per_day=settings.order_max_orders_per_day,
            kill_switch=settings.order_kill_switch,
        )
        adapter = ShioajiAdapter(
            lambda: Session(get_engine()), asyncio.get_running_loop(),
            mode=settings.order_mode, api_key=settings.shioaji_trade_api_key,
            secret_key=settings.shioaji_trade_secret_key, ca_path=settings.shioaji_ca_path,
            ca_passwd=settings.shioaji_ca_passwd, person_id=settings.shioaji_person_id,
            risk_guard=guard, order_sim_fee=settings.order_sim_fee,
        )
        fill_worker = FillWorker(lambda: Session(get_engine()), tracker=PositionTracker())
        adapter.on_fill(fill_worker.enqueue)
        try:
            await adapter.connect()  # readiness gate（codex E2）：連線成功才 publish service
        except Exception as exc:
            order_state.last_error = str(exc)
            log.error("order session 連線失敗，下單子系統 fail closed（/orders 不可用）：%s", exc)
        else:
            order_state.connected = True
            order_service = adapter
            tasks.append(asyncio.create_task(fill_worker.run()))
            tasks.append(asyncio.create_task(_order_watchdog(
                adapter, fill_worker, order_state, lambda: Session(get_engine()), owner_ids,
            )))
            log.info("order session enabled (mode=%s, owners=%s)", settings.order_mode, sorted(owner_ids))
    app.state.order_service = order_service
    app.state.order_risk_guard = guard
    app.state.order_state = order_state
```
（`OrderSessionState` 需在 import 區補上 `from quanquant.broker.base import OrderSessionState`。）

`finally:` 區塊改為（先停 callback → drain inbox，才 cancel 背景任務——codex E3 shutdown 排序）：
```python
    try:
        yield
    finally:
        if order_service is not None:
            await order_service.close()  # 先停 callback（logout，不再有新 callback 進來）
            if fill_worker is not None:
                await fill_worker.drain(timeout=10.0)  # 再 drain 佇列裡已進來的 fill
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        await source.close()
```

- [ ] **Step 11：跑測試確認 GREEN（watchdog + 全部 Task 8 測試）**

```bash
uv run pytest tests/test_order_watchdog.py tests/test_order_settings.py tests/test_shioaji_adapter.py -q
```

- [ ] **Step 12：跑全測試確認未回歸**

```bash
uv run pytest -q
```
預期既有測試全綠（測試環境未設 `shioaji_trade_api_key`/`order_owner_user_ids` → `_order_subsystem_preflight` 回 `(False, None)`，下單子系統整段 skip，`app.state.order_service` 為 `None`，`/healthz` 不含 `order_session` 鍵，行為與 Task 8 之前完全一致）。

- [ ] **Step 13：Commit（scoped）**

```bash
git add src/quanquant/config.py src/quanquant/broker/base.py src/quanquant/broker/shioaji_adapter.py \
  src/quanquant/web/deps.py src/quanquant/web/routers/health.py src/quanquant/web/app.py \
  tests/test_order_settings.py tests/test_order_watchdog.py tests/test_shioaji_adapter.py
git commit -m "feat: 下單設定(ORDER_MODE typo拒啟)+lifespan readiness gate(fail closed反映healthz)+watchdog(探活重連+對帳)+shutdown(停callback→drain→關loop)"
```

---

### Task 9：`orders` 路由 + UI（codex D3/F1/F4/H）

下單面板（sim 直接送，real 兩階段確認：第一次 POST 未帶 `confirm_token` → router 算好 `payload_hash`＋簽發 token，回一個確認 partial 帶隱藏 `client_order_id`/`confirm_token`，同一顆按鈕重新 POST 才真的送）、委託列表（mode scope）、部位、cancel/update（`AuthorizationError`→403）、owner-only 即時 kill switch 開關（`HX-Refresh` 全頁刷新反映新狀態）。**下單 mode 恆等於 `service.mode`（server-side session），與頁面 tab 選擇無關**——tab 只是切換「檢視哪個 mode 的委託/日誌」，不是切換要送去哪個 session（因為全站只有一個下單 session，見 Task 8）；`service.mode != tab` 時顯示提示。仿 `trades.py` 樣板，HTMX-first。

**Files:**
- Create: `src/quanquant/web/routers/orders.py`
- Create: `src/quanquant/web/templates/orders.html`
- Create: `src/quanquant/web/templates/partials/order_table.html`
- Create: `src/quanquant/web/templates/partials/position_table.html`
- Create: `src/quanquant/web/templates/partials/order_confirm.html`
- Create: `src/quanquant/web/templates/partials/order_update_confirm.html`
- Modify: `src/quanquant/web/app.py`（import `orders` + `app.include_router(orders.router, dependencies=protected)`）
- Modify: `src/quanquant/web/templates/base.html`（nav 加 `/orders` 連結）
- Test: Create `tests/test_api_orders.py`

**Interfaces:**
- Consumes：Task 8 `get_order_service`/`get_order_risk_guard`、Task 2 `OrderRequest`/`OrderAck`/`Position`/`AuthorizationError`/`RiskError`/`OrderError`、Task 3 `brepo.list_orders`/`hash_payload`、Task 7 `confirm.issue`、既有 `get_current_user`/`get_session`
- Produces：`GET /orders`（頁）、`GET /orders/list`（委託列表 fragment）、`GET /orders/positions`（部位 fragment）、`POST /orders`（下單，含 real 二次確認）、`POST /orders/{broker_order_id}/cancel`、`POST /orders/{broker_order_id}/update`（含 real 二次確認）、`POST /orders/kill-switch`（owner-only 即時切換）

- [ ] **Step 1：先寫失敗測試（新檔 `tests/test_api_orders.py`，用 FakeOrderService/FakeRiskGuard 覆寫依賴）**

> GateGuard：建新檔前陳述事實（下單路由資料邏輯以 fake service/guard + in-memory DB 驗證，含 real 二次確認往返、cancel/kill-switch 授權對應 403）後重試。

```python
"""下單路由：頁面渲染、委託列表(mode scope)、部位渲染、POST 下單（sim 直送/real 二次確認
往返）、cancel/update 授權例外對應 403、kill switch owner-only 即時切換。"""
import re
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.broker import confirm
from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, RiskError
from quanquant.broker.types import OrderAck, Position
from quanquant.web.deps import get_order_risk_guard, get_order_service

from tests.conftest import _build_app


class _FakeOrderService:
    def __init__(self, mode="sim"):
        self.mode = mode
        self.placed = []
        self.cancelled = []
        self.updated = []
        self.raise_on_place = None
        self.raise_on_cancel = None

    async def place(self, req, *, actor_user_id, confirm_token=None):
        if self.raise_on_place:
            raise self.raise_on_place
        self.placed.append((req, actor_user_id, confirm_token))
        return OrderAck(client_order_id=req.client_order_id, broker_order_id="B1", ordno="O1", status="submitted")

    async def cancel(self, broker_order_id, *, actor_user_id):
        if self.raise_on_cancel:
            raise self.raise_on_cancel
        self.cancelled.append((broker_order_id, actor_user_id))
        return OrderAck(client_order_id="", broker_order_id=broker_order_id, ordno=None, status="cancelled")

    async def update(self, broker_order_id, *, actor_user_id, price=None, qty=None, confirm_token=None):
        self.updated.append((broker_order_id, actor_user_id, price, qty, confirm_token))
        return OrderAck(client_order_id="", broker_order_id=broker_order_id, ordno=None, status="updated")

    async def positions(self, *, actor_user_id):
        return [Position(symbol="TXF", direction="long", qty=2, avg_price=Decimal("18000"))]

    def on_fill(self, handler):
        pass


class _FakeRiskGuard:
    def __init__(self):
        self._kill_switch = False
        self.owner_ids = {1}

    @property
    def kill_switch(self):
        return self._kill_switch

    def set_kill_switch(self, value):
        self._kill_switch = value

    def assert_owner(self, actor_user_id):
        if actor_user_id not in self.owner_ids:
            raise AuthorizationError("not owner")


@pytest.fixture
def fake_service():
    return _FakeOrderService(mode="sim")


@pytest.fixture
def fake_guard():
    return _FakeRiskGuard()


@pytest.fixture
def order_client(engine, user, fake_service, fake_guard):
    fake_guard.owner_ids = {user.id}
    app = _build_app(engine)
    app.dependency_overrides[get_order_service] = lambda: fake_service
    app.dependency_overrides[get_order_risk_guard] = lambda: fake_guard
    c = TestClient(app)
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    return c


def setup_function():
    confirm.clear_consumed()


def test_orders_page_renders(order_client):
    assert order_client.get("/orders").status_code == 200


def test_order_list_scoped_by_mode(order_client, session, user):
    brepo.create_order(session, client_order_id="L1", user_id=user.id, mode="sim", broker="shioaji",
                       account="F1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
                       price_type="LMT", order_type="ROD", octype="New")
    brepo.create_order(session, client_order_id="L2", user_id=user.id, mode="real", broker="shioaji",
                       account="F1", symbol="MXF", action="Sell", qty=1, price=Decimal("18000"),
                       price_type="LMT", order_type="ROD", octype="New")
    sim = order_client.get("/orders/list", params={"mode": "sim"}).text
    real = order_client.get("/orders/list", params={"mode": "real"}).text
    assert "TXF" in sim and "MXF" not in sim
    assert "MXF" in real and "TXF" not in real


def test_positions_render(order_client):
    body = order_client.get("/orders/positions").text
    assert "TXF" in body and "18,000" in body


def test_place_order_sim_sends_directly(order_client, fake_service, user):
    r = order_client.post("/orders", data={
        "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert r.status_code == 200
    assert r.headers.get("HX-Trigger") == "refreshorders"
    assert len(fake_service.placed) == 1
    assert fake_service.placed[0][1] == user.id  # actor_user_id 由登入者穿入


def test_place_order_real_requires_confirmation_round_trip(order_client, fake_service, user):
    fake_service.mode = "real"
    r1 = order_client.post("/orders", data={
        "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert r1.status_code == 200
    assert len(fake_service.placed) == 0  # 第一次只給確認面板，還沒真的送單
    token = re.search(r'name="confirm_token" value="([^"]+)"', r1.text).group(1)
    client_order_id = re.search(r'name="client_order_id" value="([^"]+)"', r1.text).group(1)
    r2 = order_client.post("/orders", data={
        "client_order_id": client_order_id,
        "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New", "confirm_token": token,
    })
    assert r2.status_code == 200
    assert len(fake_service.placed) == 1 and fake_service.placed[0][2] == token


def test_place_order_without_service_errors(client):
    r = client.post("/orders", data={
        "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert r.status_code == 200
    assert r.headers.get("HX-Retarget") == ".form-error-slot"


def test_place_order_authorization_error_maps_to_403(order_client, fake_service):
    fake_service.raise_on_place = AuthorizationError("not owner")
    r = order_client.post("/orders", data={
        "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert r.status_code == 403


def test_place_order_risk_error_shows_form_error(order_client, fake_service):
    fake_service.raise_on_place = RiskError("blocked")
    r = order_client.post("/orders", data={
        "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert r.headers.get("HX-Retarget") == ".form-error-slot"


def test_cancel_order_triggers_service(order_client, fake_service, user):
    r = order_client.post("/orders/B1/cancel")
    assert r.status_code == 200
    assert fake_service.cancelled == [("B1", user.id)]


def test_cancel_order_authorization_error_maps_to_403(order_client, fake_service):
    fake_service.raise_on_cancel = AuthorizationError("not yours")
    r = order_client.post("/orders/B1/cancel")
    assert r.status_code == 403


def test_kill_switch_toggle_owner_only(order_client, fake_guard):
    r = order_client.post("/orders/kill-switch", data={"value": "on"})
    assert r.status_code == 200 and r.headers.get("HX-Refresh") == "true"
    assert fake_guard.kill_switch is True


def test_kill_switch_toggle_rejects_non_owner(engine, user, fake_service, fake_guard):
    fake_guard.owner_ids = {999}  # 目前登入者不是 owner
    app = _build_app(engine)
    app.dependency_overrides[get_order_service] = lambda: fake_service
    app.dependency_overrides[get_order_risk_guard] = lambda: fake_guard
    c = TestClient(app)
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    r = c.post("/orders/kill-switch", data={"value": "on"})
    assert r.status_code == 403
```
跑 `uv run pytest tests/test_api_orders.py -q` → RED。

- [ ] **Step 2：orders 路由（新檔 `src/quanquant/web/routers/orders.py`）**

> GateGuard：建新檔前陳述事實（下單面板/委託列表/部位 HTMX 路由，仿 trades.py，含 real 二次確認往返）後重試。

```python
"""下單面板 / 委託列表 / 部位（HTMX-first，仿 journal/trades 樣板）。

下單資料邏輯（建 Order、風控、觸發 service、cancel/update 所有權、kill switch 授權）走
pytest；純視覺互動（modal 排版、CSS）待 ORDER_MODE=sim 手動 simtrade 實測。

real 兩階段確認流程：第一次 POST /orders 未帶 confirm_token → 這裡先算好 payload_hash、
簽發 token，回一個「確認面板」partial（帶隱藏 client_order_id/confirm_token，同一顆按鈕
重新 POST 同一份表單）；第二次帶 token 才真的呼叫 service.place()。update 同理。

下單 mode 恆等於 service.mode（server-side session），與頁面 tab 選擇無關——tab 只是切換
「檢視哪個 mode 的委託」，不是切換要送去哪個 session（全站只有一個下單 session）。
"""
import uuid
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlmodel import Session

from quanquant.broker import confirm
from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, OrderError, RiskError
from quanquant.broker.types import OrderRequest
from quanquant.db.models import User
from quanquant.web.deps import get_current_user, get_order_risk_guard, get_order_service, get_session
from quanquant.web.templating import render_partial, templates

router = APIRouter()

_MODES = ("real", "sim")


def _mode(raw: str | None) -> str:
    return raw if raw in _MODES else "real"


def _form_error(message: str) -> HTMLResponse:
    html = render_partial("partials/form_error.html", message=message)
    return HTMLResponse(
        html, status_code=200,
        headers={"HX-Retarget": ".form-error-slot", "HX-Reswap": "innerHTML"},
    )


def _orders_trigger() -> HTMLResponse:
    return HTMLResponse("", headers={"HX-Trigger": "refreshorders"})


def _order_request_from_form(form, *, user_id: int) -> OrderRequest:
    client_order_id = form.get("client_order_id") or str(uuid.uuid4())
    return OrderRequest(
        client_order_id=client_order_id,
        symbol=form.get("symbol") or "",
        action=form.get("action") or "",
        qty=int(form.get("qty") or 0),
        price=Decimal(str(form.get("price") or "0")),
        price_type=form.get("price_type") or "LMT",
        order_type=form.get("order_type") or "ROD",
        octype=form.get("octype") or "Auto",
        user_id=user_id,
    )


@router.get("/orders", response_class=HTMLResponse)
async def orders_page(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
    guard=Depends(get_order_risk_guard),
    mode: str = "real",
):
    m = _mode(mode)
    return templates.TemplateResponse(
        request,
        "orders.html",
        {
            "active": "orders",
            "orders": brepo.list_orders(session, user_id=user.id, mode=m),
            "f": {"mode": m},
            "service_available": service is not None,
            "service_mode": getattr(service, "mode", None) if service is not None else None,
            "has_guard": guard is not None,
            "kill_switch": getattr(guard, "kill_switch", False) if guard is not None else False,
        },
    )


@router.get("/orders/list", response_class=HTMLResponse)
async def orders_list(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    mode: str = "real",
):
    orders = brepo.list_orders(session, user_id=user.id, mode=_mode(mode))
    return HTMLResponse(render_partial("partials/order_table.html", orders=orders))


@router.get("/orders/positions", response_class=HTMLResponse)
async def orders_positions(
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
):
    if service is None:
        return HTMLResponse(render_partial("partials/position_table.html", positions=[]))
    try:
        positions = await service.positions(actor_user_id=user.id)
    except AuthorizationError:
        positions = []
    return HTMLResponse(render_partial("partials/position_table.html", positions=positions))


@router.post("/orders", response_class=HTMLResponse)
async def place_order(
    request: Request,
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
):
    if service is None:
        return _form_error("下單服務未啟用（ORDER_MODE 未接線或連線失敗，見 /healthz）")
    form = await request.form()
    try:
        req = _order_request_from_form(form, user_id=user.id)
    except (ValueError, InvalidOperation) as exc:
        return _form_error(f"下單參數錯誤：{exc}")

    confirm_token = form.get("confirm_token") or None
    if service.mode == "real" and not confirm_token:
        payload_hash = brepo.hash_payload(
            req.client_order_id, req.symbol, req.action, req.qty, req.price, req.octype,
        )
        token = confirm.issue(user.id, payload_hash)
        return HTMLResponse(render_partial("partials/order_confirm.html", req=req, token=token))

    try:
        await service.place(req, actor_user_id=user.id, confirm_token=confirm_token)
    except AuthorizationError:
        return HTMLResponse("無權操作", status_code=403)
    except RiskError as exc:
        return _form_error(f"風控攔截：{exc}")
    except OrderError as exc:
        return _form_error(f"下單失敗：{exc}")
    return _orders_trigger()


@router.post("/orders/{broker_order_id}/cancel", response_class=HTMLResponse)
async def cancel_order(
    broker_order_id: str,
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
):
    if service is None:
        return _form_error("下單服務未啟用")
    try:
        await service.cancel(broker_order_id, actor_user_id=user.id)
    except AuthorizationError:
        return HTMLResponse("無權操作", status_code=403)
    except OrderError as exc:
        return _form_error(f"刪單失敗：{exc}")
    return _orders_trigger()


@router.post("/orders/{broker_order_id}/update", response_class=HTMLResponse)
async def update_order(
    broker_order_id: str,
    request: Request,
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
):
    if service is None:
        return _form_error("下單服務未啟用")
    form = await request.form()
    raw_qty = form.get("qty") or None
    raw_price = form.get("price") or None
    try:
        qty = int(raw_qty) if raw_qty else None
        price = Decimal(str(raw_price)) if raw_price else None
    except (ValueError, InvalidOperation) as exc:
        return _form_error(f"改單參數錯誤：{exc}")

    confirm_token = form.get("confirm_token") or None
    if service.mode == "real" and not confirm_token:
        payload_hash = brepo.hash_payload(broker_order_id, qty, price)
        token = confirm.issue(user.id, payload_hash)
        return HTMLResponse(render_partial(
            "partials/order_update_confirm.html",
            broker_order_id=broker_order_id, qty=qty, price=price, token=token,
        ))

    try:
        await service.update(broker_order_id, actor_user_id=user.id, price=price, qty=qty,
                             confirm_token=confirm_token)
    except AuthorizationError:
        return HTMLResponse("無權操作", status_code=403)
    except RiskError as exc:
        return _form_error(f"風控攔截：{exc}")
    except OrderError as exc:
        return _form_error(f"改單失敗：{exc}")
    return _orders_trigger()


@router.post("/orders/kill-switch", response_class=HTMLResponse)
async def toggle_kill_switch(
    request: Request,
    user: User = Depends(get_current_user),
    guard=Depends(get_order_risk_guard),
):
    if guard is None:
        return _form_error("下單服務未啟用")
    try:
        guard.assert_owner(user.id)
    except AuthorizationError:
        return HTMLResponse("無權操作", status_code=403)
    form = await request.form()
    guard.set_kill_switch(form.get("value") == "on")
    return HTMLResponse("", headers={"HX-Refresh": "true"})
```

- [ ] **Step 3：模板（五個新檔）**

> GateGuard：建新檔前陳述事實（下單頁 + 委託/部位/確認 partial）後重試。

`src/quanquant/web/templates/orders.html`：
```html
{% extends "base.html" %}
{% block content %}
<div>
  <div class="journal-head">
    <h3>下單（{{ '模擬' if f.mode == 'sim' else '正式' }}）</h3>
    {% if has_guard %}
    <form hx-post="/orders/kill-switch" hx-swap="none">
      <input type="hidden" name="value" value="{{ 'off' if kill_switch else 'on' }}">
      <button type="submit" class="{{ '' if kill_switch else 'contrast' }}">
        {{ '解除 KILL SWITCH' if kill_switch else '緊急 KILL SWITCH' }}
      </button>
    </form>
    {% endif %}
  </div>

  <div class="mode-tabs" role="tablist">
    <a role="button" class="{{ '' if f.mode == 'sim' else 'contrast' }}" href="/orders?mode=real">正式</a>
    <a role="button" class="{{ 'contrast' if f.mode == 'sim' else '' }}" href="/orders?mode=sim">模擬</a>
  </div>

  {% if not service_available %}
  <div class="error-banner">⚠ 下單服務未啟用（未設定 ORDER_MODE/CA 或連線失敗，見 /healthz）。</div>
  {% elif service_mode != f.mode %}
  <div class="error-banner">⚠ 目前下單 session 為「{{ service_mode }}」，與此頁 tab 不同——
    下單一律走 session mode，與 tab 選擇無關（tab 只切換檢視哪個 mode 的委託/日誌）。</div>
  {% endif %}

  <form hx-post="/orders" hx-target="#order-confirm-slot" hx-swap="innerHTML"
        hx-disabled-elt="find button[type='submit']">
    <div class="form-error-slot"></div>
    <div class="grid">
      <label>商品<input name="symbol" value="TXF" required></label>
      <label>方向
        <select name="action"><option value="Buy">買進</option><option value="Sell">賣出</option></select>
      </label>
      <label>開平
        <select name="octype">
          <option value="Auto">自動</option><option value="New">新倉</option><option value="Cover">平倉</option>
        </select>
      </label>
    </div>
    <div class="grid">
      <label>口數<input type="number" step="1" name="qty" value="1" required></label>
      <label>價格<input type="number" step="any" name="price" value="" required></label>
      <label>價別
        <select name="price_type"><option value="LMT">限價</option><option value="MKT">市價</option></select>
      </label>
      <label>委託
        <select name="order_type">
          <option value="ROD">ROD</option><option value="IOC">IOC</option><option value="FOK">FOK</option>
        </select>
      </label>
    </div>
    <footer class="form-actions">
      <button type="submit">送出委託</button>
    </footer>
  </form>
  <div id="order-confirm-slot"></div>

  <h4>委託列表</h4>
  <div class="table-wrap">
    <table>
      <thead><tr><th>時間</th><th>商品</th><th>方向</th><th>口數</th><th>價格</th><th>狀態</th><th>券商單號</th><th>操作</th></tr></thead>
      <tbody id="orders-tbody"
             hx-get="/orders/list?mode={{ f.mode }}" hx-trigger="load, refreshorders from:body"
             hx-swap="innerHTML">
        {% include "partials/order_table.html" %}
      </tbody>
    </table>
  </div>

  <h4>部位</h4>
  <div id="positions" hx-get="/orders/positions" hx-trigger="load, refreshorders from:body"
       hx-swap="innerHTML">
    {% include "partials/position_table.html" %}
  </div>
</div>
{% endblock %}
```

`src/quanquant/web/templates/partials/order_table.html`：
```html
{% if not orders %}
<tr class="empty"><td colspan="8">尚無委託。</td></tr>
{% endif %}
{% for o in orders %}
<tr>
  <td class="nowrap">{{ o.created_at | dt }}</td>
  <td>{{ o.symbol }}</td>
  <td>{{ '買' if o.action == 'Buy' else '賣' }}</td>
  <td class="numcell">{{ o.qty }}</td>
  <td class="numcell">{{ o.price | num }}</td>
  <td>{{ o.status }}</td>
  <td>{{ o.broker_order_id or '—' }}</td>
  <td class="nowrap">
    {% if o.broker_order_id and o.status in ('submitted', 'partfilled') %}
    <form hx-post="/orders/{{ o.broker_order_id }}/update" hx-target="#order-confirm-slot" hx-swap="innerHTML">
      <input type="number" step="1" name="qty" placeholder="新口數">
      <input type="number" step="any" name="price" placeholder="新價格">
      <button type="submit" class="outline mini">改單</button>
    </form>
    <a href="#" role="button" class="outline mini contrast"
       hx-post="/orders/{{ o.broker_order_id }}/cancel" hx-swap="none"
       hx-confirm="確定刪單？">刪單</a>
    {% endif %}
  </td>
</tr>
{% endfor %}
```

`src/quanquant/web/templates/partials/position_table.html`：
```html
<div class="table-wrap">
  <table>
    <thead><tr><th>商品</th><th>方向</th><th>口數</th><th>均價</th></tr></thead>
    <tbody>
      {% if not positions %}
      <tr class="empty"><td colspan="4">目前無部位。</td></tr>
      {% endif %}
      {% for p in positions %}
      <tr>
        <td>{{ p.symbol }}</td>
        <td><span class="dir {{ p.direction }}">{{ '多' if p.direction == 'long' else '空' }}</span></td>
        <td class="numcell">{{ p.qty }}</td>
        <td class="numcell">{{ p.avg_price | num }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</div>
```

`src/quanquant/web/templates/partials/order_confirm.html`：
```html
<article>
  <p class="error-banner">⚠ 正式（real）下單需二次確認：{{ req.symbol }}
     {{ '買進' if req.action == 'Buy' else '賣出' }} {{ req.qty }} 口 @ {{ req.price }}（{{ req.octype }}）</p>
  <form hx-post="/orders" hx-target="#order-confirm-slot" hx-swap="innerHTML">
    <input type="hidden" name="client_order_id" value="{{ req.client_order_id }}">
    <input type="hidden" name="symbol" value="{{ req.symbol }}">
    <input type="hidden" name="action" value="{{ req.action }}">
    <input type="hidden" name="qty" value="{{ req.qty }}">
    <input type="hidden" name="price" value="{{ req.price }}">
    <input type="hidden" name="price_type" value="{{ req.price_type }}">
    <input type="hidden" name="order_type" value="{{ req.order_type }}">
    <input type="hidden" name="octype" value="{{ req.octype }}">
    <input type="hidden" name="confirm_token" value="{{ token }}">
    <button type="submit" class="contrast">確認送出（正式單，不可撤銷）</button>
  </form>
</article>
```

`src/quanquant/web/templates/partials/order_update_confirm.html`：
```html
<article>
  <p class="error-banner">⚠ 正式（real）改單需二次確認：委託 {{ broker_order_id }}
     {% if qty %}→ 口數 {{ qty }}{% endif %} {% if price %}→ 價格 {{ price }}{% endif %}</p>
  <form hx-post="/orders/{{ broker_order_id }}/update" hx-target="#order-confirm-slot" hx-swap="innerHTML">
    {% if qty %}<input type="hidden" name="qty" value="{{ qty }}">{% endif %}
    {% if price %}<input type="hidden" name="price" value="{{ price }}">{% endif %}
    <input type="hidden" name="confirm_token" value="{{ token }}">
    <button type="submit" class="contrast">確認送出（正式改單，不可撤銷）</button>
  </form>
</article>
```

- [ ] **Step 4：註冊路由 + nav 連結**

`src/quanquant/web/app.py`：import 行改為（`trades` 之後加 `orders`，維持字母序）：
```python
from quanquant.web.routers import alerts, candles, dashboard, health, orders, stats, trades
```
`create_app()` 的 `protected` 區塊（`trades` 之後）加：
```python
    app.include_router(orders.router, dependencies=protected)
```
`src/quanquant/web/templates/base.html`：nav-links（`/stats` 連結之後）加：
```html
      <a href="/orders" class="nav-link {% if active == 'orders' %}active{% endif %}">下單</a>
```

- [ ] **Step 5：跑測試確認 GREEN**

```bash
uv run pytest tests/test_api_orders.py -q
```

- [ ] **Step 6：跑全測試 + 標記手動 E2E**

```bash
uv run pytest -q
```
預期既有測試 + 本計畫全部新增測試全綠。

> **手動 simtrade E2E（實作者於本機執行，非 pytest）**：`.env` 設 `SHIOAJI_TRADE_API_KEY`/`SHIOAJI_TRADE_SECRET_KEY`、`ORDER_MODE=sim`、`ORDER_OWNER_USER_IDS=<自己的 user id>` → `uv run quanquant-web` → `/healthz` 確認 `order_session.connected=true` → 開 `/orders`（模擬 tab）送單 → 觀察委託列表狀態轉移、`/journal?mode=sim` 自動出現一列（`source=shioaji`）、`/stats?mode=sim` 反映、`/stats?mode=real` 不受影響、kill switch 開關即時生效（不必重啟）。`_deal_to_fill` 的 `msg` 鍵名（`trade_id`/`ordno`/`order_cond` 等）若與實際 `OrderState.FuturesDeal` payload 不符，於此步對照微調（Task 6/8 已註記）。首次跑完把去識別 payload 固化成 regression fixture（spec §測試計畫）。

- [ ] **Step 7：Commit（scoped）**

```bash
git add src/quanquant/web/routers/orders.py src/quanquant/web/templates/orders.html \
  src/quanquant/web/templates/partials/order_table.html \
  src/quanquant/web/templates/partials/position_table.html \
  src/quanquant/web/templates/partials/order_confirm.html \
  src/quanquant/web/templates/partials/order_update_confirm.html \
  src/quanquant/web/app.py src/quanquant/web/templates/base.html \
  tests/test_api_orders.py
git commit -m "feat: 下單 UI＋orders 路由（下單面板/委託列表/部位/kill switch，real 兩階段確認往返，授權例外對應403）"
```

---

### Task 10：部署安全（codex F8）

`.pfx` read-only bind-mount + 0600 owner 檢查 + gitignore + secret scan + log redaction + 啟動前權限檢查。`_order_subsystem_preflight`（Task 8）擴充：`real` 模式除了驗證 CA 三欄位齊全，還要驗證 `.pfx` 檔案存在且權限恰為 0600（過寬一律拒絕啟動下單子系統）。

**Files:**
- Modify: `.gitignore`（加 `*.pfx`/`*.p12`/`*.pem`/`*.key`）
- Modify: `docker-compose.yml`（app service 加 CA 檔 read-only bind mount）
- Modify: `src/quanquant/web/app.py`（新增 `_ca_file_permissions_ok`；`_order_subsystem_preflight` 擴充呼叫）
- Modify: `docs/deployment.md`（新增「Shioaji 下單 CA 憑證部署」章節）
- Test: Create `tests/test_deploy_security.py`

**Interfaces:**
- Consumes：Task 8 `_order_subsystem_preflight`
- Produces：`web.app._ca_file_permissions_ok(ca_path: str) -> tuple[bool, str | None]`；`_order_subsystem_preflight` 對 `real` 模式多一層權限檢查（不合格 → `(False, reason)`，`reason` 含 `"600"` 字樣）

- [ ] **Step 1：先寫失敗測試（新檔 `tests/test_deploy_security.py`）**

> GateGuard：建新檔前陳述事實（部署安全：gitignore/secret scan/CA 檔權限檢查/log redaction）後重試。

```python
"""部署安全（codex F8）：.pfx 不進 git、不被 tracked、CA 檔權限需 0600、log 不含秘密欄位名。"""
import subprocess
from pathlib import Path

from quanquant.web.app import _ca_file_permissions_ok, _order_subsystem_preflight


def test_gitignore_excludes_cert_files():
    gitignore = Path(__file__).resolve().parents[1] / ".gitignore"
    content = gitignore.read_text(encoding="utf-8")
    for pattern in ("*.pfx", "*.p12", "*.pem", "*.key"):
        assert pattern in content, f".gitignore 缺 {pattern}"


def test_no_cert_files_tracked_in_git():
    tracked = subprocess.run(
        ["git", "ls-files"], capture_output=True, text=True, check=True,
    ).stdout.splitlines()
    forbidden_ext = (".pfx", ".p12", ".pem", ".key")
    leaked = [f for f in tracked if f.lower().endswith(forbidden_ext)]
    assert leaked == [], f"憑證/金鑰檔不得進 git：{leaked}"


def test_ca_file_permissions_ok_requires_0600(tmp_path):
    ca = tmp_path / "test.pfx"
    ca.write_bytes(b"fake-cert-bytes")
    ca.chmod(0o644)  # 過寬
    ok, reason = _ca_file_permissions_ok(str(ca))
    assert ok is False and reason is not None and "600" in reason

    ca.chmod(0o600)
    ok, reason = _ca_file_permissions_ok(str(ca))
    assert ok is True and reason is None


def test_ca_file_permissions_missing_file_rejected(tmp_path):
    ok, reason = _ca_file_permissions_ok(str(tmp_path / "nope.pfx"))
    assert ok is False and reason is not None


def test_preflight_real_rejects_overly_permissive_ca_file(tmp_path):
    from quanquant.config import Settings

    ca = tmp_path / "loose.pfx"
    ca.write_bytes(b"x")
    ca.chmod(0o644)
    s = Settings(_env_file=None, shioaji_trade_api_key="k", shioaji_trade_secret_key="s",
                order_owner_user_ids="1", order_mode="real",
                shioaji_ca_path=str(ca), shioaji_ca_passwd="pw", shioaji_person_id="A1")
    enabled, reason = _order_subsystem_preflight(s)
    assert enabled is False and reason is not None and "600" in reason


def test_preflight_real_accepts_properly_permissioned_ca_file(tmp_path):
    from quanquant.config import Settings

    ca = tmp_path / "ok.pfx"
    ca.write_bytes(b"x")
    ca.chmod(0o600)
    s = Settings(_env_file=None, shioaji_trade_api_key="k", shioaji_trade_secret_key="s",
                order_owner_user_ids="1", order_mode="real",
                shioaji_ca_path=str(ca), shioaji_ca_passwd="pw", shioaji_person_id="A1")
    enabled, reason = _order_subsystem_preflight(s)
    assert enabled is True and reason is None


def test_order_subsystem_source_never_logs_secrets():
    """靜態檢查：下單子系統原始碼裡任何 log 呼叫都不得帶秘密欄位名（codex F8：log redaction）。
    無法防上游 shioaji 例外訊息本身夾帶秘密（不可控），僅檢查我方程式碼的呼叫寫法。"""
    import inspect

    from quanquant.broker import confirm, risk, shioaji_adapter
    from quanquant.web import app as web_app

    forbidden = (
        "_secret_key", "_ca_passwd", "_api_key",
        "shioaji_trade_secret_key", "shioaji_ca_passwd", "shioaji_trade_api_key",
    )
    for module in (confirm, risk, shioaji_adapter, web_app):
        src = inspect.getsource(module)
        for line in src.splitlines():
            stripped = line.strip()
            if stripped.startswith(("log.", "logging.")) or ".exception(" in stripped:
                for name in forbidden:
                    assert name not in line, f"{module.__name__} 疑似把秘密寫進 log：{line!r}"
```
跑 `uv run pytest tests/test_deploy_security.py -q` → RED（`_ca_file_permissions_ok` 未定義、`.gitignore` 缺 `*.pfx` 等、`_order_subsystem_preflight` 未做權限檢查）。

- [ ] **Step 2：`.gitignore` 加憑證/金鑰副檔名**

在「Deployment artifacts」區塊（`*.dump` 那行附近）加：
```
# Shioaji CA certificate（真錢憑證，絕不進 git）
*.pfx
*.p12
*.pem
*.key
```

- [ ] **Step 3：`_ca_file_permissions_ok` + `_order_subsystem_preflight` 擴充（`src/quanquant/web/app.py`）**

在 `_order_subsystem_preflight` **之前**新增：
```python
def _ca_file_permissions_ok(ca_path: str) -> tuple[bool, str | None]:
    """啟動前檢查 .pfx 權限（codex F8）：必須存在、僅 owner 可讀寫（0600），
    group/other 不得有任何權限位元。"""
    import os
    import stat

    if not ca_path:
        return False, "CA 路徑未設定"
    if not os.path.isfile(ca_path):
        return False, f"CA 檔案不存在：{ca_path}"
    mode = stat.S_IMODE(os.stat(ca_path).st_mode)
    if mode & 0o077:
        return False, f"CA 檔案權限過寬（{oct(mode)}），需 chmod 600 {ca_path}"
    return True, None
```
`_order_subsystem_preflight` 的 `real` 分支改為（在既有 `ca_ready` 檢查之後追加權限檢查）：
```python
    if settings.order_mode == "real":
        ca_ready = bool(settings.shioaji_ca_path and settings.shioaji_ca_passwd and settings.shioaji_person_id)
        if not ca_ready:
            return False, (
                "ORDER_MODE=real 但 CA 設定不完整（ca_path/ca_passwd/person_id），下單子系統不啟動"
            )
        perms_ok, perms_reason = _ca_file_permissions_ok(settings.shioaji_ca_path)
        if not perms_ok:
            return False, f"ORDER_MODE=real 但 {perms_reason}，下單子系統不啟動（codex F8 部署安全）"
    return True, None
```

- [ ] **Step 4：docker-compose CA 唯讀掛載（`docker-compose.yml`）**

`app:` service 的 `env_file: .env` 之後加：
```yaml
    volumes:
      - ${SHIOAJI_CA_HOST_PATH:-./secrets/shioaji.pfx}:/secrets/shioaji.pfx:ro
```
（VM 上把 `.pfx` 放在 `SHIOAJI_CA_HOST_PATH` 指定路徑、`chmod 600`；容器內 `.env` 的 `SHIOAJI_CA_PATH` 設為 `/secrets/shioaji.pfx`——容器內路徑固定，唯讀掛載防程式意外寫壞憑證檔。）

- [ ] **Step 5：部署文件（`docs/deployment.md`，檔案結尾新增章節）**

在檔案結尾新增：
```markdown

## Shioaji 下單 CA 憑證部署（`ORDER_MODE=real` 專用）

- `.pfx` 憑證**不進 git**（`.gitignore` 已排除 `*.pfx`/`*.p12`/`*.pem`/`*.key`）。
- VM 上把憑證放在固定路徑（例：`/opt/quanquant/secrets/shioaji.pfx`），`chmod 600` 且 owner
  為部署帳號；`docker-compose.yml` 以 `SHIOAJI_CA_HOST_PATH` 唯讀掛載進容器
  `/secrets/shioaji.pfx`。
- VM 的 `.env` 設定：`SHIOAJI_CA_HOST_PATH=/opt/quanquant/secrets/shioaji.pfx`（host 側，供
  compose 掛載用）、`SHIOAJI_CA_PATH=/secrets/shioaji.pfx`（容器內固定路徑，app 讀這個）。
- **啟動前權限檢查**：`ORDER_MODE=real` 時，app 啟動會檢查 `.pfx` 權限是否恰為 `0600`——
  過寬（group/other 有任何權限位元）一律拒絕啟動下單子系統（其餘功能不受影響），log 會
  明確寫出需要 `chmod 600` 的路徑。
- 秘密（`SHIOAJI_CA_PASSWD`/`SHIOAJI_TRADE_SECRET_KEY` 等）只存在 `.env`，不進 log/repr。
```

- [ ] **Step 6：跑測試確認 GREEN**

```bash
uv run pytest tests/test_deploy_security.py -q
```

- [ ] **Step 7：跑全測試確認未回歸**

```bash
uv run pytest -q
```
預期既有全部測試 + 本計畫全部 10 個 task 新增測試全綠。

- [ ] **Step 8：Commit（scoped）**

```bash
git add .gitignore docker-compose.yml src/quanquant/web/app.py docs/deployment.md \
  tests/test_deploy_security.py
git commit -m "feat: 部署安全（.pfx read-only掛載+0600權限檢查+gitignore+secret scan+log redaction），real缺妥善權限CA不啟動"
```

---

## Self-Review

### codex A–H 逐條 → Task 對照

| codex 發現 | 解法摘要 | 對應 Task |
|---|---|---|
| A1 誤抓手動日誌 | 新增 `BrokerPosition` 表隔離自動部位；`Trade.source` 區分 manual/shioaji；`find_open_trade` 預設限 `source="shioaji"` | Task 1（`source` 欄位）、Task 3（`BrokerPosition` 表）、Task 4（`find_open_trade` + 隔離測試） |
| A2 分批平倉進度只在記憶體 | 進度全落 `BrokerPosition.open_qty/closed_qty/exit_notional`，`PositionTracker` 無實例狀態，可任意重建 | Task 4（跨重啟續平測試） |
| A3 超額 Cover 口數憑空消失 | 消耗 `min(fill.qty, remaining)`；超額轉記反向新倉＋`OrderAudit` | Task 4 |
| A4 New/Auto 誤配 | New 以目標 direction 查找；Auto 依雙向 open 集合推斷，歧義 fail closed | Task 4 |
| A5 缺開倉 Cover 只記 warning 不自癒 | `PositionMismatchError` → fill worker 標 `Deal.quarantine=True/processed=False`，`retry_quarantined()` 可重建 | Task 4（raise）、Task 5（quarantine + retry） |
| A6 sim fee 未真的填入 | `ShioajiAdapter` sim 模式伺服器依 `order_sim_fee` 按口計費，不信任 msg | Task 8 |
| B1 record_deal 先 commit、processed 從未設 true | `stage_deal` 只 flush，`Deal insert+部位+Trade+processed=true` 同一 `commit()` | Task 3（`stage_deal` 不 commit）、Task 5（單一交易＋失敗不留痕跡測試） |
| B2 委託關聯鍵與 fill 去重鍵共用 | `fill_id`（去重）與 `ordno`（委託關聯）分離型別/資料層 | Task 2（`Fill.fill_id`/`.ordno`）、Task 3（`Deal` 兩欄位） |
| B3 唯一性未證明跨日/重連/sim-real | `unique(broker, mode, account, trading_day, fill_id)` | Task 3 |
| B4 無 client idempotency key | `OrderRequest.client_order_id`，`create_order`/`adapter.place` 冪等 | Task 2、Task 3、Task 6 |
| B5 catch-all IntegrityError 吞掉非重播錯誤 | rollback 後重查確認命中指定唯一鍵才當重播，其餘拋出 | Task 3（`create_order`/`stage_deal`） |
| C1 mode 信任表單 | `OrderRequest` 無 `mode` 欄位；`Fill.mode`/`Order.mode` 一律蓋 `self.mode` | Task 2、Task 6 |
| C2 無 mode enum/DB 限制 | `Literal["sim","real"]`（schema/settings）+ 新表 `CheckConstraint`（Trade 因既有表 ALTER 限制記為刻意取捨） | Task 1、Task 3、Task 8 |
| D1 第二使用者可用共用憑證下真單 | `RiskGuard.assert_owner`，`place/cancel/update/positions` 服務層先驗 | Task 7、Task 6（呼叫點）、Task 9（403） |
| D2 order→user/mode 只在記憶體 | 持久化於 `Order`/`Deal`，未知回報進 quarantine 不 drop | Task 3、Task 5 |
| D3 cancel 無所有權驗證 | `(user_id, broker, mode, broker_order_id)` 驗證，不符 `AuthorizationError` | Task 6 |
| D4 positions 忽略 user_id | `assert_owner` 限 owner 才能讀 | Task 6、Task 7 |
| D5 Deal 不存 user_id/mode | `Deal` 表持久化 `user_id`/`mode`/`account`/`ordno`/`fill_id` | Task 3 |
| E1 callback 早於 ack | pending correlation（`client_order_id`→`status=pending/sending`）+ durable inbox quarantine + retry；barrier 測試 | Task 6（pending correlation）、Task 5（quarantine+barrier 測試） |
| E2 lifespan 未等就緒即公開 service | `await adapter.connect()` 成功才 `app.state.order_service`，失敗 fail closed 反映 `/healthz` | Task 8 |
| E3 無 watchdog/reconnect | `_order_watchdog`：探活失敗→重連（backoff）+ 每輪對帳 | Task 8 |
| E4 同步 handle_fill 佔用 event loop | `enqueue-only` + 單一有序 worker + `asyncio.to_thread` 開新 Session | Task 5 |
| F1 confirmed 永不傳/mode 竄改繞過 | 兩階段確認 token（短效一次性綁 `(user,payload_hash)`），adapter 依 `self.mode` 強制 | Task 7、Task 6 |
| F2 update 無風控 | `check_update` 重跑全部限制＋排除自身舊 qty | Task 7 |
| F3 qty/price/枚舉未擋 | `OrderRequest.__post_init__` fail closed | Task 2 |
| F4 kill switch 是啟動快照 | `RiskGuard._kill_switch` instance 屬性，`set_kill_switch()` 即時切換，UI 可操作 | Task 7、Task 9 |
| F5 風控攔截未寫 audit | `check_place`/`check_update`/`_reject` 成功與拒絕都寫 `OrderAudit` | Task 7 |
| F6 order_mode 任意字串 | `Settings.order_mode: Literal["sim","real"]`；real 缺 CA/權限不啟動 | Task 8、Task 10 |
| F7 quota 檢查與建單無 lock | 明訂單一 uvicorn worker 不變量，quota+建單同交易，不加 DB lock（範圍化決議） | Task 3、Task 7（文件化）|
| F8 只宣告不進 git | `.pfx` gitignore＋read-only mount＋0600 檢查＋secret scan＋log redaction | Task 10 |
| G 測試品質 | 每個失敗模式對應測試（見各 Task 測試清單） | 全部 Task |
| H spec↔plan 一致性 | watchdog 有實作、mode server-side、手動新增日誌帶 mode、update 路由補上、migration 用 `trades`+`DEFAULT`、RiskGuard 前移（改用不依賴 Settings 的建構式，徹底解掉相依） | Task 1（migration）、Task 8（watchdog）、Task 1（手動表單 mode）、Task 9（update 路由）、Task 7（RiskGuard 建構式解相依） |

### Spec §A–I 覆蓋
A（Broker 抽象層）→ Task 2；B（委託關聯/冪等）→ Task 2/3；C（資料模型）→ Task 3；D（Durable fill→部位→自動寫日誌）→ Task 4/5；E（mode 分流）→ Task 1；F（授權/風控/確認）→ Task 7；G（Session 生命週期）→ Task 6/8；H（UI）→ Task 9；I（設定/部署安全）→ Task 8/10。全部覆蓋，無遺漏章節。

### Placeholder 掃描
- 全文無 TBD/TODO/「適當處理」等占位；每個「Create 新檔」step 給完整檔案內容，每個「Modify」step 給精確可定位的錨點文字＋可照抄片段。
- 唯二標「手動實測」處：Task 9 Step 6 的**真實 simtrade E2E**（不可 pytest 化）與 `_deal_to_fill` 真 payload 鍵名對照（Task 6/8 已註記，Task 9 手動實測時微調）——皆屬 spec §測試計畫明列的「端到端（simtrade 手動）」，非邏輯占位；其餘資料/風控/授權邏輯均由單測覆蓋。
- Task 8 的 `_order_watchdog` 因無法驗證真實 Shioaji SDK 是否提供斷線 callback，改用「週期性 `positions()` 探活 + 週期性對帳」的誠實設計並完整測試（非 stub）；已在程式註解與本 Self-Review 中明確說明取捨理由，不算 placeholder。

### 型別一致性
- `OrderRequest`（無 `mode`）→ adapter 依 `self.mode` 產生 `Order`/`Fill` → `Fill.mode` → `Deal.mode`/`BrokerPosition.mode` → `TradeCreate.mode`：全程單向蓋值，無任何節點信任外部輸入的 mode。
- `Fill.fill_id`（去重鍵）與 `Fill.ordno`（委託關聯鍵）全程分離不混用；`Deal` 表兩欄位對應一致。
- `Fill.ts`（epoch-ms int）→ `Deal.ts`/`OrderAudit.ts`（`BigInteger`）一致；→ `Trade.entry_time/exit_time`（naive CST datetime）經 `_to_dt`/`_utc_dt_to_cst` 轉換，符合 `db/models.py` naive 本地慣例。
- 金額全程 `Decimal`（`DecimalText`/`compute_pnl`）；`price`/`fee` 無 float 汙染（adapter 對 shioaji 邊界才 `float(...)`，回程立即轉 `Decimal`）。
- `OrderService` Protocol（`mode` 屬性 + 四個 async 方法皆收 `actor_user_id`）與 `ShioajiAdapter`、`_FakeOrderService`（Task 9 測試）簽名一致（Task 2 的 `test_protocol_is_runtime_checkable_duck` 把關）。
- `RiskGuard`/`ShioajiAdapter` 之間是 duck-typed 協作（`assert_owner`/`check_place`/`check_update`），Task 6 先定義呼叫點、Task 7 補實作，兩邊測試各自用 stub/真實類別驗證同一組方法簽名，無漂移風險。
- 引用到的型別/函式皆有明確定義來源：`RiskGuard`/`confirm`(T7)、`PositionTracker`/`PositionMismatchError`(T4)、`FillWorker`(T5)、`ShioajiAdapter`(T6)、`Order`/`Deal`/`BrokerPosition`/`OrderAudit`/`brepo.*`(T3)、`find_open_trade`/`create_trade(commit=)`(T4，改自 T1 的 `journal/repository.py`)、`get_order_service`/`get_order_state`/`get_order_risk_guard`(T8)、`OrderRequest`/`Fill`/…(T2)。

### 相依順序驗證
T1（mode+source 基礎，無上游）→ T2（純型別，無上游，可與 T1 平行但依編號序）→ T3（資料層，需 T2 的型別契約理解，程式碼不直接 import T2）→ T4（PositionTracker，需 T1 的 `create_trade(commit=)`/`find_open_trade`＋T2 的 `Fill`＋T3 的 `brepo`）→ T5（FillWorker，需 T2 的 `Fill`＋T3 的 `stage_deal`/`trading_day_for`＋T4 的 `PositionTracker`）→ T6（ShioajiAdapter，需 T2 的型別/例外＋T3 的 `brepo`；`risk_guard` 參數 duck-typed 提前接受，T7 才補實作，呼叫點零修改——codex H 已解）→ T7（RiskGuard，刻意不依賴 T8 的 `Settings`，只需 T2/T3 的型別與倉儲；供 T6 已寫好的呼叫點使用）→ T8（設定+lifespan，組裝 T4/T5/T6/T7 全部收斂進 `app.state`）→ T9（UI，需 T8 的 `get_order_service`/`get_order_risk_guard`）→ T10（部署安全，擴充 T8 的 `_order_subsystem_preflight`）。每個 task 結尾 `uv run pytest` 全綠才進下一 task；無任何「先留空、後面 task 才補齊否則跑不過」的斷點（T6→T7 的唯一潛在斷點已透過「RiskGuard 建構式不依賴 Settings、duck-typed 呼叫點」徹底消解，不需要 `risk_guard=None` 佔位收尾）。✅

### 不得改弱既有測試
本計畫所有「Modify」既有檔案的 step 皆為**新增**參數（帶預設值，如 `create_trade(..., commit=True)`）或**新增**欄位（`mode`/`source` 有預設值），既有呼叫點/測試斷言不變、不刪減；`_MIGRATIONS` 只新增筆數；既有路由簽名新增的 Query 參數皆有預設值（`mode: str = Query("real")`），既有呼叫不受影響。實作審查以 `git diff` 逐檔確認：既有測試檔案裡原有的每一個 `def test_...` 與其內部 assertion 數量/語意必須保持不變，只允許在檔尾追加新測試。

