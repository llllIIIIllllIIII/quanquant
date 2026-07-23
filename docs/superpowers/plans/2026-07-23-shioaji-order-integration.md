# Shioaji 期貨下單整合 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 為 QuanQuant 加上單人自用的 Shioaji（永豐）期貨下單，成交回報自動寫進交易日誌並反映到 `/stats`，且模擬（sim）與正式（real）以 `mode` 欄位第一級分流、絕不混算。

**Architecture:** 新增 broker 無關的 `OrderService`（Protocol）＋ `ShioajiAdapter`（lazy-import shioaji、跨執行緒 `loop.call_soon_threadsafe` 橋接、`simulation=True` 開發）。成交 `Fill` 經去重（`Deal` 唯一鍵）後進 `PositionTracker`，用既有 `journal.repository.create_trade`／`update_trade` 把開倉 fill 映射成未平倉 `Trade`、平倉 fill 收單（pnl 自動算）。下單 session 為 lifespan 單例掛 `app.state.order_service`，與唯讀行情 streamer 完全解耦。`mode` 穿過 `list_trades`／`list_for_stats`／`stats._filtered` 與 journal/stats 兩個 filter 表單＋分頁 tab；`stats/metrics.py` 一行不改。

**Tech Stack:** FastAPI + SQLModel（雙方言 SQLite/Postgres）、HTMX/Jinja2 模板、`shioaji`（已是 core dep）、pytest（in-memory SQLite StaticPool、TestClient 簽章 cookie）、`asyncio.to_thread` + `loop.call_soon_threadsafe` 跨執行緒。

## Global Constraints

- 雙方言 SQLite/Postgres：新表/欄位必須可攜（nullable ALTER、`SQLModel.metadata.create_all` 自動建新表）；任何 epoch-ms 欄位必用 `sqlalchemy.BigInteger`（4-byte INTEGER 會溢位）。
- **不得新增任何依賴**（`shioaji>=1.5.3` 已是 core dep、見 `pyproject.toml:21`）；不得改弱既有測試；**不得 push**（主對話會 commit 並送審）。
- 全程開發用 `ORDER_MODE=sim`（`Shioaji(simulation=True)`），不碰真 CA、不送真單、不花錢。
- candle 讀取路徑（raw-SQL→float、sync `def` routes）勿改 async/ORM——本計畫**不碰 candles**。
- 每個 task 只 scoped `git add <該 task 檔>`，**嚴禁 `git add -A` / `git add .`**；commit 格式 `<type>: <繁中描述>`，attribution 全域停用（**不加任何 footer / Co-Authored-By**）。
- 全站中文一律**繁體台灣**。
- 測試指令 `uv run pytest`（既有 79+ 全綠才可繼續下一 task）。
- 資料庫遷移**無 alembic**：靠 `init_db()`（`db/engine.py:41`）的 `create_all` + `ensure_columns`（`db/migrate.py`）逐筆 nullable ALTER。
- 金額/價格一律 `Decimal`（`db/models.py` 的 `DecimalText`，存 TEXT、round-trip 不失真）；勿用 float。
- 環境有 **GateGuard hook**：第一次 `Bash` 或**建新檔前**會被擋下並要求先陳述事實——這不是故障，照錯誤訊息列點補上事實、重試同一操作即可（本計畫每個「Create 新檔」的 step 都會踩到，實作者照做）。
- `.env` 與 `*.pfx`／`*.dump` **不進 git**（CA 憑證、token 只放 VM 的 `.env`，bind-mount 進 Docker）。

---

## 對 spec 的兩點澄清（實作者須知，已併入下列 task）

1. **`Fill` 需帶 `user_id` 與 `mode`**：spec §A 的 `Fill(broker, seqno, symbol, action, price, qty, fee, octype, ts, order_ref)` 未列 `user_id`／`mode`，但 §D.2 用 `fill.user_id`、§難點2 要求「下單當下把 user_id 綁在 order context，成交回來才知道寫進誰的日誌」。故 `Fill` dataclass **加 `user_id: int` 與 `mode: str`**，由 adapter 在成交 callback 時依 `seqno` 從下單當下暫存的 order context 補上（Task 5）。
2. **`Trade.entry_time`/`exit_time` 是 naive 本地（Asia/Taipei）datetime，不是 epoch-ms**（`db/models.py:5-6, 39, 41`）。`Fill.ts` 是 epoch-ms int（存進 `Deal.ts` 的 `BigInteger`），PositionTracker 寫 `Trade` 前用 `datetime.fromtimestamp(ts/1000, CST).replace(tzinfo=None)` 轉成 naive CST（Task 4 的 `_to_dt`）。

---

### Task 1: `mode` 分流基礎（Trade.mode + 遷移 + schema + 全鏈路過濾 + journal/stats tab）

把 `mode`（real/sim）打通整條讀寫鏈，讓後續 Task 4 寫入的 sim/real 交易在日誌與 `/stats` 天然隔離、`stats/metrics.py` 零改。

**Files:**
- Modify: `src/quanquant/db/models.py`（`Trade` class，L31-60 之間加 `mode` 欄位）
- Modify: `src/quanquant/db/migrate.py`（`_MIGRATIONS` L11-16 追加一筆）
- Modify: `src/quanquant/journal/schemas.py`（`TradeCreate` L24、`TradeUpdate` L47、`TradeRead` L64 + `from_trade` L86 加 `mode`）
- Modify: `src/quanquant/journal/repository.py`（`create_trade` L27、`list_trades` L95、`list_for_stats` L124 穿 `mode`）
- Modify: `src/quanquant/web/routers/trades.py`（`journal_page` L75、`list_trades_fragment` L110 加 `mode` Query + 傳給 repo + `f.mode`）
- Modify: `src/quanquant/web/routers/stats.py`（`_filtered` L19、`stats_page` L40、`stats_data` L66、`export_csv` L79、`export_xlsx` L96 加 `mode`）
- Modify: `src/quanquant/web/templates/journal.html`（filter 表單加 hidden `mode` + 頂部 mode tab）
- Modify: `src/quanquant/web/templates/stats.html`（filter 表單加 hidden `mode` + 頂部 mode tab + 匯出 qs 帶 mode）
- Test: `tests/test_repository.py`（新增 mode round-trip + 過濾）、`tests/test_migrate.py`（新增 mode 欄位遷移 + 既有列預設 real）、新增 `tests/test_mode_split.py`（sim/real 統計隔離）

**Interfaces:**
- Consumes:（無上游 task）
- Produces（Task 4 依賴）：
  - `Trade.mode: str`（default `"real"`, index）
  - `TradeCreate.mode: str = "real"`、`TradeUpdate.mode: str | None = None`
  - `repo.create_trade(session, TradeCreate, *, user_id)`（會寫入 `data.mode`）
  - `repo.list_trades(session, *, user_id, mode="real", symbol=None, tag=None, date_from=None, date_to=None, status="all")`
  - `repo.list_for_stats(session, *, user_id, mode="real", ...)`

- [ ] **Step 1: 先寫失敗測試 — Trade.mode round-trip + 過濾（`tests/test_repository.py`）**

在 `tests/test_repository.py` 末端新增（`make_create` 已在 `tests/conftest.py`，`TradeCreate` 允許 `mode` 需 Step 3 先加，故此測試現在會 RED）：
```python
def test_mode_defaults_real(session, user):
    t = repo.create_trade(session, make_create(), user_id=user.id)
    assert t.mode == "real"


def test_mode_round_trip_sim(session, user):
    t = repo.create_trade(session, make_create(mode="sim"), user_id=user.id)
    assert repo.get_trade(session, t.id, user_id=user.id).mode == "sim"


def test_list_trades_scoped_by_mode(session, user):
    repo.create_trade(session, make_create(symbol="TXF"), user_id=user.id)              # real
    repo.create_trade(session, make_create(symbol="MXF", mode="sim"), user_id=user.id)  # sim
    real = repo.list_trades(session, user_id=user.id, mode="real")
    sim = repo.list_trades(session, user_id=user.id, mode="sim")
    assert [t.symbol for t in real] == ["TXF"]
    assert [t.symbol for t in sim] == ["MXF"]
```

- [ ] **Step 2: 跑測試確認 RED**

```bash
uv run pytest tests/test_repository.py -q
```
預期 `TypeError: ... unexpected keyword argument 'mode'`（`TradeCreate` 尚無 mode）→ RED，符合預期。

- [ ] **Step 3: 最小實作 — 加 `Trade.mode` 欄位（`src/quanquant/db/models.py`）**

在 `Trade` 的 `user_id` 欄位（L58）**之前**插入：
```python
    mode: str = Field(default="real", index=True)         # "real"（正式/手動）| "sim"（模擬/紙上）
```

- [ ] **Step 4: 加 schema 欄位（`src/quanquant/journal/schemas.py`）**

`TradeCreate`（L38 `tags` 之後、`_exit_pair` validator 之前）加：
```python
    mode: str = "real"
```
`TradeUpdate`（L61 `tags` 之後）加：
```python
    mode: str | None = None
```
`TradeRead`（L80 `tags` 之後、`is_open` 之前）加：
```python
    mode: str
```
`TradeRead.from_trade`（L103 `tags=split_tags(t.tags),` 之後）加：
```python
            mode=t.mode,
```

- [ ] **Step 5: repo 寫入與過濾穿 `mode`（`src/quanquant/journal/repository.py`）**

`create_trade` 的 `Trade(...)`（L28-45）加一行（放在 `symbol=data.symbol,` 之後）：
```python
        mode=data.mode,
```
`list_trades` 簽名（L95-104）加 `mode` 參數並加 where。改為：
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
（其餘 L106 起的 symbol/status/date/order_by 不動。）
`list_for_stats` 簽名（L124-132）加 `mode` 並轉傳。改為：
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

- [ ] **Step 6: 跑測試確認 GREEN（repository）**

```bash
uv run pytest tests/test_repository.py -q
```
預期新增 3 個 mode 測試 + 既有全綠（既有 trade 皆 real、`mode` 預設 real，不受影響）。

- [ ] **Step 7: 遷移測試（RED→GREEN）— `mode` 欄位 + 既有列預設 real（`tests/test_migrate.py`）**

先在 `tests/test_migrate.py` 末端新增失敗測試：
```python
def test_adds_mode_to_trades_defaults_real(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'old_trades.db'}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE trades (id INTEGER PRIMARY KEY, symbol TEXT)"))
        conn.execute(text("INSERT INTO trades (id, symbol) VALUES (1, 'TXF')"))
    ensure_columns(eng)
    insp = inspect(eng)
    assert "mode" in {c["name"] for c in insp.get_columns("trades")}
    with eng.begin() as conn:
        row = conn.execute(text("SELECT mode FROM trades WHERE id = 1")).one()
    assert row[0] == "real"  # 既有列以 DEFAULT 回填 real
```
跑 `uv run pytest tests/test_migrate.py -q` → RED（無 mode 欄位）。
然後在 `src/quanquant/db/migrate.py` 的 `_MIGRATIONS`（L11-16）末端加一筆（**DDL 帶 DEFAULT 'real'** 讓兩方言的既有列都回填 real）：
```python
    ("trades", "mode", "VARCHAR DEFAULT 'real'"),
```
再跑 `uv run pytest tests/test_migrate.py -q` → GREEN。

- [ ] **Step 8: sim/real 統計隔離測試（新檔 `tests/test_mode_split.py`）**

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

- [ ] **Step 9: routes 穿 `mode`（`src/quanquant/web/routers/trades.py`）**

`journal_page`（L75-107）與 `list_trades_fragment`（L110-130）各加 `mode` Query 與轉傳。`journal_page` 改：簽名加（放在 `status: str = Query("all"),` 之前）：
```python
    mode: str = Query("real"),
```
`repo.list_trades(...)` 呼叫（L87-95）加 `mode=mode,`（放在 `user_id=user.id,` 之後）；context 的 `"f": {...}`（L104-105）加 `"mode": mode`：
```python
            "f": {"symbol": symbol or "", "tag": tag or "", "date_from": date_from or "",
                  "date_to": date_to or "", "status": status, "mode": mode},
```
`list_trades_fragment`（L110-130）同樣：簽名加 `mode: str = Query("real"),`；`repo.list_trades(...)` 加 `mode=mode,`。

- [ ] **Step 10: stats routes 穿 `mode`（`src/quanquant/web/routers/stats.py`）**

`_filtered`（L19-37）簽名加 `mode` 並轉傳兩個 repo 呼叫。改為：
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
四個 route（`stats_page` L40、`stats_data` L66、`export_csv` L79、`export_xlsx` L96）各加 `mode: str = Query("real"),`（放在 `symbol: ...` 參數群之前）並把 `_filtered(session, user.id, ...)` 改成 `_filtered(session, user.id, mode, symbol, tag, date_from, date_to)`。`stats_page` 的 context `"f"`（L60-61）加 `"mode": mode`：
```python
            "f": {"symbol": symbol or "", "tag": tag or "", "date_from": date_from or "",
                  "date_to": date_to or "", "mode": mode},
```

- [ ] **Step 11: journal 模板 mode tab + hidden（`src/quanquant/web/templates/journal.html`）**

把 `journal-head`（L5-10）之後、filter form（L12）之前插入 mode tab；並在 filter `<form ... hx-get="/trades" ...>`（L12-13）內第一個 `<select>` 之前加 hidden `mode`，讓表格重抓沿用當前 mode。將 L5-13 區塊改為：
```html
  <div class="journal-head">
    <h3>交易日記</h3>
    <button hx-get="/trades/new" hx-target="#modal-body" hx-swap="innerHTML" @click="open = true">
      + 新增交易
    </button>
  </div>

  <div class="mode-tabs" role="tablist">
    <a role="button" class="{{ '' if f.mode == 'sim' else 'contrast' }}"
       href="/journal?mode=real">正式</a>
    <a role="button" class="{{ 'contrast' if f.mode == 'sim' else '' }}"
       href="/journal?mode=sim">模擬</a>
  </div>

  <form class="filterbar card-bar" hx-get="/trades" hx-target="#trade-tbody" hx-swap="innerHTML"
        hx-trigger="change, submit, refreshtable from:body" @submit.prevent>
    <input type="hidden" name="mode" value="{{ f.mode }}">
```

- [ ] **Step 12: stats 模板 mode tab + hidden + 匯出 qs（`src/quanquant/web/templates/stats.html`）**

L42 的 `qs` 加上 `mode`：
```html
{% set qs = 'mode=' ~ f.mode ~ '&symbol=' ~ (f.symbol | urlencode) ~ '&tag=' ~ (f.tag | urlencode) ~ '&date_from=' ~ f.date_from ~ '&date_to=' ~ f.date_to %}
```
在 `journal-head`（L44-50）之後、filter form（L52）之前加 mode tab；並在 filter `<form ... action="/stats">`（L52）內第一個 `<select>` 之前加 hidden `mode`。將 L52 改為：
```html
<div class="mode-tabs" role="tablist">
  <a role="button" class="{{ '' if f.mode == 'sim' else 'contrast' }}" href="/stats?mode=real">正式</a>
  <a role="button" class="{{ 'contrast' if f.mode == 'sim' else '' }}" href="/stats?mode=sim">模擬</a>
</div>

<form class="filterbar card-bar" method="get" action="/stats">
  <input type="hidden" name="mode" value="{{ f.mode }}">
```

- [ ] **Step 13: 跑全測試確認整鏈綠**

```bash
uv run pytest -q
```
預期既有 79+ 全綠（既有交易全 real、預設 mode=real，行為不變）＋ 本 task 新增測試綠。頁面 tab 的視覺留待 Task 8 併同 simtrade 手動實測。

- [ ] **Step 14: Commit（scoped）**

```bash
git add src/quanquant/db/models.py src/quanquant/db/migrate.py \
  src/quanquant/journal/schemas.py src/quanquant/journal/repository.py \
  src/quanquant/web/routers/trades.py src/quanquant/web/routers/stats.py \
  src/quanquant/web/templates/journal.html src/quanquant/web/templates/stats.html \
  tests/test_repository.py tests/test_migrate.py tests/test_mode_split.py
git commit -m "feat: Trade 加 mode(real/sim) 全鏈路過濾＋journal/stats 模擬|正式分頁，統計絕不跨 mode 聚合"
```

---

### Task 2: `Order`／`Deal` 資料模型 + broker 倉儲（去重）

新增委託單 `Order` 與成交 `Deal` 兩表（`Deal` 以 `UniqueConstraint(broker, seqno)` 冪等去重，epoch-ms 用 `BigInteger`），並提供 broker 倉儲（建委託、記成交去重、查列表、當日計數供 Task 7 風控）。

**Files:**
- Create: `src/quanquant/broker/__init__.py`
- Modify: `src/quanquant/db/models.py`（檔尾新增 `Order`、`Deal` 兩 class）
- Create: `src/quanquant/broker/repository.py`
- Test: Create `tests/test_broker_repo.py`

**Interfaces:**
- Consumes:（無上游 task；`Fill` 型別在 Task 3 定義，本 task 的 `record_deal` 先以「具 `.broker/.seqno/.price/.qty/.fee/.ts/.order_ref/.user_id/.mode` 屬性的物件」為契約，Task 3 的 `Fill` 完全相容）
- Produces（Task 4/5/7 依賴）：
  - `db.models.Order`（table `orders`）、`db.models.Deal`（table `deals`, unique(broker, seqno)）
  - `broker.repository.create_order(session, *, user_id, mode, broker, symbol, action, qty, price, price_type, order_type, octype) -> Order`
  - `broker.repository.set_order_ack(session, order_id, *, broker_order_id, status) -> Order | None`
  - `broker.repository.update_order_status(session, broker_order_id, *, status, filled_qty=None, avg_fill_price=None) -> Order | None`
  - `broker.repository.find_order_by_broker_id(session, broker_order_id) -> Order | None`
  - `broker.repository.record_deal(session, fill, *, order_id) -> Deal | None`（重播撞唯一鍵回 `None`）
  - `broker.repository.list_orders(session, *, user_id, mode, limit=100) -> list[Order]`
  - `broker.repository.count_orders_today(session, *, user_id, mode, now=None) -> int`
  - `broker.repository.sum_qty_today(session, *, user_id, mode, now=None) -> int`

- [ ] **Step 1: 建 broker 套件（`src/quanquant/broker/__init__.py`）**

> GateGuard：建新檔前照提示陳述事實（新增 broker 套件容器）後重試。

空檔即可：
```python
"""Broker-agnostic order subsystem (OrderService interface + Shioaji adapter)."""
```

- [ ] **Step 2: 先寫失敗測試 — 去重 + 建委託 + 當日計數（新檔 `tests/test_broker_repo.py`）**

> GateGuard：建新檔前陳述事實（Order/Deal 倉儲去重與當日計數測試）後重試。

```python
"""Order/Deal 倉儲：建委託、成交去重（seqno 重播不重寫）、當日計數。"""
import datetime as dt
from decimal import Decimal
from types import SimpleNamespace

from quanquant.broker import repository as brepo


def _fill(**over):
    base = dict(
        broker="shioaji", seqno="S1", symbol="TXF", action="Buy",
        price=Decimal("18000"), qty=1, fee=Decimal("50"), octype="New",
        ts=1_780_000_000_000, order_ref="O1", user_id=1, mode="sim",
    )
    base.update(over)
    return SimpleNamespace(**base)


def test_create_order_defaults_pending(session):
    o = brepo.create_order(
        session, user_id=1, mode="sim", broker="shioaji", symbol="TXF",
        action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New",
    )
    assert o.id is not None and o.status == "pending" and o.mode == "sim"


def test_record_deal_dedup_on_replay(session):
    first = brepo.record_deal(session, _fill(), order_id=None)
    assert first is not None
    replay = brepo.record_deal(session, _fill(), order_id=None)  # 同 (broker, seqno)
    assert replay is None  # 撞唯一鍵 → 跳過，不重寫


def test_record_deal_distinct_seqno_ok(session):
    assert brepo.record_deal(session, _fill(seqno="A"), order_id=None) is not None
    assert brepo.record_deal(session, _fill(seqno="B"), order_id=None) is not None


def test_count_and_sum_today_scoped_by_mode(session):
    now = dt.datetime(2026, 6, 16, 12, 0)  # naive UTC 中午 → CST 同一天
    brepo.create_order(session, user_id=1, mode="sim", broker="shioaji", symbol="TXF",
                       action="Buy", qty=2, price=Decimal("1"), price_type="MKT",
                       order_type="IOC", octype="New")
    brepo.create_order(session, user_id=1, mode="real", broker="shioaji", symbol="TXF",
                       action="Buy", qty=5, price=Decimal("1"), price_type="MKT",
                       order_type="IOC", octype="New")
    assert brepo.count_orders_today(session, user_id=1, mode="sim", now=now) == 1
    assert brepo.sum_qty_today(session, user_id=1, mode="sim", now=now) == 2
    assert brepo.sum_qty_today(session, user_id=1, mode="real", now=now) == 5


def test_update_order_status_by_broker_id(session):
    o = brepo.create_order(session, user_id=1, mode="sim", broker="shioaji", symbol="TXF",
                           action="Buy", qty=1, price=Decimal("18000"), price_type="LMT",
                           order_type="ROD", octype="New")
    brepo.set_order_ack(session, o.id, broker_order_id="B1", status="submitted")
    updated = brepo.update_order_status(session, "B1", status="filled",
                                        filled_qty=1, avg_fill_price=Decimal("18000"))
    assert updated is not None and updated.status == "filled" and updated.filled_qty == 1
```
跑 `uv run pytest tests/test_broker_repo.py -q` → RED（`Order`/`Deal`/`brepo` 未定義）。

- [ ] **Step 3: 新增 `Order`／`Deal` 表（`src/quanquant/db/models.py` 檔尾）**

在檔尾（`UserChartState` 之後）新增：
```python
class Order(SQLModel, table=True):
    """一張委託單（下單面板／委託列表的資料來源）。"""

    __tablename__ = "orders"

    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(index=True)                      # 下單當下綁定的擁有者
    mode: str = Field(default="real", index=True)         # "real" | "sim"
    broker: str = "shioaji"

    symbol: str = Field(index=True)                       # "TXF"
    action: str                                           # "Buy" | "Sell"
    qty: int
    price: Decimal = Field(sa_column=Column(DecimalText, nullable=False))
    price_type: str                                       # "LMT" | "MKT"
    order_type: str                                       # "ROD" | "IOC" | "FOK"
    octype: str                                           # "New" | "Cover" | "Auto"

    broker_order_id: str | None = Field(default=None, index=True)  # place 回來的券商單號
    status: str = "pending"  # pending|submitted|partfilled|filled|cancelled|failed
    filled_qty: int = 0
    avg_fill_price: Decimal | None = Field(default=None, sa_column=Column(DecimalText))

    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)


class Deal(SQLModel, table=True):
    """一筆成交（冪等去重來源：unique(broker, seqno)）。"""

    __tablename__ = "deals"
    __table_args__ = (UniqueConstraint("broker", "seqno"),)

    id: int | None = Field(default=None, primary_key=True)
    order_id: int | None = Field(default=None, foreign_key="orders.id", index=True)
    broker: str
    seqno: str = Field(index=True)                        # 券商成交序號（去重鍵之一）
    price: Decimal = Field(sa_column=Column(DecimalText, nullable=False))
    qty: int
    fee: Decimal | None = Field(default=None, sa_column=Column(DecimalText))
    # 成交時間 epoch-ms UTC — BigInteger（epoch-ms 溢位 Postgres 4-byte INTEGER）
    ts: int = Field(sa_column=Column(BigInteger, nullable=False))
    processed: bool = False                               # position-tracker 已消化
    created_at: datetime = Field(default_factory=_utcnow)
```
（`Column`／`BigInteger`／`UniqueConstraint`／`DecimalText`／`_utcnow` 皆已在本檔 import/定義，見 L10-11、L14、L27。）

- [ ] **Step 4: broker 倉儲（新檔 `src/quanquant/broker/repository.py`）**

> GateGuard：建新檔前陳述事實（Order/Deal CRUD＋去重＋當日計數倉儲）後重試。

```python
"""Order/Deal 倉儲：建委託、成交去重、委託查詢、當日風控計數。

雙方言可攜：只用 SQLModel/select，無 raw SQL。去重靠 Deal 的
UniqueConstraint(broker, seqno)：重播的成交 commit 撞唯一鍵 → rollback → 回 None。
"""
from datetime import datetime, timedelta, timezone

from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from quanquant.db.models import Deal, Order

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


def create_order(
    session: Session,
    *,
    user_id: int,
    mode: str,
    broker: str,
    symbol: str,
    action: str,
    qty: int,
    price,
    price_type: str,
    order_type: str,
    octype: str,
) -> Order:
    order = Order(
        user_id=user_id, mode=mode, broker=broker, symbol=symbol, action=action,
        qty=qty, price=price, price_type=price_type, order_type=order_type, octype=octype,
    )
    session.add(order)
    session.commit()
    session.refresh(order)
    return order


def set_order_ack(session: Session, order_id: int, *, broker_order_id: str, status: str) -> Order | None:
    order = session.get(Order, order_id)
    if order is None:
        return None
    order.broker_order_id = broker_order_id
    order.status = status
    order.updated_at = _utcnow()
    session.add(order)
    session.commit()
    session.refresh(order)
    return order


def find_order_by_broker_id(session: Session, broker_order_id: str) -> Order | None:
    return session.exec(
        select(Order).where(Order.broker_order_id == broker_order_id)
    ).first()


def update_order_status(
    session: Session,
    broker_order_id: str,
    *,
    status: str,
    filled_qty: int | None = None,
    avg_fill_price=None,
) -> Order | None:
    order = find_order_by_broker_id(session, broker_order_id)
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


def record_deal(session: Session, fill, *, order_id: int | None) -> Deal | None:
    """插入成交列；撞 unique(broker, seqno)（重播）→ rollback 回 None。"""
    deal = Deal(
        order_id=order_id, broker=fill.broker, seqno=fill.seqno,
        price=fill.price, qty=fill.qty, fee=fill.fee, ts=fill.ts,
    )
    session.add(deal)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        return None
    session.refresh(deal)
    return deal


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
```

- [ ] **Step 5: 跑測試確認 GREEN**

```bash
uv run pytest tests/test_broker_repo.py -q
```
預期全綠（去重回 None、當日計數依 mode 隔離）。

- [ ] **Step 6: Commit（scoped）**

```bash
git add src/quanquant/broker/__init__.py src/quanquant/broker/repository.py \
  src/quanquant/db/models.py tests/test_broker_repo.py
git commit -m "feat: 新增 Order/Deal 委託成交表＋broker 倉儲，Deal unique(broker,seqno) 冪等去重"
```

---

### Task 3: broker domain 型別 + `OrderService` 介面（純型別）

定義 broker 無關的 domain 型別（`OrderRequest`/`OrderAck`/`Fill`/`Position`）與 `OrderService` Protocol、以及風控/下單例外，供 adapter 與 tracker 消費。純資料、無 DB/框架依賴。

**Files:**
- Create: `src/quanquant/broker/types.py`
- Create: `src/quanquant/broker/base.py`
- Test: Create `tests/test_broker_types.py`

**Interfaces:**
- Consumes:（無上游 task）
- Produces（Task 4/5/6/7/8 依賴）：
  - `broker.types.OrderRequest(symbol, action, qty, price, price_type, order_type, octype, user_id, mode)`（dataclass）
  - `broker.types.OrderAck(broker_order_id, seqno, status)`
  - `broker.types.Fill(broker, seqno, symbol, action, price, qty, fee, octype, ts, order_ref, user_id, mode)`
  - `broker.types.Position(symbol, direction, qty, avg_price)`
  - `broker.base.OrderService`（Protocol：`async place/cancel/update/positions` + `on_fill`）
  - `broker.base.OrderError`、`broker.base.RiskError`（例外；後者 Task 7 用）

- [ ] **Step 1: 先寫失敗測試（新檔 `tests/test_broker_types.py`）**

> GateGuard：建新檔前陳述事實（broker 純型別與 Protocol 契約測試）後重試。

```python
"""broker 純型別：欄位齊全、Fill 帶 user_id/mode、Protocol 可被 duck-typed。"""
from decimal import Decimal

from quanquant.broker.base import OrderError, OrderService, RiskError
from quanquant.broker.types import Fill, OrderAck, OrderRequest, Position


def test_order_request_fields():
    req = OrderRequest(
        symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", user_id=7, mode="sim",
    )
    assert req.user_id == 7 and req.mode == "sim" and req.octype == "New"


def test_fill_carries_user_and_mode():
    f = Fill(
        broker="shioaji", seqno="S1", symbol="TXF", action="Sell",
        price=Decimal("18100"), qty=1, fee=Decimal("50"), octype="Cover",
        ts=1_780_000_000_000, order_ref="O1", user_id=7, mode="sim",
    )
    assert f.user_id == 7 and f.mode == "sim" and f.action == "Sell"


def test_order_ack_and_position():
    ack = OrderAck(broker_order_id="B1", seqno="S1", status="submitted")
    pos = Position(symbol="TXF", direction="long", qty=2, avg_price=Decimal("18000"))
    assert ack.status == "submitted" and pos.qty == 2


def test_exceptions_are_distinct():
    assert issubclass(RiskError, Exception)
    assert issubclass(OrderError, Exception)
    assert RiskError is not OrderError


def test_protocol_is_runtime_checkable_duck():
    class _Impl:
        async def place(self, req): ...
        async def cancel(self, broker_order_id): ...
        async def update(self, broker_order_id, *, price=None, qty=None): ...
        async def positions(self, user_id): ...
        def on_fill(self, handler): ...

    assert isinstance(_Impl(), OrderService)
```
跑 `uv run pytest tests/test_broker_types.py -q` → RED。

- [ ] **Step 2: domain 型別（新檔 `src/quanquant/broker/types.py`）**

> GateGuard：建新檔前陳述事實（broker 無關 domain dataclass）後重試。

```python
"""Broker 無關的 domain 型別（純資料、無 DB/框架依賴）。

YuantaAdapter 日後把 SendFutureOrder / RR_RealReport 映射到同一組型別即可接同介面。
"""
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class OrderRequest:
    symbol: str          # "TXF"
    action: str          # "Buy" | "Sell"
    qty: int
    price: Decimal
    price_type: str      # "LMT" | "MKT"
    order_type: str      # "ROD" | "IOC" | "FOK"
    octype: str          # "New" | "Cover" | "Auto"
    user_id: int         # 下單當下綁定，一路帶到 Fill
    mode: str            # "real" | "sim"


@dataclass(frozen=True, slots=True)
class OrderAck:
    broker_order_id: str
    seqno: str | None
    status: str          # "submitted" | "cancelled" | "updated" | "failed"


@dataclass(frozen=True, slots=True)
class Fill:
    broker: str          # "shioaji"
    seqno: str           # 券商成交序號（去重鍵）
    symbol: str
    action: str          # "Buy" | "Sell"
    price: Decimal
    qty: int
    fee: Decimal | None
    octype: str          # "New" | "Cover" | "Auto"
    ts: int              # 成交時間 epoch-ms UTC
    order_ref: str | None  # 對應 Order.broker_order_id
    user_id: int         # 由 adapter 依 order context 補上
    mode: str            # 由下單 session 決定


@dataclass(frozen=True, slots=True)
class Position:
    symbol: str
    direction: str       # "long" | "short"
    qty: int
    avg_price: Decimal
```

- [ ] **Step 3: `OrderService` Protocol + 例外（新檔 `src/quanquant/broker/base.py`）**

> GateGuard：建新檔前陳述事實（OrderService Protocol 與例外型別）後重試。

```python
"""Broker 無關的下單服務介面與例外。"""
from collections.abc import Callable
from typing import Protocol, runtime_checkable

from quanquant.broker.types import Fill, OrderAck, OrderRequest, Position


class OrderError(Exception):
    """下單/改單/刪單失敗（券商拒單、連線異常等）。"""


class RiskError(Exception):
    """風控攔截（超限、非白名單、kill switch、real 未二次確認）。"""


@runtime_checkable
class OrderService(Protocol):
    async def place(self, req: OrderRequest) -> OrderAck: ...
    async def cancel(self, broker_order_id: str) -> OrderAck: ...
    async def update(self, broker_order_id: str, *, price=None, qty=None) -> OrderAck: ...
    async def positions(self, user_id: int) -> list[Position]: ...
    def on_fill(self, handler: Callable[[Fill], None]) -> None: ...
```

- [ ] **Step 4: 跑測試確認 GREEN**

```bash
uv run pytest tests/test_broker_types.py -q
```

- [ ] **Step 5: Commit（scoped）**

```bash
git add src/quanquant/broker/types.py src/quanquant/broker/base.py tests/test_broker_types.py
git commit -m "feat: broker domain 型別（OrderRequest/OrderAck/Fill/Position）＋OrderService Protocol＋例外"
```

---

### Task 4: PositionTracker（Fill → 自動寫日誌）+ `find_open_trade`

成交 `Fill`（已去重）進來 → 開倉建未平倉 `Trade`（帶 user_id+mode、同向多筆聚合加權均價）、平倉找未平倉列補出場（pnl 自動算、多筆分次收依聚合均價、缺開倉的平倉安全記錄不亂配）。

**Files:**
- Modify: `src/quanquant/journal/repository.py`（新增 `find_open_trade`）
- Create: `src/quanquant/broker/position_tracker.py`
- Test: Create `tests/test_position_tracker.py`；`tests/test_repository.py` 補 `find_open_trade` scope 測試

**Interfaces:**
- Consumes：
  - Task 1：`repo.create_trade(session, TradeCreate, *, user_id)`（`TradeCreate.mode`）、`repo.update_trade(session, id, TradeUpdate, *, user_id)`、`TradeUpdate.mode/entry_price/size/fee/exit_time/exit_price`
  - Task 2：`brepo.record_deal(session, fill, *, order_id)`、`brepo.find_order_by_broker_id(session, broker_order_id)`
  - Task 3：`broker.types.Fill`
- Produces（Task 5/6 依賴）：
  - `repo.find_open_trade(session, symbol, *, user_id, mode, direction=None) -> Trade | None`（FIFO 最舊未平倉）
  - `broker.position_tracker.PositionTracker(session_factory)`；`tracker.handle_fill(fill: Fill) -> None`（可當 `on_fill` handler）

- [ ] **Step 1: 先寫失敗測試 — `find_open_trade` scope（`tests/test_repository.py`）**

在 `tests/test_repository.py` 末端新增：
```python
def test_find_open_trade_scoped(session, user):
    from quanquant.auth import service as auth_service
    other = auth_service.create_user(session, "carol", "pw", display_name="Carol")
    a = repo.create_trade(session, make_create(symbol="TXF"), user_id=user.id)        # real open
    repo.create_trade(session, make_create(symbol="TXF", mode="sim"), user_id=user.id)  # sim open
    repo.create_trade(session, make_create(symbol="TXF"), user_id=other.id)            # 別人 open

    found = repo.find_open_trade(session, "TXF", user_id=user.id, mode="real")
    assert found is not None and found.id == a.id           # 只配到自己的 real
    assert repo.find_open_trade(session, "MXF", user_id=user.id, mode="real") is None  # 別商品
    assert repo.find_open_trade(session, "TXF", user_id=user.id, mode="sim").mode == "sim"
```
跑 `uv run pytest tests/test_repository.py::test_find_open_trade_scoped -q` → RED。

- [ ] **Step 2: `find_open_trade`（`src/quanquant/journal/repository.py`）**

在 `list_for_stats`（L124-143）之後新增：
```python
def find_open_trade(
    session: Session,
    symbol: str,
    *,
    user_id: int,
    mode: str,
    direction: str | None = None,
) -> Trade | None:
    """(user_id, symbol, mode) scope 下最舊（FIFO）的未平倉列；可選 direction 過濾。"""
    stmt = select(Trade).where(
        Trade.user_id == user_id,
        Trade.symbol == symbol,
        Trade.mode == mode,
        Trade.exit_time.is_(None),  # type: ignore[union-attr]
    )
    if direction is not None:
        stmt = stmt.where(Trade.direction == direction)
    stmt = stmt.order_by(Trade.entry_time.asc())  # type: ignore[union-attr]
    return session.exec(stmt).first()
```
跑 `uv run pytest tests/test_repository.py::test_find_open_trade_scoped -q` → GREEN。

- [ ] **Step 3: 先寫失敗測試 — PositionTracker 開/平/聚合/去重/異常（新檔 `tests/test_position_tracker.py`）**

> GateGuard：建新檔前陳述事實（Fill→日誌 position-tracker 行為測試）後重試。

```python
"""PositionTracker：開倉建列、平倉收單、同向聚合加權均價、重播去重、缺開倉安全處理。"""
from decimal import Decimal
from types import SimpleNamespace

from sqlmodel import Session

from quanquant.broker.position_tracker import PositionTracker
from quanquant.broker.types import Fill
from quanquant.journal import repository as repo


def _tracker(engine):
    return PositionTracker(lambda: Session(engine))


def _fill(**over):
    base = dict(
        broker="shioaji", seqno="S1", symbol="TXF", action="Buy",
        price=Decimal("18000"), qty=1, fee=Decimal("50"), octype="New",
        ts=1_780_000_000_000, order_ref=None, user_id=1, mode="sim",
    )
    base.update(over)
    return Fill(**base)


def test_new_fill_opens_trade(engine, session):
    _tracker(engine).handle_fill(_fill())
    opens = repo.list_trades(session, user_id=1, mode="sim", status="open")
    assert len(opens) == 1
    t = opens[0]
    assert t.direction == "long" and t.size == 1 and t.entry_price == Decimal("18000")
    assert t.mode == "sim" and t.exit_time is None


def test_cover_fill_closes_and_computes_pnl(engine, session):
    tr = _tracker(engine)
    tr.handle_fill(_fill(seqno="A", action="Buy", octype="New", price=Decimal("18000")))
    tr.handle_fill(_fill(seqno="B", action="Sell", octype="Cover", price=Decimal("18100")))
    closed = repo.list_trades(session, user_id=1, mode="sim", status="closed")
    assert len(closed) == 1
    # (18100-18000)*1*200 - (50+50) = 20000 - 100 = 19900
    assert closed[0].pnl == Decimal("19900")


def test_same_direction_new_fills_aggregate_weighted_avg(engine, session):
    tr = _tracker(engine)
    tr.handle_fill(_fill(seqno="A", price=Decimal("18000"), qty=1))
    tr.handle_fill(_fill(seqno="B", price=Decimal("18100"), qty=1))
    opens = repo.list_trades(session, user_id=1, mode="sim", status="open")
    assert len(opens) == 1
    assert opens[0].size == 2
    assert opens[0].entry_price == Decimal("18050")  # (18000+18100)/2


def test_partial_covers_finalize_on_full_close(engine, session):
    tr = _tracker(engine)
    tr.handle_fill(_fill(seqno="A", price=Decimal("18000"), qty=2))            # open 2
    tr.handle_fill(_fill(seqno="B", action="Sell", octype="Cover",
                         price=Decimal("18100"), qty=1))                        # cover 1 → 仍開
    assert len(repo.list_trades(session, user_id=1, mode="sim", status="closed")) == 0
    tr.handle_fill(_fill(seqno="C", action="Sell", octype="Cover",
                         price=Decimal("18200"), qty=1))                        # cover 1 → 收單
    closed = repo.list_trades(session, user_id=1, mode="sim", status="closed")
    assert len(closed) == 1
    assert closed[0].exit_price == Decimal("18150")  # (18100+18200)/2


def test_replayed_seqno_not_reprocessed(engine, session):
    tr = _tracker(engine)
    tr.handle_fill(_fill(seqno="A", qty=1))
    tr.handle_fill(_fill(seqno="A", qty=1))  # 重播 → Deal 去重 → 不再開一列
    assert len(repo.list_trades(session, user_id=1, mode="sim", status="open")) == 1


def test_cover_without_open_is_safe(engine, session):
    _tracker(engine).handle_fill(_fill(seqno="Z", action="Sell", octype="Cover"))
    # 缺對應未平倉列 → 不亂配、不炸；不建任何 Trade
    assert repo.list_trades(session, user_id=1, mode="sim") == []


def test_cross_user_and_cross_mode_not_misassigned(engine, session):
    tr = _tracker(engine)
    tr.handle_fill(_fill(seqno="A", user_id=1, mode="sim", octype="New"))
    # user 2 的平倉不得收掉 user 1 的部位；real 的平倉不得收掉 sim 的部位
    tr.handle_fill(_fill(seqno="B", user_id=2, mode="sim", action="Sell", octype="Cover"))
    tr.handle_fill(_fill(seqno="C", user_id=1, mode="real", action="Sell", octype="Cover"))
    assert len(repo.list_trades(session, user_id=1, mode="sim", status="open")) == 1
    assert repo.list_trades(session, user_id=2, mode="sim") == []
```
跑 `uv run pytest tests/test_position_tracker.py -q` → RED（`PositionTracker` 未定義）。

- [ ] **Step 4: PositionTracker（新檔 `src/quanquant/broker/position_tracker.py`）**

> GateGuard：建新檔前陳述事實（成交→開/平倉映射，聚合加權均價、去重閘、缺開倉安全處理）後重試。

```python
"""成交 Fill → 交易日誌（round-trip 一列）自動映射。

契約：
- 每筆 Fill 先過 Deal 去重閘（record_deal 回 None 代表重播 → 直接跳過）。
- octype 分流：New = 開倉、Cover = 平倉、Auto 依當前部位方向推斷。
- 開倉：同 (user, symbol, mode, direction) 已有未平倉列 → 聚合加權均價、size 累加；否則建新列。
- 平倉：找未平倉列補 exit；多筆分次收 → 累積加權均價，covered 達 size 時才 set exit_time 收單
  （partial 累積存在 tracker 記憶體，單人 sim-first 可接受；重啟未平倉的續平會走 find_open_trade
  的 DB 事實，缺對應開倉一律「記錄異常不自動收單」）。
- 缺對應未平倉列的平倉：log warning、不建/不改任何 Trade（不亂配）。
- user_id/mode 一路由 Fill 帶入，寫入前必存在。
"""
import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlmodel import Session

from quanquant.broker import repository as brepo
from quanquant.broker.types import Fill
from quanquant.journal import repository as repo
from quanquant.journal.schemas import TradeCreate, TradeUpdate

log = logging.getLogger(__name__)
_CST = timezone(timedelta(hours=8))


def _to_dt(ts_ms: int) -> datetime:
    """epoch-ms UTC → naive CST（對齊 Trade.entry_time 的 naive 本地慣例）。"""
    return datetime.fromtimestamp(ts_ms / 1000, _CST).replace(tzinfo=None)


def _is_opening(fill: Fill, open_trade) -> bool:
    if fill.octype == "New":
        return True
    if fill.octype == "Cover":
        return False
    # Auto：無同向反向未平倉即視為開倉
    covered_dir = "long" if fill.action == "Sell" else "short"
    return not (open_trade is not None and open_trade.direction == covered_dir)


class PositionTracker:
    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory
        # 開放中 round-trip 的分次平倉累積：trade_id -> (加權出場 notional, 已平口數)
        self._partial: dict[int, tuple[Decimal, int]] = {}

    def handle_fill(self, fill: Fill) -> None:
        with self._session_factory() as session:
            order = (
                brepo.find_order_by_broker_id(session, fill.order_ref)
                if fill.order_ref else None
            )
            if brepo.record_deal(session, fill, order_id=order.id if order else None) is None:
                return  # 重播 → 去重跳過
            existing = repo.find_open_trade(
                session, fill.symbol, user_id=fill.user_id, mode=fill.mode
            )
            if _is_opening(fill, existing):
                self._apply_open(session, fill, existing)
            else:
                self._apply_close(session, fill)

    def _apply_open(self, session: Session, fill: Fill, existing) -> None:
        direction = "long" if fill.action == "Buy" else "short"
        if existing is not None and existing.direction == direction:
            new_size = existing.size + fill.qty
            new_entry = (
                existing.entry_price * existing.size + fill.price * fill.qty
            ) / Decimal(new_size)
            new_fee = (existing.fee or Decimal(0)) + (fill.fee or Decimal(0))
            repo.update_trade(
                session, existing.id,
                TradeUpdate(entry_price=new_entry, size=new_size, fee=new_fee),
                user_id=fill.user_id,
            )
            return
        repo.create_trade(
            session,
            TradeCreate(
                symbol=fill.symbol, direction=direction,
                entry_time=_to_dt(fill.ts), entry_price=fill.price,
                size=fill.qty, fee=fill.fee, mode=fill.mode,
            ),
            user_id=fill.user_id,
        )

    def _apply_close(self, session: Session, fill: Fill) -> None:
        covered_dir = "long" if fill.action == "Sell" else "short"
        open_trade = repo.find_open_trade(
            session, fill.symbol, user_id=fill.user_id, mode=fill.mode,
            direction=covered_dir,
        )
        if open_trade is None:
            log.warning(
                "cover fill 無對應未平倉列，記錄異常不自動收單: seqno=%s symbol=%s user=%s mode=%s",
                fill.seqno, fill.symbol, fill.user_id, fill.mode,
            )
            return
        acc_notional, acc_qty = self._partial.get(open_trade.id, (Decimal(0), 0))
        acc_notional += fill.price * fill.qty
        acc_qty += fill.qty
        total_fee = (open_trade.fee or Decimal(0)) + (fill.fee or Decimal(0))
        if acc_qty >= open_trade.size:
            exit_price = acc_notional / Decimal(acc_qty)  # 全期加權均價
            repo.update_trade(
                session, open_trade.id,
                TradeUpdate(exit_time=_to_dt(fill.ts), exit_price=exit_price, fee=total_fee),
                user_id=fill.user_id,
            )
            self._partial.pop(open_trade.id, None)
        else:
            self._partial[open_trade.id] = (acc_notional, acc_qty)
            repo.update_trade(
                session, open_trade.id, TradeUpdate(fee=total_fee), user_id=fill.user_id,
            )
```

- [ ] **Step 5: 跑測試確認 GREEN**

```bash
uv run pytest tests/test_position_tracker.py tests/test_repository.py -q
```
預期全綠（開/平/聚合/分次收/去重/缺開倉安全/跨 user 跨 mode 不誤配）。

- [ ] **Step 6: Commit（scoped）**

```bash
git add src/quanquant/journal/repository.py src/quanquant/broker/position_tracker.py \
  tests/test_position_tracker.py tests/test_repository.py
git commit -m "feat: PositionTracker 成交→日誌（開倉聚合加權均價/平倉收單pnl自動/去重/缺開倉安全）＋find_open_trade"
```

---

### Task 5: `ShioajiAdapter`（lazy-import shioaji、下單、成交 callback → on_fill）

實作 `OrderService`：lazy-import `shioaji`、`simulation=True`（sim）或 `activate_ca`（real）；`place`/`cancel`/`update` 回 `OrderAck`；`set_order_callback` 由 solace 執行緒觸發 → `loop.call_soon_threadsafe` → 映射 `Fill`（依 order context 補 user_id/mode）呼叫 on_fill handler。

**Files:**
- Create: `src/quanquant/broker/shioaji_adapter.py`
- Test: Create `tests/test_shioaji_adapter.py`

**Interfaces:**
- Consumes：
  - Task 3：`OrderRequest`/`OrderAck`/`Fill`/`Position`、`OrderService`、`OrderError`
  - Task 2：`brepo.create_order`、`brepo.set_order_ack`、`brepo.update_order_status`
- Produces（Task 6/7/8 依賴）：
  - `broker.shioaji_adapter.ShioajiAdapter(session_factory, loop, *, mode, api_key="", secret_key="", ca_path="", ca_passwd="", person_id="", risk_guard=None)`
  - `async connect() -> None`（登入 + sim/CA + set_order_callback）
  - `async place(req) -> OrderAck`、`async cancel(id) -> OrderAck`、`async update(id, *, price=None, qty=None) -> OrderAck`、`async positions(user_id) -> list[Position]`
  - `on_fill(handler)`、內部 `_deal_to_fill(msg, *, user_id, mode) -> Fill`、`_on_order_cb(stat, msg)`

- [ ] **Step 1: 先寫失敗測試（新檔 `tests/test_shioaji_adapter.py`，用 fake shioaji，不 import 原生 client）**

> GateGuard：建新檔前陳述事實（adapter 下單/回報以 fake 驗證，不碰原生 shioaji）後重試。

```python
"""ShioajiAdapter：以 fake api + recording loop 驗 place/cancel/update 回 OrderAck、
成交 callback → 映射 Fill → 觸發 on_fill（依 order context 補 user_id/mode）。"""
import asyncio
from decimal import Decimal
from types import SimpleNamespace

from sqlmodel import Session

from quanquant.broker.shioaji_adapter import ShioajiAdapter
from quanquant.broker.types import OrderRequest


class _RecordingLoop:
    def call_soon_threadsafe(self, fn, *args):
        fn(*args)  # inline，方便斷言


class _FakeTrade:
    def __init__(self, ordno, seqno):
        self.order = SimpleNamespace(id=ordno, seqno=seqno)
        self.status = SimpleNamespace(id=ordno, status="Submitted")


class _FakeShioaji:
    """只實作 adapter 會呼叫的最小面。"""
    def __init__(self):
        self.placed = []
        self.cancelled = []
        self.updated = []
        self.futopt_account = SimpleNamespace(account_id="F123")

    # 下單面
    def Order(self, **kw):
        return SimpleNamespace(**kw)

    def place_order(self, contract, order):
        self.placed.append((contract, order))
        return _FakeTrade(ordno="B1", seqno="S1")

    def cancel_order(self, trade):
        self.cancelled.append(trade)
        return _FakeTrade(ordno="B1", seqno="S1")

    def update_order(self, trade, price=None, qty=None):
        self.updated.append((trade, price, qty))
        return _FakeTrade(ordno="B1", seqno="S1")


def _adapter(engine):
    a = ShioajiAdapter(lambda: Session(engine), _RecordingLoop(), mode="sim")
    a._api = _FakeShioaji()  # 略過真 connect
    a._contract_for = lambda symbol: SimpleNamespace(code="TXF202607")  # 免查合約
    return a


def _req(**over):
    base = dict(symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
               price_type="LMT", order_type="ROD", octype="New", user_id=7, mode="sim")
    base.update(over)
    return OrderRequest(**base)


def test_place_returns_ack_and_persists_order(engine, session):
    a = _adapter(engine)
    ack = asyncio.run(a.place(_req()))
    assert ack.broker_order_id == "B1" and ack.seqno == "S1" and ack.status == "submitted"
    from quanquant.broker import repository as brepo
    orders = brepo.list_orders(session, user_id=7, mode="sim")
    assert len(orders) == 1 and orders[0].broker_order_id == "B1"


def test_place_binds_user_context_for_fill(engine):
    a = _adapter(engine)
    asyncio.run(a.place(_req(user_id=7, mode="sim")))
    fills = []
    a.on_fill(fills.append)
    # 模擬 FuturesDeal callback：msg 帶 seqno（對應剛下的單）
    msg = {"code": "TXF", "action": "Sell", "price": 18100, "quantity": 1,
           "seqno": "S1", "ts": 1_780_000_000, "order_type": "Cover"}
    a._on_order_cb(SimpleNamespace(value="FuturesDeal"), msg)
    assert len(fills) == 1
    f = fills[0]
    assert f.user_id == 7 and f.mode == "sim"      # 由 order context 補上
    assert f.price == Decimal("18100") and f.octype == "Cover" and f.action == "Sell"
    assert f.ts == 1_780_000_000_000              # 秒 → 毫秒


def test_cancel_and_update_return_ack(engine):
    a = _adapter(engine)
    c = asyncio.run(a.cancel("B1"))
    assert c.status == "cancelled"
    u = asyncio.run(a.update("B1", price=Decimal("18010")))
    assert u.status == "updated"


def test_on_order_cb_without_context_drops_fill(engine):
    a = _adapter(engine)
    fills = []
    a.on_fill(fills.append)
    a._on_order_cb(SimpleNamespace(value="FuturesDeal"),
                   {"code": "TXF", "action": "Buy", "price": 1, "quantity": 1,
                    "seqno": "UNKNOWN", "ts": 1, "order_type": "New"})
    assert fills == []  # 無 user context → 不亂寫別人日誌
```
跑 `uv run pytest tests/test_shioaji_adapter.py -q` → RED。

- [ ] **Step 2: ShioajiAdapter（新檔 `src/quanquant/broker/shioaji_adapter.py`）**

> GateGuard：建新檔前陳述事實（Shioaji 下單 adapter：lazy-import、sim/CA、跨執行緒 callback 橋接）後重試。

```python
"""Shioaji 下單 adapter（實作 OrderService）。

- lazy-import shioaji（同 sources/shioaji_stream.py，避免測試/非下單情境載入原生 client）。
- sim：Shioaji(simulation=True) 免 CA；real：login + activate_ca(person_id)。
- place/cancel/update 走 asyncio.to_thread（shioaji 呼叫為阻塞），回 OrderAck。
- set_order_callback 由 solace 執行緒觸發 → loop.call_soon_threadsafe → 映射 Fill → on_fill。
- 下單當下把 (user_id, mode) 以 seqno/ordno 暫存 order context；成交 callback 依 seqno 補回。
"""
import asyncio
import logging
from collections.abc import Callable
from decimal import Decimal, InvalidOperation

from sqlmodel import Session

from quanquant.broker import repository as brepo
from quanquant.broker.base import OrderError
from quanquant.broker.types import Fill, OrderAck, OrderRequest, Position

log = logging.getLogger(__name__)


def _dec(value, fallback: str = "0") -> Decimal:
    try:
        return Decimal(str(value)) if value not in (None, "") else Decimal(fallback)
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(fallback)


class ShioajiAdapter:
    def __init__(
        self,
        session_factory: Callable[[], Session],
        loop,
        *,
        mode: str,
        api_key: str = "",
        secret_key: str = "",
        ca_path: str = "",
        ca_passwd: str = "",
        person_id: str = "",
        risk_guard=None,
    ) -> None:
        self._session_factory = session_factory
        self._loop = loop
        self._mode = mode
        self._api_key = api_key
        self._secret_key = secret_key
        self._ca_path = ca_path
        self._ca_passwd = ca_passwd
        self._person_id = person_id
        self._risk_guard = risk_guard
        self._api = None
        self._fill_handlers: list[Callable[[Fill], None]] = []
        # order context：seqno -> (user_id, mode)；成交回報依 seqno 補回下單者
        self._context: dict[str, tuple[int, str]] = {}

    # ---- 生命週期（asyncio 側；阻塞呼叫走 to_thread）----

    async def connect(self) -> None:
        await asyncio.to_thread(self._connect_blocking)

    def _connect_blocking(self) -> None:
        import shioaji as sj  # lazy：keep native client off the import path

        api = sj.Shioaji(simulation=(self._mode == "sim"))
        api.login(self._api_key, self._secret_key, subscribe_trade=True, fetch_contract=True)
        if self._mode == "real":
            api.activate_ca(ca_path=self._ca_path, ca_passwd=self._ca_passwd,
                            person_id=self._person_id)
        api.set_order_callback(self._on_order_cb)
        self._api = api
        log.info("Shioaji order session connected (mode=%s)", self._mode)

    def _contract_for(self, symbol: str):
        """前月 TXF 合約（沿用 streamer 的近月挑選，測試以 monkeypatch 覆寫）。"""
        from quanquant.sources.shioaji_stream import ShioajiStreamer

        picker = ShioajiStreamer(self._api_key, self._secret_key, symbol, None, self._loop)
        picker._api = self._api
        return picker._front_contract(self._api)

    # ---- OrderService 介面 ----

    def on_fill(self, handler: Callable[[Fill], None]) -> None:
        self._fill_handlers.append(handler)

    async def place(self, req: OrderRequest) -> OrderAck:
        if self._risk_guard is not None:
            with self._session_factory() as session:
                self._risk_guard.check(req, session=session)  # 違規 raise RiskError
        with self._session_factory() as session:
            order = brepo.create_order(
                session, user_id=req.user_id, mode=req.mode, broker="shioaji",
                symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
                price_type=req.price_type, order_type=req.order_type, octype=req.octype,
            )
            order_id = order.id
        ack = await asyncio.to_thread(self._place_blocking, req)
        self._context[ack.seqno] = (req.user_id, req.mode)
        with self._session_factory() as session:
            brepo.set_order_ack(session, order_id,
                                broker_order_id=ack.broker_order_id, status=ack.status)
        return ack

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
            broker_order_id=str(trade.order.id), seqno=str(trade.order.seqno),
            status="submitted",
        )

    async def cancel(self, broker_order_id: str) -> OrderAck:
        return await asyncio.to_thread(self._cancel_blocking, broker_order_id)

    def _cancel_blocking(self, broker_order_id: str) -> OrderAck:
        if self._api is None:
            raise OrderError("order session not connected")
        trade = self._resolve_trade(broker_order_id)
        result = self._api.cancel_order(trade)
        with self._session_factory() as session:
            brepo.update_order_status(session, broker_order_id, status="cancelled")
        return OrderAck(broker_order_id=broker_order_id,
                        seqno=str(getattr(result.order, "seqno", "")), status="cancelled")

    async def update(self, broker_order_id: str, *, price=None, qty=None) -> OrderAck:
        return await asyncio.to_thread(self._update_blocking, broker_order_id, price, qty)

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
        return OrderAck(broker_order_id=broker_order_id,
                        seqno=str(getattr(result.order, "seqno", "")), status="updated")

    def _resolve_trade(self, broker_order_id: str):
        """以 broker_order_id 從 api 現有委託找回 trade 物件（真整合時 api.list_trades()）。"""
        for trade in (self._api.list_trades() if hasattr(self._api, "list_trades") else []):
            if str(getattr(trade.order, "id", "")) == broker_order_id:
                return trade
        raise OrderError(f"unknown broker_order_id: {broker_order_id}")

    async def positions(self, user_id: int) -> list[Position]:
        raw = await asyncio.to_thread(
            lambda: self._api.list_positions(self._api.futopt_account)
            if self._api is not None else []
        )
        out: list[Position] = []
        for p in raw:
            out.append(Position(
                symbol=str(getattr(p, "code", "")),
                direction="long" if str(getattr(p, "direction", "")).lower() == "buy" else "short",
                qty=int(getattr(p, "quantity", 0) or 0),
                avg_price=_dec(getattr(p, "price", 0)),
            ))
        return out

    # ---- 成交/委託 callback（solace 執行緒）----

    def _on_order_cb(self, stat, msg) -> None:
        """solace 執行緒：期貨成交事件 → 映射 Fill → 丟回 event loop 上的 handlers。"""
        state = getattr(stat, "value", stat)
        if state != "FuturesDeal":
            return  # 委託狀態變更（FuturesOrder）此處不寫日誌
        seqno = str(msg.get("seqno", ""))
        ctx = self._context.get(seqno)
        if ctx is None:
            log.warning("成交回報無 order context（seqno=%s）→ 不寫日誌，避免誤配", seqno)
            return
        user_id, mode = ctx
        fill = self._deal_to_fill(msg, user_id=user_id, mode=mode)
        for handler in self._fill_handlers:
            self._loop.call_soon_threadsafe(handler, fill)

    def _deal_to_fill(self, msg: dict, *, user_id: int, mode: str) -> Fill:
        return Fill(
            broker="shioaji",
            seqno=str(msg.get("seqno", "")),
            symbol=str(msg.get("code", "")),
            action=str(msg.get("action", "")),
            price=_dec(msg.get("price")),
            qty=int(msg.get("quantity", 0) or 0),
            fee=_dec(msg.get("fee")) if msg.get("fee") is not None else None,
            octype=str(msg.get("order_type", "")),
            ts=int(float(msg.get("ts", 0)) * 1000),  # 秒 → 毫秒
            order_ref=str(msg.get("id", "")) or None,
            user_id=user_id,
            mode=mode,
        )
```

> 註：`_deal_to_fill` 的 `msg` 欄位鍵名以官方 `OrderState.FuturesDeal` payload 為準——真整合（Task 8 的 simtrade 手動 E2E）時需對照實際 dict 微調鍵名；本 task 的單測以合成 dict 鎖定行為，先確保橋接與 context 綁定正確。

- [ ] **Step 3: 跑測試確認 GREEN**

```bash
uv run pytest tests/test_shioaji_adapter.py -q
```

- [ ] **Step 4: Commit（scoped）**

```bash
git add src/quanquant/broker/shioaji_adapter.py tests/test_shioaji_adapter.py
git commit -m "feat: ShioajiAdapter 下單/改單/刪單回 OrderAck＋成交callback映射Fill(依seqno補user/mode)觸發on_fill"
```

---

### Task 6: 設定 + lifespan 接線（下單 session 單例掛 `app.state.order_service`）

`config.py` 加下單設定（sim/real、CA 路徑、person_id、風控上限、kill switch、sim fee 估算）；`web/app.py` lifespan 起下單 session 單例，掛 `PositionTracker` 到 adapter 的 on_fill，暴露 `app.state.order_service`；`deps.py` 加 `get_order_service`。與行情 streamer 完全解耦（另一個 `Shioaji` 實例）。

**Files:**
- Modify: `src/quanquant/config.py`（`Settings` 加欄位）
- Modify: `src/quanquant/web/app.py`（lifespan 起下單 session；import）
- Modify: `src/quanquant/web/deps.py`（`get_order_service` + `__all__`）
- Test: Create `tests/test_order_settings.py`

**Interfaces:**
- Consumes：Task 5 `ShioajiAdapter`、Task 4 `PositionTracker`、Task 7 的 `RiskGuard`（Task 7 尚未做時 `risk_guard=None`，Task 7 完成後回填）
- Produces（Task 7/8 依賴）：
  - `Settings.order_mode/shioaji_trade_api_key/shioaji_trade_secret_key/shioaji_ca_path/shioaji_ca_passwd/shioaji_person_id/order_max_qty_per_order/order_max_qty_per_day/order_max_orders_per_day/order_symbol_whitelist/order_kill_switch/order_sim_fee`
  - `app.state.order_service`（未啟用時不設）
  - `deps.get_order_service(request) -> OrderService | None`

- [ ] **Step 1: 先寫失敗測試（新檔 `tests/test_order_settings.py`）**

> GateGuard：建新檔前陳述事實（下單設定預設值與 env 命名回歸）後重試。

```python
"""下單設定：安全預設（sim、kill switch off、上限保守）＋ env 覆寫。"""
from decimal import Decimal

from quanquant.config import Settings


def test_order_defaults_are_safe():
    s = Settings(_env_file=None)
    assert s.order_mode == "sim"              # 預設模擬，不碰真錢
    assert s.order_kill_switch is False
    assert s.order_max_qty_per_order >= 1
    assert s.order_max_qty_per_day >= 1
    assert s.order_max_orders_per_day >= 1
    assert "TXF" in s.order_symbol_whitelist
    assert s.order_sim_fee == Decimal("50")


def test_order_env_override(monkeypatch):
    monkeypatch.setenv("ORDER_MODE", "real")
    monkeypatch.setenv("ORDER_KILL_SWITCH", "true")
    monkeypatch.setenv("ORDER_MAX_QTY_PER_ORDER", "3")
    s = Settings(_env_file=None)
    assert s.order_mode == "real" and s.order_kill_switch is True
    assert s.order_max_qty_per_order == 3
```
跑 `uv run pytest tests/test_order_settings.py -q` → RED。

- [ ] **Step 2: 設定欄位（`src/quanquant/config.py`）**

檔首（L1-3 之間）加 `from decimal import Decimal`：
```python
from decimal import Decimal
from functools import lru_cache
```
在 `Settings` 的 `tv_symbol`（L51）之後、`class` 結束前新增：
```python
    # ---- 下單子系統（Shioaji 期貨；預設 sim，不碰真 CA/真錢）----
    order_mode: str = "sim"                    # "sim"（simulation=True）| "real"（activate_ca）
    shioaji_trade_api_key: str = ""            # 下單專用 token（可與行情 key 不同帳戶）
    shioaji_trade_secret_key: str = ""
    shioaji_ca_path: str = ""                  # .pfx 路徑（real；bind-mount，不進 git）
    shioaji_ca_passwd: str = ""
    shioaji_person_id: str = ""                # CA 綁定身分
    order_max_qty_per_order: int = 2           # 單筆口數上限
    order_max_qty_per_day: int = 10            # 單日累計口數上限
    order_max_orders_per_day: int = 20         # 單日委託次數上限
    order_symbol_whitelist: str = "TXF,MXF"    # 逗號分隔可下單商品白名單
    order_kill_switch: bool = False            # 全域急停：True 一律擋所有下單
    order_sim_fee: Decimal = Decimal("50")     # sim 成交手續費估算（real 取券商回報）
```

- [ ] **Step 3: 跑設定測試 GREEN**

```bash
uv run pytest tests/test_order_settings.py -q
```

- [ ] **Step 4: `get_order_service` 依賴（`src/quanquant/web/deps.py`）**

`__all__`（L13-14）加 `"get_order_service"`：
```python
__all__ = ["get_session", "get_poller", "get_pulse", "parse_date",
           "get_current_user", "require_admin", "get_order_service"]
```
在 `get_pulse`（L22-24）之後新增：
```python
def get_order_service(request: Request):
    """下單服務單例（未啟用/測試未接線時為 None）。"""
    return getattr(request.app.state, "order_service", None)
```

- [ ] **Step 5: lifespan 起下單 session（`src/quanquant/web/app.py`）**

在 import 區（L28-31 附近）加：
```python
from quanquant.web.deps import get_current_user
```
（已存在，不重複）——於 lifespan `yield` 之前、`tasks` 建立之後（約 L166 `streamer = None` 附近）新增下單 session 接線：
```python
    order_service = None
    if settings.shioaji_trade_api_key and settings.shioaji_trade_secret_key:
        from quanquant.broker.position_tracker import PositionTracker
        from quanquant.broker.risk import RiskGuard
        from quanquant.broker.shioaji_adapter import ShioajiAdapter

        tracker = PositionTracker(lambda: Session(get_engine()))
        guard = RiskGuard(settings)
        order_service = ShioajiAdapter(
            lambda: Session(get_engine()), asyncio.get_running_loop(),
            mode=settings.order_mode,
            api_key=settings.shioaji_trade_api_key,
            secret_key=settings.shioaji_trade_secret_key,
            ca_path=settings.shioaji_ca_path,
            ca_passwd=settings.shioaji_ca_passwd,
            person_id=settings.shioaji_person_id,
            risk_guard=guard,
        )
        order_service.on_fill(tracker.handle_fill)
        tasks.append(asyncio.create_task(order_service.connect()))
        log.info("order session enabled (mode=%s)", settings.order_mode)
    app.state.order_service = order_service
```

> 註：`RiskGuard` 於 Task 7 建立；本 step 的 import 與接線在 Task 7 完成前會讓「有設 trade key 時」啟動失敗，但**開發全程 `shioaji_trade_api_key` 未設（.env 空）→ 這段 skip**，既有啟動與測試不受影響。實作順序上 Task 7 緊接其後補上 `broker/risk.py`；若要 Task 6 單獨可跑，先把 `from quanquant.broker.risk import RiskGuard` 與 `guard = RiskGuard(settings)` 兩行連同 `risk_guard=guard` 暫時省略（改 `risk_guard=None`），待 Task 7 回填。

- [ ] **Step 6: 跑全測試確認未回歸**

```bash
uv run pytest -q
```
預期全綠（測試環境未設 trade key、lifespan 亦不在 TestClient 非 context 模式啟動；`app.state.order_service` 於路由用 `get_order_service` 取，缺省 None）。

- [ ] **Step 7: Commit（scoped）**

```bash
git add src/quanquant/config.py src/quanquant/web/app.py src/quanquant/web/deps.py \
  tests/test_order_settings.py
git commit -m "feat: 下單設定(sim預設/CA/風控上限/kill switch)＋lifespan 下單session單例掛 app.state.order_service"
```

---

### Task 7: 風控 `RiskGuard`（`OrderService.place` 前置檢查）

`place` 前檢查：全域 kill switch、商品白名單、單筆口數上限、單日口數/次數上限、real 單二次確認旗標；違規 raise `RiskError`。已在 Task 5 的 `adapter.place` 呼叫 `risk_guard.check(req, session=...)`。

**Files:**
- Create: `src/quanquant/broker/risk.py`
- Test: Create `tests/test_risk_guard.py`

**Interfaces:**
- Consumes：Task 3 `OrderRequest`/`RiskError`、Task 6 `Settings`、Task 2 `brepo.count_orders_today`/`sum_qty_today`
- Produces（Task 5 adapter 已呼叫、Task 8 router 可捕捉）：
  - `broker.risk.RiskGuard(settings)`；`check(req: OrderRequest, *, session, confirmed: bool = False) -> None`（違規 raise `RiskError`）

- [ ] **Step 1: 先寫失敗測試（新檔 `tests/test_risk_guard.py`）**

> GateGuard：建新檔前陳述事實（風控各上限與 kill switch/白名單/二次確認）後重試。

```python
"""RiskGuard：kill switch、白名單、單筆/單日口數、單日次數、real 二次確認。"""
from decimal import Decimal

import pytest

from quanquant.broker import repository as brepo
from quanquant.broker.base import RiskError
from quanquant.broker.risk import RiskGuard
from quanquant.broker.types import OrderRequest
from quanquant.config import Settings


def _req(**over):
    base = dict(symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
               price_type="LMT", order_type="ROD", octype="New", user_id=1, mode="sim")
    base.update(over)
    return OrderRequest(**base)


def _settings(**over):
    base = dict(order_kill_switch=False, order_symbol_whitelist="TXF,MXF",
                order_max_qty_per_order=2, order_max_qty_per_day=5,
                order_max_orders_per_day=3)
    base.update(over)
    return Settings(_env_file=None, **base)


def test_pass_within_limits(session):
    RiskGuard(_settings()).check(_req(), session=session)  # 不 raise


def test_kill_switch_blocks(session):
    with pytest.raises(RiskError):
        RiskGuard(_settings(order_kill_switch=True)).check(_req(), session=session)


def test_symbol_not_whitelisted(session):
    with pytest.raises(RiskError):
        RiskGuard(_settings()).check(_req(symbol="ZZZ"), session=session)


def test_per_order_qty_limit(session):
    with pytest.raises(RiskError):
        RiskGuard(_settings()).check(_req(qty=3), session=session)  # > 2


def test_per_day_qty_limit(session):
    g = RiskGuard(_settings(order_max_qty_per_day=3))
    brepo.create_order(session, user_id=1, mode="sim", broker="shioaji", symbol="TXF",
                       action="Buy", qty=2, price=Decimal("1"), price_type="MKT",
                       order_type="IOC", octype="New")
    with pytest.raises(RiskError):
        g.check(_req(qty=2), session=session)  # 2 已下 + 2 = 4 > 3


def test_per_day_order_count_limit(session):
    g = RiskGuard(_settings(order_max_orders_per_day=1))
    brepo.create_order(session, user_id=1, mode="sim", broker="shioaji", symbol="TXF",
                       action="Buy", qty=1, price=Decimal("1"), price_type="MKT",
                       order_type="IOC", octype="New")
    with pytest.raises(RiskError):
        g.check(_req(), session=session)  # 已 1 筆 + 這筆 = 2 > 1


def test_real_requires_confirmation(session):
    g = RiskGuard(_settings())
    with pytest.raises(RiskError):
        g.check(_req(mode="real"), session=session, confirmed=False)
    g.check(_req(mode="real"), session=session, confirmed=True)  # 確認後放行


def test_sim_no_confirmation_needed(session):
    RiskGuard(_settings()).check(_req(mode="sim"), session=session, confirmed=False)
```
跑 `uv run pytest tests/test_risk_guard.py -q` → RED。

- [ ] **Step 2: RiskGuard（新檔 `src/quanquant/broker/risk.py`）**

> GateGuard：建新檔前陳述事實（下單前風控閘：上限/白名單/kill switch/二次確認）後重試。

```python
"""下單前風控閘：全域 kill switch、白名單、單筆/單日口數、單日次數、real 二次確認。

違規一律 raise RiskError（adapter.place 於送單前呼叫；router 捕捉成表單錯誤）。
"""
from quanquant.broker import repository as brepo
from quanquant.broker.base import RiskError
from quanquant.broker.types import OrderRequest


class RiskGuard:
    def __init__(self, settings) -> None:
        self._s = settings

    def check(self, req: OrderRequest, *, session, confirmed: bool = False) -> None:
        s = self._s
        if s.order_kill_switch:
            raise RiskError("全域 kill switch 已啟動，暫停所有下單")

        whitelist = {x.strip() for x in s.order_symbol_whitelist.split(",") if x.strip()}
        if req.symbol not in whitelist:
            raise RiskError(f"商品 {req.symbol} 不在下單白名單")

        if req.qty > s.order_max_qty_per_order:
            raise RiskError(f"單筆 {req.qty} 口超過上限 {s.order_max_qty_per_order}")

        if req.mode == "real" and not confirmed:
            raise RiskError("正式下單需二次確認")

        qty_today = brepo.sum_qty_today(session, user_id=req.user_id, mode=req.mode)
        if qty_today + req.qty > s.order_max_qty_per_day:
            raise RiskError(
                f"單日累計 {qty_today + req.qty} 口超過上限 {s.order_max_qty_per_day}"
            )

        orders_today = brepo.count_orders_today(session, user_id=req.user_id, mode=req.mode)
        if orders_today + 1 > s.order_max_orders_per_day:
            raise RiskError(
                f"單日委託 {orders_today + 1} 次超過上限 {s.order_max_orders_per_day}"
            )
```

- [ ] **Step 3: 補 adapter 對 RiskError 的透傳測試（`tests/test_shioaji_adapter.py`）**

在 `tests/test_shioaji_adapter.py` 末端新增（確認 Task 5 的 `place` 確有呼叫 guard）：
```python
def test_place_blocked_by_risk_guard(engine):
    import asyncio

    from quanquant.broker.base import RiskError

    class _RejectGuard:
        def check(self, req, *, session, confirmed=False):
            raise RiskError("blocked")

    a = _adapter(engine)
    a._risk_guard = _RejectGuard()
    try:
        asyncio.run(a.place(_req()))
        assert False, "should have raised"
    except RiskError:
        pass
```

- [ ] **Step 4: 跑測試確認 GREEN**

```bash
uv run pytest tests/test_risk_guard.py tests/test_shioaji_adapter.py -q
```

- [ ] **Step 5: Commit（scoped）**

```bash
git add src/quanquant/broker/risk.py tests/test_risk_guard.py tests/test_shioaji_adapter.py
git commit -m "feat: RiskGuard 下單前風控（kill switch/白名單/單筆單日口數/單日次數/real二次確認）"
```

---

### Task 8: 下單 UI + 路由（`web/routers/orders.py`）+ 註冊

新增下單面板/委託列表/部位路由（仿 `trades.py`：HTMX-first、`Depends(get_current_user)`、`user.id` 穿入、`get_order_service` 取單例），並註冊進 app。純渲染部分標「手動 simtrade 實測」，路由資料邏輯有 pytest。

**Files:**
- Create: `src/quanquant/web/routers/orders.py`
- Create: `src/quanquant/web/templates/orders.html`
- Create: `src/quanquant/web/templates/partials/order_table.html`
- Create: `src/quanquant/web/templates/partials/position_table.html`
- Modify: `src/quanquant/web/app.py`（`from ... import ... orders` + `app.include_router(orders.router, dependencies=protected)`）
- Modify: `src/quanquant/web/templates/base.html`（nav 加 `/orders` 連結）
- Test: Create `tests/test_api_orders.py`

**Interfaces:**
- Consumes：Task 6 `get_order_service`、Task 3 `OrderRequest`/`OrderAck`/`Position`/`RiskError`/`OrderError`、Task 2 `brepo.list_orders`、Task 1 `mode` scope、既有 `get_current_user`/`get_session`
- Produces：`GET /orders`（頁）、`GET /orders/list`（委託列表 fragment）、`GET /orders/positions`（部位 fragment）、`POST /orders`（下單）、`POST /orders/{broker_order_id}/cancel`（刪單）

- [ ] **Step 1: 先寫失敗測試（新檔 `tests/test_api_orders.py`，用 FakeOrderService 覆寫依賴）**

> GateGuard：建新檔前陳述事實（下單路由資料邏輯以 fake service + in-memory DB 驗證）後重試。

```python
"""下單路由：頁面渲染、委託列表(mode scope)、部位渲染、POST 下單觸發 service。"""
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.broker import repository as brepo
from quanquant.broker.types import OrderAck, Position
from quanquant.web.deps import get_order_service

from tests.conftest import _build_app


class _FakeOrderService:
    def __init__(self):
        self.placed = []
        self.cancelled = []

    async def place(self, req):
        self.placed.append(req)
        return OrderAck(broker_order_id="B1", seqno="S1", status="submitted")

    async def cancel(self, broker_order_id):
        self.cancelled.append(broker_order_id)
        return OrderAck(broker_order_id=broker_order_id, seqno=None, status="cancelled")

    async def update(self, broker_order_id, *, price=None, qty=None):
        return OrderAck(broker_order_id=broker_order_id, seqno=None, status="updated")

    async def positions(self, user_id):
        return [Position(symbol="TXF", direction="long", qty=2, avg_price=Decimal("18000"))]

    def on_fill(self, handler):
        pass


@pytest.fixture
def fake_service():
    return _FakeOrderService()


@pytest.fixture
def order_client(engine, user, fake_service):
    app = _build_app(engine)
    app.dependency_overrides[get_order_service] = lambda: fake_service
    c = TestClient(app)
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    return c


def test_orders_page_renders(order_client):
    assert order_client.get("/orders").status_code == 200


def test_order_list_scoped_by_mode(order_client, session, user):
    brepo.create_order(session, user_id=user.id, mode="sim", broker="shioaji", symbol="TXF",
                       action="Buy", qty=1, price=Decimal("18000"), price_type="LMT",
                       order_type="ROD", octype="New")
    brepo.create_order(session, user_id=user.id, mode="real", broker="shioaji", symbol="MXF",
                       action="Sell", qty=1, price=Decimal("18000"), price_type="LMT",
                       order_type="ROD", octype="New")
    sim = order_client.get("/orders/list", params={"mode": "sim"}).text
    real = order_client.get("/orders/list", params={"mode": "real"}).text
    assert "TXF" in sim and "MXF" not in sim
    assert "MXF" in real and "TXF" not in real


def test_positions_render(order_client):
    body = order_client.get("/orders/positions").text
    assert "TXF" in body and "18,000" in body


def test_place_order_triggers_service(order_client, fake_service, user):
    r = order_client.post("/orders", data={
        "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New", "mode": "sim",
    })
    assert r.status_code == 200
    assert r.headers.get("HX-Trigger") == "refreshorders"
    assert len(fake_service.placed) == 1
    assert fake_service.placed[0].user_id == user.id  # user_id 由登入者穿入
    assert fake_service.placed[0].mode == "sim"


def test_place_order_without_service_errors(client):
    # 未接下單服務（get_order_service → None）時，下單回表單錯誤而非 500
    r = client.post("/orders", data={
        "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New", "mode": "sim",
    })
    assert r.status_code == 200
    assert r.headers.get("HX-Retarget") == ".form-error-slot"
```
跑 `uv run pytest tests/test_api_orders.py -q` → RED。

- [ ] **Step 2: orders 路由（新檔 `src/quanquant/web/routers/orders.py`）**

> GateGuard：建新檔前陳述事實（下單面板/委託列表/部位 HTMX 路由，仿 trades.py）後重試。

```python
"""下單面板 / 委託列表 / 部位（HTMX-first，仿 journal/trades 樣板）。

下單資料邏輯（建 Order、風控、觸發 service）走 pytest；純視覺互動（modal、二次確認 UX）
待 ORDER_MODE=sim 手動 simtrade 實測。
"""
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlmodel import Session

from quanquant.broker import repository as brepo
from quanquant.broker.base import OrderError, RiskError
from quanquant.broker.types import OrderRequest
from quanquant.db.models import User
from quanquant.web.deps import get_current_user, get_order_service, get_session
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


@router.get("/orders", response_class=HTMLResponse)
async def orders_page(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
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
    positions = await service.positions(user.id) if service is not None else []
    return HTMLResponse(render_partial("partials/position_table.html", positions=positions))


@router.post("/orders", response_class=HTMLResponse)
async def place_order(
    request: Request,
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
):
    if service is None:
        return _form_error("下單服務未啟用（ORDER_MODE 未接線）")
    form = await request.form()
    try:
        req = OrderRequest(
            symbol=form.get("symbol") or "",
            action=form.get("action") or "",
            qty=int(form.get("qty") or 0),
            price=Decimal(str(form.get("price") or "0")),
            price_type=form.get("price_type") or "LMT",
            order_type=form.get("order_type") or "ROD",
            octype=form.get("octype") or "Auto",
            user_id=user.id,
            mode=_mode(form.get("mode")),
        )
    except (ValueError, InvalidOperation) as exc:
        return _form_error(f"下單參數錯誤：{exc}")
    try:
        await service.place(req)
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
        await service.cancel(broker_order_id)
    except OrderError as exc:
        return _form_error(f"刪單失敗：{exc}")
    return _orders_trigger()
```

- [ ] **Step 3: 模板（三個新檔）**

> GateGuard：建新檔前陳述事實（下單頁 + 委託/部位 partial）後重試。

`src/quanquant/web/templates/orders.html`：
```html
{% extends "base.html" %}
{% block content %}
<div>
  <div class="journal-head">
    <h3>下單（{{ '模擬' if f.mode == 'sim' else '正式' }}）</h3>
  </div>

  <div class="mode-tabs" role="tablist">
    <a role="button" class="{{ '' if f.mode == 'sim' else 'contrast' }}" href="/orders?mode=real">正式</a>
    <a role="button" class="{{ 'contrast' if f.mode == 'sim' else '' }}" href="/orders?mode=sim">模擬</a>
  </div>

  <form hx-post="/orders" hx-swap="none" hx-disabled-elt="find button[type='submit']"
        hx-trigger="submit">
    <div class="form-error-slot"></div>
    <input type="hidden" name="mode" value="{{ f.mode }}">
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
    {% if o.broker_order_id and o.status in ('submitted', 'pending', 'partfilled') %}
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

- [ ] **Step 4: 註冊路由 + nav 連結**

`src/quanquant/web/app.py`：import 行（L28）加 `orders`：
```python
from quanquant.web.routers import alerts, candles, dashboard, health, orders, stats, trades
```
`create_app()` 的 `protected` 區塊（L207-212 之間，`trades` 之後）加：
```python
    app.include_router(orders.router, dependencies=protected)
```
`src/quanquant/web/templates/base.html`：nav-links（L37 `/stats` 連結之後）加：
```html
      <a href="/orders" class="nav-link {% if active == 'orders' %}active{% endif %}">下單</a>
```

- [ ] **Step 5: 跑測試確認 GREEN**

```bash
uv run pytest tests/test_api_orders.py -q
```
預期全綠（頁面渲染、委託列表 mode scope、部位渲染、下單觸發 fake service、無 service 回表單錯誤）。

- [ ] **Step 6: 跑全測試 + 標記手動 E2E**

```bash
uv run pytest -q
```
預期既有 79+ + 本計畫全部新增測試全綠。

> **手動 simtrade E2E（實作者於本機執行，非 pytest）**：於 `.env` 設 `source=shioaji`、`shioaji_trade_api_key`/`shioaji_trade_secret_key`、`ORDER_MODE=sim` → `uv run quanquant-web` → 開 `/orders`（模擬 tab）送單 → 觀察委託列表狀態、`/journal?mode=sim` 自動出現一列、`/stats?mode=sim` 反映、`/stats?mode=real` 不受影響。`_deal_to_fill` 的 `msg` 鍵名若與實際 `OrderState.FuturesDeal` payload 不符，於此步對照微調（見 Task 5 註）。

- [ ] **Step 7: Commit（scoped）**

```bash
git add src/quanquant/web/routers/orders.py src/quanquant/web/templates/orders.html \
  src/quanquant/web/templates/partials/order_table.html \
  src/quanquant/web/templates/partials/position_table.html \
  src/quanquant/web/app.py src/quanquant/web/templates/base.html \
  tests/test_api_orders.py
git commit -m "feat: 下單 UI＋orders 路由（下單面板/委託列表/部位，模擬|正式 tab，風控攔截成表單錯誤）"
```

---

## Self-Review

### Spec §A–G 覆蓋逐節對照
- **§A Broker 抽象層**：Task 3（`OrderRequest`/`OrderAck`/`Fill`/`Position` + `OrderService` Protocol + `OrderError`/`RiskError`）。✅
- **§B Shioaji 下單 session 生命週期**：Task 5（`ShioajiAdapter` lazy-import、sim/CA、`set_order_callback`→`loop.call_soon_threadsafe`）+ Task 6（lifespan 單例、與行情 streamer 解耦、獨立 `Shioaji` 實例）。✅
- **§C 委託資料模型 + 去重**：Task 2（`Order`/`Deal`，`Deal` unique(broker,seqno)，`ts` 用 `BigInteger`，`record_deal` 撞唯一鍵回 None）。✅
- **§D Position-tracker → 自動寫日誌**：Task 4（New 開倉/Cover 平倉/Auto 推斷、同向聚合加權均價、`find_open_trade`、分次收依聚合均價、缺開倉安全處理、`mode`/`fee` 蓋在 `TradeCreate`）。✅ 未實現損益 mark 仍走 `poller.last.price`（`trades.py` 未動）。✅
- **§E `mode` 分流**：Task 1（`Trade.mode`+遷移 DEFAULT 'real'、schema、`list_trades`/`list_for_stats`/`_filtered`、journal/stats tab+hidden、手動表單本就走 `TradeCreate` 預設 real）；`stats/metrics.py` 一行未改。✅
- **§F 下單 UI + 風控**：Task 8（`orders.py` 路由 + 模板）+ Task 7（`RiskGuard`：單筆/單日口數、次數、白名單、kill switch、real 二次確認）。✅ audit 以 `Order`/`Deal` + log 落地（多人版再擴 audit_log，spec 已標）。
- **§G 設定/部署**：Task 6（`.env` 對應設定欄位、sim 預設、CA 路徑/person_id/kill switch/上限）；`.pfx` bind-mount 與三步部署沿用既有（CLAUDE.md/deployment）。✅

### 關鍵設計難點對照
1. Fill 流 → round-trip 一列：Task 4 聚合模型（開倉加權均價、平倉分次收）。✅
2. callback 無 HTTP user：Task 5 order context（seqno→user_id/mode），`Fill` 帶 user_id/mode，寫入前必存在（無 context 直接 drop）。✅
3. 回報重播冪等去重：Task 2 `Deal` unique + Task 4 `record_deal` 去重閘。✅
4. sim/real 絕不混算：Task 1 `mode` 第一級 scope + `test_mode_split.py` 隔離斷言。✅

### Placeholder 掃描
- 全文無 TBD/TODO/「適當處理」等占位；每個「Create 新檔」step 給完整檔案內容，每個「Modify」step 給精確行號 + 可照抄片段。
- 唯二標「手動實測」處為**不可 pytest 化的真實 simtrade E2E**（Task 8 Step 6）與 `_deal_to_fill` 真 payload 鍵名對照（Task 5 註）——皆屬 spec §測試計畫明列的「端到端（simtrade 手動）」，非邏輯占位；其資料邏輯均已由單測覆蓋。

### 型別一致性
- `Fill.ts`（epoch-ms int）→ `Deal.ts`（`BigInteger`）一致；→ `Trade.entry_time/exit_time`（naive CST datetime）經 `_to_dt` 轉換，符合 `db/models.py` naive 本地慣例。✅
- `OrderRequest.user_id/mode` 一路帶到 `Fill.user_id/mode` 再到 `TradeCreate.mode`/`create_trade(user_id=)`。✅
- 金額全程 `Decimal`（`DecimalText`/`compute_pnl`）；`price`/`fee` 無 float 汙染（adapter 對 shioaji 邊界才 `float(req.price)`，回程 `_dec(...)`）。✅
- `OrderService` Protocol 的 async 簽名與 `ShioajiAdapter`、`_FakeOrderService` 一致（Task 3 `test_protocol_is_runtime_checkable_duck` 把關）。✅
- 引用到的型別/函式皆有定義來源：`RiskGuard`(T7)、`PositionTracker`(T4)、`ShioajiAdapter`(T5)、`Order`/`Deal`/`brepo.*`(T2)、`find_open_trade`(T4)、`get_order_service`(T6)、`Fill`/`OrderRequest`/…(T3)。✅

### 相依順序驗證
T1（mode 基礎）→ T2（Order/Deal repo）→ T3（型別）→ T4（tracker，需 T1+T2+T3）→ T5（adapter，需 T2+T3）→ T6（lifespan，需 T4+T5，前置 T7 的 RiskGuard 以註記處理）→ T7（風控，需 T2+T3+T6）→ T8（UI，需 T1+T2+T3+T6）。每 task 結尾 `uv run pytest` 全綠才進下一 task；T6 對 T7 的前向依賴已在 Step 5 給「可先 `risk_guard=None`、T7 回填」的安全路徑。✅
