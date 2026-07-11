# 休市與資料停滯防呆 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 台指期休市（含臨時颱風）或來源停滯時，K 棒不再產生 phantom／凍結棒、指標不被汙染、前端不亂跳，並讓使用者看得到休市狀態。

**Architecture:** 集中式新鮮度判斷（`FreshnessTracker` 掛在 `QuotePoller.publish` 唯一 bus 進入點，兩條發布路徑共用），三層防線把關——時段層（calendar，含 env 假日覆寫）、資料層（is_fresh 閘門）、前端層（NaN/單調/交易時段防呆＋狀態 banner）。

**Tech Stack:** Python 3 / FastAPI / pydantic-settings / frozen dataclass；前端 vanilla JS + KLineCharts v9.8.12；測試 pytest（+ respx、TestClient）。

## Global Constraints

以下為專案級鐵律，**每個 task 的要求都隱含包含本節**（逐字取自 `CLAUDE.md`）：

- `Candle.ts` 必須是 `BigInteger`（epoch-ms 會溢位 Postgres 4-byte INTEGER）
- 資料庫雙方言：本機 SQLite / 雲端 Postgres — 新增 raw SQL 必須兩邊可攜
- pyproject 的 hatch wheel 設定**不可加 force-include**
- KLineCharts 釘版 v9.8.12；`chart.js` 的四道渲染防線勿移除
- KLineCharts locale 必須用繁體 `zh-TW`；全站中文一律繁體台灣
- 本機 `quanquant.db` 含珍貴回補歷史，驗證清理時勿刪
- candle 讀取路徑走 raw-SQL→float（FastCandle），routes 為 sync `def` — 勿改 async/ORM
- `.env` 與 `*.dump` 不進 git
- `uv run pytest` 必須全綠才可部署（79+ 測試）
- commit 訊息格式 `<type>: <描述>`，attribution 全域停用（不加署名）

## 實作期對 spec 的微調（已於計畫落地）

1. `_is_stale_day_quote` **保留為互補**（非取代）——freshness gate 對「停滯首筆重播」有盲點，舊網補上。
2. env 變數用 `EXTRA_HOLIDAYS`（專案無 env_prefix）。
3. `is_fresh` 在 `QuotePoller.publish` 以 `dataclasses.replace` 注入（`FuturesSnapshot` 為 frozen）。spec ③（require_price fallback）以「taifex 無 CLastPrice 時直接標 `is_fresh=False` + tracker 只降級不升級」實作。
4. taifex 合約後綴選擇維持時鐘判斷；只改報價面板顯示為 calendar-aware。
5. `trade_time` 僅作新鮮度訊號，bucket 不變。

---

## File Structure

| 檔案 | 動作 | 職責 |
|---|---|---|
| `src/quanquant/models.py` | Modify | `FuturesSnapshot` 新增 `trade_time` / `is_fresh` 欄位 |
| `src/quanquant/sources/taifex.py` | Modify | `_parse_row` 解析 `CTime`→`trade_time`；新增 `_parse_trade_time` helper |
| `src/quanquant/freshness.py` | Create | `FreshnessTracker`：集中式新鮮度判斷 |
| `src/quanquant/poller.py` | Modify | `publish` 接 tracker、`freshness` property |
| `src/quanquant/candles/builder.py` | Modify | `on_snapshot` 新增 `is_fresh` 閘門 |
| `src/quanquant/config.py` | Modify | `Settings` 新增 `extra_holidays` 欄位 |
| `src/quanquant/candles/market_calendar.py` | Modify | env 假日 union；`session_now`、`resolve_market_status` |
| `src/quanquant/web/routers/dashboard.py` | Modify | `_quote_context` 改 calendar-aware + market_status |
| `src/quanquant/web/templates/partials/quote.html` | Modify | 新屬性 `data-qq-market-status`/`data-qq-fresh` + banner |
| `src/quanquant/web/static/chart.js` | Modify | `_applyBar` 守衛；`onQuote`/`_bindQuoteSync` 交易時段/fresh 閘門 |
| `src/quanquant/web/static/app.css` | Modify | `.market-banner` 樣式 |
| `tests/test_freshness.py` | Create | `FreshnessTracker` + poller 整合測試 |
| `tests/test_taifex.py` | Modify | CTime 解析測試 |
| `tests/test_candle_builder.py` | Modify | is_fresh 閘門測試 |
| `tests/test_market_calendar.py` | Modify | env 假日、`session_now`、`resolve_market_status` 測試 |
| `tests/test_quote_status.py` | Create | `_quote_context` + quote.html render 測試 |

---

## Task 1: FuturesSnapshot 新欄位 + taifex 解析 CTime

**Files:**
- Modify: `src/quanquant/models.py:6-19`
- Modify: `src/quanquant/sources/taifex.py`（imports、新增 `_parse_trade_time`、`_parse_row`）
- Test: `tests/test_taifex.py`

**Interfaces:**
- Produces: `FuturesSnapshot(..., trade_time: datetime | None = None, is_fresh: bool = True)`（兩欄位有預設，既有建構相容）
- Produces: `taifex._parse_trade_time(cdate: str, ctime: str) -> datetime | None`

- [ ] **Step 1: 加模型欄位**

在 `src/quanquant/models.py` 的 `FuturesSnapshot` 末尾（`contract_month` 之後）新增兩欄位：

```python
@dataclass(frozen=True, slots=True)
class FuturesSnapshot:
    symbol: str
    price: Decimal
    change: Decimal
    change_pct: float
    volume: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    fetched_at: datetime       # UTC
    data_date: str             # trading date from API, e.g. "2026-06-04"
    contract_month: str        # nearest active contract, e.g. "202607"
    trade_time: datetime | None = None  # 來源最後成交時間（CST），解析失敗為 None
    is_fresh: bool = True       # 本筆是否為新成交（FreshnessTracker 於 publish 注入）
```

- [ ] **Step 2: 寫失敗測試（CTime 解析）**

在 `tests/test_taifex.py` 末尾新增（含一個帶 `CTime` 的 sample）：

```python
from datetime import datetime

from quanquant.market_hours import CST

_SAMPLE_CTIME = {
    "RtCode": "0",
    "RtMsg": "OK",
    "RtData": {
        "QuoteList": [
            {
                "SymbolID": "TXFF6-F", "CLastPrice": "18000", "CRefPrice": "17950",
                "CDiff": "50", "CDiffRate": "0.28", "CTotalVolume": "12345",
                "COpenPrice": "17960", "CHighPrice": "18050", "CLowPrice": "17940",
                "CDate": "2026-06-01", "CTime": "10:30:45",
            },
        ]
    },
}


@pytest.mark.asyncio
@respx.mock
async def test_parse_row_reads_ctime_as_trade_time():
    respx.post("https://mis.taifex.com.tw/futures/api/getQuoteList").mock(
        return_value=Response(200, json=_SAMPLE_CTIME)
    )
    async with TaifexSource() as src:
        snap = await src.fetch_snapshot("TXF")
    assert snap.trade_time == datetime(2026, 6, 1, 10, 30, 45, tzinfo=CST)


@pytest.mark.asyncio
@respx.mock
async def test_missing_ctime_gives_none_trade_time():
    respx.post("https://mis.taifex.com.tw/futures/api/getQuoteList").mock(
        return_value=Response(200, json=_SAMPLE)  # 既有 sample 無 CTime
    )
    async with TaifexSource() as src:
        snap = await src.fetch_snapshot("TXF")
    assert snap.trade_time is None
```

- [ ] **Step 3: 執行測試，確認失敗**

Run: `uv run pytest tests/test_taifex.py::test_parse_row_reads_ctime_as_trade_time -v`
Expected: FAIL（`trade_time` 目前恆為 None / 需先有 Step 1 欄位）

- [ ] **Step 4: 實作 `_parse_trade_time` 並接進 `_parse_row`**

在 `src/quanquant/sources/taifex.py` 頂部 import 補上 `CST`（原本只 import `get_session`）：

```python
from quanquant.market_hours import CST, get_session
```

新增 module-level helper（放在 `_d` helper 附近）：

```python
def _parse_trade_time(cdate: str, ctime: str) -> datetime | None:
    """由 MIS CDate + CTime 合成 CST 成交時間；格式異常回 None（防禦式）。"""
    d = "".join(ch for ch in (cdate or "") if ch.isdigit())
    t = "".join(ch for ch in (ctime or "") if ch.isdigit())
    if len(d) < 8 or len(t) < 6:
        return None
    try:
        return datetime(
            int(d[:4]), int(d[4:6]), int(d[6:8]),
            int(t[:2]), int(t[2:4]), int(t[4:6]), tzinfo=CST,
        )
    except ValueError:
        return None
```

在 `_parse_row` 的 `return FuturesSnapshot(...)` 補一個 kwarg（放在 `contract_month=...` 之後）：

```python
            contract_month=row.get("SymbolID", ""),
            trade_time=_parse_trade_time(row.get("CDate", ""), row.get("CTime", "")),
            is_fresh=bool(row["CLastPrice"]),  # 無 CLastPrice（休市結算價 fallback）→ 標非新成交（實作 spec ③）
```

並在 Step 2 的測試區塊追加一個結算價 fallback 測試（整張 QuoteList 皆無 CLastPrice → 走 require_price=False，該筆應 `is_fresh=False`）：

```python
_SAMPLE_CLOSED = {
    "RtCode": "0", "RtMsg": "OK",
    "RtData": {"QuoteList": [
        {"SymbolID": "TXFF6-F", "CLastPrice": "", "CRefPrice": "17950",
         "SettlementPrice": "17950", "CTotalVolume": "12345", "CDate": "2026-06-01"},
    ]},
}


@pytest.mark.asyncio
@respx.mock
async def test_settlement_fallback_marked_not_fresh():
    respx.post("https://mis.taifex.com.tw/futures/api/getQuoteList").mock(
        return_value=Response(200, json=_SAMPLE_CLOSED)
    )
    async with TaifexSource() as src:
        snap = await src.fetch_snapshot("TXF")
    assert snap.is_fresh is False   # 結算價 fallback，非新成交
```

- [ ] **Step 5: 執行測試，確認通過**

Run: `uv run pytest tests/test_taifex.py -v`
Expected: PASS（含既有測試不回歸）

- [ ] **Step 6: Commit**

```bash
git add src/quanquant/models.py src/quanquant/sources/taifex.py tests/test_taifex.py
git commit -m "feat: FuturesSnapshot 新增 trade_time/is_fresh，taifex 解析 CTime"
```

---

## Task 2: FreshnessTracker（集中式新鮮度判斷）

**Files:**
- Create: `src/quanquant/freshness.py`
- Test: `tests/test_freshness.py`

**Interfaces:**
- Consumes: `FuturesSnapshot`（Task 1 的 `trade_time`）
- Produces: `FreshnessTracker().evaluate(snap: FuturesSnapshot) -> FuturesSnapshot`（回傳已設 `is_fresh` 的新 snapshot）
- Produces: `FreshnessTracker().last_advance_at -> datetime | None`

- [ ] **Step 1: 寫失敗測試**

建立 `tests/test_freshness.py`：

```python
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

from quanquant.freshness import FreshnessTracker
from quanquant.models import FuturesSnapshot

_AT = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)


def snap(vol, contract="TXFF6-F", trade_time=None, at=_AT, price="18000"):
    p = Decimal(price)
    return FuturesSnapshot(
        symbol="TXF", price=p, change=Decimal(0), change_pct=0.0, volume=vol,
        open_price=p, high_price=p, low_price=p, fetched_at=at,
        data_date="2026-06-16", contract_month=contract, trade_time=trade_time,
    )


def test_first_with_volume_is_fresh():
    t = FreshnessTracker()
    assert t.evaluate(snap(1000)).is_fresh is True
    assert t.last_advance_at == _AT


def test_first_zero_volume_not_fresh():
    t = FreshnessTracker()
    assert t.evaluate(snap(0)).is_fresh is False
    assert t.last_advance_at is None


def test_volume_advance_is_fresh():
    t = FreshnessTracker()
    t.evaluate(snap(1000))
    assert t.evaluate(snap(1010)).is_fresh is True


def test_volume_stall_not_fresh():
    t = FreshnessTracker()
    t.evaluate(snap(1000, at=_AT))
    later = datetime(2026, 6, 16, 10, 1, tzinfo=timezone.utc)
    out = t.evaluate(snap(1000, at=later))
    assert out.is_fresh is False
    assert t.last_advance_at == _AT  # 停滯不推進


def test_ctime_advance_rescues_frozen_volume():
    t = FreshnessTracker()
    t0 = datetime(2026, 6, 16, 10, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 6, 16, 10, 0, 5, tzinfo=timezone.utc)
    t.evaluate(snap(1000, trade_time=t0))
    assert t.evaluate(snap(1000, trade_time=t1)).is_fresh is True  # 量凍結但成交時間前進


def test_cumulative_drop_resets_baseline():
    t = FreshnessTracker()
    t.evaluate(snap(5000))
    assert t.evaluate(snap(3)).is_fresh is True  # 跨盤重數 → 重置，3>0 視為 fresh


def test_identity_change_resets_baseline():
    t = FreshnessTracker()
    t.evaluate(snap(1000, contract="TXFF6-F"))
    assert t.evaluate(snap(1000, contract="TXFG6")).is_fresh is True  # 身分改變 → 重置


def test_source_marked_not_fresh_is_respected():
    t = FreshnessTracker()
    s = replace(snap(1000), is_fresh=False)  # 來源（結算價 fallback）已標記 not-fresh
    assert t.evaluate(s).is_fresh is False   # tracker 不上升級
```

- [ ] **Step 2: 執行測試，確認失敗**

Run: `uv run pytest tests/test_freshness.py -v`
Expected: FAIL（`ModuleNotFoundError: quanquant.freshness`）

- [ ] **Step 3: 實作 FreshnessTracker**

建立 `src/quanquant/freshness.py`：

```python
"""Central freshness judgment for the snapshot stream.

單一有狀態 tracker 掛在 pub/sub 發布邊界（QuotePoller.publish），讓每個下游
（CandleBuilder、報價 SSE、alert）對同一筆快照看到一致的 is_fresh。"Fresh" =
這筆反映了「新成交」；休市/假日重播會凍結累積量與最後成交時間，兩者皆不前進即
判為 stale。

兩個獨立訊號 OR（分層聯集）：真實成交必使累積量前進，但若來源凍結某一路訊號，
用另一路救援；兩路皆凍 → stale。
"""
from dataclasses import replace
from datetime import datetime

from quanquant.models import FuturesSnapshot


class FreshnessTracker:
    def __init__(self) -> None:
        self._last_cum_vol: int | None = None
        self._last_contract: str | None = None
        self._last_trade_time: datetime | None = None
        self._last_advance_at: datetime | None = None

    @property
    def last_advance_at(self) -> datetime | None:
        """最近一筆判為 fresh 的快照牆鐘（UTC）。"""
        return self._last_advance_at

    def evaluate(self, snap: FuturesSnapshot) -> FuturesSnapshot:
        """回傳把 is_fresh 依量/成交時間前進填好的 snapshot。"""
        # 尊重來源已標記的 not-fresh（結算價 fallback）——tracker 只降級不升級。
        fresh = snap.is_fresh and self._is_fresh(snap)
        if fresh:
            self._last_advance_at = snap.fetched_at
        self._last_cum_vol = snap.volume
        self._last_contract = snap.contract_month
        if snap.trade_time is not None:
            self._last_trade_time = snap.trade_time
        return replace(snap, is_fresh=fresh)

    def _is_fresh(self, snap: FuturesSnapshot) -> bool:
        prev_vol = self._last_cum_vol
        # 新身分，或累積量下降（新盤重數）→ 以本筆為基準；有量才算 fresh。
        if (
            prev_vol is None
            or snap.contract_month != self._last_contract
            or snap.volume < prev_vol
        ):
            return snap.volume > 0
        volume_advanced = snap.volume > prev_vol
        time_advanced = (
            snap.trade_time is not None
            and self._last_trade_time is not None
            and snap.trade_time > self._last_trade_time
        )
        return volume_advanced or time_advanced
```

- [ ] **Step 4: 執行測試，確認通過**

Run: `uv run pytest tests/test_freshness.py -v`
Expected: PASS（7 個測試全綠）

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/freshness.py tests/test_freshness.py
git commit -m "feat: 新增 FreshnessTracker 集中式新鮮度判斷"
```

---

## Task 3: 把 FreshnessTracker 接進 QuotePoller.publish

**Files:**
- Modify: `src/quanquant/poller.py`（imports、`__init__`、`publish`、新 property）
- Test: `tests/test_freshness.py`（追加 poller 整合測試）

**Interfaces:**
- Consumes: `FreshnessTracker`（Task 2）
- Produces: `QuotePoller.freshness -> FreshnessTracker` property；`publish` 後 `poller.last.is_fresh` 已設定

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_freshness.py` 追加：

```python
from datetime import timedelta

from quanquant.poller import QuoteEvent, QuotePoller


def _event(vol, at=_AT, contract="TXFF6-F"):
    return QuoteEvent(snapshot=snap(vol, contract=contract, at=at), error=None, at=at)


def test_poller_publish_annotates_is_fresh_and_advance():
    p = QuotePoller(source=None, symbol="TXF", interval=5.0)
    p.publish(_event(1000, at=_AT))
    assert p.last.is_fresh is True
    assert p.freshness.last_advance_at == _AT

    later = _AT + timedelta(minutes=1)
    p.publish(_event(1000, at=later))  # 停滯
    assert p.last.is_fresh is False
    assert p.freshness.last_advance_at == _AT  # 未推進


def test_poller_publish_ignores_error_events():
    p = QuotePoller(source=None, symbol="TXF", interval=5.0)
    p.publish(QuoteEvent(snapshot=None, error="boom", at=_AT))
    assert p.last is None
    assert p.freshness.last_advance_at is None
```

- [ ] **Step 2: 執行測試，確認失敗**

Run: `uv run pytest tests/test_freshness.py::test_poller_publish_annotates_is_fresh_and_advance -v`
Expected: FAIL（`AttributeError: 'QuotePoller' object has no attribute 'freshness'`）

- [ ] **Step 3: 接線**

在 `src/quanquant/poller.py` 頂部 import 調整（把 `from dataclasses import dataclass` 改為含 `replace`）：

```python
from dataclasses import dataclass, replace
```

新增 import：

```python
from quanquant.freshness import FreshnessTracker
```

在 `__init__` 末尾新增 tracker：

```python
    def __init__(self, source: DataSource, symbol: str, interval: float) -> None:
        self._source = source
        self._symbol = symbol
        self._interval = interval
        self._subscribers: set[asyncio.Queue[QuoteEvent]] = set()
        self._last: FuturesSnapshot | None = None
        self._last_snapshot_at: float | None = None
        self._freshness = FreshnessTracker()
```

新增 property（緊接 `last` property 之後）：

```python
    @property
    def freshness(self) -> FreshnessTracker:
        """共用的新鮮度判斷器（供報價路由讀 last_advance_at）。"""
        return self._freshness
```

改寫 `publish`（在唯一 bus 進入點注入 is_fresh）：

```python
    def publish(self, event: QuoteEvent) -> None:
        """Record + fan out one event. The single entry point for every producer
        (this poller's own loop, the MIS fallback, and the Shioaji streamer)."""
        if event.snapshot is not None:
            snap = self._freshness.evaluate(event.snapshot)
            event = replace(event, snapshot=snap)
            self._last = snap
            self._last_snapshot_at = time.monotonic()
        self._broadcast(event)
```

- [ ] **Step 4: 執行測試，確認通過**

Run: `uv run pytest tests/test_freshness.py -v`
Expected: PASS

- [ ] **Step 5: 跑全測試套件，確認無回歸**

Run: `uv run pytest -q`
Expected: 全綠。若有 poller/整合測試因 snapshot 現在多帶 `is_fresh` 而斷言物件相等失敗，調整該斷言為比對關鍵欄位（price/volume 等）而非整個物件。

- [ ] **Step 6: Commit**

```bash
git add src/quanquant/poller.py tests/test_freshness.py
git commit -m "feat: QuotePoller.publish 接 FreshnessTracker，暴露 freshness"
```

---

## Task 4: CandleBuilder 新鮮度閘門

**Files:**
- Modify: `src/quanquant/candles/builder.py:31-77`（`on_snapshot`）
- Test: `tests/test_candle_builder.py`

**Interfaces:**
- Consumes: `snap.is_fresh`（Task 1/3）
- Produces: 行為——`is_fresh=False` 時 `on_snapshot` 回 `[]` 且**不動** in-progress bar

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_candle_builder.py` 末尾新增（沿用該檔既有 `snap(hh,mm,ss,price,cum_vol,...)` helper，並用 `dataclasses.replace` 造 not-fresh 快照）：

```python
from dataclasses import replace


def test_stale_snapshot_emits_nothing_and_preserves_bar():
    b = CandleBuilder("TXF")
    b.on_snapshot(snap(9, 0, 0, "18000", 1000))          # fresh（預設 is_fresh=True）
    stale = replace(snap(9, 0, 20, "17990", 1000), is_fresh=False)
    rows = b.on_snapshot(stale)
    assert rows == []                                     # 停滯 → 不出棒
    # in-progress bar 仍在：下一筆 fresh 同分鐘應合併回同一根
    rows2 = b.on_snapshot(snap(9, 0, 40, "18050", 1010))
    assert len(rows2) == 1
    assert rows2[0].ts == ms(9, 0)
    assert rows2[0].close == Decimal("18050")
    assert rows2[0].volume == 0 + 10                      # 只累加 fresh 的量差


def test_stale_snapshot_in_night_session_also_gated():
    b = CandleBuilder("TXF")
    b.on_snapshot(snap(16, 0, 0, "18000", 5000))         # 夜盤 fresh
    stale = replace(snap(16, 0, 20, "18000", 5000), is_fresh=False)
    assert b.on_snapshot(stale) == []                    # 夜盤停滯也被擋
```

- [ ] **Step 2: 執行測試，確認失敗**

Run: `uv run pytest tests/test_candle_builder.py::test_stale_snapshot_emits_nothing_and_preserves_bar -v`
Expected: FAIL（目前 not-fresh 仍會更新/新增 bar）

- [ ] **Step 3: 加入閘門**

在 `src/quanquant/candles/builder.py` `on_snapshot` 中，於現有「stale day-quote net」之後、`bucket = ...` 之前插入新鮮度閘門（保留既有兩道 gate，見微調 1）：

```python
        # Staleness net (day session only): ...（既有段落，保留不動）
        if session == "day" and self._is_stale_day_quote(snap.data_date, ts_ms):
            self._prev_cum_vol = None
            self._cur = None
            return []

        # 資料級新鮮度閘門（日/夜盤皆適用）：非新成交 → 凍結 in-progress bar，不出棒。
        # 不重置 _cur / _prev_cum_vol，讓下一筆 fresh 能接續同一根並正確累加量差。
        if not snap.is_fresh:
            return []

        bucket = bucket_start_ms(ts_ms, "1m")
```

- [ ] **Step 4: 執行測試，確認通過**

Run: `uv run pytest tests/test_candle_builder.py -v`
Expected: PASS（含既有測試不回歸——既有 snapshot 預設 `is_fresh=True`）

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/candles/builder.py tests/test_candle_builder.py
git commit -m "feat: CandleBuilder 停滯（非 fresh）不出棒，補夜盤缺口"
```

---

## Task 5: config + market_calendar 假日 env 覆寫

**Files:**
- Modify: `src/quanquant/config.py`（`Settings` 新欄位）
- Modify: `src/quanquant/candles/market_calendar.py`（imports、`_parse_holidays`、`is_trading_day`）
- Test: `tests/test_market_calendar.py`

**Interfaces:**
- Produces: `Settings.extra_holidays: str`（env `EXTRA_HOLIDAYS`）
- Produces: `market_calendar._parse_holidays(raw: str) -> frozenset[date]`
- 行為：`is_trading_day` 併入 env 假日

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_market_calendar.py` 末尾新增（`get_settings` 與 `_parse_holidays` 皆 lru_cache，測試前後清快取）：

```python
from quanquant.candles import market_calendar
from quanquant.config import get_settings


def test_extra_holidays_env_makes_weekday_non_trading(monkeypatch):
    monkeypatch.setenv("EXTRA_HOLIDAYS", "2026-07-23")  # 週四，颱風臨時休市
    get_settings.cache_clear()
    market_calendar._parse_holidays.cache_clear()
    try:
        assert is_trading_day(date(2026, 7, 23)) is False
        assert is_trading_session(ms(2026, 7, 23, 10, 0)) is None
    finally:
        monkeypatch.delenv("EXTRA_HOLIDAYS", raising=False)
        get_settings.cache_clear()
        market_calendar._parse_holidays.cache_clear()


def test_parse_holidays_skips_malformed():
    assert market_calendar._parse_holidays("2026-07-23, , not-a-date,2026-08-01") == frozenset(
        {date(2026, 7, 23), date(2026, 8, 1)}
    )
```

- [ ] **Step 2: 執行測試，確認失敗**

Run: `uv run pytest tests/test_market_calendar.py::test_parse_holidays_skips_malformed -v`
Expected: FAIL（`AttributeError: module ... has no attribute '_parse_holidays'`）

- [ ] **Step 3: config 加欄位**

在 `src/quanquant/config.py` `Settings` 內（`# Web / dashboard` 區塊之前）新增：

```python
    # 臨時休市（颱風）覆寫：逗號分隔 ISO 日期（YYYY-MM-DD），與內建假日清單 union。
    # 用 env（非資料檔）以避開 wheel/Docker force-include 限制；VM 設 env 重啟即生效。
    extra_holidays: str = ""
```

- [ ] **Step 4: market_calendar 併入 env 假日**

在 `src/quanquant/candles/market_calendar.py` 頂部 import 補上：

```python
from functools import lru_cache

from quanquant.config import get_settings
```

新增 helper 並改寫 `is_trading_day`：

```python
@lru_cache(maxsize=8)
def _parse_holidays(raw: str) -> frozenset[date]:
    """解析 EXTRA_HOLIDAYS（逗號分隔 ISO 日期）；壞值略過（防禦式）。"""
    out: set[date] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(date.fromisoformat(part))
        except ValueError:
            continue
    return frozenset(out)


def is_trading_day(d: date) -> bool:
    """Mon–Fri 且不在 TAIFEX 假日（內建清單 ∪ EXTRA_HOLIDAYS env）。"""
    if d.weekday() >= 5:
        return False
    if d in _HOLIDAYS:
        return False
    return d not in _parse_holidays(get_settings().extra_holidays)
```

（移除舊的單行 `is_trading_day`；其餘 `session_anchor_date` / `is_trading_session` 不動。）

- [ ] **Step 5: 執行測試，確認通過**

Run: `uv run pytest tests/test_market_calendar.py -v`
Expected: PASS（含既有 calendar 測試不回歸）

- [ ] **Step 6: Commit**

```bash
git add src/quanquant/config.py src/quanquant/candles/market_calendar.py tests/test_market_calendar.py
git commit -m "feat: EXTRA_HOLIDAYS env 覆寫臨時休市，併入交易日判斷"
```

---

## Task 6: market_calendar session_now + resolve_market_status

**Files:**
- Modify: `src/quanquant/candles/market_calendar.py`（新增兩函式 + 常數）
- Test: `tests/test_market_calendar.py`

**Interfaces:**
- Produces: `session_now(now: datetime) -> str | None`（"day"/"night"/None）
- Produces: `resolve_market_status(now: datetime, last_advance_at: datetime | None) -> str`（"open"/"closed"/"suspected_halt"）

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_market_calendar.py` 末尾新增：

```python
from datetime import timedelta

from quanquant.candles.market_calendar import resolve_market_status, session_now


def test_session_now_normal_and_holiday():
    assert session_now(datetime(2026, 6, 16, 10, 0, tzinfo=CST)) == "day"    # Tue 日盤
    assert session_now(datetime(2026, 6, 16, 16, 0, tzinfo=CST)) == "night"  # 夜盤
    assert session_now(datetime(2026, 6, 19, 10, 0, tzinfo=CST)) is None     # 端午假日


def test_resolve_market_status_states():
    day = datetime(2026, 6, 16, 10, 0, tzinfo=CST)          # 日盤中
    assert resolve_market_status(day, day - timedelta(seconds=10)) == "open"
    assert resolve_market_status(day, day - timedelta(minutes=6)) == "suspected_halt"
    assert resolve_market_status(day, None) == "open"       # 冷啟動不誤報
    gap = datetime(2026, 6, 16, 14, 0, tzinfo=CST)          # 盤間 14:00
    assert resolve_market_status(gap, gap - timedelta(seconds=10)) == "closed"
```

- [ ] **Step 2: 執行測試，確認失敗**

Run: `uv run pytest tests/test_market_calendar.py::test_resolve_market_status_states -v`
Expected: FAIL（`ImportError: cannot import name 'resolve_market_status'`）

- [ ] **Step 3: 實作兩函式**

在 `src/quanquant/candles/market_calendar.py` 末尾新增：

```python
_HALT_THRESHOLD_SECONDS = 300.0  # 該開盤但無新成交達 5 分鐘 → 疑似臨時休市


def session_now(now: datetime) -> str | None:
    """以牆鐘瞬間 `now`（tz-aware）判定交易時段（calendar-aware）。"""
    return is_trading_session(int(now.timestamp() * 1000))


def resolve_market_status(now: datetime, last_advance_at: datetime | None) -> str:
    """市場狀態：

    closed         — 非交易時段（週末/假日/盤間）
    suspected_halt — 該開盤但無新成交 >= 5 分鐘
    open           — 正常交易
    """
    if session_now(now) is None:
        return "closed"
    if last_advance_at is None:
        return "open"  # 尚無快照 — 冷啟動不誤報 halt
    if (now - last_advance_at).total_seconds() >= _HALT_THRESHOLD_SECONDS:
        return "suspected_halt"
    return "open"
```

- [ ] **Step 4: 執行測試，確認通過**

Run: `uv run pytest tests/test_market_calendar.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/candles/market_calendar.py tests/test_market_calendar.py
git commit -m "feat: market_calendar 新增 session_now/resolve_market_status"
```

---

## Task 7: 報價路由與 partial — 統一 session + market_status/fresh

**Files:**
- Modify: `src/quanquant/web/routers/dashboard.py`（imports、`_quote_context`、四處呼叫）
- Modify: `src/quanquant/web/templates/partials/quote.html`
- Test: `tests/test_quote_status.py`（Create）

**Interfaces:**
- Consumes: `session_now`、`resolve_market_status`（Task 6）、`poller.freshness.last_advance_at`（Task 3）
- Produces: context 新增 `market_status`；partial 根 div 帶 `data-qq-market-status`/`data-qq-fresh` + banner

- [ ] **Step 1: 寫失敗測試**

建立 `tests/test_quote_status.py`：

```python
from datetime import datetime, timezone
from decimal import Decimal

from quanquant.models import FuturesSnapshot
from quanquant.web.routers.dashboard import _quote_context
from quanquant.web.templating import render_partial


def _snap(is_fresh=True):
    p = Decimal("18000")
    return FuturesSnapshot(
        symbol="TXF", price=p, change=Decimal(0), change_pct=0.0, volume=1000,
        open_price=p, high_price=p, low_price=p,
        fetched_at=datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc),
        data_date="2026-06-16", contract_month="TXFF6-F", is_fresh=is_fresh,
    )


def test_quote_context_has_market_status():
    ctx = _quote_context(_snap(), None, poller=None)
    assert "market_status" in ctx
    assert ctx["market_status"] in {"open", "closed", "suspected_halt"}
    assert ctx["session"] in {"day", "night", "closed"}


def test_quote_partial_renders_status_attrs():
    ctx = _quote_context(_snap(is_fresh=True), None, poller=None)
    html = render_partial("partials/quote.html", **ctx)
    assert "data-qq-market-status" in html
    assert "data-qq-fresh" in html
```

- [ ] **Step 2: 執行測試，確認失敗**

Run: `uv run pytest tests/test_quote_status.py -v`
Expected: FAIL（`_quote_context` 尚無 `poller` 參數 / context 無 `market_status`）

- [ ] **Step 3: 改 `_quote_context` 與 imports**

在 `src/quanquant/web/routers/dashboard.py`：把 `from quanquant.market_hours import get_session`（約 L14）改為：

```python
from quanquant.candles.market_calendar import resolve_market_status, session_now
```

改寫 `_quote_context`（新增 `poller` 參數；session 改 calendar-aware；加 `market_status`）：

```python
def _quote_context(
    snap: FuturesSnapshot | None,
    error: str | None,
    as_of: datetime | None = None,
    pulse: PulseEngine | None = None,
    poller: QuotePoller | None = None,
) -> dict:
    now = datetime.now(timezone.utc)
    sess = session_now(now)           # "day"/"night"/None（calendar-aware，含假日）
    session = sess or "closed"
    last_adv = poller.freshness.last_advance_at if poller is not None else None
    market_status = resolve_market_status(now, last_adv)
    pulse_level, pulse_state = (0, "silent")
    if pulse is not None:
        pulse_level, pulse_state = pulse.level_at()
    return {
        "snap": snap,
        "error": error,
        "session": session,
        "session_label": SESSION_LABEL.get(session, SESSION_LABEL["closed"]),
        "market_status": market_status,
        "as_of": as_of,
        "pulse_level": pulse_level,
        "pulse_state": pulse_state,
    }
```

四處呼叫 `_quote_context(...)` 補傳 `poller=poller`：
- `/quote` route（`quote_now`）：`_quote_context(snap, None, pulse=pulse, poller=poller)`
- SSE 初始 emit：`_quote_context(poller.last, None, pulse=pulse, poller=poller)`
- SSE 迴圈 emit：`_quote_context(snap, error, as_of, pulse=pulse, poller=poller)`

（確認 `datetime, timezone` 已於檔頂 import；SSE heartbeat 已用 `datetime.now(timezone.utc)`，故已 import。`QuotePoller` 型別已於 Depends 使用，亦已 import。）

- [ ] **Step 4: 改 quote.html partial**

在 `src/quanquant/web/templates/partials/quote.html` 的根 `.quote` div 補兩個屬性，並在 div 內最上方加 banner：

```html
{% if snap %}
<div class="quote {{ 'up' if snap.change >= 0 else 'down' }}"
     data-qq-price="{{ snap.price }}" data-qq-session="{{ session }}"
     data-qq-market-status="{{ market_status }}"
     data-qq-fresh="{{ 'true' if snap.is_fresh else 'false' }}"
     data-qq-pulse-level="{{ pulse_level }}" data-qq-pulse-state="{{ pulse_state }}">
  {% if market_status == 'suspected_halt' %}
  <div class="market-banner halt">疑似臨時休市／資料停滯，圖表已凍結</div>
  {% elif market_status == 'closed' %}
  <div class="market-banner closed">非交易時段</div>
  {% endif %}
  <div class="quote-main">
```

（其餘 partial 內容不動——`quote-main`、`quote-fields`、`quote-side`、`{% elif error %}`、`{% else %}` 皆保留。）

- [ ] **Step 5: 執行測試，確認通過**

Run: `uv run pytest tests/test_quote_status.py -v`
Expected: PASS

- [ ] **Step 6: 跑全套件確認報價路由不回歸**

Run: `uv run pytest -q`
Expected: 全綠。若既有 dashboard/quote 路由測試斷言舊的時鐘式 session 標籤，更新為 calendar-aware 預期值。

- [ ] **Step 7: Commit**

```bash
git add src/quanquant/web/routers/dashboard.py src/quanquant/web/templates/partials/quote.html tests/test_quote_status.py
git commit -m "feat: 報價面板統一 calendar-aware session + market_status/fresh 屬性與 banner"
```

---

## Task 8: chart.js `_applyBar` 資料防呆（NaN／單調遞增）

**Files:**
- Modify: `src/quanquant/web/static/chart.js:232-242`（`_applyBar`）
- 驗證：無 JS 測試框架 → 程式審查 + 手動瀏覽器驗證

**Interfaces:**
- Consumes: KLineCharts `chart.updateData`
- Produces: `_applyBar` 對無效／亂序 bar 直接丟棄

- [ ] **Step 1: 改寫 `_applyBar` 加守衛**

把 `src/quanquant/web/static/chart.js` 的 `_applyBar` 改為：

```javascript
    _applyBar(bar) {
      // 資料防呆：OHLC + timestamp 必須有限；亂序（早於已見最後一根）直接丟棄，
      // 避免休市/來源異常造成的 NaN 破圖與「K 線亂跳」。
      const finite = (v) => Number.isFinite(v);
      if (!bar || !finite(bar.timestamp) ||
          !finite(bar.open) || !finite(bar.high) ||
          !finite(bar.low) || !finite(bar.close)) return;
      if (this.lastBarTs && bar.timestamp < this.lastBarTs) return; // 亂序 → 丟棄

      // keep chart + tfCache consistent for one updated/appended bar
      this.chart.updateData(bar);
      const cached = this.tfCache.get(this._cacheKey());
      if (cached && cached.bars.length) {
        const last = cached.bars[cached.bars.length - 1];
        if (last.timestamp === bar.timestamp) cached.bars[cached.bars.length - 1] = bar;
        else if (bar.timestamp > last.timestamp) cached.bars.push(bar);
      }
      if (bar.timestamp > this.lastBarTs) this.lastBarTs = bar.timestamp;
    },
```

- [ ] **Step 2: 語法檢查**

Run: `node --check src/quanquant/web/static/chart.js`
Expected: 無輸出（語法正確）

- [ ] **Step 3: 手動驗證（記錄於 PR）**

啟動 `uv run quanquant-web` → 開圖表：
- 正常盤：K 棒照常推進、無視覺回歸。
- 亂序測試：於程式暫時注入一個 `bar.timestamp` 小於當前最後一根者 → 確認被丟棄、K 棒不倒退亂跳。
- NaN 測試：注入 OHLC 含 NaN 的 bar → 確認不丟例外、圖不破。

- [ ] **Step 4: Commit**

```bash
git add src/quanquant/web/static/chart.js
git commit -m "fix: chart _applyBar 加 NaN/單調遞增守衛，防 K 線亂跳"
```

---

## Task 9: chart.js `onQuote`/`_bindQuoteSync` — 休市/停滯不覆寫真棒

**Files:**
- Modify: `src/quanquant/web/static/chart.js`（`_bindQuoteSync` L315-336、`onQuote` L338-358）
- 驗證：程式審查 + 手動瀏覽器驗證

**Interfaces:**
- Consumes: partial 的 `data-qq-market-status`/`data-qq-fresh`（Task 7）
- Produces: `onQuote(price, quoteSession, meta)`，`meta={status, fresh}`；非 open 或非 fresh 時不覆寫最後一根

- [ ] **Step 1: 改 `_bindQuoteSync` 讀新屬性**

把 `sync()` 改為多讀 market-status / fresh 並傳入：

```javascript
      const sync = () => {
        const node = el.querySelector("[data-qq-price]");
        if (!node) return;
        this.onQuote(parseFloat(node.dataset.qqPrice), node.dataset.qqSession, {
          status: node.dataset.qqMarketStatus,
          fresh: node.dataset.qqFresh === "true",
        });
      };
```

- [ ] **Step 2: 改 `onQuote` 加閘門**

在 `onQuote` 開頭（`if (this.session !== "all") return;` 之前）加入 market_status/fresh 守衛，簽名多收 `meta`：

```javascript
    onQuote(price, quoteSession, meta) {
      if (!Number.isFinite(price)) return;
      // 休市／停滯（非 open 或非 fresh）時，不可用凍結報價覆寫最後一根真棒。
      if (meta && (meta.fresh === false || (meta.status && meta.status !== "open"))) return;
      // A day/night-filtered chart intentionally shows that session's last bar; a
      // live quote from the other session must not overwrite it. Only sync when
      // the chart shows the combined series (matches the quote's contract).
      if (this.session !== "all") return;
      if (!this.chart) return;
      const list = this.chart.getDataList();
      if (!list || !list.length) return;
      const last = list[list.length - 1];
      if (price === last.close) return;
      this._applyBar({
        timestamp: last.timestamp,
        open: last.open,
        high: Math.max(last.high, price),
        low: Math.min(last.low, price),
        close: price,
        volume: last.volume,
      });
      this.refreshDeduction();
    },
```

- [ ] **Step 3: 語法檢查**

Run: `node --check src/quanquant/web/static/chart.js`
Expected: 無輸出

- [ ] **Step 4: 手動驗證（記錄於 PR）**

啟動 `uv run quanquant-web`：
- 正常盤（`market_status=open` 且 `fresh=true`）：大報價變動時最後一根 close 仍即時同步（行為不變）。
- 模擬停滯：暫時把 partial 的 `data-qq-fresh` 設為 `false`（或後端造停滯）→ 確認最後一根真棒的 close/high/low **不再被凍結價覆寫**。
- 休市（`suspected_halt`/`closed`）→ 確認圖表凍結、最後一根不被改寫。

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/web/static/chart.js
git commit -m "fix: onQuote 休市/停滯不覆寫最後一根真棒"
```

---

## Task 10: banner 樣式 + 全套件回歸

**Files:**
- Modify: `src/quanquant/web/static/app.css`（`.market-banner` 樣式）
- 驗證：手動 + 全 pytest

**Interfaces:**
- Consumes: Task 7 的 `.market-banner.halt` / `.market-banner.closed`

- [ ] **Step 1: 加樣式**

在 `src/quanquant/web/static/app.css` 末尾新增（沿用專案既有色票變數；若無對應 token 則用下列 fallback，並在 PR 註明可換為 token）：

```css
.market-banner {
  font-size: 0.78rem;
  padding: 2px 8px;
  border-radius: 6px;
  margin-bottom: 6px;
  text-align: center;
}
.market-banner.halt {
  background: rgba(220, 160, 40, 0.18);
  color: #c8860a;
}
.market-banner.closed {
  background: rgba(120, 120, 120, 0.16);
  color: #888;
}
```

- [ ] **Step 2: 手動驗證外觀（記錄於 PR）**

啟動 `uv run quanquant-web`，於非交易時段（或造停滯）確認 banner 出現、深淺模式皆可讀、不破版。

- [ ] **Step 3: 全套件回歸**

Run: `uv run pytest -q`
Expected: 全綠（79+）。

- [ ] **Step 4: Commit**

```bash
git add src/quanquant/web/static/app.css
git commit -m "feat: 休市/停滯狀態 banner 樣式"
```

---

## 完成後

- 全 `uv run pytest` 綠、前端手動驗證清單全過後，用 `superpowers:finishing-a-development-branch` 決定合併 `feat/market-halt-stale-guard` → main 的方式。
- 部署照 `CLAUDE.md`：`git push` →（合 main 後）`./scripts/deploy.sh`。臨時颱風日在 VM `.env` 設 `EXTRA_HOLIDAYS=YYYY-MM-DD` 並重啟。
