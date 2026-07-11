# 休市與資料停滯防呆（Market Halt / Stale-Data Guard）— 設計

- **日期**：2026-07-11
- **狀態**：設計已核准，待寫實作計畫
- **範圍決策**：分層聯集偵測 · 5 分鐘臨時休市門檻 · 一次做全套

---

## 1. 問題陳述

台指期（TXF）在平常工作日若因天災、臨時停市等原因**不開盤**，但 TAIFEX MIS 來源仍持續回傳（多為凍結的舊快照或結算/參考價），造成三個症狀：

| 症狀 | 根因 |
|---|---|
| 休市時網站仍更新 | 開盤判斷有**兩套且不一致**：K 棒走「有假日日曆」的 `candles/market_calendar.py`；但報價面板（`dashboard.py:30`）與合約選擇（`sources/taifex.py:62`）走「只看時鐘、無假日」的 `market_hours.py` → 颱風日平日白天仍判為「日盤開盤中」。另 `_select_nearest_contract`（`taifex.py:80-83`）的 `require_price=False` fallback 會拿**結算價**硬湊出一筆「像成交」的快照，讓面板持續更新。 |
| K 線亂跳、顯示錯誤 | 前端 `_applyBar`（`chart.js:232-242`）對後端 bar **全盤信任**，無 NaN／重複／時間戳亂序防呆；SSE 報價（`onQuote`, `chart.js:323-358`）會把休市的凍結價**覆寫到最後一根真棒**的 close/high/low。 |
| 均線／指標被影響 | 指標直接吃 `chart.getDataList()` 的 K 棒，只要混進一根 phantom／凍結棒，MA/WR/BIAS/MACD 全被拖歪。後端**夜盤完全沒有停滯防護**（現有停滯網只涵蓋日盤，且需 MIS 回夠舊的 `CDate` 才擋得住）。 |

### 最深層根因（單一點）

`sources/taifex.py:105` 把每次輪詢的**當下牆鐘時間**當成成交時間（`fetched_at=now`），並**完全未讀** MIS 回傳的成交時間 `CTime`。系統因此**無法從資料本身分辨「新成交」與「休市重播」**，所有下游只能靠一份**硬寫死的假日清單**（`market_calendar._HOLIDAYS`，2024–2027）兜底，臨時颱風沒手動加入就全破。

---

## 2. 目標與非目標

### 目標
1. 休市（含已知假日與臨時颱風）時，K 棒不再產生 phantom／凍結棒，指標/均線不被汙染。
2. 前端不再因 NaN／亂序／重複資料而「K 線亂跳」，休市凍結價不覆寫最後一根真棒。
3. 使用者能一眼分辨「非交易時段 / 疑似臨時休市」，而非誤以為系統壞掉。
4. 臨時颱風休市無需改 code／重 build 即可反映到歷史查詢。

### 非目標（YAGNI／避險）
- **不**改 K 棒 bucket 時間戳為成交時間（動最敏感的 bucketing 邏輯，另案處理）。
- **不**自動抓取外部假日 API（維持無外部相依）。
- **不**動 KLineCharts 版本與 `chart.js` 四道渲染防線。
- **不**改 candle 讀取路徑的 raw-SQL→float / sync `def` 架構。

---

## 3. 設計決策（已與使用者敲定）

| 決策 | 選擇 | 理由 |
|---|---|---|
| 休市/停滯偵測策略 | **分層聯集**：行事曆層 + 資料新鮮度層獨立把關，任一判休市即凍結 | 直接對應「來源不能全信」，多訊號互相 fallback |
| 臨時休市判定門檻 | **5 分鐘** 無新成交 | 夜盤深夜低流動可數分鐘無成交，5 分鐘較不誤報 |
| 修正範圍 | **全套**（六個工作項全做） | 一次到位 |

---

## 4. 架構總覽

核心：**一份「新鮮度」判斷，集中算一次、所有下游共用**，再由三層防線各自把關。

```
上游來源 ──► FreshnessTracker（新增，有狀態）──► QuotePoller.publish ──► 訂閱者（吃同一份 is_fresh）
  taifex (MIS, 5s, 新讀 CTime)   算 is_fresh / trade_time      ├─ CandleBuilder（②不 fresh 不出棒）
  shioaji_stream                 追蹤「上次量前進時刻」          ├─ /quote SSE → quote.html（③④⑤帶狀態屬性）
                                                                └─ alert / pulse engine
                             market_calendar（①唯一時段真實來源，含假日 + ⑥ env 覆寫）
```

三層防線：
- **時段層（①⑥）**：`market_calendar` 判定「本來該不該有盤」（含假日與臨時颱風 env 覆寫）。
- **資料層（②③）**：`FreshnessTracker` 判定「這筆是不是真的新成交」，補抓行事曆沒擋到的臨時休市，並涵蓋現有夜盤缺口。
- **前端層（④⑤）**：拿到「非 fresh / 非交易時段」訊號時凍結圖表、不覆寫真棒、顯示狀態。

---

## 5. 核心元件：集中式新鮮度判斷

### 5.1 資料模型變更

`models.py` `FuturesSnapshot` 新增兩欄位：
- `trade_time: datetime | None` — 解析自 MIS `CTime`（最後成交時間），無法解析時為 `None`。
- `is_fresh: bool` — 本筆是否為新成交（由 `FreshnessTracker` 填入，預設保守處理見下）。

### 5.2 `sources/taifex.py` 讀取 CTime

- `_parse_row` 新讀 `CTime`（格式須實測，常見為 `"HH:MM:SS"` 或 `"HHMMSS"`），以 `CDate` + `CTime` 合成 CST datetime，防禦式解析：格式異常、空值 → `trade_time=None`，不拋錯。
- 保留既有 `fetched_at=now`（bucketing 仍用它，見非目標）。

### 5.3 `FreshnessTracker`（新增，掛在「快照發布進 bus」之前）

置於快照發布進 pub/sub bus 的共同邊界，**poller（MIS 輪詢）與 shioaji_stream（推播）兩條發布路徑共用同一個 tracker 實例**，確保所有下游吃到一致的 `is_fresh`。單例、有狀態，state：
- `_last_cum_vol: float | None`
- `_last_contract_key: str | None`（合約身分，與 builder 共用同一 `contract_identity()` 判斷，見 5.4）
- `_last_trade_time: datetime | None`
- `_last_advance_ts: datetime | None`（上次「量或成交時間前進」的牆鐘時刻，供 5 分鐘門檻）

每筆快照演算法：
1. 算合約身分 `key = contract_identity(snap)`。
2. **重置條件**：`key != _last_contract_key` 或 `cum_vol < _last_cum_vol`（跨盤重數）→ 重置基準，`is_fresh = (cum_vol > 0)`，更新 `_last_advance_ts = fetched_at`。
3. **一般情況**（同身分、`cum_vol >= _last_cum_vol`）：
   - `volume_advanced = cum_vol > _last_cum_vol`
   - `time_advanced = trade_time is not None and _last_trade_time is not None and trade_time > _last_trade_time`
   - **`is_fresh = volume_advanced or time_advanced`**（分層聯集：量或成交時間任一前進即算新成交，對付「來源某一路訊號凍結」）
   - 若 `is_fresh` → 更新 `_last_advance_ts = fetched_at`
4. 更新 `_last_cum_vol` / `_last_trade_time` / `_last_contract_key`。

> 為何 OR 而非 AND：真實成交必然使量前進，正常時兩訊號同步；OR 只在「某一路訊號被來源凍結」時互補救援。休市重播時**兩者皆凍結** → `is_fresh=False`，仍被正確攔下。

### 5.4 共用合約身分 `contract_identity(snap)`

抽出 builder `_volume_delta`（`builder.py:88-103`）現有的身分判斷為共用函式，讓 `FreshnessTracker` 與 `CandleBuilder` 用**同一套身分定義**，避免分歧（例如 Shioaji `TXFG6` 與 MIS `TXFG6-M/-F` 視為同一根的既有規則）。

---

## 6. 六個工作項

### ① 時段層 — 統一開盤判斷
- `market_calendar` 定為**唯一時段真實來源**。
- 報價面板（`dashboard.py:30`）與合約選擇（`taifex.py:62`）改為委派 `market_calendar`；`market_hours.py` 保留對外簽名不變、內部補假日判斷（薄包裝），呼叫端最小改動。
- 新增便利函式（例 `market_calendar.session_now(now) -> "day"|"night"|None`）供「以現在時刻」查詢，內部轉 epoch-ms 走既有 `is_trading_session`。

### ② 資料層 — 新鮮度閘門
- `CandleBuilder.on_snapshot`：收到 `is_fresh=False` → **不建新棒、不推進、不覆寫**，直接 return。取代現有殘缺的「日盤限定停滯網」（`builder.py:43-46,79-86`），夜盤一併涵蓋。
- 正常低量安靜時刻也 `is_fresh=False` → 不動 K 棒，本即正確（沒成交本該平盤），不誤傷。
- 保留 `_volume_delta` 供 fresh 時累加實際量。

### ③ 來源層 — 改造 `require_price=False` fallback
- `_select_nearest_contract` 抓不到有成交價合約時，不再讓結算價偽裝成成交：該筆快照 `is_fresh=False`（結算價無量前進，天然被②過濾），報價面板據此不顯示為「即時」。
- 仍照常輪詢發布（符合 requirements「盤中不可停更新」），但不汙染資料。

### ④ 前端層 — 資料防呆 + 不覆寫真棒
- `chart.js` `_applyBar`：守衛——OHLCV 全部 `Number.isFinite`；時間戳須 `>=` 最後一根（等於則 in-place 更新，較舊則丟棄）。擋 NaN／亂序／重複造成的亂跳。
- `chart.js` `onQuote`：僅在**交易時段為真且 `is_fresh`** 時才把報價貼到最後一根 bar；休市/停滯時不覆寫真棒。
- 狀態經報價 SSE partial（`quote.html`）帶 `data-qq-session` / `data-qq-market-status` / `data-qq-fresh` 屬性下傳，前端複用既有讀 `data-qq-price` 的管線。

### ⑤ 狀態 UI — 休市／停滯 banner
- 後端計算 `market_status`：
  - `is_trading_session(now) is None` → **`closed`**（非交易時段）
  - 該開盤但 `now - _last_advance_ts >= 5min` → **`suspected_halt`**（疑似臨時休市／資料停滯）
  - 否則 → **`open`**（正常，不顯示 banner）
- 提供 helper `resolve_market_status(now, tracker)`（置於 `market_calendar` 或新 status 模組）。
- 前端狀態列文案（預設，可於審查調整）：
  - `closed` → 「非交易時段」
  - `suspected_halt` → 「疑似臨時休市／資料停滯，圖表已凍結」
  - `open` → 不顯示

### ⑥ 行事曆熱更新 — 臨時颱風免改 code
- `market_calendar` 讀環境變數 `QUANQUANT_EXTRA_HOLIDAYS`，格式 `YYYY-MM-DD` 逗號分隔（例 `"2026-07-23,2026-08-01"`），與硬寫死 `_HOLIDAYS` 取 union。
- 刻意用 env（非資料檔）→ 避開 CLAUDE.md 的 wheel/Docker force-include 雷；VM 設 env 重啟即生效。
- 定位：②的資料層當天即時凍結臨時颱風；此項補強**歷史 K 棒查詢**（歷史無即時量訊號，只能靠日曆排除該日），並讓 `clean-nontrading` 清理指令能涵蓋臨時颱風日。

---

## 7. 前端後端資料契約

報價 SSE partial（`quote.html`）新增屬性（前端讀取）：

| 屬性 | 值 | 用途 |
|---|---|---|
| `data-qq-session` | `day` / `night` / `closed` | 統一時段（來自 `market_calendar`） |
| `data-qq-market-status` | `open` / `closed` / `suspected_halt` | banner 顯示 |
| `data-qq-fresh` | `true` / `false` | `onQuote` 是否覆寫最後一根 bar |

---

## 8. 測試策略

### 後端（pytest，部署前必須全綠）
- `FreshnessTracker`：量前進→fresh、量凍結→not fresh、CTime 前進但量凍結→fresh（OR 救援）、跨盤 cum_vol 下降→重置、合約身分改變→重置、首筆（`cum_vol>0`→fresh / `=0`→not fresh）。
- `taifex` CTime 防禦式解析：正常格式、空值、異常格式→`trade_time=None` 不拋錯。
- 統一時段：假日/颱風日（含 `QUANQUANT_EXTRA_HOLIDAYS`）→ `session_now` 回 `None`／`market_status=closed`。
- `CandleBuilder`：`is_fresh=False` → 不出棒、不推進、不覆寫；夜盤停滯亦被攔。
- `require_price` fallback → 快照 `is_fresh=False`。
- `resolve_market_status`：三態邊界（含 5 分鐘門檻）。
- `QUANQUANT_EXTRA_HOLIDAYS` 與 `_HOLIDAYS` union、格式解析容錯。

### 前端（vanilla JS，無測試框架）
- `_applyBar`／`onQuote` 守衛以程式審查 + 手動驗證為主。手動情境：注入 NaN bar、亂序時間戳、重複 bar、休市凍結報價，確認圖表不亂跳、最後真棒不被覆寫、banner 正確顯示。實作計畫附逐步驗證清單。

---

## 9. 明確不做（重申）
- 不改 bucket 時間戳為成交時間（另案）。
- 不接外部假日 API。
- 不動 KLineCharts 版本與四道渲染防線。
- 不改 candle 讀取路徑 raw-SQL→float／sync `def`。

---

## 10. 預設待確認項（可於 spec 審查調整）
- ⑥ env 格式採 `YYYY-MM-DD` 逗號分隔（本 spec 預設）。
- ⑤ banner 文案採上述預設用字。
- MIS `CTime` 實際格式須於實作首步以真實回應確認（影響 5.2 解析）。
