# QuanQuant 系統需求規格書

> 版本：v1.0（2026-06-11）
> 本文件是《[AI Stock Workflow](./AI%20Stock%20Workflow.md)》的工程化細則：把該文件中抽象的需求轉成可實作、可驗收的精確規格。原文件保持不動，作為產品願景來源；兩者衝突時以本文件為準。

---

## 1. 目的與範圍

QuanQuant 是單一使用者的台指期交易工作流系統，依序提供：

| 階段 | 模組 | 狀態 |
|---|---|---|
| 1 | 即時報價擷取（TAIFEX MIS） | ✅ 已完成 |
| 2 | Web 儀表板 + 交易日記 + 績效統計 | ✅ 已完成 |
| 3 | 多週期 K 線管線 + 歷史回補 + 圖表（指標/繪圖/週期切換） | 🔨 本輪 |
| 4 | 技術指標警示 + Telegram 通知 | 規劃中 |
| 5 | 交易日曆與提醒 | 規劃中 |
| 6 | 新聞爬蟲 + LLM 摘要 | 規劃中 |
| 7 | 標的篩選器 | 規劃中 |
| 8 | No-Code 條件編輯器 | 規劃中 |
| 9 | 券商 API（Shioaji）/ 自動化 / 回測 | 規劃中 |

---

## 2. 名詞定義

| 名詞 | 定義 |
|---|---|
| 日盤 | TAIFEX 一般交易時段 08:45–13:45（台北時間，CST/UTC+8） |
| 夜盤 | 盤後交易時段 15:00–次日 05:00 |
| 交易日（trading date） | 日盤所在的日曆日期（`YYYY-MM-DD` CST） |
| 近月合約（front month） | 尚未到期的最近交割月份合約；**含結算日當天**仍視為近月，次一交易日起轉倉（roll）至下一月份 |
| 結算日 | 交割月份的第三個星期三 |
| 連續月序列 | 逐日取近月合約拼接成的連續價格序列；轉倉日的跳價**不做平滑調整** |
| K 棒桶（bucket） | 一根 K 棒涵蓋的時間區間；桶起點為該 K 棒的時間戳 |
| 正準週期（canonical TF） | 實際存進資料庫的週期：`1m` 與 `1d`；其餘週期皆即時衍生 |

---

## 3. 即時資料擷取

- 來源：TAIFEX MIS API（`POST mis.taifex.com.tw/futures/api/getQuoteList`），免費、無認證。
- 頻率：每 5 秒（`POLL_INTERVAL_SECONDS` 可調）；單次抓取必須 < 5 秒（timeout 10s）。
- **盤中不可中斷**：抓取失敗只記錯誤事件並繼續輪詢，絕不停止迴圈。
- 全程式只有一個輪詢迴圈（`QuotePoller`），CLI / Web / 持久化任務皆為其訂閱者。
- 時間戳規範：抓取時間以 UTC 記錄；K 棒時間戳一律為 **epoch 毫秒 UTC（整數）**；台北時間只在「分桶計算」與「前端顯示」兩處出現。
- 報價欄位：成交價、漲跌、漲跌幅、**累計成交量**、開高低、資料日期、近月合約代碼。

## 4. K 線資料規格

### 4.1 正準儲存

| 週期 | 來源 | 建構方式 |
|---|---|---|
| `1m` | 自家即時擷取 | `CandleBuilder` 將 5 秒快照即時聚合，**每筆快照都 upsert**（in-progress 棒防當機）|
| `1d` | FinMind 歷史回補 | `quanquant-backfill daily`，近月連續月序列（§4.5）|

### 4.2 十二種週期與分桶規則

支援週期：`1m, 5m, 10m, 15m, 20m, 30m, 60m, 4h, 1d, 3d, 1w, 1M`。

**盤中週期（1m–4h）採 session-anchored 分桶**（符合台灣看盤軟體慣例）：

```
bucket_start = session_open + floor((ts − session_open) / tf_minutes) × tf_minutes
```

- 日盤 anchor = 08:45；夜盤 anchor = 15:00（跨午夜不重置）。
- 恰好等於收盤時刻的 tick（13:45 / 05:00）歸入該盤**最後一桶**。
- 休市時段不產生 K 棒。

邊界示例：

| 週期 | 日盤桶 | 夜盤桶 |
|---|---|---|
| 10m | 08:45, 08:55, …, 13:35 | 15:00, 15:10, …, 23:50, 00:00, …, 04:50 |
| 60m | 08:45, 09:45, 10:45, 11:45, 12:45 | 15:00, 16:00, …（跨午夜照常）|
| 4h | 08:45, 12:45（第二桶為 1 小時殘桶）| 15:00, 19:00, 23:00, 03:00（末桶 2 小時殘桶）|

### 4.3 日線及以上

- **日K = 日盤 OHLC**（慣例；夜盤不併入日K）。當日的即時日K由當日日盤 1m 即時聚合（合成棒，不入庫），隔日由回補的官方資料取代。
- `1w` = ISO 週（週一起算，跨年依 ISO 8601）。
- `1M` = 日曆月。
- `3d` = 自最早一根日K起，每 3 個**交易日**為一組（位置分組）。注意：日後往更早回補歷史會使分組位移——此為已接受的行為（棒為即時重算，無庫存損壞問題）。

### 4.4 成交量計算

TAIFEX 提供**當盤累計量**，每根 1m 棒的量為相鄰快照差分：

| 情況 | 桶內增量 dv |
|---|---|
| 第一筆快照（基準未知） | 0 |
| 累計量 ≥ 前值 | 差值 |
| 累計量 < 前值（跨盤重新計數） | 取原始值 |
| 休市 | 重置基準、不出棒 |

### 4.5 歷史回補

- 介面：`HistoryProvider`（`timeframe` + `async fetch_candles(symbol, start, end)`），新增資料源不需改動 K 線管線。
- **日 K：FinMind `TaiwanFuturesDaily`**（免費 token；`data_id`：TXF→`TX`、MXF→`MTX`）。
  - 只取一般時段（`trading_session == "position"`）。
  - 排除價差列（`contract_date` 非 6 位數字者，如 `202606/202607`）。
  - 近月選擇：取「結算日（第三個星期三）≥ 該日」的最小月份合約；無符合者 fallback 至最小月份並記 warning。
  - 轉倉跳價不平滑。假日零值列丟棄。
- **分鐘 K：FinMind `TaiwanFuturesTick`（Sponsor 方案）逐筆成交聚合**——`quanquant-backfill minute`。
  - 資料涵蓋 2011-01-03 至今；一次請求一個日曆日（TX 約 19 萬筆/19MB），逐日抓取、即抓即聚合成 1 分 K（不落地 tick）。
  - 近月選擇按 tick 時間分段：15:00 前參考當日、15:00 起參考次日（結算日夜盤已轉倉至次月）；價差列排除；盤外時間（13:45–15:00、05:00–08:45）tick 丟棄。
  - 成交量 = tick 量逐分鐘加總（**真實分鐘量**，精確度優於即時擷取的累計差分；回補會覆寫同分鐘的 live 棒）。
  - 預設回補近 90 天，可用 `--start` 回溯至 2011。
- **備援：Shioaji（永豐）分鐘級**——新增 `timeframe="1m"` Provider 即可；需永豐帳戶 + API Key。
- 回補指令冪等（PK upsert），可重複執行。
- `rebuild-1m`：以庫存原始快照重建 1m K 棒（quotes 為輔助保留，見 §7）。

## 5. 技術指標

### 5.1 公式

| 指標 | 公式 | 顯示位置 |
|---|---|---|
| SMA（均線）| `SMA(n) = Σ(close, n) / n` | **疊於主圖（K 線圖上）** |
| WR（威廉指標）| `WR(n) = (HHV(n) − C) / (HHV(n) − LLV(n)) × −100`，值域 −100…0 | 副圖 |
| BIAS（乖離率）| `BIAS(n) = (C − SMA(n)) / SMA(n) × 100%` | 副圖 |

（HHV/LLV = n 期內最高價最高值 / 最低價最低值）

### 5.2 多參數設定（核心需求）

每個指標**可同時設定多組參數**（畫多條線，供交叉判斷），每條線可自訂顏色：

```json
{
  "ma":   {"enabled": true, "params": [
            {"period": 5,  "color": "#f0b90b"},
            {"period": 10, "color": "#ff9800"},
            {"period": 20, "color": "#2196f3"},
            {"period": 60, "color": "#e91e63"}]},
  "wr":   {"enabled": false, "params": [
            {"period": 14, "color": "#f0b90b"},
            {"period": 28, "color": "#2196f3"}]},
  "bias": {"enabled": false, "params": [
            {"period": 6,  "color": "#f0b90b"},
            {"period": 12, "color": "#2196f3"},
            {"period": 24, "color": "#e91e63"}]},
  "vol":  {"enabled": true}
}
```

- 本期顯示端由 KLineCharts 內建指標計算（`calcParams` 多值 → 多線；`styles.lines[i].color` 配色）。
- Phase 4 警示需要**伺服器端**指標計算時，依本 schema 以相同公式實作（pandas-ta 或自寫純函式），確保畫面與警示一致。
- 同一指標需能依「商品 + 週期 + 參數」分開計算（原文件 §2.2 要求）——schema 不綁定週期，套用於任何週期。

## 6. 圖表功能

| 功能 | 規格 |
|---|---|
| 圖表庫 | KLineCharts v9.8.x（Apache-2.0，CDN 釘版） |
| 週期切換 | 12 個週期按鈕；切換 = 重新載入該週期資料 |
| 無限歷史 | 左捲到頭觸發 loadMore（`before` 分頁，每頁 ≤ 500 棒，`hasMore=false` 停止）|
| 即時更新 | 每 5 秒輪詢 `/api/candles/latest?since=最後一棒`，僅重算尾端 1–2 棒 |
| 繪圖工具 | 趨勢線、射線、水平線、垂直線、平行通道、價格線、Fibonacci、清除全部 |
| 指標 UI | 設定 dialog：每指標 enable + 多組 (週期, 顏色) 增刪 |
| 持久化 | 指標設定與繪圖存 DB（`chart_states`），重新整理／換瀏覽器不丟失；繪圖以 (timestamp, price) 錨定，跨週期共用 |
| 時區 | 圖表顯示 Asia/Taipei；資料層一律 UTC ms |

## 7. 資料模型

| 表 | 主鍵 | 用途 |
|---|---|---|
| `candles` | (symbol, timeframe, ts) | 正準 K 棒（只存 1m/1d）；`source` 標記 live/finmind/rebuild/(shioaji)；`session`、`trading_date` 輔助查詢 |
| `chart_states` | (symbol, kind) UNIQUE | 圖表 UI 狀態：`indicators` / `drawings` JSON（≤256KB）|
| `quotes` | id | 原始 5 秒快照——僅供除錯與 1m 重建；**保留 `QUOTE_RETENTION_DAYS`（預設 7）天後清除** |
| `trades` | id | 交易日記（Phase 2，全欄位見原文件 §4.1）|

- 價格欄位以 **Decimal-as-TEXT** 儲存（精確往返，無浮點漂移），API 邊界才轉 float。
- 資料層可一行 `db_url` 切換 SQLite ↔ Postgres（見架構書）。

## 8. 警示規則 Schema（Phase 4，先定資料結構）

```json
{
  "symbol": "TXF",
  "tf": "5m",
  "conditions": [
    {"left": {"kind": "indicator", "name": "sma", "params": {"period": 20}, "field": "value"},
     "op": ">",
     "right": {"kind": "price", "field": "close"}},
    {"logic": "AND",
     "left": {"kind": "indicator", "name": "wr", "params": {"period": 14}},
     "op": "<", "right": {"kind": "const", "value": -80}}
  ],
  "trigger": "on_bar_close",
  "notify": ["telegram"],
  "enabled": true
}
```

- 條件元素 = 指標值 / 價格欄位 / 常數 / AND-OR 組合 ——即原文件 §8.2 No-Code 積木的底層資料結構：**先把規則做成資料，UI 積木是後期的呈現層**。
- 評估時機：K 棒收盤時（`on_bar_close`）；通知共用單一 `notify()` 抽象（日曆提醒、新聞推送共用，原文件 §11-4）。

## 9. 未來模組具體化（原文件抽象處的精確化）

| 模組 | 原文件描述 | 精確化 |
|---|---|---|
| 交易日曆（§3）| 「需要有資料來源，可以找找看」 | 事件表 `calendar_events(date, time?, name, type, note, remind_at)`；資料源：TAIFEX 假日表（官網 CSV）+ 總經數據日（經濟日曆 API，如 FRED/investpy 類）；提醒走 Phase 4 notify() |
| 新聞摘要（§5）| 「抓哪些資訊，Perplexity 下 prompt」 | `news(id, source, url, title, summary, symbols[], tags[], published_at)`；LLM 任務鏈：抓取→摘要→關聯標的→分類→推送；僅資訊整理，不做交易決策 |
| 篩選器（§9）| 「連續 3 日 …」 | 重用警示規則 schema 加 `"consecutive": N` 修飾詞，對多商品批次評估；需先有多商品 K 線資料 |
| 回測（§10-3）| 未定義 | 對警示規則做歷史重放：規則已是資料，回測引擎 = 同一評估器跑歷史 K 棒 |
| 移動停利（§7.3）| 描述性 | `trailing_stop(activation_price, trail_points)`，每根 1m 棒收盤評估，觸發記錄寫回交易日記 |

## 10. 非功能需求

| 項目 | 要求 |
|---|---|
| 效能 | 單次報價抓取 < 5s；`/api/candles` 單頁回應 < 1s（一般情況 < 100ms）；衍生週期單頁 1m 讀取上限 20 萬列 |
| 可用性 | 盤中資料擷取不可中斷；任何子任務（持久化/清理）失敗不影響輪詢 |
| 時區 | 儲存一律 UTC（K 棒 epoch ms）；顯示一律 Asia/Taipei |
| 精度 | 價格 Decimal 全程精確；禁止 float 進資料庫 |
| 可測試性 | 分桶/聚合/量差分/近月選擇皆為純函式，單元測試覆蓋邊界（收盤 tick、跨午夜、轉倉日）|
| 可攜性 | 容器化、12-factor 設定（env vars）、單一 `db_url` 切換資料庫——詳見架構書 |
| 備份 | 見架構書 §6 |
