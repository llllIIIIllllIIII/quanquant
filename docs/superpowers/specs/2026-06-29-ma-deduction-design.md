# 主圖均線扣抵（即時扣抵 MVP）設計

> 狀態：已定案，待實作
> 日期：2026-06-29
> 來源規格：`~/Downloads/ma_deduction_feature_spec.md`（完整版，含游標扣抵與後續階段）
> 本文件範圍：**僅即時扣抵 MVP**，依與使用者確認後的三項決策收斂。

## 1. 目標與定位

在 K 線主圖加入「均線扣抵」輔助判斷工具：以**最新一根 K 棒**為基準，顯示各啟用 MA 週期目前的扣抵位置、扣抵值與方向傾向（上彎／走平／下彎）。

- 這是**均線方向輔助判斷工具，不是買賣訊號**。
- 使用者**自行開關**（預設關），與市場脈搏音效一致的互動定位。

## 2. 已定案決策（與規格的差異一併記錄）

| 項目 | 決策 | 與原規格關係 |
|------|------|--------------|
| 功能範圍 | **即時扣抵 MVP**（live） | 游標扣抵列為後續階段（規格 §5） |
| 顯示方式 | **狀態列（永遠可見）＋ 圖上三角** | 規格 §8 為圖上三角；新增狀態列解決長均線扣抵常落在畫面外的問題 |
| 開關位置／預設 | **工具列切換鈕，預設關，狀態存 localStorage** | 規格 §7.2 預設開啟＝是、放設定對話框；改為使用者主動開啟、放工具列 |
| 扣抵值來源 | 收盤價 `close` | 規格 §4.5；與 KLineCharts 內建 MA（以 close 計算）一致 |
| 走平容忍 | `flatThresholdPercent = 0.1`（固定，MVP 不開放自訂） | 規格 §4.8 |
| 畫面外行為 | 三角隨 K 棒錨定、捲出畫面由 KLineCharts 自然裁切；狀態列恆顯示 | 規格 §12.2「畫面外不顯示」由自然裁切達成；§4.9「最新 K 棒不在畫面則整組隱藏」MVP 不另外實作，因三角錨定真實扣抵 K 棒不致誤導，且狀態列已涵蓋恆顯示需求 |

## 3. 架構與檔案切分

依「小而專注的單元」原則，純計算與 KLineCharts 細節獨立成新檔，避免 `chart.js`（已 696 行）續膨脹。

| 檔案 | 變更 | 職責 |
|------|------|------|
| `src/quanquant/web/static/ma_deduction.js`（新） | 新增 | **純計算** `computeLive()` + 註冊自訂三角 overlay + `draw()` / `clear()`。無 DOM 依賴，可單測。 |
| `src/quanquant/web/static/chart.js` | 整合 | 新增 `deduction` 狀態與 `refreshDeduction()`，掛進既有資料/設定更新點；以 callback 把結果交給 Alpine 狀態列。 |
| `src/quanquant/web/templates/dashboard.html` | 加 UI | 工具列切換鈕（仿 `♥`）＋ 圖頭右上 Alpine 反應式狀態列 chips。 |
| `src/quanquant/web/static/app.css` | 加樣式 | 狀態列 chips、上彎/走平/下彎箭頭與配色、切換鈕作用態。 |
| `tests/js/ma_deduction.test.mjs`（新） | 新增 | Node 內建 `node --test`（零額外依賴）單測純計算邏輯。 |

## 4. 計算邏輯（`computeLive`，純函式）

輸入：`bars`（升序，取自 `chart.getDataList()`，不額外打 API）、`params`（`[{period, color}]`，來自 `settings.ma.params`）、`flatPct`（預設 0.1）。

```text
latestIndex = bars.length - 1
basePrice   = bars[latestIndex].close
對每個 {period, color}：
  deductionIndex = latestIndex - period
  若 deductionIndex < 0 或 bars[deductionIndex] 缺 close：
      → status = "insufficient-data"（不畫三角、狀態列顯示「—」）
  deductionValue = bars[deductionIndex].close
  diff           = basePrice - deductionValue
  diffPercent    = deductionValue === 0 ? null : Math.abs(diff)/deductionValue*100
  status:
      diffPercent != null && diffPercent <= flatPct → "flat"
      否則 diff > 0 → "upward"
      否則 diff < 0 → "downward"
      （diff === 0 → "flat"）
```

回傳每條：
```ts
{
  period, color,
  deductionIndex, deductionTime,   // = bars[deductionIndex].timestamp
  deductionValue, basePrice,
  diff, diffPercent,               // diffPercent 可為 null
  status                           // "upward" | "downward" | "flat" | "insufficient-data"
}
```

## 5. 圖上三角標記

- 用 `klinecharts.registerOverlay` 註冊自訂型別 `maDeduction`：在扣抵 K 棒、對應價格位置**下方**畫一個 **MA 同色倒三角**。
- 建立時用 `groupId: "ma-deduction"`（**獨立於** `"user-drawings"`）、`lock: true`：
  - 不可拖拉/刪除。
  - **不寫入** `/api/chart/state/drawings`（不持久化，與使用者繪圖完全隔離）。
- 每次重算：先 `chart.removeOverlay({ groupId: "ma-deduction" })`，再為每條（非 `insufficient-data`）建立 overlay。
- 三角錨定扣抵 K 棒時間戳，捲動時自動隨之移動；扣抵位置捲出畫面時由 KLineCharts 自然裁切。
- hover 帶精簡原生提示；完整資訊由狀態列提供。

## 6. 狀態列（永遠可見）

圖頭右上一排 Alpine 反應式 chips，每條啟用 MA 一個：`MA20 17,980 ↑`

- 文字與箭頭顏色＝該 MA 線色。
- 箭頭：`↑` upward／`→` flat／`↓` downward／`—` insufficient-data。
- `title` 屬性帶完整文案：模式（即時扣抵）、扣抵位置（N 根前）、扣抵價、基準價、差值、diff%、狀態描述。
- 解決長均線（如 MA60）扣抵位置常落在畫面外、圖上看不到的問題。

## 7. 重算觸發點（即時模式，不另開計時器）

`refreshDeduction()` 掛進既有路徑：

- `loadInitial()` 後（首次/重載）
- `_applyBar()` 後（5 秒輪詢 `pollLatest` 與 SSE 報價 `onQuote` 同步更新最新棒）
- `setTf()` / `setSession()` 後（換週期/時段）
- `saveIndicators()` 後（改 MA 週期/顏色）

計算量極低（只看最新棒與各 N 根前），無效能疑慮（規格 §13）。

## 8. 開關與預設

- 工具列新增切換鈕（仿市場脈搏 `♥`）：`aria-pressed`、`title` 反映開關狀態。
- 預設**關**；狀態存 `localStorage["qq_ma_deduction"]`，重載沿用。
- 開 → `refreshDeduction()` 開始繪製與更新狀態列。
- 關 → `clear()` 移除三角 group、清空狀態列。
- 扣抵讀 `settings.ma.params`（period + color）；若無任何週期，狀態列顯示「請先在指標設定加入 MA」。

## 9. 邊界處理（規格 §12）

- 資料不足 N 根（`deductionIndex < 0`）、扣抵值為 0、缺漏價格欄位 → 該條不畫、狀態列顯示「—」。
- 計算/繪製全程沿用既有 `try/catch` 防線，任一條失敗不影響其他 MA 與 K 線渲染。

## 10. 測試

- `computeLive` 為純函式（無 DOM/chart 依賴）→ `node --test tests/js/ma_deduction.test.mjs`（**Node 內建測試器，零額外套件**）。
- 涵蓋案例：
  - upward（basePrice > deductionValue 且超出容忍）
  - downward（basePrice < deductionValue 且超出容忍）
  - flat（差距 ≤ 0.1%）
  - insufficient-data（`deductionIndex < 0`）
  - 除以 0（`deductionValue === 0` → `diffPercent` 為 null，靠 diff 判方向）
  - 多週期同時計算
- **不引入前端 build 工具**，避免動到 Docker/hatch 設定（CLAUDE.md 約束）。

## 11. 不做（本次 MVP 範圍外）

游標扣抵、歷史全量扣抵三角、畫面外邊界提示箭頭、標記聚合顯示、自訂容忍區間、自訂扣抵值來源、扣抵警示通知、扣抵轉買賣訊號 —— 皆列為後續階段（規格 §15.2 / §15.3）。

## 12. 不可回退的既有約束（CLAUDE.md，本功能須遵守）

- KLineCharts 釘版 v9.8.12，`registerOverlay` / overlay API 以 v9 為準（v10 改名，勿升）。
- `chart.js` 四道渲染防線（`_nextFrame` 排程、`_watchdog`、try/catch、`resize` 重繪）勿移除；新 overlay 操作不得破壞之。
- 純前端功能，不動 pyproject hatch wheel / force-include 設定。
- 扣抵 overlay 與使用者繪圖（`user-drawings` group）完全隔離，不進 drawings 持久化。
