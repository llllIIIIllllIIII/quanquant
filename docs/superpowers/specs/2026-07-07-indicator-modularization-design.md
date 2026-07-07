# 指標模組化設計（Indicator Modularization）

**日期：** 2026-07-07
**狀態：** 已核准，待 writing-plans

## 目標

把目前散在多處、單一設定視窗擁擠的指標系統，收斂成一張 **registry（登錄表）** 驅動的架構：所有指標的定義、預設值、參數 UI、渲染派發、警示下拉，全部從同一份自我描述的資料衍生。解決兩件事：

1. **擴充性**：未來新增指標（含 MACD/KDJ 這類多參數指標）只需在 registry 加一個描述物件，其餘自動生效。
2. **設定視窗擁擠**：把「全部指標擠在一個 dialog」改成**主從式 + 指標目錄**，一次只看一個指標的參數。

範圍為前端為主的重構，後端持久化、routes、candle 讀取路徑幾乎零改動。

## 已確認的設計決策

- **schema 驅動的 registry**：確定會有 MACD/KD 等多參數指標，故 `paramSchema` 從一開始就通用化。
- **主從式 + 指標目錄 UI**：左欄啟用清單 ＋「新增指標」目錄；右欄顯示選取指標的參數。
- **每種指標單一實例**：目錄是固定的可用指標庫，「新增」= 從庫裡啟用一種；`enabled` 旗標即代表「在啟用清單中」。資料模型維持一包 JSON。

## 綁定約束（勿回退）

- KLineCharts 釘版 **9.8.12**，不升版；`chart.js` 四道渲染防線（stale-response guard、`_watchdog`、`_nextFrame` 逐格分幀、完整 `_lineStyle`）**不可移除或改動**。
- candle 讀取路徑 raw-SQL→float、routes 為 sync `def` —— 本設計完全不碰。
- 指標持久化維持既有一包 JSON（`ChartState`/`UserChartState` kind=`indicators`），走既有 `PUT /api/chart/state/indicators`，SQLite/Postgres 雙方言不變。
- 模板注入 JS 變數一律用 `| tojson`。
- commit 格式 `<type>: <description>`（無 attribution）；每 task 結尾 `uv run pytest` 全綠 + 觸及 JS 檔跑 `node --check`。
- 部署不自動：完成後由使用者決定何時 `./scripts/deploy.sh`（本功能會與尚未部署的配色修正 commit `e0276f9` 一起批次上線）。

## §1 架構：指標登錄表 + 獨立模組

**單一真相來源**：新增靜態模組 `src/quanquant/web/static/indicators.js`，掛 `window.QQIndicators`，在 `chart.js` **之前**載入（比照現有 `ma_deduction.js` / `window.MADeduction` 模式）。

衍生關係：

```
window.QQIndicators（registry 陣列 + 純函式 helper）
   ├─→ DEFAULT_SETTINGS 等價物（預設值）
   ├─→ 設定 dialog 的左欄清單、目錄、右欄參數欄位
   ├─→ applyIndicators 的渲染派發
   └─→ 警示條件的指標下拉選單
```

**理由**：`chart.js` 已 988 行且隨每個新指標膨脹；把「會頻繁增長的指標定義」抽出，`chart.js` 只留渲染引擎，兩檔各自聚焦。未來加指標幾乎只動 `indicators.js`。

**有利事實**：MA、WR、BIAS、VOL、MACD、KDJ、RSI 皆為 KLineCharts 9.8.12 **內建指標**，registry 以 `klineName` 對應內建名，`createIndicator` 即可開，多數新指標「設定即可」，不需自寫計算。需自訂計算者（未來若有）才走 `registerIndicator`，非本次範圍。

## §2 registry 欄位結構 + paramSchema

每個指標一個描述物件。`paramSchema` 以 `repeatable` 旗標同時容納兩種參數形態。

**形態 A — 可重複的線陣列**（MA/WR/BIAS，現況）：

```js
{
  key: "ma", title: "均線 MA", hint: "疊於主圖",
  pane: "main",           // "main"=疊主圖 | "sub"=獨立副圖
  klineName: "MA",        // KLineCharts 內建指標名
  alertTarget: true,      // 是否出現在警示條件下拉
  repeatable: true,       // 使用者可 ＋加一條 / −刪一條
  paramSchema: [
    { field: "period", type: "number", label: "週期", min: 1, step: 1 },
    { field: "color",  type: "color",  label: "顏色" },
  ],
  defaults: { enabled: true, params: [
    { period: 5, color: "#f0b90b" }, { period: 10, color: "#ff9800" },
    { period: 20, color: "#2196f3" }, { period: 60, color: "#e91e63" },
  ]},
}
```

**形態 B — 固定欄位**（MACD，未來）：

```js
{
  key: "macd", title: "MACD", hint: "副圖",
  pane: "sub", klineName: "MACD", alertTarget: false,
  repeatable: false,
  paramSchema: [
    { field: "fast",   type: "number", label: "快線", min: 1 },
    { field: "slow",   type: "number", label: "慢線", min: 1 },
    { field: "signal", type: "number", label: "訊號", min: 1 },
  ],
  defaults: { enabled: false, params: { fast: 12, slow: 26, signal: 9 } },
}
```

**兩者差別只在 `repeatable`：**

- `repeatable: true` → `params` 是**陣列**；UI 對每條線用 `x-for` 渲染 `paramSchema` 欄位 + ＋/− 按鈕；`calcParams` = 各條 period 陣列。
- `repeatable: false` → `params` 是**單一物件**；UI 直接鋪 `paramSchema` 欄位；`calcParams` = 依 `paramSchema` 中 `type==="number"` 欄位順序取值（MACD → `[fast, slow, signal]`）。

**VOL 特例消失**：即 `paramSchema: []`、`pane: "sub"`、`repeatable: false` 的一般條目。

**helper（純函式，不碰 DOM / chart）：**

- `QQIndicators.list` → registry 陣列。
- `QQIndicators.defaults()` → 組出等同現在 `DEFAULT_SETTINGS` 的物件（`{ key: {enabled, params} }`）。
- `QQIndicators.alertTargets()` → 警示下拉清單（`alertTarget:true` 的條目 + 常數 `price` 併入）。
- `QQIndicators.calcParams(entry, conf)` → 依 `repeatable` 算出傳給 KLineCharts 的參數陣列。

## §3 applyIndicators 改 registry 驅動迴圈

現行 `chart.js:438-528`「MA 特例 → WR/BIAS 通用 → VOL 特例」改為單一迴圈：

```
for (const entry of QQIndicators.list) {
  const conf = this.settings[entry.key];
  if (enabled) createOrOverride(entry, conf);   // 依 entry.pane 疊主圖或建副圖
  else         removeIfPresent(entry);
  if (entry.pane === "sub") await this._nextFrame();  // 保留逐格建立
}
await this._nextFrame(); this.chart.resize();          // 收尾重排（不變）
```

- `createOrOverride`：`pane==="main"` → `createIndicator(override, true, {id:"candle_pane"})`；`pane==="sub"` → `createIndicator(override, false, {height:90})`。`override.name = entry.klineName`、`calcParams = QQIndicators.calcParams(entry, conf)`、線色由 params 取（repeatable 逐條、fixed 用 schema color 欄位或預設調色盤）。
- **四道防線保留**：副圖之間仍 `await this._nextFrame()` 逐格建立；`_watchdog`、stale-response guard、`_lineStyle` 完整線樣式不動。
- `paneIds` 統一以 `entry.key` 為鍵（現況 ma 用布林、其餘用 pane id → 統一 `paneIds[key]`）。
- **MA 扣抵**：`refreshDeduction` 仍綁 `ma` 條目（MA 專屬功能），維持現狀。

## §4 設定 dialog 改主從式 + 目錄（dashboard.html）

現行單一長清單（`dashboard.html:201-240` 的 `x-for in indicatorDefs`）改為左右兩欄：

- **左欄**：已啟用指標清單（`settings` 中 `enabled` 者）＋底部「＋ 新增指標」。點「新增指標」展開**目錄**（registry 中尚未啟用者），選一個 → 該指標 `enabled=true` 並成為選取項；清單項旁 − 移除（`enabled=false`）。
- **右欄**：顯示 `activeIndicator`（選取 key）的參數，由 `paramSchema` 用 `x-for` 生成欄位；`repeatable` 者附 ＋加一條 / −刪一條。
- **Alpine 新增狀態**：`activeIndicator`（選取 key）、`catalogOpen`（目錄展開）。左欄清單與目錄皆從 `QQIndicators.list` + `settings` 衍生，不再讀寫死的 `indicatorDefs`。
- **儲存流程不變**：底部「儲存」仍 `saveSettings() → applyIndicators() → PUT /api/chart/state/indicators`。
- **Task 9 圖上 ⚙ 整合**：圖上指標 tooltip 的 ⚙ → `openSettings()` 並設 `activeIndicator` 為該指標，直接跳到它的參數頁。

## §5 警示條件下拉改吃 registry

`indTargets`/`rightTargets`（`chart.js:793-800`）現手列 price/ma/wr/bias，改由 `QQIndicators.alertTargets()` 衍生（`price` 常數併入）。加一個 `alertTarget:true` 的指標，警示下拉自動多一項，一處維護。

## §6 持久化、向後相容、錯誤處理、測試

**持久化（不動）**：指標設定維持一包 JSON（kind=`indicators`），走既有 `PUT /api/chart/state/indicators`、sync `def` route、雙方言。`enabled` 同時代表「在啟用清單中」。

**向後相容（關鍵）**：`mergeSettings` 改成以 **registry defaults 為底、疊上使用者存檔**：

- 舊存檔只有 ma/wr/bias/vol → 照常載入，新指標以 defaults（`enabled:false`）補上。
- 存檔含 registry 已移除的 key → 略過不報錯。
- 保證線上既有使用者的指標設定升級後原樣保留。

**錯誤處理**：每個指標 `createOrOverride` 各自 try/catch（現況已有）——單一指標建立失敗不影響其他指標與 K 棒；未知 key / 缺欄位走 defaults 補齊。

**測試：**

- `indicators.js` 純函式（`defaults()` / `alertTargets()` / `calcParams()`）→ node 輕量斷言；`calcParams` 對 repeatable 與 fixed 兩形態皆正確。
- `mergeSettings` 向後相容：舊 blob（無 MACD）載入後含全部 registry key 且舊值保留。
- dashboard render 斷言：dialog 由 registry 驅動（每個 registry 指標一個左欄項、目錄含未啟用者）。
- 既有指標回歸：MA 疊主圖、WR/BIAS/VOL 副圖行為不變。
- 實際 `createIndicator` 視覺行為 → 手動瀏覽器驗證（清單列於下）。
- 全程 `uv run pytest` 全綠 + `node --check`。

**手動瀏覽器驗證清單（實作後由使用者執行）：**

1. 既有 MA/WR/BIAS/VOL 開關與參數行為與升級前一致。
2. 新增指標目錄可展開、啟用一個指標即出現在左欄並可調參。
3. 移除指標（−）後圖上該 pane 消失、儲存後重整仍為移除狀態。
4. repeatable 指標 ＋/− 線正常；fixed 指標（MACD）三欄可調並正確反映到圖上。
5. Task 9 圖上 ⚙ 點擊跳到對應指標參數頁。
6. 警示條件下拉的指標選項與啟用指標一致。
7. 舊帳號（既有指標存檔）升級後設定原樣保留。

## 交付切分（單一 spec，非多子系統）

1. `indicators.js` registry + 純函式 helper + 測試（不接 UI/渲染，先立骨架）。
2. `applyIndicators` / `paneIds` / `mergeSettings` 改 registry 驅動（既有三指標行為不變，回歸測試顧著）。
3. 設定 dialog 改主從式 + 目錄。
4. 警示下拉改吃 registry。
5. Task 9 圖上 ⚙ 帶 `activeIndicator` 跳頁。
6. 加一個真正的新指標（MACD）作「擴充性驗證」，證明加指標只動 registry。
7. 手動驗證清單 + 收尾。

## 非目標（YAGNI）

- 同種指標多實例（例如兩組不同參數的 RSI）—— 已確認不做。
- 自訂計算指標（`registerIndicator`）—— 內建指標已涵蓋近期需求，不在本次。
- 指標拖曳排序、pane 高度自訂 —— 未提出，不做。
- 後端指標 schema / 每指標獨立資料列 —— 維持一包 JSON，不做。
