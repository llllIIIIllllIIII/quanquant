# QuanQuant UI/UX 大改版 — 設計文件

日期：2026-07-06
狀態：已與使用者逐段確認

## 1. 背景與目標

現況問題：

1. 儀表板工具列 8 顆控制項擠在一排，缺乏分組與層次
2. 全站為 Pico CSS 預設外觀，缺乏設計感
3. 操作流程有摩擦（如左上角 QuanQuant 為純文字、不可點擊回首頁）

目標：在**不影響現有功能**的前提下，將全站改造為專業交易終端風格（TradingView / Binance 取向），並優化操作流程。

已確認的決策：

| 決策點 | 結論 |
|---|---|
| 視覺風格 | 專業交易終端（深色、高資訊密度、K 線為主角） |
| 範圍 | 全站 6 頁一次到位 |
| 技術路線 | 保留 Pico 2 為基底 + 自建 design-token 覆寫層（方案 A）；tokens 未來可整套遷移到 SPA |
| 主題 | 深色為主 + 淺色切換（使用者層級偏好，仿 `chart_color_scheme` 模式） |
| 流程優化 | logo 回首頁、工具列重組、RWD、圖表全螢幕、指標線上調參 |

## 2. 視覺系統（Design Tokens）

新增 `static/tokens.css`（獨立檔案，未來遷移 SPA 可整檔帶走），以 CSS custom properties 定義；深淺主題透過 `html[data-theme]` 換值，並映射到對應的 `--pico-*` 變數。注意 `login.html` 不繼承 `base.html`，字體與 CSS 引入需在兩處同步。

### 2.1 色彩（深色，主戰場）

基底來自 ui-ux-pro-max「Financial Dashboard」色票：

| Token | 深色值 | 用途 |
|---|---|---|
| `--qq-bg` | `#020617` | 頁面底色 |
| `--qq-surface` | `#0E1223` | 卡片、面板、工具列、導航 |
| `--qq-surface-2` | `#1A1E2F` | 懸浮層、hover 底色 |
| `--qq-border` | `#26304a` | 弱邊框 |
| `--qq-border-strong` | `#334155` | 強邊框 |
| `--qq-text` | `#F8FAFC` | 主文字 |
| `--qq-text-muted` | `#94A3B8` | 次要文字 |
| `--qq-accent` | `#3B82F6` | 主操作色（按鈕、focus、active） |
| `--up` / `--down` | `#16c784` / `#ea3943` | 漲跌語意色（沿用，跟隨使用者紅漲/綠漲偏好） |

淺色主題：同名 token 換值（白底 `#FFFFFF`、面 `#F8FAFC`、文字 `#0F172A` 等），漲跌色微調深度；兩套主題文字對比均需 ≥ 4.5:1（次要文字 ≥ 3:1）。

### 2.2 字體

- UI 與內文：`"Noto Sans TC", system-ui, sans-serif`（Google Fonts，`font-display: swap`）
- 數字（價格、統計、表格數字欄）：`"JetBrains Mono", monospace` + `font-variant-numeric: tabular-nums`——數字跳動不位移
- 型階：12 / 13 / 14 / 16 / 20 / 28px；行高內文 1.5–1.6、數字 1.2

### 2.3 其他

- 間距：4px 節奏（4/8/12/16/24/32）
- 圓角：6px（按鈕、輸入框）/ 10px（卡片、dialog）
- 陰影：兩級（懸浮層 `0 4px 12px rgba(0,0,0,.35)`、toast `0 8px 24px rgba(0,0,0,.45)`）
- 動效：150ms ease-out；`prefers-reduced-motion: reduce` 時關閉非必要動畫
- Icon：全面使用 Lucide inline SVG（stroke 2px、統一 18px），取代 emoji（♥ 🔔 🔴 ▽ ⋯）；icon-only 按鈕必附 `aria-label` 與 `title`

## 3. 導航列（base.html）

- **QuanQuant logo 改為 `<a href="/">`**，加 SVG 標誌（K 棒圖形）+ 文字，hover 有回饋
- sticky 置頂、`--qq-surface` 底 + 底部細邊框
- 分頁連結 active 態加**底線指示條**（2px `--qq-accent`）
- 使用者選單：圓形字首頭像 + 名稱；選單項加 icon；**登出用危險色、與其他項目以分隔線隔開**
- 導航列新增**深淺主題切換鈕**（太陽/月亮）
- <768px：分頁連結收進漢堡選單（Alpine 控制開合），logo 與使用者選單保留

## 4. 儀表板

### 4.1 報價面板

資訊不變、重新排版：

- 價格：JetBrains Mono 28px 粗體
- 漲跌幅：語意色 pill（10% 透明度背景 + 文字色）
- 量/高/低：帶小標籤的欄位組
- 時段：帶圓點的 status badge（日盤綠、夜盤黃、收盤灰）
- SSE 更新時價格背景閃淡色 150ms

### 4.2 工具列重組（核心）

現有 8 控制項 + 獨立時框列，重組為單列三群：

```
[ 時段 ▾ ]  [ 1m 5m 15m 1h 1d … ]     [⛶] [指標設定] [警示] [⋯]
```

- **時框列合併進工具列**（原 `tf-bar` 一排移除）
- 常用操作（指標設定、警示）保留為帶文字按鈕
- **狀態型開關收進「⋯ 更多」dropdown**：市場脈搏、TG 通知、K 線配色、均線扣抵、跨框模式。每項為「icon + 名稱 + 狀態」選單列；任一功能開啟時 ⋯ 鈕顯示小圓點
- **全螢幕鈕 ⛶**：CSS class 切換隱藏導航與報價列；Esc 或再按退出；不動 chart 邏輯，退出後觸發 `chart.resize()`
- 繪圖 rail 位置不變，樣式重繪（icon 按鈕、active 高亮）；均線扣抵 legend 位置不變
- <768px：工具列折為兩列（時框一列、其餘一列）；繪圖 rail 改為圖表上方橫向一列

### 4.3 指標線上調參（新功能）

- 目標：點擊圖上指標 tooltip 區域 → 打開指標設定 dialog 並定位到該指標
- 實作前先做 KLineCharts 9.8.12 API 技術驗證（tooltip icon / 事件支援度）
- 退階方案：若 API 不支援，改為 hover 顯示「點此編輯」提示條連到設定 dialog
- 不改動指標儲存邏輯，僅新增入口

### 4.4 圖表高度

固定 680px 改為 `clamp(420px, calc(100vh - 320px), 760px)`，筆電視窗不再超出視野；變更後觸發 resize 防線。

## 5. 其餘頁面

- **交易日記**：篩選列改 `--qq-surface` 卡片列；表格表頭 sticky、行 hover 高亮、多/空與損益用語意色、標籤 pill 統一；「+ 新增交易」為頁面唯一 primary 按鈕
- **績效統計**：指標卡數字用 JetBrains Mono；總損益卡放大為焦點；卡片左側 2px 語意色條；分組表格與日記共用樣式；匯出鈕降為次要
- **登入頁**：置中卡片 + logo + 深色底；密碼顯示/隱藏切換；錯誤訊息為欄位下標準 error 樣式
- **帳戶設定／使用者管理**：套用卡片與表單樣式，不改結構

## 6. RWD

- 斷點：768px / 1024px
- 已涵蓋於各節：導航漢堡、工具列兩列、rail 橫排、表格橫向捲動（保留 `table-wrap`）、統計卡 2 欄
- 觸控目標 ≥ 44px；不得出現水平捲動（表格容器除外）

## 7. 深淺主題切換（新功能）

完全複製 `chart_color_scheme` 的三層模式：

1. **DB**：`User.theme` 欄位（`'dark'`/`'light'`，預設 `'dark'`）+ migration（SQLite/Postgres 雙方言可攜）
2. **API**：`PUT /api/user/theme`，service 層驗證，非法值回 422
3. **前端**：導航列切換鈕；未登入看 `localStorage`；登入後以 DB 為準；`html[data-theme]` 切換 + `chart.setStyles()` 同步 KLineCharts 底色/格線/十字線（K 棒漲跌色不動，仍由紅漲/綠漲偏好控制）
4. 模板注入時用 `tojson` 轉義（比照 `QQ_COLOR_SCHEME` 的 T5-M2 加固）

## 8. 不影響功能的保證措施

- 所有元素 **id、Alpine `x-data` 結構、HTMX 屬性、SSE 端點、API 路由不動**；只改 class、DOM 排列與 CSS
- `chart.js` 四道渲染防線不碰；KLineCharts 版本釘住 9.8.12
- candle 讀取路徑（raw-SQL→float、sync def routes）完全不動
- 每頁改完跑 `uv run pytest` 全綠；手動驗證清單：報價 SSE、指標增刪改、警示 CRUD、畫線與跨框、K 線配色切換、市場脈搏、TG toggle、日記 CRUD 與篩選、統計匯出 CSV/Excel、登入登出、密碼變更
- 新功能（主題 API、全螢幕、tooltip 調參）依 TDD：先寫測試再實作

## 9. 錯誤處理

- 主題 API：非法 body → 422；未登入 → 401（沿用既有 auth middleware）
- 主題切換時 chart 尚未初始化 → 只切頁面主題，chart 初始化時讀當前主題
- 指標 tooltip 調參技術驗證失敗 → 採退階方案，不阻塞其餘改版

## 10. 交付切分（供實作計畫參考）

1. Design tokens + base.html 導航（logo 連結、active 指示、選單、sticky）
2. 深淺主題切換（DB + API + 前端 + chart 同步）
3. 儀表板：報價面板 + 工具列重組 + 全螢幕 + 圖表高度
4. 指標線上調參（含技術驗證 spike）
5. 交易日記 + 績效統計
6. 登入 + 帳戶 + 使用者管理
7. RWD 收尾 + 全站手動驗證
