# 指標隱藏 + KDJ/BOLL + 模組化收尾 — 設計文件

**日期**：2026-07-12
**狀態**：已核准，待 writing-plans
**相關記憶**：`project_indicator_modularization.md`（2026-07-07 registry 化 `window.QQIndicators` 上線）、`project_market_halt_guard.md`（chart-guards 純函式前端防線範式）

## 背景與目標

台指期即時監控圖表目前有 5 個指標（MA/WR/BIAS/VOL/MACD），全部透過 `indicators.js` 的
`window.QQIndicators` registry 宣告、由 KLineCharts v9.8.12 原生 indicator 繪製，UI 表單由 registry +
`paramSchema` 自動生成。本次三個訴求：

1. **隱藏指標**：讓裸K交易員能一鍵得到只剩K線的乾淨圖，同時也能單獨隱藏某條指標。
2. **新增指標**：加入台股常用的 KDJ 與布林通道（BOLL）。
3. **模組化收尾**：消滅殘留硬編，讓「新增指標＝只改 REGISTRY 一處」真正成立。

## 現況要點（調查結論）

- **registry 單一真相**：`indicators.js:13-65` 的 `REGISTRY` 陣列，每指標一物件，欄位含
  `key/title/hint/pane/klineName/alertTarget/repeatable/paramSchema/defaults`。公開 API 於 `indicators.js:104`
  （`list/byKey/byKlineName/defaults/merge/alertTargets/calcParams`）。
- **全部走 KLineCharts 內建**：沒有任何自算/自繪指標；唯一自繪 overlay 是 MA 扣抵三角（`ma_deduction.js`，
  掛 `window.MADeduction`，不在 registry）。
- **消費流程**：`chart.js` 的 `applyIndicators()`（`chart.js:437-445`）遍歷 registry 逐一呼叫
  `_applyIndicator`（`chart.js:473-510`），依 pane 呼叫原生 `createIndicator/overrideIndicator/removeIndicator`。
- **UI**：指標設定 dialog（`dashboard.html:188-267`），左欄已啟用清單（`enabled` 開關）、右欄
  `paramSchema` 自動生成參數表單。`enabled` 目前是唯一狀態＝完全 create/remove，無「保留設定但暫時隱藏」。
- **扣抵三角 toggle 範式**（可直接參考）：工具列 more 選單 row + `aria-pressed`（`dashboard.html:85-93`）→
  Alpine `toggleDeduction()`（`chart.js:892-896`）寫 `localStorage["qq_ma_deduction"]` →
  引擎 `setDeduction/refreshDeduction`（`chart.js:526-555`）。
- **殘留硬編**：`chart.js:34` 的 `const IND_NAMES = { ma:"MA", wr:"WR", bias:"BIAS" }`，僅供 `alertText`
  組警示文字，與 registry 的 `klineName` 重複且需手動同步——registry 化的唯一漏網。

## 已定決策（brainstorming 逐條拍板）

- **隱藏模型**：兩者都做——每指標眼睛開關 ＋ 全域裸K總開關。
- **狀態保留**：每指標 `visible` 跟帳號（併入現有 enabled/params 後端 PUT）；全域裸K跟裝置（localStorage）。
- **新指標範圍**：KDJ/BOLL 先只畫圖、不進警示（`alertTarget:false`）。
- **裸K範圍**：一併隱藏所有 registry 指標 ＋ MA 扣抵三角。
- **呈現**：BOLL 疊主圖（main pane）、KDJ 走副圖（sub pane）；參數標準值 BOLL 20/2、KDJ 9/3/3。

## 設計

### A. 新增 KDJ / BOLL（僅擴充 REGISTRY）

在 `indicators.js` 的 `REGISTRY` 各加一筆。因兩者皆 KLineCharts 內建，繪製零改動。

| key | title | klineName | pane | repeatable | paramSchema（順序＝calcParams 順序）| 產生 calcParams |
|-----|-------|-----------|------|-----------|--------------------------------------|-----------------|
| `boll` | 布林通道 | `BOLL` | `main` | `false` | `period`(週期,20,min2,step1)、`std`(標準差,2,min1,step1) | `[20, 2]` |
| `kdj` | KDJ | `KDJ` | `sub` | `false` | `k`(9)、`d`(3)、`j`(3) | `[9, 3, 3]` |

- 兩者 `alertTarget:false`。
- `defaults` 皆含 `{ enabled:false, visible:true, params:{...} }`（見 B 的 `visible`）。
- `calcParams`（`indicators.js:92-102`）對 fixed 指標取 paramSchema 中 number 欄位「依序」組陣列，故
  paramSchema 欄位順序必須正確（BOLL: period→std；KDJ: k→d→j）。

### B. 隱藏指標：新增 `visible` 狀態 + 全域裸K

**資料模型**：每指標 conf 新增布林 `visible`（預設 `true`），與 `enabled/params` 同級。
- `enabled`＝是否在使用者的指標清單中；`visible`＝清單中的此指標當前要不要畫。
- `defaults()`、`merge(saved)`、`saveSettings` 清洗都納入 `visible`；`merge` 保留使用者存的 `visible`。

**可見性判定（純函式，可單測）**：新增
```
resolveVisibility(entry, nakedK) => entry.enabled && entry.visible && !nakedK
```
放到可被 node 測試 import 的模組（比照 `chart-guards.js` 範式），作為前端回歸防線。

**引擎面**（`chart.js`）：
- `_applyIndicator` 改用 `resolveVisibility(entry, this.nakedK)` 決定 create/override（true）或 remove（false）。
- 新增引擎狀態 `this.nakedK`（由 Alpine 傳入/同步）。
- `setDeduction` 額外被 `!nakedK` 閘住：裸K ON 時強制隱藏扣抵三角，OFF 時回歸 `deductionOn`。
- 切換 `nakedK` → 重跑 `applyIndicators()` + refresh 扣抵；切換單指標 `visible` → 重套用該指標。

**UI**：
- **每指標眼睛**：設定 dialog 左欄（`dashboard.html:199-207` 一帶）每個已啟用指標 row 加 👁 toggle，
  翻 `form[key].visible`，隨 `saveSettings()` 一起 PUT 後端。眼睛需有明確 aria 狀態（`aria-pressed` /
  `aria-label` 顯示/隱藏）。
- **全域裸K**：工具列 more 選單新增一 row（沿用扣抵三角 pattern，`dashboard.html:85-93`），
  `@click="toggleNakedK()"` + `:aria-pressed` + 文案「裸K：開/關」。Alpine `toggleNakedK()`：翻
  `nakedK` → 寫 `localStorage["qq_naked_k"]` → 同步引擎 `this.nakedK` → 重套指標 + 扣抵。init 時讀回 localStorage。

**互動規則**：裸K ON 無條件蓋掉一切非K線圖層，OFF 回歸各指標眼睛狀態，切換裸K不弄丟個別 `visible` 設定
（因 `visible` 與 `nakedK` 正交）。

### C. 模組化收尾

- **消滅 `IND_NAMES`（`chart.js:34`）**：`alertText`（`chart.js:37-38`）改由
  `QQIndicators.byKey(name)?.klineName ?? name` 取短名（三者值本就相同），刪除該手動同步表。
  之後新增 alertTarget 指標無需再改此處。
- **`visible` 全流程納管**：確保 `defaults/merge/saveSettings` 三處一致，加指標時 `visible` 自動生效。
- 結果：新增一個 KLineCharts 內建指標＝只在 `REGISTRY` 加一筆；若要當警示目標，僅需設 `alertTarget:true`
  （不必再改任何硬編表）。

### D. 不做（YAGNI）

- 不做 KDJ/BOLL 的警示比較目標（多線選單 UI 這次不碰）。
- 不改 KLineCharts 版本（釘 v9.8.12）。
- 不動 candle 讀取路徑（raw-SQL→FastCandle、sync route）與後端結構（settings 為 JSON blob，`visible`
  併入即可，無 schema migration）。

## 測試計畫

- `tests/js/indicators.test.mjs`：
  - KDJ/BOLL 存在，`pane`/`klineName`/`repeatable` 正確；`calcParams` 分別為 `[20,2]`、`[9,3,3]`。
  - `defaults()` 每指標帶 `visible:true`。
  - `merge(saved)` 保留使用者存的 `visible`（含 false）。
- 新增 `resolveVisibility` 的 node 斷言：enabled×visible×nakedK 八組合結果正確（比照 `chart-guards` 測試風格）。
- `uv run pytest` 全綠（79+），確認後端 settings 存取不受 `visible` 影響。

## 風險與防線

- **KLineCharts v9 靜默卡死**：新增指標沿用現有五道渲染防線（資料先於指標、完整 line style、逐格建 pane、
  每指標 try/catch、watchdog 自癒），不移除。
- **裸K與扣抵/警示交互**：以純函式 `resolveVisibility` 集中判定並單測，避免散落條件式。
- **後端相容**：`visible` 缺省即 `true`，舊存檔經 `merge` 自動補上，向後相容。
