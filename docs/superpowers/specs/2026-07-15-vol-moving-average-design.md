# VOL 量能均線（可重複線 + 可調參數）— 設計文件

**日期**：2026-07-15
**狀態**：已核准，待 writing-plans
**相關記憶**：`project_indicator_modularization.md`（registry 化 `window.QQIndicators`；已有 repeatable 線指標 MA/WR/BIAS、visible/裸K、固定線型調色）

## 背景與目標

VOL 指標目前 `repeatable:false`、`paramSchema:[]`、`params:{}`——不傳任何 calcParams，KLineCharts 只畫量柱、沒有均線。需求：在 VOL 疊加**量能均線**，並讓使用者在指標設定**調整均線參數（週期）**、自由增減條數。

**目標**：VOL 支援使用者可增減的量能均線（每條週期+顏色可調），量柱維持全站紅漲/綠漲全域配色不變。

## 現況要點（調查結論，KLineCharts v9.8.12）

- **VOL 原生支援動態 MA**：KLineCharts `volume.ts` 有 `regenerateFigures(params)`（同 WR/BIAS 範式），依 `calcParams.length` 生成 N 條 `MA{period}` line figure，末端再接 1 條 volume bar figure。傳 N 個週期 → N 條 MA 線。
- **量柱獨立**：volume bar 跟隨 K 棒漲跌（全域紅漲/綠漲），非 line figure；`override.styles.lines[i]` 只對應第 i 條 **line** figure（bar 自動跳過），故對 MA 線上色不會動到量柱。
- **現有 repeatable 機制可直接重用**：`_applyIndicator` 的 repeatable 分支已 `override.styles = { lines: conf.params.map(p => this._lineStyle(p.color)) }`；`calcParams`（indicators.js）repeatable 分支回 `conf.params.map(p => Math.round(p.period))` 即 KLineCharts VOL 要的週期陣列。chart.js 無任何 VOL 特例分支。
- **UI 自動化**：設定 dialog 右欄依 `e.repeatable` 生成「週期+顏色+加一條/刪除」編輯器；VOL 變 repeatable 後自動套用，顏色隨每條線免費具備。

## VOL 的特殊性（設計關鍵）

VOL 與純線指標（MA/WR/BIAS）不同：**除 MA 線外還有量柱本體**。現有可見性規則「repeatable 指標需 ≥1 條線才 active」對純線正確，但對 VOL 會導致「0 條 MA 線時連量柱一起消失」。且既有使用者存檔 VOL 的 `params` 是 `{}`（舊固定形態），與 repeatable 期望的陣列型別不符。故需兩處小處理：

1. **`bars` 旗標**：VOL registry 加 `bars:true`，可見性判定改「repeatable 指標：有 `bars` 則 enabled 即 active（量柱恆畫）；否則需 ≥1 條線」。
2. **型別防呆**：可見性/`calcParams`/引擎對 `conf.params` 一律 `Array.isArray` 守衛，舊存檔 `{}` 安全視為 `[]`。

## 已定決策（brainstorming 拍板）

- **形態**：可重複線（同 MA/WR/BIAS），每條 `{period, color}`，可增減。
- **預設**：MA5 / MA10 兩條（各帶預設色）；VOL 維持 `enabled:true/visible:true/alertTarget:false`。
- **量柱**：維持全域紅漲/綠漲，不受 MA 線上色影響。
- **既有使用者遷移（非破壞）**：舊存檔 `params:{}` → 防呆當 `[]` → 量柱照顯示、暫無 MA 線，使用者自行加；新使用者才拿到 MA5/MA10 預設。

## 設計

### A. registry（indicators.js）

VOL 條目改為：
```
{
  key: "vol", title: "成交量 VOL", hint: "副圖",
  pane: "sub", klineName: "VOL", alertTarget: false,
  repeatable: true, bars: true,
  paramSchema: [ {period 週期}, {color 顏色} ],   // 同 MA/WR/BIAS
  defaults: { enabled: true, visible: true, params: [ {period:5, color:<c1>}, {period:10, color:<c2>} ] },
}
```
預設色 `<c1>/<c2>` 於 plan 釘死（沿用 MA 系配色，如 #f0b90b / #935EBD），與現有可重複線風格一致。

### B. 可見性純函式 `resolveVisibility`（indicators.js）

repeatable 分支由 `(conf.params||[]).length > 0` 改為：
```
entry.bars || (Array.isArray(conf.params) && conf.params.length > 0)
```
語意：VOL（bars:true）enabled 即 active（量柱恆畫，含 0 條 MA）；MA/WR/BIAS（無 bars）維持需 ≥1 條線。nakedK/visible 判定不變。

### C. calcParams（indicators.js）與引擎（chart.js）

- `calcParams` repeatable 分支加 `Array.isArray` 守衛：非陣列（舊 `{}`）→ 回 `[]`（VOL 只畫量柱）。
- `_applyIndicator` repeatable 分支的 `conf.params.map(...)` 同樣 `Array.isArray` 守衛，避免舊存檔 `{}` 觸發 `.map` 例外。

### D. UI

**零改動**：VOL 變 repeatable 後自動套用現成 repeatable 編輯器（週期數字 + 原生選色器 + 加一條/刪除）。原「此指標無可調參數」不再命中 VOL。

### E. 持久化 / 後端

VOL 的 `params`（陣列）隨既有 `saveSettings` 深拷貝 → `PUT /api/chart/state/indicators`，與其他 repeatable 指標同路徑。**後端零改動、無 schema migration**。

## 不做（YAGNI）

- VOL 量柱獨立配色（維持全域紅漲/綠漲）。
- VOL MA 線作為警示標的（`alertTarget:false`）。
- 線寬/線型調整（沿用 `_lineStyle` 固定值）。
- 主動把既有使用者存檔遷移成 MA5/MA10（採非破壞：舊存檔維持量柱 only）。
- KLineCharts 升版（釘 v9.8.12）。

## 測試計畫

- `tests/js/indicators.test.mjs`：
  - VOL `repeatable:true`、`bars:true`；`defaults().vol.params` 為 MA5/MA10 兩筆 `{period,color}`。
  - `resolveVisibility`：VOL enabled + 0 條線 → **true**（bars）；MA enabled + 0 條線 → false（無 bars）；VOL visible:false/nakedK → false。
  - `calcParams(vol, {params:{}})` → `[]`（型別防呆，遷移安全）；`calcParams(vol, {params:[{period:5},{period:10}]})` → `[5,10]`。
  - `merge`：舊存檔 `vol:{...,params:{}}` 載入不炸（保留 `{}`，由防呆處理）；未存 VOL → 預設 MA5/MA10。
- 引擎映射（VOL MA 線上色、量柱不受影響）屬 DOM/KLineCharts，`node --check` + **手動瀏覽器實測**（改 static 後硬重載 Cmd+Shift+R 清快取）。
- `uv run pytest` 全綠（270+，確認 VOL params 陣列 round-trip 不受影響——既有 chart-state round-trip 已涵蓋）。

## 風險與防線

- **既有使用者 VOL 消失**：`bars` 旗標確保 0 條線仍 active；`Array.isArray` 守衛確保舊 `{}` 不炸——兩者皆有單測把關。
- **量柱被誤染**：`override.styles.lines` 僅對 line figure，量柱為 bar figure 自動跳過；不設 `styles.bars`。
- **可見性純函式回歸**：`resolveVisibility` 為既有前端防線核心，改動以完整真值表單測（VOL×bars×0線、MA×0線、nakedK、visible）。
- **五道渲染防線**：僅在既有 repeatable 分支加 `Array.isArray` 守衛，不移除任何防線；`_lineStyle` 回完整物件。
- **快取幽靈**：改 static 後實測前硬重載。
