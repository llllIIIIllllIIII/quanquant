# 指標線顏色（固定指標 MACD/BOLL/KDJ 每條子線可調色 + 恢復預設）— 設計文件

**日期**：2026-07-12
**狀態**：已核准，待 writing-plans
**相關記憶**：`project_indicator_modularization.md`（registry 化 `window.QQIndicators`；2026-07-12 已加 visible/裸K/KDJ/BOLL、消滅 IND_NAMES）

## 背景與目標

指標表單由 registry 的 `paramSchema` 自動生成。**可重複指標（MA/WR/BIAS）本來每條線就帶 `color`**（`paramSchema` 含 color 欄、`<input type="color">` 編輯、每條 `{period,color}`），使用者早就能各別調色。缺顏色設定的只有**固定（非可重複）線型指標**：目前 `_applyIndicator` 只對 repeatable 指標套 `override.styles.lines`，MACD/BOLL/KDJ 走 KLineCharts 預設色、使用者無法改。

**目標**：讓 MACD/BOLL/KDJ 的每條子線可由使用者設定顏色（跟帳號、透過現有自動生成表單），並提供每指標「恢復預設色」。MA/WR/BIAS 已有色不動；VOL 沿用全站紅漲/綠漲全域配色不動。

## 現況要點（調查結論，KLineCharts v9.8.12）

- **repeatable 已有色**：`wr`（預設 2 條）、`bias`（預設 3 條）與 `ma` 同為 repeatable，`paramSchema=[period,color]`，`_applyIndicator`（`chart.js:486` 一帶）已 `override.styles.lines = conf.params.map(p => this._lineStyle(p.color))`。**不在本次範圍**。
- **固定線型指標缺色**（本次目標，line figure 順序即 `styles.lines[i]` 對應順序）：
  - `macd`（fixed）：line figures `[dif, dea]`（2 條）；另有 `macd` 柱（bar，**排除**，沿用預設）。
  - `boll`（fixed）：line figures `[up, mid, dn]`（3 條）。
  - `kdj`（fixed，KLineCharts 名 `KDJ`）：line figures `[k, d, j]`（3 條）。
- **覆寫結構**（`Indicator.ts::eachFigures`）：`overrideIndicator({ name, styles: { lines: [...] } })`，`styles.lines[i]` 由**只數 line 型 figure** 的計數器索引、**自動跳過 bar/circle**——故 MACD 的 `lines[0]→dif、lines[1]→dea`，柱另走 `styles.bars` 不受影響。
- **`_lineStyle(color)`**（`chart.js:409-411`）回**完整** style 物件 `{ color, size:1, style:"solid", smooth:false, dashedValue:[2,2] }`（`chart.js:405-408` 註明：傳部分物件會讓 painter 崩，四道防線之一）。本次沿用，不得回部分物件。
- **持久化**：指標 conf 為一包 JSON，隨 `saveSettings` 深拷貝 → `PUT /api/chart/state/indicators`（`candles.py` 整包 wholesale 存取、無 Pydantic 過濾），任意欄位原樣往返（已由 visible 功能實證）。

## 已定決策（brainstorming 拍板）

- **範圍**：只補固定線型指標 MACD/BOLL/KDJ，每條子線各一個原生選色器。MA/WR/BIAS 不動（已有色）、VOL 不動（沿用全域）、所有柱狀（VOL/MACD 柱）本輪不做。
- **控制項**：重用 MA 的 `<input type="color">`（原生選色器），視覺一致。
- **恢復預設色**：每個（有子線色的）指標一個「恢復預設色」動作，一鍵把該指標所有子線色還原為 registry 宣告的預設。
- **預設色**：registry 為每條子線宣告預設 hex，**種子成 KLineCharts v9.8.12 預設 line 調色盤對應色**（dif/up/k = 調色盤[0]、dea/mid/d = [1]、dn/j = [2]），使未編輯前外觀與現況一致。實際調色盤 hex 於 plan 階段自 KLineCharts 預設 `styles.indicator.lines` 釘死。

## 設計

### A. 資料模型（registry 驅動）

固定線型指標（macd/boll/kdj）registry 條目新增 `lines` 描述——有序陣列宣告可上色子線，順序**對齊 KLineCharts line figure 順序**：

```
boll.lines = [
  { key: "up",  label: "上軌", default: "<palette[0]>" },
  { key: "mid", label: "中軌", default: "<palette[1]>" },
  { key: "dn",  label: "下軌", default: "<palette[2]>" },
]
macd.lines = [ { key:"dif", label:"DIF", default:"<palette[0]>" },
               { key:"dea", label:"DEA", default:"<palette[1]>" } ]
kdj.lines  = [ { key:"k", label:"K", default:"<palette[0]>" },
               { key:"d", label:"D", default:"<palette[1]>" },
               { key:"j", label:"J", default:"<palette[2]>" } ]
```

conf 新增 `colors`（與 `lines` 對齊的 hex 陣列），與 `enabled/visible/params` 同級：

- `defaults()`：對有 `lines` 的指標，`colors` 種子為各 `lines[i].default`（外觀不變）。
- `merge(saved)`：缺 `colors` 補預設；長度不符（未來增/減子線）以預設補齊到 `lines.length`，多餘截斷。向後相容（舊存檔無 colors → 預設）。
- 純函式 `defaultColors(entry) => string[]`（回 `entry.lines?.map(l => l.default) ?? []`）集中預設來源，供 defaults/merge/「恢復預設」共用，可 node 單測。

### B. 引擎（`chart.js` `_applyIndicator`）

現況 `override.styles.lines` 只在 `entry.repeatable` 分支設定。改為：**repeatable → 依 params 逐條（現況不變）；有 `entry.lines` 的固定指標 → `override.styles.lines = conf.colors.map(c => this._lineStyle(c))`**。兩分支互斥（repeatable 指標無 `lines` 描述）。其餘 create/override/remove 與可見性（`resolveVisibility`）流程不動；MACD 柱不設 `styles.bars`，沿用預設。

### C. UI（自動生成表單 + 恢復預設）

- 表單渲染器擴充：`form[e.key]` 對應指標若有 `entry.lines`，在數值參數下方渲染每條子線一列 `label + <input type="color" x-model="form[e.key].colors[i]">`（label 取 `lines[i].label`）。重用 MA 原生選色控制項。
- **恢復預設色**：該區塊一個「恢復預設色」按鈕，`@click` 將 `form[e.key].colors` 重設為 `QQIndicators.defaultColors(entry)`（回傳新陣列以維持 Alpine reactivity）。
- 顏色改動隨 `saveSettings` 一起 PUT（跟帳號）；即時/批次比照現有表單行為（Save 後生效，與 enabled/params/visible 一致）。

### D. 持久化

`colors` 隨既有 `saveSettings` 深拷貝 → `PUT /api/chart/state/indicators`，與 visible/MA 顏色同路徑。**後端零改動、無 schema migration**。

## 不做（YAGNI）

- VOL 量柱色（沿用全域紅漲/綠漲）、MACD 柱色、任何 bar/circle figure 色。
- 線寬/線型/平滑度調整（`_lineStyle` 其餘欄位維持固定）。
- MA/WR/BIAS 既有配色流程改動。
- 主題預設組、全域套色。
- KLineCharts 升版（釘 v9.8.12）。

## 測試計畫

- `tests/js/indicators.test.mjs`：
  - `macd/boll/kdj` 各有 `lines`，`lines.length` 與宣告一致（2/3/3），label/key 正確。
  - `defaults()` 對三者帶 `colors`，長度＝`lines.length` 且等於各 `default`。
  - `merge(saved)`：保留使用者存的 `colors`；缺 `colors` 補預設；長度不符補齊/截斷到 `lines.length`。
  - `defaultColors(entry)` 回正確陣列；無 `lines` 的指標回 `[]`。
- 引擎映射（`override.styles.lines` 對齊順序）屬 DOM/KLineCharts，`node --check` + **手動瀏覽器實測**（改 static 後務必硬重載 Cmd+Shift+R 清快取）。
- `uv run pytest`：`colors` 隨 indicators payload round-trip（擴充既有 chart-state round-trip 測試涵蓋 colors）。全綠（270+）。

## 風險與防線

- **順序錯位**：`colors[i]` 必須對齊 KLineCharts line figure 順序（macd dif→dea、boll up→mid→dn、kdj k→d→j）；plan 釘死順序與預設 hex，測試以 `lines` 宣告把關。
- **部分 style 崩 painter**：一律走 `_lineStyle` 回完整物件（第 405-408 行防線），不得傳 `{color}` 部分物件。
- **五道渲染防線**：新增只在 `_applyIndicator` 既有 try/catch 內加一分支，不移除任何防線；KLineCharts v9 靜默卡死沿用 watchdog 自癒。
- **後端相容**：`colors` 缺省經 `merge` 補預設，舊存檔向後相容；後端整包 JSON 不受影響。
- **快取幽靈 bug**：改 static JS/CSS 後實測前硬重載（本專案已踩過一次）。
