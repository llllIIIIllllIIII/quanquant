# 指標線顏色（MACD/BOLL/KDJ 子線調色 + 恢復預設）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 讓固定線型指標 MACD/BOLL/KDJ 的每條子線可由使用者調色（跟帳號），並提供每指標「恢復預設色」。

**Architecture:** 延續 registry 驅動——固定指標條目新增 `lines` 描述子線（label+預設 hex），conf 新增 `colors` 陣列（比照 `visible` 隨既有 PUT 持久化）。引擎 `_applyIndicator` 對有 `lines` 的固定指標套 `override.styles.lines`（重用 `_lineStyle`），以 `entry.lines` 為長度來源、`conf.colors[i]` 缺值退回預設。表單自動生成每子線選色器 + 恢復鍵。後端零改動。

**Tech Stack:** vanilla JS UMD（`window.QQIndicators`）、Alpine.js、KLineCharts v9.8.12（CDN）、FastAPI、node:test、pytest。

## Global Constraints

- KLineCharts 釘版 v9.8.12 不升；`chart.js` 五道渲染防線一道都不得移除或弱化。
- `_lineStyle` 必須回**完整** style 物件（傳部分物件會讓 painter 崩，第 405-408 行防線）；新增上色一律走 `_lineStyle`。
- 可上色線與順序（對齊 KLineCharts line figure，`styles.lines[i]` 自動跳過 bar）：`macd`=[dif, dea]（2）、`boll`=[up, mid, dn]（3）、`kdj`=[k, d, j]（3）。MACD 柱（bar）不上色。
- 預設 hex 種子自 KLineCharts v9.8.12 預設 line 調色盤：palette[0]=`#FF9600`、[1]=`#935EBD`、[2]=`#1677FF`；各指標線內從 palette[0] 起算（外觀不變）。
- `colors` 跟帳號，隨既有 `saveSettings` 深拷貝 → `PUT /api/chart/state/indicators`；後端整包 JSON、**無 schema migration**。
- `merge()` 維持通用 shallow-spread（`colors` 比照 `visible` 隨帶，不特例化）；長度安全在引擎消費點處理。
- MA/WR/BIAS（repeatable，已有色）不動；VOL（沿用全域紅漲/綠漲）不動。
- locale 繁體 zh-TW，全站中文繁體台灣；commit 格式 `<type>: <描述>`，attribution 全域停用；改 static JS/CSS 後實測前務必硬重載（Cmd+Shift+R）清快取。
- 每 task 只 scoped `git add` 該 task 檔案，嚴禁 `git add -A`/`git add .`。

## File Structure

- `src/quanquant/web/static/indicators.js` — registry 加 `lines`＋`colors` defaults，新增並匯出 `defaultColors(entry)`（Task 1）。
- `tests/js/indicators.test.mjs` — lines/colors/defaultColors/merge 斷言（Task 1）。
- `src/quanquant/web/static/chart.js` — `_applyIndicator` 加 fixed-lines 上色分支（Task 2）。
- `src/quanquant/web/templates/dashboard.html`、`src/quanquant/web/static/app.css` — 每子線選色器 + 恢復預設色（Task 3）。
- `tests/test_api_candles.py` — colors round-trip 回歸測試（Task 4）。
- 驗證 + 記憶（Task 5）。

---

### Task 1: registry 加 lines/colors + defaultColors 純函式

**Files:**
- Modify: `src/quanquant/web/static/indicators.js`（macd L55-64、boll L65-73、kdj L74-83；defaults/merge L91-106；export L134）
- Test: `tests/js/indicators.test.mjs`

**Interfaces:**
- Produces: `byKey('boll').lines` = 3 筆 `{key,label,default}`；`byKey('macd').lines` = 2 筆；`byKey('kdj').lines` = 3 筆。
- Produces: `defaults().boll.colors` = `["#FF9600","#935EBD","#1677FF"]`（macd 2 色、kdj 3 色）。
- Produces: `QQIndicators.defaultColors(entry) => string[]`（有 lines 回預設陣列，否則 `[]`）。

- [ ] **Step 1: 寫失敗測試**

在 `tests/js/indicators.test.mjs` 檔末追加：

```javascript
test('macd/boll/kdj 有 lines 描述，長度 2/3/3、label 正確', () => {
  assert.deepEqual(QQI.byKey('macd').lines.map((l) => l.label), ['DIF', 'DEA']);
  assert.deepEqual(QQI.byKey('boll').lines.map((l) => l.label), ['上軌', '中軌', '下軌']);
  assert.deepEqual(QQI.byKey('kdj').lines.map((l) => l.label), ['K', 'D', 'J']);
  assert.deepEqual(QQI.byKey('boll').lines.map((l) => l.key), ['up', 'mid', 'dn']);
});

test('defaults() 三者 colors 種子＝各 lines 預設（palette 起算，外觀不變）', () => {
  const d = QQI.defaults();
  assert.deepEqual(d.macd.colors, ['#FF9600', '#935EBD']);
  assert.deepEqual(d.boll.colors, ['#FF9600', '#935EBD', '#1677FF']);
  assert.deepEqual(d.kdj.colors, ['#FF9600', '#935EBD', '#1677FF']);
});

test('defaultColors: 有 lines 回預設陣列、無 lines（ma/vol）回 []', () => {
  assert.deepEqual(QQI.defaultColors(QQI.byKey('boll')), ['#FF9600', '#935EBD', '#1677FF']);
  assert.deepEqual(QQI.defaultColors(QQI.byKey('ma')), []);
  assert.deepEqual(QQI.defaultColors(QQI.byKey('vol')), []);
  assert.deepEqual(QQI.defaultColors(null), []);
  // 與 defaults 種子一致（防漂移）
  assert.deepEqual(QQI.defaults().boll.colors, QQI.defaultColors(QQI.byKey('boll')));
});

test('merge 保留使用者存的 colors；缺 colors → 補預設（通用 shallow-spread）', () => {
  const saved = { boll: { enabled: true, visible: true, params: { period: 20, std: 2 }, colors: ['#111111', '#222222', '#333333'] } };
  const m = QQI.merge(saved);
  assert.deepEqual(m.boll.colors, ['#111111', '#222222', '#333333']);
  assert.deepEqual(m.kdj.colors, ['#FF9600', '#935EBD', '#1677FF']); // 未存 → 預設
});
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `node --test tests/js/indicators.test.mjs`
Expected: FAIL —`lines`/`colors`/`defaultColors` 尚未存在。

- [ ] **Step 3: registry 加 lines + colors defaults**

在 `indicators.js` 把 macd 條目（L55-64）替換為：

```javascript
    {
      key: "macd", title: "MACD", hint: "副圖",
      pane: "sub", klineName: "MACD", alertTarget: false, repeatable: false,
      paramSchema: [
        { field: "fast", type: "number", label: "快線", min: 1, step: 1 },
        { field: "slow", type: "number", label: "慢線", min: 1, step: 1 },
        { field: "signal", type: "number", label: "訊號", min: 1, step: 1 },
      ],
      lines: [
        { key: "dif", label: "DIF", default: "#FF9600" },
        { key: "dea", label: "DEA", default: "#935EBD" },
      ],
      defaults: { enabled: false, visible: true, params: { fast: 12, slow: 26, signal: 9 }, colors: ["#FF9600", "#935EBD"] },
    },
```

把 boll 條目（L65-73）替換為：

```javascript
    {
      key: "boll", title: "布林通道 BOLL", hint: "疊於主圖",
      pane: "main", klineName: "BOLL", alertTarget: false, repeatable: false,
      paramSchema: [
        { field: "period", type: "number", label: "週期", min: 2, step: 1 },
        { field: "std", type: "number", label: "標準差", min: 1, step: 1 },
      ],
      lines: [
        { key: "up", label: "上軌", default: "#FF9600" },
        { key: "mid", label: "中軌", default: "#935EBD" },
        { key: "dn", label: "下軌", default: "#1677FF" },
      ],
      defaults: { enabled: false, visible: true, params: { period: 20, std: 2 }, colors: ["#FF9600", "#935EBD", "#1677FF"] },
    },
```

把 kdj 條目（L74-83）替換為：

```javascript
    {
      key: "kdj", title: "KDJ", hint: "副圖",
      pane: "sub", klineName: "KDJ", alertTarget: false, repeatable: false,
      paramSchema: [
        { field: "k", type: "number", label: "K", min: 1, step: 1 },
        { field: "d", type: "number", label: "D", min: 1, step: 1 },
        { field: "j", type: "number", label: "J", min: 1, step: 1 },
      ],
      lines: [
        { key: "k", label: "K", default: "#FF9600" },
        { key: "d", label: "D", default: "#935EBD" },
        { key: "j", label: "J", default: "#1677FF" },
      ],
      defaults: { enabled: false, visible: true, params: { k: 9, d: 3, j: 3 }, colors: ["#FF9600", "#935EBD", "#1677FF"] },
    },
```

> 註：boll/kdj 的 `title`/`hint`/`paramSchema` 請以檔案現有值為準（上面照現況謄寫）；若現況與此處有出入，保留現況、只加 `lines` 與 `defaults.colors` 兩處。

- [ ] **Step 4: 加 defaultColors 並匯出**

在 `indicators.js` `merge()` 之後（L106 附近）新增：

```javascript
  function defaultColors(entry) {
    return (entry && entry.lines) ? entry.lines.map((l) => l.default) : [];
  }
```

把 export（L134）補上 `defaultColors`：

```javascript
  return { list: REGISTRY, byKey, byKlineName, defaults, merge, alertTargets, calcParams, resolveVisibility, shortName, defaultColors };
```

- [ ] **Step 5: 跑測試確認通過**

Run: `node --test tests/js/indicators.test.mjs`
Expected: PASS（全部）。

- [ ] **Step 6: Commit**

```bash
git add src/quanquant/web/static/indicators.js tests/js/indicators.test.mjs
git commit -m "feat: 指標 registry 加 lines/colors 與 defaultColors 純函式"
```

---

### Task 2: 引擎 _applyIndicator 固定線型上色分支

**Files:**
- Modify: `src/quanquant/web/static/chart.js`（`_applyIndicator` L474+，repeatable 分支 L484-486）

**Interfaces:**
- Consumes: `entry.lines`、`conf.colors`（Task 1）；`this._lineStyle`（既有 L409-411）。

- [ ] **Step 1: 在 repeatable 分支後加 fixed-lines 分支**

在 `chart.js` 把 L484-486：

```javascript
          if (entry.repeatable) {
            override.styles = { lines: conf.params.map((p) => this._lineStyle(p.color)) };
          }
```

替換為：

```javascript
          if (entry.repeatable) {
            override.styles = { lines: conf.params.map((p) => this._lineStyle(p.color)) };
          } else if (entry.lines) {
            // 固定線型指標（MACD/BOLL/KDJ）：每條子線依 colors 上色，順序對齊 KLineCharts line figure；
            // 以 entry.lines 為長度來源，colors 缺值退回 registry 預設（長度安全，毋須動 merge）。
            override.styles = { lines: entry.lines.map((ln, i) => this._lineStyle((conf.colors && conf.colors[i]) || ln.default)) };
          }
```

（其餘 create/override/remove、可見性 `resolveVisibility`、try/catch、五道防線一律不動。）

- [ ] **Step 2: 語法自檢**

Run: `node --check src/quanquant/web/static/chart.js`
Expected: 無輸出。

- [ ] **Step 3: Commit**

```bash
git add src/quanquant/web/static/chart.js
git commit -m "feat: chart 引擎對固定線型指標套子線顏色 override"
```

---

### Task 3: UI——每子線選色器 + 恢復預設色

**Files:**
- Modify: `src/quanquant/web/templates/dashboard.html`（右欄固定參數區塊，L266-275 一帶，active entry 綁為 `e`）
- Modify: `src/quanquant/web/static/app.css`（末端追加樣式）

**Interfaces:**
- Consumes: `e.lines`、`form[e.key].colors`（Task 1 defaults/merge 保證存在）、`window.QQIndicators.defaultColors(e)`。

- [ ] **Step 1: 加每子線色列 + 恢復預設鍵**

在 `dashboard.html` 右欄、固定參數 `x-if="!e.repeatable && e.paramSchema.length"` 區塊（L266-275）**之後**，插入：

```html
              <!-- 固定線型指標（MACD/BOLL/KDJ）：每條子線顏色 + 恢復預設 -->
              <template x-if="e.lines && e.lines.length">
                <div class="ind-colors">
                  <template x-for="(ln, i) in e.lines" :key="ln.key">
                    <label class="ind-color-row">
                      <span x-text="ln.label"></span>
                      <input type="color" x-model="form[e.key].colors[i]">
                    </label>
                  </template>
                  <button type="button" class="outline mini"
                          @click="form[e.key].colors = window.QQIndicators.defaultColors(e)">恢復預設色</button>
                </div>
              </template>
```

> 若 `form[e.key].colors` 尚未存在會導致 `x-model` 綁定失敗——Task 1 已在 defaults/merge 確保三個指標的 conf 皆帶 `colors` 陣列，故正常。

- [ ] **Step 2: 加樣式**

在 `src/quanquant/web/static/app.css` 末端追加：

```css
/* 指標子線調色：label＋原生選色器逐列，恢復鍵置底 */
.ind-colors { display: flex; flex-direction: column; gap: 6px; margin-top: 8px; }
.ind-color-row { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
.ind-color-row input[type="color"] { width: 40px; height: 24px; padding: 0; border: none; background: none; cursor: pointer; }
```

- [ ] **Step 3: 語法自檢（chart.js 未動，僅確認無誤觸）**

Run: `node --check src/quanquant/web/static/chart.js`
Expected: 無輸出。（HTML/CSS 由 Task 5 手動瀏覽器實測；記得硬重載清快取。）

- [ ] **Step 4: Commit**

```bash
git add src/quanquant/web/templates/dashboard.html src/quanquant/web/static/app.css
git commit -m "feat: 指標設定加每子線選色器與恢復預設色"
```

---

### Task 4: 後端 colors round-trip 回歸測試

**Files:**
- Modify: `tests/test_api_candles.py`（chart-state 區塊，`test_chart_state_preserves_visible_field` 之後）

**Interfaces:**
- Consumes: 既有 `client` fixture；`PUT/GET /api/chart/state`（不改任何 API）。

- [ ] **Step 1: 寫測試**

在 `tests/test_api_candles.py` 的 `test_chart_state_preserves_visible_field` 之後新增：

```python
def test_chart_state_preserves_colors_field(client):
    # 子線顏色（colors 陣列）跟帳號：payload 整包 JSON 存取，colors 須原樣往返。
    ind = {
        "boll": {"enabled": True, "visible": True, "params": {"period": 20, "std": 2},
                 "colors": ["#111111", "#222222", "#333333"]},
        "macd": {"enabled": True, "visible": True, "params": {"fast": 12, "slow": 26, "signal": 9},
                 "colors": ["#aabbcc", "#ddeeff"]},
    }
    assert client.put("/api/chart/state/indicators", json=ind).status_code == 204
    got = client.get("/api/chart/state").json()["indicators"]
    assert got == ind
    assert got["boll"]["colors"] == ["#111111", "#222222", "#333333"]
```

- [ ] **Step 2: 跑測試確認通過**

Run: `uv run pytest tests/test_api_candles.py -q`
Expected: PASS（含新測試）。

- [ ] **Step 3: Commit**

```bash
git add tests/test_api_candles.py
git commit -m "test: 補 chart-state colors 欄位 round-trip 回歸測試"
```

---

### Task 5: 整合驗證 + 更新記憶

**Files:** 無（驗證任務，Step 4 除外）

- [ ] **Step 1: node 前端測試全綠**

Run: `node --test tests/js/*.test.mjs tests/js/*.test.js`
Expected: PASS（`indicators.test.mjs` 含新斷言 + `chart-guards`/`ma_deduction`）。
> 註：`node --test tests/js/`（傳目錄）在 node 24 會 MODULE_NOT_FOUND，須用上面的 glob。

- [ ] **Step 2: 後端測試全綠**

Run: `uv run pytest`
Expected: PASS（270+，全綠）。

- [ ] **Step 3: 實跑 app 手動驗證（硬重載清快取）**

Run: `uv run quanquant-web` → http://127.0.0.1:8000 登入後，**Cmd+Shift+R 硬重載**，逐項確認：
1. 啟用 BOLL → 設定 dialog 右欄出現「上軌/中軌/下軌」三個選色器 + 「恢復預設色」；改上軌顏色 → 儲存 → 主圖上軌變色；重整後仍是新色（跟帳號）。
2. 啟用 MACD → 出現 DIF/DEA 兩選色器（無柱色）；改色儲存生效；MACD 柱維持預設。
3. 啟用 KDJ → K/D/J 三選色器；改色生效。
4. 點「恢復預設色」→ 三色回 `#FF9600/#935EBD/#1677FF`，圖表回預設外觀。
5. MA/WR/BIAS 既有每條線調色不受影響；VOL 無選色器（沿用全域紅漲/綠漲）。
6. 圖表無空白卡死（五道防線/watchdog 生效）。

Expected: 全部符合。

- [ ] **Step 4: 更新記憶**

更新 `project_indicator_modularization.md`：記錄 2026-07-12 固定線型指標 MACD/BOLL/KDJ 每子線調色（`lines` 描述 + `colors` conf + `defaultColors` + 恢復預設；引擎以 `entry.lines` 為長度來源）。同步 `MEMORY.md` 該行 hook。

- [ ] **Step 5:（選）部署** — 需使用者明確指示才執行：`git push` → `./scripts/deploy.sh`（部署前 Step 1/2 全綠）。

---

## Self-Review

**Spec 覆蓋**：
- 固定線型 MACD/BOLL/KDJ 每子線調色 → Task 1（lines/colors）+ Task 2（引擎）+ Task 3（UI）。✅
- 恢復預設色 → Task 1（defaultColors）+ Task 3（按鈕）。✅
- 順序/預設 hex 釘死（macd dif/dea、boll up/mid/dn、kdj k/d/j；palette #FF9600/#935EBD/#1677FF）→ Global Constraints + Task 1。✅
- 持久化跟帳號、後端零改動 → Task 4 round-trip 驗證。✅
- MA/WR/BIAS/VOL 不動、柱不上色 → Task 2 分支互斥（僅 `entry.lines`）、Global Constraints。✅
- 測試（lines/colors/defaultColors/merge、pytest round-trip）→ Task 1/4/5。✅

**Placeholder 掃描**：無 TBD/TODO；每個改碼步驟均附完整程式碼與確切行號，palette hex 已釘死。

**型別/命名一致**：`defaultColors(entry)` Task 1 定義/匯出、Task 3 呼叫一致；`entry.lines`/`conf.colors` 跨 Task 1/2/3 一致；引擎以 `entry.lines.map` 為長度來源、`conf.colors[i]||ln.default` 退階與 merge 通用性相容。
