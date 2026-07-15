# VOL 量能均線（可重複線 + 可調參數）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 讓 VOL 指標支援使用者可增減的量能均線（每條週期+顏色可調），量柱維持全站紅漲/綠漲配色不變。

**Architecture:** 沿用既有 repeatable 線指標機制（MA/WR/BIAS）——把 VOL 從 `repeatable:false` 改成 `repeatable:true`，KLineCharts `volume.ts` 依 `calcParams` 動態生成 N 條 MA line figure，量柱本體（bar figure）獨立於 line figure 之外自動跳過上色。VOL 的特殊性在於「除 MA 線外還有量柱」，故加一個 `bars:true` 旗標讓「0 條 MA 線」時量柱仍畫；並在 `merge()` 加通用規則「repeatable ⇒ params 必為陣列」，安全遷移既有使用者的 `params:{}` 存檔（→ `[]`，只剩量柱、可自行加線）。

**Tech Stack:** Vanilla JS（UMD registry `window.QQIndicators` + Alpine.js chart controller）、KLineCharts v9.8.12（釘版）、node:test 純函式單測、FastAPI + pytest（後端 round-trip 回歸）。

## Global Constraints

- KLineCharts 釘版 **v9.8.12**，勿升版（v10 改 API 名）；`chart.js` 五道渲染防線勿移除。
- `_lineStyle(color)` 一律回**完整** style 物件 `{color, size:1, style:"solid", smooth:false, dashedValue:[2,2]}`（部分物件會讓 painter 靜默死在 requestAnimationFrame）。
- VOL 量柱維持**全域紅漲/綠漲**，不受 MA 線上色影響（不設 `styles.bars`；`override.styles.lines[i]` 只對應第 i 條 **line** figure，bar 自動跳過）。
- VOL MA 線**不進警示標的**（`alertTarget:false`）。
- `merge()` 維持通用（不為 VOL 特例分支）；新增的正規化是對**所有 repeatable 指標**成立的通用規則。
- 非破壞遷移：既有使用者存檔 `vol:{...,params:{}}` 載入後 → 量柱照顯示、無 MA 線，使用者自行加；只有新使用者（無 VOL 存檔）才拿到 MA5/MA10 預設。
- 後端**零改動、無 schema migration**（VOL params 陣列隨既有 `PUT /api/chart/state/indicators` 整包 JSON round-trip）。
- 改 static JS 後實測前務必**硬重載 Cmd+Shift+R** 清快取（本專案踩過「明明改了卻沒反應」＝快取幽靈）。
- node 單測用 glob：`node --test tests/js/*.test.mjs tests/js/*.test.js`（傳目錄在 node 24 會 MODULE_NOT_FOUND）。
- 每 task 只 scoped `git add` 該 task 的檔，嚴禁 `git add -A`/`git add .`；commit 格式 `<type>: <描述>`，attribution 全域停用（不加署名 footer）。
- 全站中文一律繁體台灣。

**VOL 預設均線（本 plan 釘死）：** MA5 = `#f0b90b`（金）、MA10 = `#935EBD`（紫）。

---

### Task 1: registry 核心（indicators.js）— VOL 可重複化 + merge 正規化 + 可見性/calcParams 防呆

把 VOL 改成 repeatable 線指標並補齊 3 個純函式的防呆。全部在 `indicators.js` + 一個 node 測試檔，node 可獨立驗證。

**Files:**
- Modify: `src/quanquant/web/static/indicators.js`（VOL 條目 L49-54；`merge` L111-120；`calcParams` repeatable 分支 L132-134；`resolveVisibility` repeatable 分支 L148）
- Test: `tests/js/indicators.test.mjs`（既有 201 行，新增 VOL 相關 test；注意既有 L16、L27-32、L82、L132 對 VOL 的舊斷言需同步更新，見 Step 1）

**Interfaces:**
- Consumes:（無上游 task）
- Produces:
  - VOL registry 條目：`{ key:"vol", title:"成交量 VOL", hint:"副圖", pane:"sub", klineName:"VOL", alertTarget:false, repeatable:true, bars:true, paramSchema:[period,color], defaults:{enabled:true, visible:true, params:[{period:5,color:"#f0b90b"},{period:10,color:"#935EBD"}]} }`
  - `merge(saved)` 新後置規則：對每個 `repeatable` 條目，若合併後 `params` 非陣列 → 設為 `[]`。
  - `resolveVisibility(entry, conf, nakedK)` repeatable 分支：`!!entry.bars || (Array.isArray(conf.params) && conf.params.length > 0)`
  - `calcParams(entry, conf)` repeatable 分支：`Array.isArray(conf.params)` 才 `.map`，否則回 `[]`
  - Task 2（chart.js 引擎）依賴：VOL 走 repeatable 分支、`calcParams` 回週期陣列、`resolveVisibility` 對 VOL（bars:true）0 線仍回 true。

- [ ] **Step 1: 更新既有 VOL 斷言為「新事實」的失敗測試**

既有測試把 VOL 當 fixed/無參數指標，改成 repeatable。改動下列既有斷言：

`tests/js/indicators.test.mjs` L16：
```javascript
  assert.equal(QQI.byKey('vol').repeatable, true);
```

L27-32 區塊（`defaults() 等同舊 DEFAULT_SETTINGS 結構`）中 `assert.equal(d.vol.enabled, true);` 之後補：
```javascript
  assert.equal(d.vol.enabled, true);
  assert.equal(Array.isArray(d.vol.params), true);
  assert.deepEqual(d.vol.params.map((p) => p.period), [5, 10]);
```

L82（`calcParams: 空/缺 conf 安全回空陣列`）該行語意從「fixed 無 number 欄位」變「repeatable 非陣列防呆」，斷言值不變 `[]`，保留即可：
```javascript
  assert.deepEqual(QQI.calcParams(QQI.byKey('vol'), { params: {} }), []);
```

L132（`resolveVisibility` 內對 VOL 的斷言與註解）改為 bars 語意：
```javascript
  // VOL（bars:true）enabled 即畫，含 0 條 MA 線；visible 預設缺省視為顯示
  assert.equal(QQI.resolveVisibility(vol, { enabled: true, params: {} }, false), true);
```

- [ ] **Step 2: 新增 VOL 專屬失敗測試**

在測試檔末端（既有末行 `});` 之後、檔尾）新增：
```javascript
test('vol 註冊為 repeatable+bars，預設 MA5/MA10', () => {
  const vol = QQI.byKey('vol');
  assert.equal(vol.repeatable, true);
  assert.equal(vol.bars, true);
  assert.equal(vol.pane, 'sub');
  assert.equal(vol.klineName, 'VOL');
  assert.equal(vol.alertTarget, false);
  assert.deepEqual(vol.paramSchema.map((f) => f.field), ['period', 'color']);
  assert.deepEqual(vol.defaults.params, [
    { period: 5, color: '#f0b90b' }, { period: 10, color: '#935EBD' },
  ]);
});

test('resolveVisibility: VOL bars 使 0 條線仍畫；MA 0 條線不畫', () => {
  const vol = QQI.byKey('vol');
  const ma = QQI.byKey('ma');
  // VOL enabled + 0 條 MA 線 → true（量柱恆畫）
  assert.equal(QQI.resolveVisibility(vol, { enabled: true, visible: true, params: [] }, false), true);
  // VOL 舊存檔 params:{} → true（防呆 + bars）
  assert.equal(QQI.resolveVisibility(vol, { enabled: true, visible: true, params: {} }, false), true);
  // MA（無 bars）0 條線 → false
  assert.equal(QQI.resolveVisibility(ma, { enabled: true, visible: true, params: [] }, false), false);
  // VOL visible:false → false；nakedK → false
  assert.equal(QQI.resolveVisibility(vol, { enabled: true, visible: false, params: [{ period: 5 }] }, false), false);
  assert.equal(QQI.resolveVisibility(vol, { enabled: true, visible: true, params: [{ period: 5 }] }, true), false);
});

test('calcParams VOL: 陣列回週期、非陣列（舊 {}）防呆回 []', () => {
  const vol = QQI.byKey('vol');
  assert.deepEqual(QQI.calcParams(vol, { params: [{ period: 5, color: '#a' }, { period: 10, color: '#b' }] }), [5, 10]);
  assert.deepEqual(QQI.calcParams(vol, { params: {} }), []);
  assert.deepEqual(QQI.calcParams(vol, { params: undefined }), []);
});

test('merge: repeatable 指標 params 非陣列一律正規化為 []（VOL 舊存檔遷移）', () => {
  // 既有使用者存檔 vol 為舊 fixed 形態 params:{}
  const m = QQI.merge({ vol: { enabled: true, visible: true, params: {} } });
  assert.equal(Array.isArray(m.vol.params), true);
  assert.deepEqual(m.vol.params, []);            // {} → [] → 只剩量柱、無 MA 線
  assert.equal(m.vol.enabled, true);
  // 未存 VOL → 拿預設 MA5/MA10
  const d = QQI.merge({ ma: { enabled: false } });
  assert.deepEqual(d.vol.params.map((p) => p.period), [5, 10]);
  // 使用者已存陣列 → 原樣保留
  const keep = QQI.merge({ vol: { enabled: true, params: [{ period: 20, color: '#123456' }] } });
  assert.deepEqual(keep.vol.params, [{ period: 20, color: '#123456' }]);
});
```

- [ ] **Step 3: 跑測試確認失敗**

Run: `node --test tests/js/*.test.mjs tests/js/*.test.js`
Expected: FAIL —「vol repeatable」等新斷言 fail（VOL 目前仍 `repeatable:false`、`params:{}`、`merge` 無正規化）。

- [ ] **Step 4: 改 VOL registry 條目**

`src/quanquant/web/static/indicators.js` L49-54 整段替換為：
```javascript
    {
      key: "vol", title: "成交量 VOL", hint: "副圖",
      pane: "sub", klineName: "VOL", alertTarget: false, repeatable: true, bars: true,
      paramSchema: [
        { field: "period", type: "number", label: "週期", min: 1, step: 1 },
        { field: "color", type: "color", label: "顏色" },
      ],
      defaults: { enabled: true, visible: true, params: [
        { period: 5, color: "#f0b90b" }, { period: 10, color: "#935EBD" },
      ] },
    },
```

- [ ] **Step 5: 在 merge() 加「repeatable ⇒ params 為陣列」正規化**

`indicators.js` `merge` 函式（L111-120）改為（在 for 迴圈合併後補正規化）：
```javascript
  function merge(saved) {
    const out = defaults();
    if (saved && typeof saved === "object") {
      for (const e of REGISTRY) {
        const s = saved[e.key];
        if (s && typeof s === "object") out[e.key] = { ...out[e.key], ...s };
      }
    }
    // repeatable 指標的 params 契約為線陣列；舊存檔（如 VOL 的 {}）正規化為 []，
    // 確保 UI「＋加一條」的 .push 與下游消費一律面對陣列。
    for (const e of REGISTRY) {
      if (e.repeatable && !Array.isArray(out[e.key].params)) out[e.key].params = [];
    }
    return out;
  }
```

- [ ] **Step 6: calcParams repeatable 分支加 Array.isArray 防呆**

`indicators.js` `calcParams`（L132-134）repeatable 分支改為：
```javascript
    if (entry.repeatable) {
      const arr = Array.isArray(conf.params) ? conf.params : [];
      return arr.map((p) => Math.round(p && p.period));
    }
```

- [ ] **Step 7: resolveVisibility repeatable 分支加 bars + Array.isArray**

`indicators.js` `resolveVisibility`（L148）該行改為：
```javascript
    if (entry.repeatable) return !!entry.bars || (Array.isArray(conf.params) && conf.params.length > 0);
```
同時把上方註解 `repeatable 需至少一條線` 補為：`repeatable 需至少一條線；有 bars（VOL）者 enabled 即畫`。

- [ ] **Step 8: 跑測試確認全綠**

Run: `node --test tests/js/*.test.mjs tests/js/*.test.js`
Expected: PASS，全部 test（既有 + 新增 4 個 VOL test）通過。

- [ ] **Step 9: 語法檢查**

Run: `node --check src/quanquant/web/static/indicators.js`
Expected: 無輸出（語法 OK）。

- [ ] **Step 10: Commit**

```bash
git add src/quanquant/web/static/indicators.js tests/js/indicators.test.mjs
git commit -m "feat: VOL 改可重複量能均線（bars 旗標 + merge params 正規化 + 防呆），預設 MA5/MA10"
```

---

### Task 2: 引擎 `_applyIndicator` repeatable 分支型別防呆（chart.js）

引擎 repeatable 分支的 `conf.params.map(...)` 目前假設 params 是陣列。Task 1 的 merge 正規化已保證 `this.settings` 內 params 為陣列，但依「五道渲染防線」一致性，在消費點加一層 `Array.isArray` 守衛，避免任何非陣列 params 觸發 `.map` 例外炸掉整張圖。純 DOM/KLineCharts 邏輯，無 node 單測，靠 `node --check` + Task 4 手動實測。

**Files:**
- Modify: `src/quanquant/web/static/chart.js`（`_applyIndicator` repeatable 分支 L485-486）

**Interfaces:**
- Consumes: Task 1 的 `resolveVisibility`（VOL bars 語意）、`calcParams`（VOL 週期陣列）、VOL registry `repeatable:true`。
- Produces:（無下游 task 依賴其介面；行為修正）

- [ ] **Step 1: 加 Array.isArray 守衛**

`src/quanquant/web/static/chart.js` L485-487 的 repeatable 分支：
```javascript
          if (entry.repeatable) {
            override.styles = { lines: conf.params.map((p) => this._lineStyle(p.color)) };
          } else if (entry.lines) {
```
改為：
```javascript
          if (entry.repeatable) {
            const lineParams = Array.isArray(conf.params) ? conf.params : [];
            override.styles = { lines: lineParams.map((p) => this._lineStyle(p.color)) };
          } else if (entry.lines) {
```
`_lineStyle` 不動，仍回完整 style 物件；VOL 量柱為 bar figure 自動跳過，`styles.lines` 僅對 line figure。

- [ ] **Step 2: 語法檢查**

Run: `node --check src/quanquant/web/static/chart.js`
Expected: 無輸出（語法 OK）。

- [ ] **Step 3: Commit**

```bash
git add src/quanquant/web/static/chart.js
git commit -m "fix: chart _applyIndicator repeatable 分支加 Array.isArray 守衛（VOL 遷移防線）"
```

---

### Task 3: 後端 VOL params 陣列 round-trip 回歸測試（pytest）

後端零改動，但補一條回歸測試釘住「VOL params（陣列，含 period+color）整包 JSON round-trip 不被 schema 過濾」，與既有 `visible`/`colors` round-trip 測試同一防線。

**Files:**
- Test: `tests/test_api_candles.py`（在既有 `test_chart_state_preserves_colors_field` L147-158 之後、`test_chart_state_unknown_kind_404` L161 之前新增）

**Interfaces:**
- Consumes:（既有 `client` fixture、`PUT/GET /api/chart/state`）
- Produces:（純測試，無介面）

- [ ] **Step 1: 新增 round-trip 回歸測試**

在 `tests/test_api_candles.py` L158（`test_chart_state_preserves_colors_field` 結尾 `assert got["boll"]["colors"] == [...]`）之後、L161 之前插入：
```python
def test_chart_state_preserves_vol_ma_params(client):
    # VOL 量能均線（params 為 {period,color} 陣列）跟帳號：整包 JSON 存取，
    # 陣列須原樣往返，不被 schema 過濾。
    ind = {
        "vol": {"enabled": True, "visible": True,
                "params": [{"period": 5, "color": "#f0b90b"},
                           {"period": 10, "color": "#935EBD"}]},
    }
    assert client.put("/api/chart/state/indicators", json=ind).status_code == 204
    got = client.get("/api/chart/state").json()["indicators"]
    assert got == ind
    assert [p["period"] for p in got["vol"]["params"]] == [5, 10]
```

- [ ] **Step 2: 跑該檔測試確認通過**

Run: `uv run pytest tests/test_api_candles.py -v`
Expected: PASS（含新增 `test_chart_state_preserves_vol_ma_params` 與既有全部）。

- [ ] **Step 3: Commit**

```bash
git add tests/test_api_candles.py
git commit -m "test: VOL 量能均線 params 陣列 round-trip 回歸"
```

---

### Task 4: 整合驗證 + 更新記憶

跑全套測試綠燈、確認 UI 零改動屬實、更新專案記憶。

**Files:**
- Modify:（無程式碼）記憶檔 `/Users/henrychang/.claude/projects/-Users-henrychang-Desktop-MyProjects-QuanQuant/memory/project_indicator_modularization.md` 與同目錄 `MEMORY.md`（一行指標）

**Interfaces:**
- Consumes: Task 1-3 全部成果
- Produces:（無）

- [ ] **Step 1: 全套 node 單測綠**

Run: `node --test tests/js/*.test.mjs tests/js/*.test.js`
Expected: PASS（全部）。

- [ ] **Step 2: 全套 pytest 綠**

Run: `uv run pytest`
Expected: PASS（270+ 全綠）。

- [ ] **Step 3: 確認 UI 零改動屬實（靜態核對，不改碼）**

核對 `dashboard.html` L245-296：VOL 變 `repeatable:true` 後命中 `x-if="e.repeatable"`（L250）repeatable 編輯器（週期+選色+加一條/刪除）；L292「此指標無可調參數」條件 `!e.repeatable && !e.paramSchema.length` 不再命中 VOL。無需改 `dashboard.html`。若核對發現任何 VOL 排除分支或 `params.push` 對非陣列的路徑，回報為缺口。

- [ ] **Step 4: 更新記憶**

在 `project_indicator_modularization.md` 追加一段：VOL 量能均線（2026-07-15）——VOL 改 repeatable+bars 旗標，預設 MA5(#f0b90b)/MA10(#935EBD)，量柱維持全域紅漲/綠漲；merge 加「repeatable⇒params 陣列」正規化安全遷移舊 `{}` 存檔；後端零改動。`MEMORY.md` 對應條目補一句（VOL 量能均線 2026-07-15）。

- [ ] **Step 5: 手動瀏覽器實測（交付使用者）**

在交付說明中提醒使用者：`uv run quanquant-web` → 開 http://127.0.0.1:8000 → **硬重載 Cmd+Shift+R**，驗收清單：
  1. 新帳號（無 VOL 存檔）VOL 副圖顯示 MA5/MA10 兩條均線 + 量柱（紅漲/綠漲）。
  2. 指標設定點 VOL → 出現「週期+選色+加一條/刪除」編輯器（同 MA）；改週期/顏色/增減條數 → Save → K 線更新、量柱顏色不受 MA 線色影響。
  3. 既有帳號（舊 `params:{}` 存檔）VOL 只顯示量柱、無 MA 線、不報錯；手動加一條 MA → 正常。

---

## Self-Review

- **Spec coverage**：spec §A registry→Task1 Step4；§B resolveVisibility→Task1 Step7；§C calcParams+引擎→Task1 Step6 + Task2；§D UI 零改動→Task4 Step3 核對；§E 後端→Task3；測試計畫→Task1/3 單測 + Task4 手動實測。spec 未涵蓋的 UI `{}.push` 接縫→以 Task1 Step5 merge 正規化補上（比 spec 更穩健，使用者可見結果不變）。
- **Placeholder scan**：無 TBD/TODO；每個改碼步驟均附完整程式碼與精確錨點。
- **Type consistency**：`bars`/`repeatable` 布林旗標、`params` 陣列契約、`_lineStyle` 完整物件、`calcParams` 回 number[] 在各 task 一致；預設色 `#f0b90b`/`#935EBD` 全程一致。
