# 指標隱藏 + KDJ/BOLL + 模組化收尾 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 讓交易員能一鍵裸K（隱藏所有非K線圖層）或單獨隱藏某指標，並新增台股常用的 KDJ／BOLL，同時消滅殘留硬編讓「新增指標＝只改 REGISTRY 一處」成立。

**Architecture:** 沿用既有 `window.QQIndicators` registry（`indicators.js`）為單一真相；新增純函式 `resolveVisibility` 集中「這根指標要不要畫」的判定並 node 單測；`chart.js` 引擎與 Alpine 層以「布林狀態 + localStorage/後端持久化 + refresh」範式接線（比照既有扣抵三角 toggle）；KDJ/BOLL 皆為 KLineCharts v9.8.12 原生內建，僅擴充 REGISTRY 即可繪製。

**Tech Stack:** 原生 JS（UMD `indicators.js`）、KLineCharts v9.8.12、Alpine.js、node:test（`tests/js/*.mjs`）、FastAPI 後端（settings 為 JSON blob，無 schema 變動）。

## Global Constraints

- KLineCharts 釘版 **v9.8.12**，不升版；`chart.js` 的五道渲染防線（資料先於指標／完整 line style／逐格建 pane／每指標 try-catch／watchdog 自癒）勿移除。
- KLineCharts locale 必須繁體 `zh-TW`；全站中文一律繁體台灣。
- 後端 settings 為 JSON blob，`visible` 缺省即 `true`，**不得**新增 schema migration；舊存檔經 `merge()` 自動補 `visible`。
- 指標可見性判定必須集中於純函式 `resolveVisibility(entry, conf, nakedK)`，勿散落條件式。
- 每指標 `visible` 跟帳號（隨 `saveSettings` 的 PUT 持久化）；全域裸K跟裝置（`localStorage["qq_naked_k"]`）。
- KDJ/BOLL 這次 `alertTarget:false`（只畫圖、不進警示）。
- 不動 candle 讀取路徑（raw-SQL→FastCandle、sync route）。

---

### Task 1: registry 加 `visible` 預設 + `resolveVisibility` 純函式

新增「這根指標當前要不要畫」的集中判定，並替既有 5 個指標補 `visible:true` 預設。純邏輯、可完整 node 單測。

**Files:**
- Modify: `src/quanquant/web/static/indicators.js`（`defaults` 物件 21/33/44/53/63 行；API return 104 行）
- Test: `tests/js/indicators.test.mjs`

**Interfaces:**
- Produces: `QQIndicators.resolveVisibility(entry, conf, nakedK) => boolean`
  — `entry` 為 registry 物件（提供 `repeatable`）；`conf` 為該指標使用者設定（`{enabled, visible, params}`）；`nakedK` 為全域裸K布林。回傳 true=應建立/覆寫指標，false=應移除。
- Produces: 每指標 `defaults` 物件新增 `visible: true` 欄位。

- [ ] **Step 1: 寫失敗測試（resolveVisibility 八組合 + visible 預設）**

在 `tests/js/indicators.test.mjs` 末端（第 101 行 `});` 之後）追加：

```javascript
test('defaults() 每指標帶 visible:true', () => {
  const d = QQI.defaults();
  for (const k of ['ma', 'wr', 'bias', 'vol', 'macd']) {
    assert.equal(d[k].visible, true, `${k} 應預設 visible:true`);
  }
});

test('merge 保留使用者存的 visible:false；舊存檔無 visible → 補 true', () => {
  const m = QQI.merge({ ma: { visible: false }, wr: { enabled: true } });
  assert.equal(m.ma.visible, false);   // 使用者關掉的保留
  assert.equal(m.wr.visible, true);    // 舊存檔無 visible → defaults 補
  assert.equal(m.vol.visible, true);
});

test('resolveVisibility: enabled×visible×nakedK 組合', () => {
  const ma = QQI.byKey('ma');       // repeatable
  const vol = QQI.byKey('vol');     // fixed
  const withMA = (o) => ({ enabled: true, visible: true, params: [{ period: 5 }], ...o });
  // 正常顯示
  assert.equal(QQI.resolveVisibility(ma, withMA(), false), true);
  // 裸K → 一律不畫
  assert.equal(QQI.resolveVisibility(ma, withMA(), true), false);
  // 單指標隱藏
  assert.equal(QQI.resolveVisibility(ma, withMA({ visible: false }), false), false);
  // 未啟用
  assert.equal(QQI.resolveVisibility(ma, withMA({ enabled: false }), false), false);
  // repeatable 但無線 → 不畫
  assert.equal(QQI.resolveVisibility(ma, withMA({ params: [] }), false), false);
  // fixed 啟用即畫；visible 預設缺省視為顯示
  assert.equal(QQI.resolveVisibility(vol, { enabled: true, params: {} }, false), true);
  // 缺 conf/entry 安全回 false
  assert.equal(QQI.resolveVisibility(ma, null, false), false);
  assert.equal(QQI.resolveVisibility(null, withMA(), false), false);
});
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `node --test tests/js/indicators.test.mjs`
Expected: FAIL — `QQI.resolveVisibility is not a function`，且 `defaults() 每指標帶 visible:true` 斷言失敗。

- [ ] **Step 3: 加 `visible:true` 到 5 個 defaults**

在 `src/quanquant/web/static/indicators.js` 把五個 `defaults` 物件加上 `visible: true`：

- 第 21 行 `defaults: { enabled: true, params: [` → `defaults: { enabled: true, visible: true, params: [`（ma）
- 第 33 行 `defaults: { enabled: false, params: [` → `defaults: { enabled: false, visible: true, params: [`（wr）
- 第 44 行 `defaults: { enabled: false, params: [` → `defaults: { enabled: false, visible: true, params: [`（bias）
- 第 53 行 `defaults: { enabled: true, params: {} },` → `defaults: { enabled: true, visible: true, params: {} },`（vol）
- 第 63 行 `defaults: { enabled: false, params: { fast: 12, slow: 26, signal: 9 } },` → `defaults: { enabled: false, visible: true, params: { fast: 12, slow: 26, signal: 9 } },`（macd）

- [ ] **Step 4: 實作 `resolveVisibility` 並匯出**

在 `indicators.js` 第 102 行（`calcParams` 函式的 `}` 之後、`return {` 之前）插入：

```javascript
  // 集中判定「這根指標當前要不要畫」。nakedK（全域裸K）優先蓋掉一切；
  // 其次看使用者是否啟用、是否單獨隱藏（visible===false）；repeatable 需至少一條線。
  function resolveVisibility(entry, conf, nakedK) {
    if (nakedK) return false;
    if (!entry || !conf || !conf.enabled) return false;
    if (conf.visible === false) return false;
    if (entry.repeatable) return (conf.params || []).length > 0;
    return true;
  }
```

並把第 104 行的 return 改為（新增 `resolveVisibility`）：

```javascript
  return { list: REGISTRY, byKey, byKlineName, defaults, merge, alertTargets, calcParams, resolveVisibility };
```

- [ ] **Step 5: 跑測試確認通過**

Run: `node --test tests/js/indicators.test.mjs`
Expected: PASS（全部，含新增三個 test）。

- [ ] **Step 6: Commit**

```bash
git add src/quanquant/web/static/indicators.js tests/js/indicators.test.mjs
git commit -m "feat: 指標 registry 加 visible 預設與 resolveVisibility 純函式"
```

---

### Task 2: 新增 KDJ + BOLL 指標（僅擴充 REGISTRY）

兩者皆 KLineCharts v9.8.12 原生內建，繪製零改動；只在 REGISTRY 各加一筆並補測試。

**Files:**
- Modify: `src/quanquant/web/static/indicators.js`（REGISTRY 陣列，macd 之後、第 64 行 `},` 與第 65 行 `];` 之間）
- Test: `tests/js/indicators.test.mjs`（更新 key 清單斷言 + 新增 KDJ/BOLL 斷言）

**Interfaces:**
- Produces: `byKey('boll')` → `{ pane:'main', klineName:'BOLL', repeatable:false, alertTarget:false }`，calcParams `[20,2]`
- Produces: `byKey('kdj')` → `{ pane:'sub', klineName:'KDJ', repeatable:false, alertTarget:false }`，calcParams `[9,3,3]`
- 影響：`QQIndicators.list` 的 key 順序變為 `['ma','wr','bias','vol','macd','boll','kdj']`

- [ ] **Step 1: 寫失敗測試（KDJ/BOLL 存在 + calcParams）**

先更新既有的 key 清單斷言（否則會因新指標破裂）：

- 第 10 行 `assert.deepEqual(keys, ['ma', 'wr', 'bias', 'vol', 'macd']);` → `assert.deepEqual(keys, ['ma', 'wr', 'bias', 'vol', 'macd', 'boll', 'kdj']);`
- 第 27 行 `assert.deepEqual(Object.keys(d), ['ma', 'wr', 'bias', 'vol', 'macd']);` → `assert.deepEqual(Object.keys(d), ['ma', 'wr', 'bias', 'vol', 'macd', 'boll', 'kdj']);`
- 第 44 行 `assert.deepEqual(Object.keys(m), ['ma', 'wr', 'bias', 'vol', 'macd']);` → `assert.deepEqual(Object.keys(m), ['ma', 'wr', 'bias', 'vol', 'macd', 'boll', 'kdj']);`

同時把 Task 1 已加的「defaults visible」測試中固定字串陣列 `['ma', 'wr', 'bias', 'vol', 'macd']` 補上 `'boll', 'kdj'`。

在檔末追加：

```javascript
test('boll 註冊為 main/fixed，calcParams [20, 2]', () => {
  const boll = QQI.byKey('boll');
  assert.ok(boll, 'boll 應存在');
  assert.equal(boll.pane, 'main');
  assert.equal(boll.klineName, 'BOLL');
  assert.equal(boll.repeatable, false);
  assert.equal(boll.alertTarget, false);
  assert.deepEqual(QQI.calcParams(boll, boll.defaults), [20, 2]);
  assert.equal(boll.defaults.visible, true);
});

test('kdj 註冊為 sub/fixed，calcParams [9, 3, 3]', () => {
  const kdj = QQI.byKey('kdj');
  assert.ok(kdj, 'kdj 應存在');
  assert.equal(kdj.pane, 'sub');
  assert.equal(kdj.klineName, 'KDJ');
  assert.equal(kdj.repeatable, false);
  assert.equal(kdj.alertTarget, false);
  assert.deepEqual(QQI.calcParams(kdj, kdj.defaults), [9, 3, 3]);
  assert.equal(kdj.defaults.visible, true);
});

test('alertTargets 不含 boll/kdj（只畫圖不進警示）', () => {
  const codes = QQI.alertTargets().map((x) => x.code);
  assert.deepEqual(codes, ['ma', 'wr', 'bias']);
});
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `node --test tests/js/indicators.test.mjs`
Expected: FAIL — `boll 應存在`／`kdj 應存在` 斷言失敗（byKey 回 null），且 key 清單斷言不符。

- [ ] **Step 3: 加 BOLL + KDJ 到 REGISTRY**

在 `indicators.js` 第 64 行（macd 物件結尾 `},`）之後、第 65 行 `];` 之前插入：

```javascript
    {
      key: "boll", title: "布林通道 BOLL", hint: "疊於主圖",
      pane: "main", klineName: "BOLL", alertTarget: false, repeatable: false,
      paramSchema: [
        { field: "period", type: "number", label: "週期", min: 2, step: 1 },
        { field: "std", type: "number", label: "標準差", min: 1, step: 1 },
      ],
      defaults: { enabled: false, visible: true, params: { period: 20, std: 2 } },
    },
    {
      key: "kdj", title: "KDJ", hint: "副圖",
      pane: "sub", klineName: "KDJ", alertTarget: false, repeatable: false,
      paramSchema: [
        { field: "k", type: "number", label: "K", min: 1, step: 1 },
        { field: "d", type: "number", label: "D", min: 1, step: 1 },
        { field: "j", type: "number", label: "J", min: 1, step: 1 },
      ],
      defaults: { enabled: false, visible: true, params: { k: 9, d: 3, j: 3 } },
    },
```

- [ ] **Step 4: 跑測試確認通過**

Run: `node --test tests/js/indicators.test.mjs`
Expected: PASS（全部）。

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/web/static/indicators.js tests/js/indicators.test.mjs
git commit -m "feat: 新增 KDJ 與布林通道 BOLL 指標（KLineCharts 內建）"
```

---

### Task 3: chart.js 引擎接線（nakedK 狀態 + resolveVisibility + 扣抵閘）

把純函式接到繪製引擎：新增 `nakedK` 引擎狀態與 `setNakedK`，`_applyIndicator` 改用 `resolveVisibility`，扣抵三角在裸K時強制隱藏。

**Files:**
- Modify: `src/quanquant/web/static/chart.js`（引擎狀態 ~131 行；`_applyIndicator` 473-510；`refreshDeduction` 533-555）

**Interfaces:**
- Consumes: `QQIndicators.resolveVisibility(entry, conf, nakedK)`（Task 1）
- Produces: 引擎 `QQChart.nakedK`（布林）與 `QQChart.setNakedK(on)`（供 Alpine 層 Task 5 呼叫）

- [ ] **Step 1: 加 `nakedK` 引擎狀態**

在 `chart.js` 第 131 行 `deductionEnabled: false,   // 均線扣抵三角開關（由 Alpine 依 localStorage 設定）` 之後新增一行：

```javascript
    nakedK: false,             // 全域裸K：true 時隱藏所有指標與扣抵三角（由 Alpine 依 localStorage 設定）
```

- [ ] **Step 2: `_applyIndicator` 改用 resolveVisibility**

在 `chart.js` `_applyIndicator`（473-510）把第 477-479 行：

```javascript
        const enabled = !!(conf && conf.enabled);
        // repeatable：需至少一條線；fixed（含無參數 VOL）：啟用即算
        const active = enabled && (entry.repeatable ? (conf.params || []).length > 0 : true);
```

替換為：

```javascript
        // 可見性集中判定（含裸K/單指標隱藏/repeatable 空線）——見 indicators.js
        const active = window.QQIndicators.resolveVisibility(entry, conf, this.nakedK);
```

（其餘 create/override/remove 分支不動；`active` 語意不變，只是判定來源集中化。）

- [ ] **Step 3: 扣抵三角在裸K時強制隱藏**

在 `chart.js` `refreshDeduction`（533）把第 535 行：

```javascript
      if (!this.deductionEnabled) {
```

改為：

```javascript
      if (!this.deductionEnabled || this.nakedK) {
```

- [ ] **Step 4: 加 `setNakedK` 方法**

在 `chart.js` `setDeduction`（526-529）之後、`refreshDeduction` 之前插入：

```javascript
    // 全域裸K開關：重套所有指標（resolveVisibility 依 nakedK 決定畫或移除）並刷新扣抵。
    setNakedK(on) {
      this.nakedK = !!on;
      this.applyIndicators();
      this.refreshDeduction();
    },
```

- [ ] **Step 5: 語法自檢**

Run: `node --check src/quanquant/web/static/chart.js`
Expected: 無輸出（語法正確）。

- [ ] **Step 6: Commit**

```bash
git add src/quanquant/web/static/chart.js
git commit -m "feat: chart 引擎接 nakedK 全域裸K 與集中可見性判定"
```

---

### Task 4: 模組化收尾——消滅 IND_NAMES 硬編

把警示文字的指標短名改由 registry 動態取，刪除手動同步表；短名邏輯放進 `indicators.js` 以便 node 單測。

**Files:**
- Modify: `src/quanquant/web/static/indicators.js`（新增 `shortName`）
- Modify: `src/quanquant/web/static/chart.js`（34-40 行：刪 `IND_NAMES`，`alertLabel` 改用 registry）
- Test: `tests/js/indicators.test.mjs`

**Interfaces:**
- Consumes: `QQIndicators.byKey`（既有）
- Produces: `QQIndicators.shortName(key) => string`（registry 有則回 `klineName`，否則回原 key）

- [ ] **Step 1: 寫失敗測試（shortName）**

在 `tests/js/indicators.test.mjs` 檔末追加：

```javascript
test('shortName: registry 有則回 klineName，未知 key 回原字串', () => {
  assert.equal(QQI.shortName('ma'), 'MA');
  assert.equal(QQI.shortName('bias'), 'BIAS');
  assert.equal(QQI.shortName('kdj'), 'KDJ');
  assert.equal(QQI.shortName('unknown'), 'unknown');
});
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `node --test tests/js/indicators.test.mjs`
Expected: FAIL — `QQI.shortName is not a function`。

- [ ] **Step 3: 實作 `shortName` 並匯出**

在 `indicators.js` 第 69 行 `function byKlineName(...) {...}` 之後新增：

```javascript
  function shortName(key) { const e = byKey(key); return (e && e.klineName) || key; }
```

並把 return（Task 1 已改過的那行）補上 `shortName`：

```javascript
  return { list: REGISTRY, byKey, byKlineName, defaults, merge, alertTargets, calcParams, resolveVisibility, shortName };
```

- [ ] **Step 4: 跑測試確認通過**

Run: `node --test tests/js/indicators.test.mjs`
Expected: PASS。

- [ ] **Step 5: chart.js 刪 IND_NAMES、alertLabel 改用 registry**

在 `chart.js` 把第 34-40 行：

```javascript
  const IND_NAMES = { ma: "MA", wr: "WR", bias: "BIAS" };
  const OP_LABELS = { gte: "≥", lte: "≤", cross_up: "向上突破", cross_down: "向下突破" };
  function alertLabel(a) {
    const L = a.left_kind === "price" ? "收盤" : `${IND_NAMES[a.left_name]}(${a.left_period})`;
    const R = a.right_kind === "const" ? a.right_value : `${IND_NAMES[a.right_name]}(${a.right_period})`;
    return `${a.timeframe}｜${L} ${OP_LABELS[a.op]} ${R}`;
  }
```

替換為（短名改由 registry 動態取，不再手動同步）：

```javascript
  const OP_LABELS = { gte: "≥", lte: "≤", cross_up: "向上突破", cross_down: "向下突破" };
  function alertLabel(a) {
    const short = (k) => window.QQIndicators.shortName(k);
    const L = a.left_kind === "price" ? "收盤" : `${short(a.left_name)}(${a.left_period})`;
    const R = a.right_kind === "const" ? a.right_value : `${short(a.right_name)}(${a.right_period})`;
    return `${a.timeframe}｜${L} ${OP_LABELS[a.op]} ${R}`;
  }
```

- [ ] **Step 6: 語法自檢**

Run: `node --check src/quanquant/web/static/chart.js`
Expected: 無輸出。

- [ ] **Step 7: Commit**

```bash
git add src/quanquant/web/static/indicators.js src/quanquant/web/static/chart.js tests/js/indicators.test.mjs
git commit -m "refactor: 警示短名改由 registry 動態取，消滅 IND_NAMES 硬編"
```

---

### Task 5: UI——每指標眼睛開關 + 全域裸K選單 + Alpine 接線

在設定 dialog 每個已啟用指標加眼睛 toggle，工具列 more 選單加裸K總開關（沿用扣抵三角範式），Alpine 層補狀態/init/toggle 與被隱藏指標的 CSS 淡化。

**Files:**
- Modify: `src/quanquant/web/templates/dashboard.html`（more 選單 ~94 行；設定 dialog `.ind-item` 199-206）
- Modify: `src/quanquant/web/static/chart.js`（Alpine 狀態 ~754；`init` 782-799；新增 `toggleNakedK`）
- Modify: `src/quanquant/web/static/app.css`（`.ind-hidden` 淡化樣式）

**Interfaces:**
- Consumes: `QQChart.setNakedK(on)`（Task 3）；`form[key].visible`（Task 1 defaults 保證存在）
- Produces: Alpine `nakedK` 狀態與 `toggleNakedK()` 方法

- [ ] **Step 1: 每指標眼睛按鈕（設定 dialog 左欄）**

在 `dashboard.html` 把第 200-205 行的 `.ind-item`：

```html
            <div class="ind-item" :class="{active: activeIndicator === e.key}"
                 @click="activeIndicator = e.key; catalogOpen = false">
              <span x-text="e.title"></span>
              <button type="button" class="outline mini danger"
                      @click.stop="form[e.key].enabled = false; if (activeIndicator === e.key) activeIndicator = null">−</button>
            </div>
```

替換為（加 `ind-hidden` 淡化 class 與眼睛 toggle；眼睛把 `visible` 在 true/false 間翻轉，undefined 視為顯示）：

```html
            <div class="ind-item" :class="{active: activeIndicator === e.key, 'ind-hidden': form[e.key].visible === false}"
                 @click="activeIndicator = e.key; catalogOpen = false">
              <span x-text="e.title"></span>
              <span class="ind-item-actions">
                <button type="button" class="outline mini ind-eye"
                        @click.stop="form[e.key].visible = (form[e.key].visible === false)"
                        :aria-pressed="form[e.key].visible !== false"
                        :aria-label="(form[e.key].visible !== false) ? '隱藏此指標' : '顯示此指標'"
                        :title="(form[e.key].visible !== false) ? '顯示中（點擊隱藏）' : '已隱藏（點擊顯示）'">
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                       stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
                    <path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7-10-7-10-7Z"/><circle cx="12" cy="12" r="3"/>
                    <line x1="3" y1="3" x2="21" y2="21" x-show="form[e.key].visible === false"/>
                  </svg>
                </button>
                <button type="button" class="outline mini danger"
                        @click.stop="form[e.key].enabled = false; if (activeIndicator === e.key) activeIndicator = null">−</button>
              </span>
            </div>
```

- [ ] **Step 2: 全域裸K選單列（more 選單）**

在 `dashboard.html` 第 94 行（扣抵三角 `</button>` 結尾）之後、第 95 行畫線跨時框按鈕之前插入：

```html
          <button type="button" class="menu-row" :class="{active: nakedK}"
                  @click="toggleNakedK()" :aria-pressed="nakedK"
                  title="裸K：只顯示 K 線，隱藏所有指標與扣抵三角（開/關）">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                 stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
              <path d="M6 3v18M6 7h5v5H6M14 12v9M14 12h4v6h-4"/>
            </svg>
            <span>裸K</span>
            <span class="row-state" x-text="nakedK ? '開' : '關'"></span>
          </button>
```

- [ ] **Step 3: Alpine 狀態 + init 讀 localStorage + toggleNakedK**

在 `chart.js` 第 754 行 `deductionOn: false,` 之後新增：

```javascript
    nakedK: false,       // 全域裸K（此裝置看盤模式，存 localStorage）
```

在 `init()` 第 784 行 `this.deductionOn = localStorage.getItem("qq_ma_deduction") === "1";` 之後新增：

```javascript
      this.nakedK = localStorage.getItem("qq_naked_k") === "1";
```

在 `init()` 第 788 行 `QQChart.deductionEnabled = this.deductionOn;` 之後新增：

```javascript
      QQChart.nakedK = this.nakedK;
```

在 `toggleDeduction()`（892-896）之後新增方法：

```javascript
    toggleNakedK() {
      this.nakedK = !this.nakedK;
      localStorage.setItem("qq_naked_k", this.nakedK ? "1" : "0");
      QQChart.setNakedK(this.nakedK);
    },
```

- [ ] **Step 4: `.ind-hidden` 淡化樣式**

在 `src/quanquant/web/static/app.css` 末端追加：

```css
/* 指標設定清單：被單獨隱藏（眼睛關）的指標淡化，仍可點擊編輯/重新顯示 */
.ind-item.ind-hidden > span:first-child { opacity: .45; }
.ind-item-actions { display: inline-flex; gap: 4px; align-items: center; }
.ind-eye[aria-pressed="false"] { opacity: .6; }
```

- [ ] **Step 5: 語法自檢**

Run: `node --check src/quanquant/web/static/chart.js`
Expected: 無輸出。

- [ ] **Step 6: Commit**

```bash
git add src/quanquant/web/templates/dashboard.html src/quanquant/web/static/chart.js src/quanquant/web/static/app.css
git commit -m "feat: 每指標眼睛隱藏開關與全域裸K工具列選單"
```

---

### Task 6: 整合驗證（node 測試 + pytest + 實際瀏覽器）

確認全綠並在實跑 app 中操作驗證行為（此為 UI/引擎接線，pure test 之外必須實測）。

**Files:** 無（驗證任務）

- [ ] **Step 1: node 前端測試全綠**

Run: `node --test tests/js/`
Expected: PASS（含 `indicators.test.mjs` 與既有 `chart-guards` 測試）。

- [ ] **Step 2: 後端測試全綠（確認 settings 存取不受 visible 影響）**

Run: `uv run pytest`
Expected: PASS（79+，全綠）。

- [ ] **Step 3: 實跑 app 手動驗證**

Run: `uv run quanquant-web` → 開 http://127.0.0.1:8000 登入後，逐項確認：
1. 開「指標」設定 → 每個已啟用指標旁有眼睛；點眼睛 → 該指標從圖上消失但仍在清單（淡化）、儲存後重整仍隱藏（跟帳號）。
2. more 選單「裸K：關」→ 點擊變「開」→ K 線以外全部（均線/成交量/…/扣抵三角）消失；再點回「關」→ 全部依各自眼睛狀態恢復（先前單獨隱藏的那根仍隱藏）。
3. 「＋ 新增指標」目錄出現「布林通道 BOLL」「KDJ」；啟用 BOLL → 主圖出現通道；啟用 KDJ → 副圖出現 KDJ pane；調參數（BOLL 20/2、KDJ 9/3/3）生效。
4. 裸K 開關為此裝置設定（換裝置/清 localStorage 回關）；重整後裸K狀態保留。
5. 警示 dialog 文字（若有既有警示）短名正常顯示（MA/WR/BIAS），無 `undefined`。

Expected: 全部符合；圖表無空白卡死（五道防線與 watchdog 生效）。

- [ ] **Step 4: 更新記憶**

更新 `project_indicator_modularization.md`：記錄 2026-07-12 新增 `visible`/裸K/`resolveVisibility`/`shortName`（IND_NAMES 硬編已消滅）、新增 KDJ+BOLL。同步 `MEMORY.md` 該行 hook。

- [ ] **Step 5: （選）部署**

如使用者要上線：`git push` → `./scripts/deploy.sh`（部署前確認 Step 1/2 全綠）。此步驟需使用者明確指示才執行。

---

## Self-Review

**Spec 覆蓋**：
- 隱藏指標（眼睛+裸K）→ Task 1（resolveVisibility/visible）、Task 3（引擎/扣抵閘）、Task 5（UI/Alpine）。✅
- 狀態保留（visible 跟帳號、裸K 跟裝置）→ Task 1 merge/defaults + saveSettings 既有 PUT（visible 隨 form 持久化，無需改 saveSettings）、Task 5 localStorage。✅
- 新增 KDJ/BOLL（alertTarget:false、標準參數）→ Task 2。✅
- 裸K 藏 registry 指標 + MA 扣抵三角 → Task 3 Step 2/3。✅
- 模組化收尾（消滅 IND_NAMES）→ Task 4。✅
- 測試（KDJ/BOLL、visible、merge、resolveVisibility、pytest 全綠）→ Task 1/2/4 node 測試 + Task 6。✅

**Placeholder 掃描**：無 TBD/TODO；每個改碼步驟均附完整程式碼與確切行號。

**型別/命名一致**：`resolveVisibility(entry, conf, nakedK)` 三參一致（Task 1 定義、Task 3 呼叫）；`setNakedK`/`nakedK`/`toggleNakedK` 跨 Task 3/5 命名一致；`shortName` Task 4 定義與呼叫一致；REGISTRY key 清單順序 `['ma','wr','bias','vol','macd','boll','kdj']` 於 Task 2 各斷言一致。

**備註**：`saveSettings`（chart.js:934）的參數清洗迴圈不觸及 `visible`，而 `form` 為 `QQChart.settings` 深拷貝（已含 `visible`），存檔時 `QQChart.settings = deep copy of form` 自動保留 `visible` 並隨 PUT 送出——故無需修改 saveSettings，Task 6 Step 3.1 會實測此持久化路徑。
