# 指標模組化 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 依 `docs/superpowers/specs/2026-07-07-indicator-modularization-design.md`，把指標系統收斂成一張 registry 驅動的架構（新增 `indicators.js`），並把擁擠的單一設定 dialog 改成主從式 + 指標目錄，全程不影響既有 MA/WR/BIAS/VOL 行為。

**Architecture:** 新增純資料模組 `static/indicators.js`（UMD，掛 `window.QQIndicators`，比照 `ma_deduction.js`），在 `chart.js` 之前載入。所有指標的預設值、合併、警示下拉、渲染 calcParams 都從 registry 衍生。`chart.js` 的 `applyIndicators` 從三段特例改成跑過 registry 的單一迴圈；設定 dialog 改主從式。指標持久化維持既有一包 JSON（`PUT /api/chart/state/indicators`），後端零改動。

**Tech Stack:** KLineCharts 9.8.12（釘版，內建 MA/WR/BIAS/VOL/MACD/KDJ/RSI）、Alpine 3、Jinja2、FastAPI；JS 純函式單測走 `node --test tests/js/*.test.mjs`（UMD `require`），後端測試走 `uv run pytest`。

## Global Constraints

- KLineCharts 釘版 **9.8.12**，不升版；`chart.js` 四道渲染防線（stale-response guard、`_watchdog`、`_nextFrame` 逐格分幀、完整 `_lineStyle`）**不可移除或改動**。
- candle 讀取路徑 raw-SQL→float、routes 為 sync `def` —— 本計畫完全不碰。
- 指標持久化維持一包 JSON（`ChartState`/`UserChartState` kind=`indicators`），走既有 `PUT /api/chart/state/indicators`，不改後端。
- 這些 DOM id 為 JS 綁定點，必須原樣保留：`#kchart`、`#chart-empty`、`#quote`、`#pulse-toggle`、`#pulse-tg-toggle`、`#trade-tbody`、`#modal-body`。
- 模板注入 JS 變數一律用 `| tojson`。
- 既有行為零回退：MA 疊主圖、WR/BIAS/VOL 副圖、MA 扣抵、警示 code 值（ma/wr/bias/price/const）皆不變。
- `indicators.js` 必須用 UMD 包裝（`module.exports` 供 Node require + `window.QQIndicators` 供瀏覽器），且 `<script src="/static/indicators.js">` 必須排在 `chart.js` **之前**。
- 每個 task 結尾 `uv run pytest` 全綠、觸及的 JS 檔跑 `node --check`、`indicators.js` 相關跑 `node --test tests/js/indicators.test.mjs`；commit 訊息格式 `<type>: <description>`（無 attribution）。
- 部署不自動：完成後由使用者決定何時 `./scripts/deploy.sh`（會與尚未部署的配色修正 commit `e0276f9` 一起批次上線）。

---

### Task 1: indicators.js registry 模組 + 純函式 helper + 測試

**Files:**
- Create: `src/quanquant/web/static/indicators.js`
- Create: `tests/js/indicators.test.mjs`
- Modify: `src/quanquant/web/templates/dashboard.html:326-330`（在 `chart.js` 之前插入 `<script src="/static/indicators.js">`）
- Test: `tests/test_auth_routes.py`（新增 dashboard 載入斷言）

**Interfaces:**
- Produces: `window.QQIndicators`（Node `require` 亦可）具下列成員：
  - `list` → registry 陣列（元素含 `key, title, hint, pane("main"|"sub"), klineName, alertTarget, repeatable, paramSchema, defaults`）
  - `byKey(key)` → entry | null
  - `byKlineName(name)` → entry | null
  - `defaults()` → `{ [key]: {enabled, params} }`（等同舊 `DEFAULT_SETTINGS`）
  - `merge(saved)` → 以 defaults 為底疊上 saved 的設定物件
  - `alertTargets()` → `[{code, label}]`（僅 `alertTarget:true` 的指標，不含 price/const）
  - `calcParams(entry, conf)` → `number[]`（repeatable：各條 period；fixed：paramSchema number 欄位依序）

- [ ] **Step 1: 寫失敗測試**

建立 `tests/js/indicators.test.mjs`：

```js
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const QQI = require('../../src/quanquant/web/static/indicators.js');

test('list 含 ma/wr/bias/vol，欄位齊全', () => {
  const keys = QQI.list.map((e) => e.key);
  assert.deepEqual(keys, ['ma', 'wr', 'bias', 'vol']);
  const ma = QQI.byKey('ma');
  assert.equal(ma.pane, 'main');
  assert.equal(ma.klineName, 'MA');
  assert.equal(ma.repeatable, true);
  assert.equal(ma.alertTarget, true);
  assert.equal(QQI.byKey('vol').repeatable, false);
  assert.equal(QQI.byKey('nope'), null);
});

test('byKlineName 反查', () => {
  assert.equal(QQI.byKlineName('WR').key, 'wr');
  assert.equal(QQI.byKlineName('ZZZ'), null);
});

test('defaults() 等同舊 DEFAULT_SETTINGS 結構', () => {
  const d = QQI.defaults();
  assert.deepEqual(Object.keys(d), ['ma', 'wr', 'bias', 'vol']);
  assert.equal(d.ma.enabled, true);
  assert.equal(d.ma.params.length, 4);
  assert.equal(d.ma.params[0].period, 5);
  assert.equal(d.wr.enabled, false);
  assert.equal(d.vol.enabled, true);
  // 回傳為獨立副本（改一份不影響下一次）
  d.ma.params[0].period = 999;
  assert.equal(QQI.defaults().ma.params[0].period, 5);
});

test('merge: 舊存檔（無新指標）載入後含全部 key 且舊值保留', () => {
  const saved = { ma: { enabled: false, params: [{ period: 7, color: '#111' }] } };
  const m = QQI.merge(saved);
  assert.equal(m.ma.enabled, false);
  assert.equal(m.ma.params[0].period, 7);
  assert.equal(m.wr.enabled, false);       // 未存 → 用 defaults
  assert.deepEqual(Object.keys(m), ['ma', 'wr', 'bias', 'vol']);
});

test('merge: 存檔含 registry 已無的 key → 略過', () => {
  const m = QQI.merge({ ghost: { enabled: true } });
  assert.equal(m.ghost, undefined);
});

test('merge: null/非物件輸入 → 回 defaults', () => {
  assert.deepEqual(QQI.merge(null), QQI.defaults());
  assert.deepEqual(QQI.merge('x'), QQI.defaults());
});

test('alertTargets: 僅 alertTarget 指標，不含 price/const/vol', () => {
  const t = QQI.alertTargets();
  assert.deepEqual(t.map((x) => x.code), ['ma', 'wr', 'bias']);
});

test('calcParams repeatable: 各條 period 四捨五入', () => {
  const entry = QQI.byKey('ma');
  const conf = { enabled: true, params: [{ period: 5.4, color: '#a' }, { period: 10.6, color: '#b' }] };
  assert.deepEqual(QQI.calcParams(entry, conf), [5, 11]);
});

test('calcParams fixed: 依 paramSchema number 欄位順序', () => {
  const entry = {
    key: 'x', repeatable: false,
    paramSchema: [
      { field: 'fast', type: 'number' },
      { field: 'slow', type: 'number' },
      { field: 'signal', type: 'number' },
      { field: 'color', type: 'color' },
    ],
  };
  assert.deepEqual(QQI.calcParams(entry, { params: { fast: 12, slow: 26, signal: 9, color: '#a' } }), [12, 26, 9]);
});

test('calcParams: 空/缺 conf 安全回空陣列', () => {
  assert.deepEqual(QQI.calcParams(QQI.byKey('vol'), { params: {} }), []);
  assert.deepEqual(QQI.calcParams(null, null), []);
});
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `node --test tests/js/indicators.test.mjs`
Expected: FAIL（`Cannot find module '.../indicators.js'`）

- [ ] **Step 3: 建立 `src/quanquant/web/static/indicators.js`**

完整內容：

```js
// 指標登錄表（registry）：所有指標的單一真相來源。
// UMD 包裝：瀏覽器掛 window.QQIndicators；Node 可 require 做純函式單測。
(function (root, factory) {
  const api = factory();
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  if (typeof window !== "undefined") window.QQIndicators = api;
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  // 每個指標一個自我描述物件。
  // repeatable=true  → params 為線陣列（每條符合 paramSchema，可增刪）
  // repeatable=false → params 為單一物件（依 paramSchema 欄位）
  const REGISTRY = [
    {
      key: "ma", title: "均線 MA", hint: "疊於主圖",
      pane: "main", klineName: "MA", alertTarget: true, repeatable: true,
      paramSchema: [
        { field: "period", type: "number", label: "週期", min: 1, step: 1 },
        { field: "color", type: "color", label: "顏色" },
      ],
      defaults: { enabled: true, params: [
        { period: 5, color: "#f0b90b" }, { period: 10, color: "#ff9800" },
        { period: 20, color: "#2196f3" }, { period: 60, color: "#e91e63" },
      ] },
    },
    {
      key: "wr", title: "威廉指標 WR", hint: "副圖",
      pane: "sub", klineName: "WR", alertTarget: true, repeatable: true,
      paramSchema: [
        { field: "period", type: "number", label: "週期", min: 1, step: 1 },
        { field: "color", type: "color", label: "顏色" },
      ],
      defaults: { enabled: false, params: [
        { period: 14, color: "#f0b90b" }, { period: 28, color: "#2196f3" },
      ] },
    },
    {
      key: "bias", title: "乖離率 BIAS", hint: "副圖",
      pane: "sub", klineName: "BIAS", alertTarget: true, repeatable: true,
      paramSchema: [
        { field: "period", type: "number", label: "週期", min: 1, step: 1 },
        { field: "color", type: "color", label: "顏色" },
      ],
      defaults: { enabled: false, params: [
        { period: 6, color: "#f0b90b" }, { period: 12, color: "#2196f3" },
        { period: 24, color: "#e91e63" },
      ] },
    },
    {
      key: "vol", title: "成交量 VOL", hint: "副圖",
      pane: "sub", klineName: "VOL", alertTarget: false, repeatable: false,
      paramSchema: [],
      defaults: { enabled: true, params: {} },
    },
  ];

  function clone(x) { return JSON.parse(JSON.stringify(x)); }
  function byKey(key) { return REGISTRY.find((e) => e.key === key) || null; }
  function byKlineName(name) { return REGISTRY.find((e) => e.klineName === name) || null; }

  function defaults() {
    const out = {};
    for (const e of REGISTRY) out[e.key] = clone(e.defaults);
    return out;
  }

  function merge(saved) {
    const out = defaults();
    if (saved && typeof saved === "object") {
      for (const e of REGISTRY) {
        const s = saved[e.key];
        if (s && typeof s === "object") out[e.key] = { ...out[e.key], ...s };
      }
    }
    return out;
  }

  function alertTargets() {
    return REGISTRY.filter((e) => e.alertTarget).map((e) => ({ code: e.key, label: e.title }));
  }

  function calcParams(entry, conf) {
    if (!entry || !conf) return [];
    if (entry.repeatable) {
      return (conf.params || []).map((p) => Math.round(p && p.period));
    }
    const p = conf.params || {};
    return entry.paramSchema
      .filter((f) => f.type === "number")
      .map((f) => Math.round(p[f.field]))
      .filter((n) => Number.isFinite(n));
  }

  return { list: REGISTRY, byKey, byKlineName, defaults, merge, alertTargets, calcParams };
});
```

- [ ] **Step 4: 跑 JS 測試確認通過**

Run: `node --test tests/js/indicators.test.mjs`
Expected: PASS（10 個 test 全綠）

- [ ] **Step 5: 在 dashboard 載入模組（chart.js 之前）**

`src/quanquant/web/templates/dashboard.html` 現況第 326-330 行：

```html
<script src="https://unpkg.com/klinecharts@9.8.12/dist/umd/klinecharts.min.js"></script>
<script>window.QQ_SYMBOL = "{{ symbol }}"; window.QQ_COLOR_SCHEME = {{ color_scheme | tojson }};
document.documentElement.setAttribute("data-scheme", window.QQ_COLOR_SCHEME || "green_up");</script>
<script src="/static/ma_deduction.js"></script>
<script src="/static/chart.js"></script>
<script src="/static/pulse.js"></script>
```

在 `ma_deduction.js` 與 `chart.js` 之間插入 `indicators.js`（務必在 `chart.js` 之前）：

```html
<script src="/static/ma_deduction.js"></script>
<script src="/static/indicators.js"></script>
<script src="/static/chart.js"></script>
```

- [ ] **Step 6: 寫失敗的 pytest 斷言（dashboard 載入 indicators.js）**

在 `tests/test_auth_routes.py` 檔尾加入：

```python
def test_dashboard_loads_indicators_module(client):
    body = client.get("/").text
    assert "/static/indicators.js" in body
```

- [ ] **Step 7: 跑 pytest 確認通過**

Run: `uv run pytest tests/test_auth_routes.py -k "indicators_module" -v`
Expected: PASS

- [ ] **Step 8: 全套件 + node --check**

Run: `uv run pytest -q && node --check src/quanquant/web/static/indicators.js`
Expected: all pass；chart.js 尚未改動

- [ ] **Step 9: Commit**

```bash
git add src/quanquant/web/static/indicators.js tests/js/indicators.test.mjs \
        src/quanquant/web/templates/dashboard.html tests/test_auth_routes.py
git commit -m "feat: indicator registry module (window.QQIndicators)"
```

---

### Task 2: chart.js 改用 registry 的 defaults + merge

**Files:**
- Modify: `src/quanquant/web/static/chart.js:42-55`（移除 `DEFAULT_SETTINGS` 常數）
- Modify: `src/quanquant/web/static/chart.js:144`（`settings` 初值）
- Modify: `src/quanquant/web/static/chart.js:216-224`（`mergeSettings` 委派）
- Modify: `src/quanquant/web/static/chart.js:780`（`form` 初值）

**Interfaces:**
- Consumes: `window.QQIndicators.defaults()`、`window.QQIndicators.merge(saved)`（Task 1）
- Produces: 無新對外介面；`this.settings`/`this.form` 結構與 `mergeSettings(saved)` 回傳值與改動前**完全相同**（因 `QQIndicators.defaults()` 等值於舊 `DEFAULT_SETTINGS`、`merge` 等值於舊 `mergeSettings`）

- [ ] **Step 1: 移除 `DEFAULT_SETTINGS` 常數**

`chart.js` 現況第 42-55 行為：

```js
  const DEFAULT_SETTINGS = {
    ma: { enabled: true, params: [
      { period: 5, color: "#f0b90b" }, { period: 10, color: "#ff9800" },
      { period: 20, color: "#2196f3" }, { period: 60, color: "#e91e63" },
    ]},
    wr: { enabled: false, params: [
      { period: 14, color: "#f0b90b" }, { period: 28, color: "#2196f3" },
    ]},
    bias: { enabled: false, params: [
      { period: 6, color: "#f0b90b" }, { period: 12, color: "#2196f3" },
      { period: 24, color: "#e91e63" },
    ]},
    vol: { enabled: true },
  };
```

整段刪除（`indicators.js` 的 `defaults()` 取代之；注意舊 `vol` 無 `params`，新 defaults 的 `vol.params: {}` 為相容補值，不影響 VOL 行為）。

- [ ] **Step 2: `settings` 初值改用 defaults()**

`chart.js:144` 現況：

```js
    settings: JSON.parse(JSON.stringify(DEFAULT_SETTINGS)),
```

改為：

```js
    settings: window.QQIndicators.defaults(),
```

- [ ] **Step 3: `mergeSettings` 委派給 registry**

`chart.js:216-224` 現況：

```js
    mergeSettings(saved) {
      const merged = JSON.parse(JSON.stringify(DEFAULT_SETTINGS));
      for (const key of ["ma", "wr", "bias", "vol"]) {
        if (saved[key] && typeof saved[key] === "object") {
          merged[key] = { ...merged[key], ...saved[key] };
        }
      }
      return merged;
    },
```

改為：

```js
    mergeSettings(saved) {
      return window.QQIndicators.merge(saved);
    },
```

- [ ] **Step 4: `form` 初值改用 defaults()**

`chart.js:780` 現況：

```js
    form: JSON.parse(JSON.stringify(DEFAULT_SETTINGS)),
```

改為：

```js
    form: window.QQIndicators.defaults(),
```

- [ ] **Step 5: 語法檢查 + 全套件**

Run: `node --check src/quanquant/web/static/chart.js && uv run pytest -q`
Expected: PASS（行為不變；`DEFAULT_SETTINGS` 已無其他引用——若 `node --check` 或執行報 `DEFAULT_SETTINGS is not defined`，表示尚有遺漏引用，需 grep `DEFAULT_SETTINGS` 清乾淨）

- [ ] **Step 6: 確認無殘留引用**

Run: `grep -n "DEFAULT_SETTINGS" src/quanquant/web/static/chart.js`
Expected: 無輸出（全部已改為 `window.QQIndicators.defaults()`）

- [ ] **Step 7: Commit**

```bash
git add src/quanquant/web/static/chart.js
git commit -m "refactor: chart.js consumes registry defaults/merge"
```

---

### Task 3: applyIndicators 改 registry 驅動迴圈 + paneIds 統一

**Files:**
- Modify: `src/quanquant/web/static/chart.js:124`（`paneIds` 初值不變，僅確認）
- Modify: `src/quanquant/web/static/chart.js:438-481`（`applyIndicators` 改迴圈）
- Modify: `src/quanquant/web/static/chart.js:509-528`（`applySubIndicator` → 通用 `_applyIndicator`）

**Interfaces:**
- Consumes: `window.QQIndicators.list`、`window.QQIndicators.calcParams(entry, conf)`（Task 1）；`this._lineStyle(color)`、`this._nextFrame()`（既有）
- Produces: `applyIndicators()`（async，行為對 ma/wr/bias/vol 不變）；`_applyIndicator(entry, conf)`（依 `entry.pane` 疊主圖或建/覆寫副圖）；`paneIds[key]` 對 main 用 `true/false`、對 sub 用 pane id/`null`

**背景**：現況 `paneIds: { wr: null, bias: null, vol: null, ma: false }`（chart.js:124）已是以 key 為鍵，維持不動即可容納 registry 各 key。

- [ ] **Step 1: 以 registry 迴圈取代 applyIndicators 三段特例**

`chart.js:438-481` 現況（MA 特例 + 呼叫 applySubIndicator + VOL 特例 + 收尾）整段：

```js
    async applyIndicators() {
      const s = this.settings;

      // MA overlaid on the candle pane (multi-period, per-line colors)
      try {
        if (s.ma.enabled && s.ma.params.length) {
          const override = {
            name: "MA",
            calcParams: s.ma.params.map((p) => p.period),
            styles: { lines: s.ma.params.map((p) => this._lineStyle(p.color)) },
          };
          if (!this.paneIds.ma) {
            this.chart.createIndicator(override, true, { id: "candle_pane" });
            this.paneIds.ma = true;
          } else {
            this.chart.overrideIndicator(override, "candle_pane");
          }
        } else if (this.paneIds.ma) {
          this.chart.removeIndicator("candle_pane", "MA");
          this.paneIds.ma = false;
        }
      } catch (e) { console.error("MA indicator failed:", e); }

      // sub-pane indicators — one per frame
      await this._nextFrame();
      this.applySubIndicator("WR", "wr", s.wr);
      await this._nextFrame();
      this.applySubIndicator("BIAS", "bias", s.bias);
      await this._nextFrame();

      // volume pane
      try {
        if (s.vol.enabled && !this.paneIds.vol) {
          this.paneIds.vol = this.chart.createIndicator("VOL", false, { height: 90 });
        } else if (!s.vol.enabled && this.paneIds.vol) {
          this.chart.removeIndicator(this.paneIds.vol);
          this.paneIds.vol = null;
        }
      } catch (e) { console.error("VOL indicator failed:", e); }

      // force a clean relayout/repaint after pane changes
      await this._nextFrame();
      this.chart.resize();
    },
```

改為（registry 迴圈；副圖之間逐格建立以保住渲染防線）：

```js
    async applyIndicators() {
      for (const entry of window.QQIndicators.list) {
        this._applyIndicator(entry, this.settings[entry.key]);
        if (entry.pane === "sub") await this._nextFrame(); // 逐格建立，避免同 tick 建多 pane 卡渲染
      }
      // force a clean relayout/repaint after pane changes
      await this._nextFrame();
      this.chart.resize();
    },
```

- [ ] **Step 2: 以通用 `_applyIndicator` 取代 `applySubIndicator`**

`chart.js:509-528` 現況：

```js
    applySubIndicator(name, key, conf) {
      try {
        const active = conf.enabled && conf.params.length;
        if (active) {
          const override = {
            name,
            calcParams: conf.params.map((p) => p.period),
            styles: { lines: conf.params.map((p) => this._lineStyle(p.color)) },
          };
          if (!this.paneIds[key]) {
            this.paneIds[key] = this.chart.createIndicator(override, false, { height: 90 });
          } else {
            this.chart.overrideIndicator(override, this.paneIds[key]);
          }
        } else if (this.paneIds[key]) {
          this.chart.removeIndicator(this.paneIds[key]);
          this.paneIds[key] = null;
        }
      } catch (e) { console.error(name + " indicator failed:", e); }
    },
```

改為（依 `entry.pane` 派發；repeatable 才帶線色；main 用 candle_pane 疊圖、sub 建獨立 pane）：

```js
    _applyIndicator(entry, conf) {
      const key = entry.key;
      const onMain = entry.pane === "main";
      try {
        const enabled = !!(conf && conf.enabled);
        // repeatable：需至少一條線；fixed（含無參數 VOL）：啟用即算
        const active = enabled && (entry.repeatable ? (conf.params || []).length > 0 : true);
        if (active) {
          const override = {
            name: entry.klineName,
            calcParams: window.QQIndicators.calcParams(entry, conf),
          };
          if (entry.repeatable) {
            override.styles = { lines: conf.params.map((p) => this._lineStyle(p.color)) };
          }
          if (!this.paneIds[key]) {
            if (onMain) {
              this.chart.createIndicator(override, true, { id: "candle_pane" });
              this.paneIds[key] = true;
            } else {
              this.paneIds[key] = this.chart.createIndicator(override, false, { height: 90 });
            }
          } else if (onMain) {
            this.chart.overrideIndicator(override, "candle_pane");
          } else {
            this.chart.overrideIndicator(override, this.paneIds[key]);
          }
        } else if (this.paneIds[key]) {
          if (onMain) {
            this.chart.removeIndicator("candle_pane", entry.klineName);
            this.paneIds[key] = false;
          } else {
            this.chart.removeIndicator(this.paneIds[key]);
            this.paneIds[key] = null;
          }
        }
      } catch (e) { console.error(entry.klineName + " indicator failed:", e); }
    },
```

- [ ] **Step 3: 語法檢查**

Run: `node --check src/quanquant/web/static/chart.js`
Expected: OK（無 `applySubIndicator` 殘留呼叫——若報未定義，grep `applySubIndicator` 清乾淨）

- [ ] **Step 4: 確認無殘留呼叫**

Run: `grep -n "applySubIndicator" src/quanquant/web/static/chart.js`
Expected: 無輸出

- [ ] **Step 5: 全套件**

Run: `uv run pytest -q`
Expected: PASS（後端不受影響）

- [ ] **Step 6: 手動回歸驗證（記錄於 report，非自動化）**

啟動 `uv run quanquant-web`，登入後於 dashboard 確認：MA 疊主圖四條線、開 WR/BIAS 各自副圖、VOL 副圖、關閉各指標 pane 消失、MA 扣抵仍運作。回報結果。

- [ ] **Step 7: Commit**

```bash
git add src/quanquant/web/static/chart.js
git commit -m "refactor: registry-driven applyIndicators loop"
```

---

### Task 4: 設定 dialog 改主從式 + 指標目錄

**Files:**
- Modify: `src/quanquant/web/templates/dashboard.html:201-240`（指標設定 dialog 重寫）
- Modify: `src/quanquant/web/static/chart.js:772-785`（新增 `activeIndicator`/`catalogOpen`；移除 `indicatorDefs`）
- Modify: `src/quanquant/web/static/chart.js:968-984`（`openSettings`/`saveSettings` 通用化）
- Test: `tests/test_auth_routes.py`（dialog 結構斷言）

**Interfaces:**
- Consumes: `window.QQIndicators`（list/byKey/calcParams）；`this.form`（Task 2 已為 `defaults()`）
- Produces: Alpine 狀態 `activeIndicator: string|null`、`catalogOpen: bool`；`openSettings(key)` 可帶指標 key 跳頁；`saveSettings()` 依 `repeatable` 通用化淨化參數

- [ ] **Step 1: 新增 Alpine 狀態、移除 indicatorDefs**

`chart.js:772-785` 現況：

```js
    settingsOpen: false,
    moreOpen: false,
    fullscreen: false,
    deductionOn: false,
    deductionLegend: [],
    drawScope: "hybrid", // "hybrid" | "all" — cross-timeframe drawing visibility
    drawingsVisible: true, // master show/hide switch for the whole drawing layer
    colorScheme: window.QQ_COLOR_SCHEME || "green_up",
    form: window.QQIndicators.defaults(),
    indicatorDefs: [
      { key: "ma", title: "均線 MA", hint: "疊於主圖" },
      { key: "wr", title: "威廉指標 WR", hint: "副圖" },
      { key: "bias", title: "乖離率 BIAS", hint: "副圖" },
    ],
```

改為（移除 `indicatorDefs`，改由 registry 衍生；新增選取/目錄狀態）：

```js
    settingsOpen: false,
    moreOpen: false,
    fullscreen: false,
    deductionOn: false,
    deductionLegend: [],
    drawScope: "hybrid", // "hybrid" | "all" — cross-timeframe drawing visibility
    drawingsVisible: true, // master show/hide switch for the whole drawing layer
    colorScheme: window.QQ_COLOR_SCHEME || "green_up",
    form: window.QQIndicators.defaults(),
    indicators: window.QQIndicators.list,   // registry（主從式清單/目錄的來源）
    activeIndicator: null,                  // 右欄正在編輯的指標 key
    catalogOpen: false,                     // 「新增指標」目錄是否展開
```

- [ ] **Step 2: `openSettings` 支援帶 key 跳頁**

`chart.js:968-971` 現況：

```js
    openSettings() {
      this.form = JSON.parse(JSON.stringify(QQChart.settings)); // edit a copy
      this.settingsOpen = true;
    },
```

改為：

```js
    openSettings(key) {
      this.form = JSON.parse(JSON.stringify(QQChart.settings)); // edit a copy
      // 預設選取：帶入的 key → 否則第一個已啟用指標 → 否則第一個
      const enabledKeys = this.indicators.filter((e) => this.form[e.key] && this.form[e.key].enabled).map((e) => e.key);
      this.activeIndicator = key || enabledKeys[0] || this.indicators[0].key;
      this.catalogOpen = false;
      this.settingsOpen = true;
    },
```

- [ ] **Step 3: `saveSettings` 依 repeatable 通用化淨化**

`chart.js:973-984` 現況：

```js
    async saveSettings() {
      // sanitize: positive integer periods only
      for (const key of ["ma", "wr", "bias"]) {
        this.form[key].params = this.form[key].params.filter(
          (p) => Number.isFinite(p.period) && p.period >= 1
        );
        this.form[key].params.forEach((p) => { p.period = Math.round(p.period); });
      }
      QQChart.settings = JSON.parse(JSON.stringify(this.form));
      await QQChart.saveIndicators();
      this.settingsOpen = false;
    },
```

改為（對所有 repeatable 指標淨化週期；fixed 指標的 number 欄位取整且至少 1）：

```js
    async saveSettings() {
      for (const entry of this.indicators) {
        const conf = this.form[entry.key];
        if (!conf) continue;
        if (entry.repeatable) {
          conf.params = (conf.params || []).filter((p) => Number.isFinite(p.period) && p.period >= 1);
          conf.params.forEach((p) => { p.period = Math.round(p.period); });
        } else {
          for (const f of entry.paramSchema) {
            if (f.type !== "number") continue;
            const v = Math.round(conf.params[f.field]);
            conf.params[f.field] = Number.isFinite(v) && v >= 1 ? v : (entry.defaults.params[f.field] || 1);
          }
        }
      }
      QQChart.settings = JSON.parse(JSON.stringify(this.form));
      await QQChart.saveIndicators();
      this.settingsOpen = false;
    },
```

- [ ] **Step 4: 重寫設定 dialog 為主從式 + 目錄**

`dashboard.html:201-240` 現況（`x-for in indicatorDefs` 的單一長清單 + VOL 特例區 + footer）整段，改為：

```html
  <!-- 指標設定 dialog（主從式：左清單/目錄，右參數） -->
  <dialog :open="settingsOpen">
    <article class="modal-card ind-settings">
      <header>
        <button aria-label="Close" rel="prev" @click="settingsOpen = false"></button>
        <strong>指標設定</strong>
      </header>

      <div class="ind-split">
        <!-- 左欄：已啟用清單 + 新增指標目錄 -->
        <aside class="ind-list">
          <template x-for="e in indicators.filter((x) => form[x.key] && form[x.key].enabled)" :key="e.key">
            <div class="ind-item" :class="{active: activeIndicator === e.key}"
                 @click="activeIndicator = e.key; catalogOpen = false">
              <span x-text="e.title"></span>
              <button type="button" class="outline mini danger"
                      @click.stop="form[e.key].enabled = false; if (activeIndicator === e.key) activeIndicator = null">−</button>
            </div>
          </template>

          <hr>
          <button type="button" class="outline mini" @click="catalogOpen = !catalogOpen">＋ 新增指標</button>
          <div class="ind-catalog" x-show="catalogOpen" x-cloak>
            <template x-for="e in indicators.filter((x) => !(form[x.key] && form[x.key].enabled))" :key="e.key">
              <button type="button" class="outline mini catalog-item"
                      @click="form[e.key].enabled = true; activeIndicator = e.key; catalogOpen = false"
                      x-text="e.title"></button>
            </template>
            <p class="muted" x-show="indicators.every((x) => form[x.key] && form[x.key].enabled)">全部指標已啟用</p>
          </div>
        </aside>

        <!-- 右欄：選取指標的參數（由 paramSchema 生成） -->
        <section class="ind-detail" x-show="activeIndicator" x-cloak>
          <template x-for="e in indicators.filter((x) => x.key === activeIndicator)" :key="e.key">
            <div>
              <strong x-text="e.title"></strong> <small class="muted" x-text="e.hint"></small>

              <!-- repeatable：多條線 -->
              <template x-if="e.repeatable">
                <div>
                  <template x-for="(p, i) in form[e.key].params" :key="i">
                    <div class="param-row">
                      <input type="number" min="1" step="1" x-model.number="p.period" placeholder="週期">
                      <input type="color" x-model="p.color">
                      <button type="button" class="outline mini danger"
                              @click="form[e.key].params.splice(i, 1)">−</button>
                    </div>
                  </template>
                  <button type="button" class="outline mini"
                          @click="form[e.key].params.push({period: 10, color: '#f0b90b'})">＋ 加一條</button>
                </div>
              </template>

              <!-- fixed：單一物件，依 paramSchema 鋪 number 欄位 -->
              <template x-if="!e.repeatable && e.paramSchema.length">
                <div class="param-fixed">
                  <template x-for="f in e.paramSchema" :key="f.field">
                    <label x-text="f.label">
                      <input type="number" :min="f.min" :step="f.step" x-model.number="form[e.key].params[f.field]">
                    </label>
                  </template>
                </div>
              </template>

              <!-- 無參數指標（VOL） -->
              <template x-if="!e.repeatable && !e.paramSchema.length">
                <p class="muted">此指標無可調參數。</p>
              </template>
            </div>
          </template>
        </section>
      </div>

      <footer class="form-actions">
        <button type="button" class="secondary" @click="settingsOpen = false">取消</button>
        <button type="button" @click="saveSettings()">儲存</button>
      </footer>
    </article>
  </dialog>
```

> 註：右欄 fixed 分支目前只鋪 `type==="number"` 欄位（MACD 三參數）；fixed 指標若未來需要顏色欄位，再擴充 `param-fixed` 迴圈支援 `type==="color"`。repeatable 分支維持與現行相同的「週期(number)+顏色(color)」兩欄（MA/WR/BIAS 皆此形態），確保零回退。

- [ ] **Step 5: 加最小 CSS（主從式版面）**

在 `src/quanquant/web/static/app.css` 檔尾加入：

```css
/* ---- 指標設定主從式 ---- */
.ind-settings { min-width: min(560px, 92vw); }
.ind-split { display: flex; gap: 1rem; }
.ind-list { flex: 0 0 40%; border-right: 1px solid var(--qq-border); padding-right: 0.75rem; }
.ind-detail { flex: 1; }
.ind-item { display: flex; justify-content: space-between; align-items: center;
  padding: 0.35rem 0.5rem; border-radius: var(--qq-radius-sm); cursor: pointer; }
.ind-item.active { background: var(--qq-surface-2); }
.ind-catalog { display: flex; flex-direction: column; gap: 0.25rem; margin-top: 0.4rem; }
.param-fixed { display: flex; flex-wrap: wrap; gap: 0.75rem; margin-top: 0.5rem; }
@media (max-width: 767px) { .ind-split { flex-direction: column; }
  .ind-list { flex-basis: auto; border-right: none; border-bottom: 1px solid var(--qq-border); padding-right: 0; } }
```

- [ ] **Step 6: 寫失敗的 pytest 結構斷言**

在 `tests/test_auth_routes.py` 檔尾加入：

```python
def test_indicator_dialog_is_master_detail(client):
    body = client.get("/").text
    # 主從式版面容器 + 由 registry 衍生（不再有寫死的 indicatorDefs 迴圈）
    assert 'class="ind-split"' in body
    assert 'x-text="e.title"' in body
```

- [ ] **Step 7: 跑 pytest 確認通過**

Run: `uv run pytest tests/test_auth_routes.py -k "master_detail" -v`
Expected: PASS

- [ ] **Step 8: 語法檢查 + 全套件 + 確認無 indicatorDefs 殘留**

Run: `node --check src/quanquant/web/static/chart.js && grep -n "indicatorDefs" src/quanquant/web/static/chart.js src/quanquant/web/templates/dashboard.html; uv run pytest -q`
Expected: chart.js OK；grep 無輸出；pytest 全綠

- [ ] **Step 9: 手動驗證（記錄於 report）**

啟動伺服器，確認：開設定 dialog 呈主從式；左欄僅列已啟用指標；「新增指標」展開目錄列出未啟用者；點目錄項目該指標加入左欄並在右欄顯示參數；− 可移除；MA 參數 ＋/− 線正常；儲存後圖表更新且重整後保留。

- [ ] **Step 10: Commit**

```bash
git add src/quanquant/web/templates/dashboard.html src/quanquant/web/static/chart.js \
        src/quanquant/web/static/app.css tests/test_auth_routes.py
git commit -m "feat: master-detail indicator settings dialog with catalog"
```

---

### Task 5: 警示條件下拉改吃 registry

**Files:**
- Modify: `src/quanquant/web/static/chart.js:793-800`（`indTargets`/`rightTargets` 改衍生）

**Interfaces:**
- Consumes: `window.QQIndicators.alertTargets()`（Task 1）
- Produces: `indTargets` = `[{code:"price",...}, ...alertTargets()]`；`rightTargets` = `[{code:"const",...}, ...alertTargets()]`（code 值 ma/wr/bias/price/const 不變，僅指標顯示文字統一為 registry title）

- [ ] **Step 1: 兩個下拉清單改由 registry 衍生**

`chart.js:793-800` 現況：

```js
    indTargets: [
      { code: "price", label: "收盤價" }, { code: "ma", label: "MA 均線" },
      { code: "wr", label: "WR 威廉" }, { code: "bias", label: "BIAS 乖離" },
    ],
    rightTargets: [
      { code: "const", label: "固定值" }, { code: "ma", label: "MA 均線" },
      { code: "wr", label: "WR 威廉" }, { code: "bias", label: "BIAS 乖離" },
    ],
```

改為：

```js
    indTargets: [{ code: "price", label: "收盤價" }, ...window.QQIndicators.alertTargets()],
    rightTargets: [{ code: "const", label: "固定值" }, ...window.QQIndicators.alertTargets()],
```

- [ ] **Step 2: 語法檢查**

Run: `node --check src/quanquant/web/static/chart.js`
Expected: OK

- [ ] **Step 3: 全套件**

Run: `uv run pytest -q`
Expected: PASS

- [ ] **Step 4: 手動驗證（記錄於 report）**

開警示 dialog，確認「目標」下拉為 收盤價/均線 MA/威廉指標 WR/乖離率 BIAS；「對象」下拉為 固定值 + 同三指標；建立一條 MA 突破警示可正常儲存並顯示於清單（code 值不變，功能與改動前一致）。

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/web/static/chart.js
git commit -m "refactor: alert target dropdowns derive from registry"
```

---

### Task 6: 圖上 ⚙ 跳到對應指標參數頁

**Files:**
- Modify: `src/quanquant/web/static/chart.js:147`（`onEditIndicator` 型別註解）
- Modify: `src/quanquant/web/static/chart.js:180-182`（`onTooltipIconClick` 解析指標身分）
- Modify: `src/quanquant/web/static/chart.js:818`（Alpine 端 `onEditIndicator` 帶 key）

**Interfaces:**
- Consumes: `window.QQIndicators.byKlineName(name)`（Task 1）；`this.openSettings(key)`（Task 4）
- Produces: `QQChart.onEditIndicator` 由 `() => void` 變為 `(key) => void`；點圖上指標 ⚙ 時傳入對應 registry key

**背景/風險**：KLineCharts 9.8.12 的 `onTooltipIconClick` 回呼 `data` 帶有該指標的識別欄位（多為 `data.indicator.name` 或 `data.indicatorName`）。實作時**先於瀏覽器 console 印出 `data` 確認確切欄位名**，再據以取 `klineName`。取不到時退階為不帶 key（維持現行「開設定並選第一個已啟用指標」行為），不可丟例外。

- [ ] **Step 1: `onEditIndicator` 型別註解改為帶 key**

`chart.js:147` 現況：

```js
    onEditIndicator: null,     // () => void：點擊指標 tooltip icon → 打開指標設定
```

改為：

```js
    onEditIndicator: null,     // (key) => void：點擊指標 tooltip icon → 打開該指標設定
```

- [ ] **Step 2: `onTooltipIconClick` 解析指標身分並傳 key**

`chart.js:180-182` 現況：

```js
      this.chart.subscribeAction("onTooltipIconClick", (data) => {
        if (data && data.iconId === "qq-edit" && this.onEditIndicator) this.onEditIndicator();
      });
```

改為（防禦性取名：相容 `data.indicator.name` 與 `data.indicatorName`；取不到則傳 `undefined`）：

```js
      this.chart.subscribeAction("onTooltipIconClick", (data) => {
        if (!data || data.iconId !== "qq-edit" || !this.onEditIndicator) return;
        const klineName = (data.indicator && data.indicator.name) || data.indicatorName || null;
        const entry = klineName ? window.QQIndicators.byKlineName(klineName) : null;
        this.onEditIndicator(entry ? entry.key : undefined);
      });
```

- [ ] **Step 3: Alpine 端 `onEditIndicator` 轉傳 key 給 openSettings**

`chart.js:818` 現況：

```js
      QQChart.onEditIndicator = () => this.openSettings();
```

改為：

```js
      QQChart.onEditIndicator = (key) => this.openSettings(key);
```

（`openSettings(key)` 於 Task 4 已支援：帶 key 則跳該指標，未帶則選第一個已啟用指標。）

- [ ] **Step 4: 語法檢查 + 全套件**

Run: `node --check src/quanquant/web/static/chart.js && uv run pytest -q`
Expected: PASS

- [ ] **Step 5: 手動驗證（記錄於 report）**

啟動伺服器，滑到某指標（如 WR）的 tooltip，點 ✎ icon，確認設定 dialog 開啟且右欄直接停在該指標的參數頁。先於 console 確認 `onTooltipIconClick` 的 `data` 欄位名與 §背景假設一致；若不同，依實際欄位調整 Step 2 的取值。

- [ ] **Step 6: Commit**

```bash
git add src/quanquant/web/static/chart.js
git commit -m "feat: on-chart gear jumps to that indicator's settings"
```

---

### Task 7: 加入 MACD（擴充性驗證）+ 全站手動驗證

**Files:**
- Modify: `src/quanquant/web/static/indicators.js`（registry 加 `macd` 條目）
- Modify: `tests/js/indicators.test.mjs`（MACD calcParams 測試 + 更新 keys 斷言）

**Interfaces:**
- Consumes: 前六個 task 完成的 registry 驅動渲染/UI/持久化管線
- Produces: registry 新增 `macd`（fixed，`repeatable:false`，`paramSchema` fast/slow/signal），證明「加指標只動 registry」

- [ ] **Step 1: 寫失敗測試（MACD 在 registry 且 calcParams 正確）**

在 `tests/js/indicators.test.mjs` 檔尾加入：

```js
test('macd 已註冊為 fixed 指標，calcParams 為 [fast, slow, signal]', () => {
  const macd = QQI.byKey('macd');
  assert.ok(macd, 'macd 應存在於 registry');
  assert.equal(macd.repeatable, false);
  assert.equal(macd.pane, 'sub');
  assert.equal(macd.klineName, 'MACD');
  assert.deepEqual(QQI.calcParams(macd, macd.defaults), [12, 26, 9]);
});

test('macd 預設不啟用，merge 後既有 key 不受影響', () => {
  const d = QQI.defaults();
  assert.equal(d.macd.enabled, false);
  const m = QQI.merge({ ma: { enabled: false } });
  assert.equal(m.ma.enabled, false);
  assert.equal(m.macd.enabled, false); // 舊存檔無 macd → 用 defaults 補
});
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `node --test tests/js/indicators.test.mjs`
Expected: FAIL（`macd 應存在於 registry`）

- [ ] **Step 3: registry 加入 MACD 條目**

在 `src/quanquant/web/static/indicators.js` 的 `REGISTRY` 陣列中，`vol` 條目之後加入：

```js
    {
      key: "macd", title: "MACD", hint: "副圖",
      pane: "sub", klineName: "MACD", alertTarget: false, repeatable: false,
      paramSchema: [
        { field: "fast", type: "number", label: "快線", min: 1, step: 1 },
        { field: "slow", type: "number", label: "慢線", min: 1, step: 1 },
        { field: "signal", type: "number", label: "訊號", min: 1, step: 1 },
      ],
      defaults: { enabled: false, params: { fast: 12, slow: 26, signal: 9 } },
    },
```

- [ ] **Step 4: 更新 Task 1 中會因新增 key 而失效的斷言**

Task 1 的 `test('list 含 ma/wr/bias/vol...')` 與 `test('merge: 舊存檔...')` 斷言了 `Object.keys` 恰為 `['ma','wr','bias','vol']`。將這兩處預期改為包含 `'macd'`：

- `list` keys 斷言：`assert.deepEqual(keys, ['ma', 'wr', 'bias', 'vol', 'macd']);`
- `defaults()` keys 斷言：`assert.deepEqual(Object.keys(d), ['ma', 'wr', 'bias', 'vol', 'macd']);`
- merge keys 斷言：`assert.deepEqual(Object.keys(m), ['ma', 'wr', 'bias', 'vol', 'macd']);`

- [ ] **Step 5: 跑 JS 測試確認通過**

Run: `node --test tests/js/indicators.test.mjs`
Expected: PASS（全部綠，含新增 2 個 MACD test）

- [ ] **Step 6: 語法檢查 + 全套件**

Run: `node --check src/quanquant/web/static/indicators.js && uv run pytest -q`
Expected: PASS

- [ ] **Step 7: 全站手動驗證清單（記錄於 report）**

啟動 `uv run quanquant-web`，登入後逐項確認（spec §6 清單）：

1. 既有 MA/WR/BIAS/VOL 開關與參數行為與升級前一致。
2. 「新增指標」目錄可展開；啟用 MACD 後出現在左欄並可調 fast/slow/signal。
3. MACD 副圖正確渲染；改參數儲存後圖上反映；重整後保留。
4. 移除 MACD（−）後副圖消失、儲存重整仍為移除。
5. repeatable 指標（MA/WR/BIAS）＋/− 線正常。
6. 圖上 MACD 的 ✎ 點擊跳到 MACD 參數頁。
7. 舊帳號（僅存 ma/wr/bias/vol）升級後設定原樣保留、MACD 預設關閉。

- [ ] **Step 8: Commit**

```bash
git add src/quanquant/web/static/indicators.js tests/js/indicators.test.mjs
git commit -m "feat: add MACD indicator (registry extensibility validation)"
```

---

## 完成後

全部 7 task 完成後，功能與尚未部署的配色修正（commit `e0276f9`）一起，由使用者決定何時 `git push` 並 `./scripts/deploy.sh`。部署前 `uv run pytest` 全綠、`node --test tests/js/indicators.test.mjs` 全綠。
