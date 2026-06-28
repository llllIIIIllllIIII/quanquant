# 主圖均線扣抵（即時扣抵 MVP）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 K 線主圖加入使用者可開關的「均線扣抵」即時模式：以最新 K 棒為基準，於各啟用 MA 週期的扣抵 K 棒畫同色倒三角，並在圖頭右上顯示永遠可見的扣抵狀態列。

**Architecture:** 純計算與 KLineCharts overlay 細節獨立成新檔 `ma_deduction.js`（UMD 包裝，瀏覽器掛 `window.MADeduction`、Node 可 `require` 做單測）。`chart.js` 只加薄整合層 `refreshDeduction()`，掛進既有資料/設定更新點，並以 callback 把結果交給 Alpine 狀態列。UI（工具列切換鈕＋狀態列 chips）加在 `dashboard.html` / `app.css`。

**Tech Stack:** Vanilla JS（IIFE/UMD）、KLineCharts v9.8.12 `registerOverlay`/`createOverlay`、Alpine.js、Node 內建 `node:test`（零額外套件）。

## Global Constraints

- KLineCharts 釘版 **v9.8.12**；overlay API 以 v9 為準（v10 改名，勿升版）。
- 扣抵 overlay 一律用 `groupId: "ma-deduction"`、`lock: true`，**不寫入** `/api/chart/state/drawings`（與使用者繪圖 `user-drawings` 完全隔離）。
- 扣抵值來源固定 `close`；走平容忍 `flatThresholdPercent = 0.1`（百分比，MVP 不開放自訂）。
- 開關預設**關**，狀態存 `localStorage["qq_ma_deduction"]`（`"1"`/`"0"`）。
- 不引入前端 build 工具、不動 `pyproject.toml` hatch wheel / force-include 設定。
- 不移除 `chart.js` 既有四道渲染防線（`_nextFrame` 排程、`_watchdog`、try/catch、`resize` 重繪）。
- 部署前 `uv run pytest` 必須全綠（CLAUDE.md）。
- 分支：`feat/ma-deduction`（已建立，spec 已 commit）。

---

### Task 1: 純計算 `computeLive` + UMD 骨架 + 單測

建立 `ma_deduction.js` 的 UMD 包裝與純計算函式，並用 `node:test` 完整覆蓋邏輯分支。

**Files:**
- Create: `src/quanquant/web/static/ma_deduction.js`
- Test: `tests/js/ma_deduction.test.mjs`

**Interfaces:**
- Consumes: 無（純函式，輸入為一般陣列/物件）。
- Produces:
  - `MADeduction.computeLive(bars, params, flatPct)` →
    `bars: {timestamp:number, close:number, ...}[]`（升序）、
    `params: {period:number, color:string}[]`、
    `flatPct?:number`（預設 `0.1`）。
    回傳 `Result[]`，每個 param 一筆（含資料不足者）：
    ```
    {
      period:number, color:string, basePrice:number,
      deductionIndex:number, deductionTime:number|null, deductionValue:number,
      diff:number, diffPercent:number|null,
      status:"upward"|"downward"|"flat"|"insufficient-data"
    }
    ```

- [ ] **Step 1: 寫失敗測試**

建立 `tests/js/ma_deduction.test.mjs`：

```js
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { computeLive } = require('../../src/quanquant/web/static/ma_deduction.js');

// 以收盤價序列造 bars（升序，timestamp 每根 +60s）
function bars(closes) {
  return closes.map((c, i) => ({
    timestamp: 1_000_000 + i * 60_000,
    open: c, high: c, low: c, close: c, volume: 1,
  }));
}
const P = (period, color = '#abc') => ({ period, color });

test('upward: 基準價高於扣抵值且超出容忍', () => {
  const [r] = computeLive(bars([100, 100, 110]), [P(2)], 0.1);
  assert.equal(r.period, 2);
  assert.equal(r.deductionIndex, 0);
  assert.equal(r.deductionValue, 100);
  assert.equal(r.basePrice, 110);
  assert.equal(r.status, 'upward');
});

test('downward: 基準價低於扣抵值且超出容忍', () => {
  const [r] = computeLive(bars([110, 110, 100]), [P(2)], 0.1);
  assert.equal(r.status, 'downward');
  assert.equal(r.diff, -10);
});

test('flat: 差距 <= 0.1%', () => {
  const [r] = computeLive(bars([100, 100, 100.05]), [P(2)], 0.1);
  assert.equal(r.status, 'flat');
});

test('insufficient-data: deductionIndex < 0', () => {
  const [r] = computeLive(bars([100, 101, 102]), [P(5)], 0.1);
  assert.equal(r.status, 'insufficient-data');
  assert.equal(r.deductionIndex, -1);
  assert.equal(r.deductionTime, null);
});

test('除以 0: deductionValue 為 0 時 diffPercent 為 null，靠 diff 判方向', () => {
  const [r] = computeLive(bars([0, 50, 60]), [P(2)], 0.1);
  assert.equal(r.deductionValue, 0);
  assert.equal(r.diffPercent, null);
  assert.equal(r.status, 'upward');
});

test('多週期: 同時回傳，短週期計算、長週期資料不足', () => {
  const out = computeLive(bars([100, 105, 110]), [P(2), P(5)], 0.1);
  assert.equal(out.length, 2);
  assert.equal(out[0].status, 'upward');
  assert.equal(out[1].status, 'insufficient-data');
});

test('deductionTime 取扣抵 K 棒的 timestamp', () => {
  const b = bars([100, 100, 110]);
  const [r] = computeLive(b, [P(2)], 0.1);
  assert.equal(r.deductionTime, b[0].timestamp);
});

test('空 bars 回傳空陣列', () => {
  assert.deepEqual(computeLive([], [P(2)], 0.1), []);
});
```

- [ ] **Step 2: 執行測試確認失敗**

Run: `node --test tests/js/ma_deduction.test.mjs`
Expected: FAIL，錯誤類似 `Cannot find module '.../ma_deduction.js'`（檔案尚未建立）。

- [ ] **Step 3: 建立 `ma_deduction.js`（UMD 包裝 + computeLive）**

建立 `src/quanquant/web/static/ma_deduction.js`：

```js
// 主圖均線扣抵（即時扣抵 MVP）。
// UMD 包裝：瀏覽器掛 window.MADeduction；Node 可 require 做純計算單測。
// register/draw/clear 於 Task 2 補上（依賴瀏覽器全域 klinecharts，故不單測）。
(function (root, factory) {
  const api = factory();
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  if (typeof window !== "undefined") window.MADeduction = api;
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  const GROUP_ID = "ma-deduction";
  const OVERLAY_NAME = "maDeduction";
  const DEFAULT_FLAT_PCT = 0.1;

  function isNum(x) {
    return typeof x === "number" && Number.isFinite(x);
  }

  // 純函式：以最新 K 棒為基準，算出各週期的扣抵狀態。無 DOM 依賴。
  function computeLive(bars, params, flatPct) {
    const pct = isNum(flatPct) ? flatPct : DEFAULT_FLAT_PCT;
    const out = [];
    if (!Array.isArray(bars) || !bars.length || !Array.isArray(params)) return out;

    const latestIndex = bars.length - 1;
    const baseBar = bars[latestIndex];
    const basePrice = baseBar ? baseBar.close : NaN;

    for (const p of params) {
      const period = Math.round(p && p.period);
      const color = p && p.color;
      const di = latestIndex - period;
      const db = bars[di];

      if (!isNum(period) || period < 1 || di < 0 || !db || !isNum(db.close) || !isNum(basePrice)) {
        out.push({
          period, color, basePrice,
          deductionIndex: -1, deductionTime: null, deductionValue: NaN,
          diff: NaN, diffPercent: null, status: "insufficient-data",
        });
        continue;
      }

      const deductionValue = db.close;
      const diff = basePrice - deductionValue;
      const diffPercent = deductionValue === 0 ? null : (Math.abs(diff) / deductionValue) * 100;

      let status;
      if (diffPercent != null && diffPercent <= pct) status = "flat";
      else if (diff > 0) status = "upward";
      else if (diff < 0) status = "downward";
      else status = "flat";

      out.push({
        period, color, basePrice,
        deductionIndex: di, deductionTime: db.timestamp, deductionValue,
        diff, diffPercent, status,
      });
    }
    return out;
  }

  return { GROUP_ID, OVERLAY_NAME, computeLive };
});
```

- [ ] **Step 4: 執行測試確認通過**

Run: `node --test tests/js/ma_deduction.test.mjs`
Expected: PASS（8 個測試全綠）。

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/web/static/ma_deduction.js tests/js/ma_deduction.test.mjs
git commit -m "feat: add MA deduction live-mode pure compute with node tests"
```

---

### Task 2: 自訂三角 overlay 註冊與 draw/clear

在 `ma_deduction.js` 補上 `register()`（註冊 KLineCharts 自訂 overlay 畫倒三角＋ hover 文字）與 `draw()` / `clear()`。加最小防呆測試（不需 DOM）。

**Files:**
- Modify: `src/quanquant/web/static/ma_deduction.js`
- Test: `tests/js/ma_deduction.test.mjs`（追加防呆測試）

**Interfaces:**
- Consumes: `computeLive` 的 `Result[]`（Task 1）；瀏覽器全域 `klinecharts`、KLineCharts `chart` 實例。
- Produces:
  - `MADeduction.register()` → 冪等註冊自訂 overlay（`klinecharts` 不存在時安全 no-op）。
  - `MADeduction.draw(chart, results)` → 先清掉 `ma-deduction` group，再為非 `insufficient-data` 的每筆建立 `lock:true` overlay。`chart` 為 falsy 時 no-op。
  - `MADeduction.clear(chart)` → 移除整個 `ma-deduction` group。`chart` 為 falsy 時 no-op。

- [ ] **Step 1: 追加防呆失敗測試**

在 `tests/js/ma_deduction.test.mjs` 末端追加（與 Task 1 同一 require）：

```js
const MAD = require('../../src/quanquant/web/static/ma_deduction.js');

test('register/draw/clear 皆為函式', () => {
  assert.equal(typeof MAD.register, 'function');
  assert.equal(typeof MAD.draw, 'function');
  assert.equal(typeof MAD.clear, 'function');
});

test('register 在無 klinecharts 環境安全 no-op（不丟例外）', () => {
  assert.doesNotThrow(() => MAD.register());
});

test('draw/clear 對 falsy chart 安全 no-op', () => {
  assert.doesNotThrow(() => MAD.draw(null, []));
  assert.doesNotThrow(() => MAD.clear(undefined));
});

test('GROUP_ID 與 user-drawings 隔離', () => {
  assert.equal(MAD.GROUP_ID, 'ma-deduction');
  assert.notEqual(MAD.GROUP_ID, 'user-drawings');
});
```

- [ ] **Step 2: 執行測試確認失敗**

Run: `node --test tests/js/ma_deduction.test.mjs`
Expected: FAIL（`typeof MAD.register` 為 `'undefined'`，register/draw/clear 尚未實作）。

- [ ] **Step 3: 在 `ma_deduction.js` 實作 register/draw/clear**

在 `computeLive` 之後、`return { ... }` 之前插入：

```js
  let registered = false;

  // 註冊自訂 overlay：在扣抵 K 棒、扣抵價下方畫 MA 同色倒三角；hover 時加精簡文字。
  function register() {
    if (registered) return;
    if (typeof klinecharts === "undefined" || !klinecharts.registerOverlay) return;
    klinecharts.registerOverlay({
      name: OVERLAY_NAME,
      totalStep: 1,
      needDefaultPointFigure: false,
      needDefaultXAxisFigure: false,
      needDefaultYAxisFigure: false,
      createPointFigures: function (params) {
        const coordinates = params.coordinates || [];
        const overlay = params.overlay || {};
        const c = coordinates[0];
        if (!c) return [];
        const ext = overlay.extendData || {};
        const color = ext.color || "#888888";
        const w = 5, h = 7, gap = 6;
        const top = c.y + gap;
        const figures = [{
          type: "polygon",
          attrs: { coordinates: [
            { x: c.x - w, y: top },
            { x: c.x + w, y: top },
            { x: c.x, y: top + h },
          ]},
          styles: { style: "fill", color: color },
        }];
        if (ext.hovered) {
          const val = Number.isFinite(ext.deductionValue)
            ? Math.round(ext.deductionValue).toLocaleString() : "";
          figures.push({
            type: "text",
            attrs: { x: c.x + w + 2, y: top, text: "MA" + ext.period + " 扣抵 " + val, baseline: "top" },
            styles: { color: color, size: 11, family: "inherit",
              backgroundColor: "rgba(0,0,0,0.7)",
              paddingLeft: 4, paddingRight: 4, paddingTop: 1, paddingBottom: 1 },
          });
        }
        return figures;
      },
    });
    registered = true;
  }

  function setHover(chart, overlay, hovered) {
    try {
      chart.overrideOverlay({
        id: overlay.id,
        extendData: Object.assign({}, overlay.extendData, { hovered: hovered }),
      });
    } catch (e) { /* hover is cosmetic */ }
  }

  // 先清整個 group，再依結果重建。三角錨定扣抵 K 棒時間戳，捲動自動跟隨、出界自然裁切。
  function draw(chart, results) {
    if (!chart) return;
    clear(chart);
    if (!Array.isArray(results)) return;
    for (const r of results) {
      if (!r || r.status === "insufficient-data") continue;
      try {
        chart.createOverlay({
          name: OVERLAY_NAME,
          groupId: GROUP_ID,
          lock: true,
          points: [{ timestamp: r.deductionTime, value: r.deductionValue }],
          extendData: {
            color: r.color, period: r.period,
            deductionValue: r.deductionValue, hovered: false,
          },
          onMouseEnter: function (e) { setHover(chart, e.overlay, true); return false; },
          onMouseLeave: function (e) { setHover(chart, e.overlay, false); return false; },
        });
      } catch (e) { /* 單條失敗不影響其他 MA 與 K 線渲染 */ }
    }
  }

  function clear(chart) {
    if (!chart) return;
    try { chart.removeOverlay({ groupId: GROUP_ID }); } catch (e) { /* idempotent */ }
  }
```

並把回傳改為：

```js
  return { GROUP_ID, OVERLAY_NAME, computeLive, register, draw, clear };
```

- [ ] **Step 4: 執行測試確認通過**

Run: `node --test tests/js/ma_deduction.test.mjs`
Expected: PASS（12 個測試全綠）。

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/web/static/ma_deduction.js tests/js/ma_deduction.test.mjs
git commit -m "feat: add MA deduction triangle overlay register/draw/clear"
```

---

### Task 3: `chart.js` 整合 `refreshDeduction()`

在 `QQChart` 加扣抵狀態與 `refreshDeduction()` / `setDeduction()`，於 `init()` 註冊 overlay，並把 `refreshDeduction()` 掛進既有的 5 個資料/設定更新點。

**Files:**
- Modify: `src/quanquant/web/static/chart.js`

**Interfaces:**
- Consumes: `MADeduction.register/computeLive/draw/clear`（Task 1、2）；`this.settings.ma.params`（既有）。
- Produces（供 Task 4 的 Alpine 使用）：
  - `QQChart.deductionEnabled: boolean`
  - `QQChart.onDeductionUpdate: ((results) => void) | null`
  - `QQChart.setDeduction(on: boolean): void`
  - `QQChart.refreshDeduction(): void`

- [ ] **Step 1: 加狀態欄位**

在 `QQChart` 物件的 `settings:` 那行之後加入（`chart.js` 約 line 87 附近）：

```js
    deductionEnabled: false,   // 均線扣抵開關（由 Alpine 依 localStorage 設定）
    onDeductionUpdate: null,   // (results) => void：把扣抵結果交給狀態列
```

- [ ] **Step 2: `init()` 註冊 overlay**

在 `init()` 內、`this.chart = klinecharts.init(...)` 之後、`await this.loadInitial();` 之前加入一行（約 line 99 附近）：

```js
      if (window.MADeduction) window.MADeduction.register();
```

- [ ] **Step 3: 新增 `refreshDeduction()` 與 `setDeduction()`**

在 `saveIndicators()` 方法之後（約 line 432 之後）插入：

```js
    // ---- MA deduction (live mode) ----

    setDeduction(on) {
      this.deductionEnabled = !!on;
      this.refreshDeduction();
    },

    // 以最新 K 棒重算各啟用 MA 的扣抵，重畫三角並更新狀態列。冪等、計算量低。
    refreshDeduction() {
      if (!this.chart || !window.MADeduction) return;
      const cb = this.onDeductionUpdate;
      if (!this.deductionEnabled) {
        window.MADeduction.clear(this.chart);
        if (cb) cb([]);
        return;
      }
      const bars = this.chart.getDataList() || [];
      const params = (this.settings.ma && this.settings.ma.params) || [];
      const results = window.MADeduction.computeLive(bars, params, 0.1);
      window.MADeduction.draw(this.chart, results);
      if (cb) cb(results);
    },
```

- [ ] **Step 4: 掛進 5 個更新點**

逐處在資料/設定就緒後加 `this.refreshDeduction();`：

1. `loadInitial()` 結尾，`this._watchdog();` 之後（約 line 193）：

```js
      this._watchdog();
      this.refreshDeduction();
```

2. `_switchFromCacheOrLoad()` 內 cache 命中分支，`this.pollLatest();` 之前（約 line 292）：

```js
        this._watchdog();
        this.refreshDeduction();
        this.pollLatest();
```

3. `pollLatest()` 內 for 迴圈之後（約 line 233）。把：

```js
        for (const bar of data.bars || []) this._applyBar(bar);
```

改為：

```js
        for (const bar of data.bars || []) this._applyBar(bar);
        if ((data.bars || []).length) this.refreshDeduction();
```

4. `onQuote()` 結尾，`this._applyBar({...});` 之後（約 line 280，方法最後一行之後）：

```js
        volume: last.volume,
      });
      this.refreshDeduction();
    },
```

5. `saveIndicators()` 內 `await this.applyIndicators();` 之後（約 line 424）：

```js
      await this.applyIndicators();
      this.refreshDeduction();
```

- [ ] **Step 5: 語法檢查**

Run: `node --check src/quanquant/web/static/chart.js`
Expected: 無輸出（exit 0）即語法正確。

- [ ] **Step 6: Commit**

```bash
git add src/quanquant/web/static/chart.js
git commit -m "feat: wire MA deduction refresh into chart data/settings updates"
```

---

### Task 4: UI — 工具列切換鈕、狀態列、樣式、Alpine 方法

加工具列切換鈕（仿市場脈搏，預設關、localStorage 記憶）、圖頭右上狀態列 chips、對應 CSS，與 Alpine `chartPanel()` 的狀態與方法；並把 `ma_deduction.js` 加入頁面載入（在 `chart.js` 之前）。

**Files:**
- Modify: `src/quanquant/web/templates/dashboard.html`
- Modify: `src/quanquant/web/static/chart.js`（`chartPanel()` Alpine 元件）
- Modify: `src/quanquant/web/static/app.css`

**Interfaces:**
- Consumes: `QQChart.setDeduction`、`QQChart.onDeductionUpdate`、`QQChart.deductionEnabled`（Task 3）。
- Produces: Alpine 狀態 `deductionOn:boolean`、`deductionLegend:Result[]`；方法 `toggleDeduction()`、`dedArrow(status)`、`dedTitle(r)`。

- [ ] **Step 1: 載入 `ma_deduction.js`**

在 `dashboard.html` line 181（`window.QQ_SYMBOL` 那行）之後、line 182（chart.js）之前插入：

```html
<script src="/static/ma_deduction.js"></script>
```

- [ ] **Step 2: 工具列加切換鈕**

在 `dashboard.html` 的 `#pulse-toggle` 按鈕（line 22-24）之後加入：

```html
      <button type="button" class="outline mini ded-toggle" :class="{active: deductionOn}"
              @click="toggleDeduction()" :aria-pressed="deductionOn" aria-label="均線扣抵"
              :title="deductionOn ? '均線扣抵：開（點擊關閉）' : '均線扣抵：關（點擊開啟）'">▽ 扣抵</button>
```

- [ ] **Step 3: 圖頭下方加狀態列**

在 `dashboard.html` 的 `.chart-head` 結束 `</div>`（line 26）之後、`.tf-bar`（line 28）之前插入：

```html
  <div class="ded-legend" x-show="deductionOn" x-cloak>
    <span class="muted ded-legend-label">即時扣抵</span>
    <template x-if="deductionLegend.length === 0">
      <small class="muted">請先在指標設定加入 MA</small>
    </template>
    <template x-for="r in deductionLegend" :key="r.period">
      <span class="ded-chip" :style="`color:${r.color}`" :title="dedTitle(r)">
        <span x-text="'MA' + r.period"></span>
        <span x-text="r.status === 'insufficient-data' ? '—' : Math.round(r.deductionValue).toLocaleString()"></span>
        <span class="ded-arrow" x-text="dedArrow(r.status)"></span>
      </span>
    </template>
  </div>
```

- [ ] **Step 4: Alpine 加狀態與方法**

在 `chart.js` 的 `window.chartPanel = () => ({ ... })` 內，於 `settingsOpen: false,`（約 line 546）附近加狀態：

```js
    deductionOn: false,
    deductionLegend: [],
```

在 `init()` 方法（約 line 579）內，`QQChart.init();` 之後加入：

```js
      this.deductionOn = localStorage.getItem("qq_ma_deduction") === "1";
      QQChart.onDeductionUpdate = (results) => { this.deductionLegend = results; };
      QQChart.deductionEnabled = this.deductionOn;
```

在 `draw(name)` / `clearDrawings()`（約 line 671）附近加方法：

```js
    toggleDeduction() {
      this.deductionOn = !this.deductionOn;
      localStorage.setItem("qq_ma_deduction", this.deductionOn ? "1" : "0");
      QQChart.setDeduction(this.deductionOn);
    },
    dedArrow(status) {
      return { upward: "↑", downward: "↓", flat: "→" }[status] || "—";
    },
    dedTitle(r) {
      if (r.status === "insufficient-data") return `MA${r.period} 扣抵：資料不足`;
      const dv = Math.round(r.deductionValue).toLocaleString();
      const bp = Math.round(r.basePrice).toLocaleString();
      const pct = r.diffPercent == null ? "—" : r.diffPercent.toFixed(2) + "%";
      const lbl = { upward: "傾向上彎", downward: "傾向下彎", flat: "傾向走平" }[r.status];
      return `MA${r.period} 即時扣抵\n扣抵位置：${r.period} 根前\n扣抵價：${dv}\n基準價：${bp}\n差距：${pct}\n狀態：MA${r.period} ${lbl}`;
    },
```

- [ ] **Step 5: 加樣式**

在 `app.css` 末端加入（沿用既有 mini 按鈕風格）：

```css
/* 均線扣抵 */
[x-cloak] { display: none !important; }
.ded-toggle.active { color: #f0b90b; border-color: #f0b90b; }
.ded-legend {
  display: flex; flex-wrap: wrap; align-items: center; gap: 10px;
  margin: 4px 0 2px; font-variant-numeric: tabular-nums;
}
.ded-legend-label { font-size: 12px; }
.ded-chip {
  display: inline-flex; align-items: center; gap: 4px;
  font-size: 12px; font-weight: 600;
}
.ded-chip .ded-arrow { font-weight: 700; }
```

- [ ] **Step 6: 語法檢查**

Run: `node --check src/quanquant/web/static/chart.js`
Expected: 無輸出（exit 0）。

- [ ] **Step 7: Commit**

```bash
git add src/quanquant/web/templates/dashboard.html src/quanquant/web/static/chart.js src/quanquant/web/static/app.css
git commit -m "feat: add MA deduction toggle, status legend, and styles"
```

---

### Task 5: 整合驗證與回歸

跑全部測試（JS 純計算 + 後端 pytest 回歸），手動啟動驗證開關／三角／狀態列，最後收束分支。

**Files:**
- 無新增；驗證既有變更。

**Interfaces:**
- Consumes: 全部前述任務成果。
- Produces: 可部署的綠燈狀態。

- [ ] **Step 1: JS 單測全綠**

Run: `node --test tests/js/ma_deduction.test.mjs`
Expected: PASS（12 個測試）。

- [ ] **Step 2: 後端 pytest 回歸（CLAUDE.md：必須全綠）**

Run: `uv run pytest -q`
Expected: 全綠（本功能為純前端，後端不應受影響；若有紅燈須先修）。

- [ ] **Step 3: 手動啟動驗證**

Run: `uv run quanquant-web`，瀏覽 `http://127.0.0.1:8000`，逐項確認：

- 工具列出現「▽ 扣抵」鈕，預設**未** active，狀態列不顯示。
- 點擊後鈕變 active；MA 設定有 5/10/20/60 時，圖上在對應扣抵 K 棒出現各 MA 同色倒三角；右上狀態列顯示 `MA5 … ↑/→/↓` 等。
- 滑鼠移到三角上出現「MAxx 扣抵 17,9xx」精簡文字。
- 滑鼠移到狀態列 chip 上，`title` 顯示完整多行（扣抵位置/扣抵價/基準價/差距/狀態）。
- 切換週期（1分↔日）、時段（全部/日盤/夜盤）後，扣抵自動重算。
- 在「指標設定」改 MA 週期/顏色並儲存，三角與狀態列同步更新顏色/位置。
- 把長均線（MA60）扣抵捲出畫面：圖上三角消失但狀態列仍顯示該 MA 狀態。
- 關閉開關：三角全消、狀態列收起；重整頁面後維持關閉（localStorage）。再開→重整→維持開啟。
- 全程 K 線、既有 MA 線、使用者繪圖正常，無空白圖（watchdog 未觸發 console 警告）。

- [ ] **Step 4: 確認扣抵 overlay 未污染繪圖持久化**

開啟扣抵後重整頁面，確認扣抵三角**不會**因 `restoreDrawings` 而重複堆疊或變成可刪繪圖（它們走 `ma-deduction` group、`lock:true`、不進 `/api/chart/state/drawings`）。可在 DevTools Network 檢視 `PUT /api/chart/state/drawings` 的 body 不含扣抵點。

- [ ] **Step 5: 收束分支**

依 `superpowers:finishing-a-development-branch` 處理（合併 / 開 PR / 清理），交由使用者選擇。

---

## Self-Review

**1. Spec coverage（對照 `docs/superpowers/specs/2026-06-29-ma-deduction-design.md`）：**
- §2 決策（範圍/顯示/開關/來源/容忍/畫面外）→ Task 1（來源、容忍、計算）、Task 2（三角、出界裁切）、Task 4（開關、狀態列）。✓
- §4 計算邏輯（含 diff===0、除以 0、資料不足）→ Task 1 測試全覆蓋。✓
- §5 三角 overlay（同色、獨立 group、lock、不持久化、重畫）→ Task 2 + Task 5 Step 4 驗證。✓
- §6 狀態列（chips、箭頭、title 完整文案、無 MA 提示）→ Task 4 Step 3/4。✓
- §7 重算觸發點（5 處）→ Task 3 Step 4。✓
- §8 開關/預設/localStorage/無 MA 提示 → Task 4 Step 2/4。✓
- §9 邊界 → Task 1（insufficient/除以 0/缺欄位）+ Task 2 try/catch。✓
- §10 測試（node:test、6 類案例、零套件）→ Task 1/2 測試。✓
- §12 既有約束（釘版、防線、隔離、不動 hatch）→ Global Constraints + Task 5 Step 4。✓

**2. Placeholder scan：** 無 TBD/TODO；每個程式步驟均含完整程式碼與預期輸出。✓

**3. Type consistency：** `computeLive(bars, params, flatPct)` 回傳的 `Result` 欄位（`period/color/basePrice/deductionIndex/deductionTime/deductionValue/diff/diffPercent/status`）在 Task 2 `draw`、Task 4 `dedTitle/dedArrow` 一致引用；`GROUP_ID="ma-deduction"`、`OVERLAY_NAME="maDeduction"`、`setDeduction/refreshDeduction/onDeductionUpdate/deductionEnabled` 命名跨任務一致。✓
