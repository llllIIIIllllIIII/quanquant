// 主圖均線扣抵（即時扣抵 MVP）。
// UMD 包裝：瀏覽器掛 window.MADeduction；Node 可 require 做純計算單測。
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

  return { GROUP_ID, OVERLAY_NAME, computeLive, register, draw, clear };
});
