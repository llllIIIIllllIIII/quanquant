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

  return { GROUP_ID, OVERLAY_NAME, computeLive };
});
