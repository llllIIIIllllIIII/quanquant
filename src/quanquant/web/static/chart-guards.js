// 前端資料防呆的純判斷邏輯，抽出以便 node 單元測試（chart.js 於 <script> 委派使用）。
// 與 chart.js 內聯版本行為等價；無 DOM/框架依賴。
(function (root, factory) {
  if (typeof module !== "undefined" && module.exports) module.exports = factory();
  else root.QQChartGuards = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  const finite = (v) => Number.isFinite(v);

  // 一根 bar 是否可安全寫入圖表：OHLC + timestamp 必須有限，且不得早於已見最後一根
  // （亂序）。lastBarTs 為 0/falsy（初始）時放行首根。
  function isRenderableBar(bar, lastBarTs) {
    if (!bar || !finite(bar.timestamp) ||
        !finite(bar.open) || !finite(bar.high) ||
        !finite(bar.low) || !finite(bar.close)) return false;
    if (lastBarTs && bar.timestamp < lastBarTs) return false; // 亂序
    return true;
  }

  // 是否應凍結（不以此報價覆寫最後一根真棒）：休市／停滯——meta.fresh 明確為 false，
  // 或 meta.status 存在且非 "open"。meta 缺失時不凍結（維持相容）。
  function shouldFreezeQuote(meta) {
    return !!(meta && (meta.fresh === false || (meta.status && meta.status !== "open")));
  }

  return { isRenderableBar, shouldFreezeQuote };
});
