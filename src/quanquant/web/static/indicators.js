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
