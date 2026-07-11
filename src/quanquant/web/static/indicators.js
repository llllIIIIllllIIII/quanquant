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
      defaults: { enabled: true, visible: true, params: [
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
      defaults: { enabled: false, visible: true, params: [
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
      defaults: { enabled: false, visible: true, params: [
        { period: 6, color: "#f0b90b" }, { period: 12, color: "#2196f3" },
        { period: 24, color: "#e91e63" },
      ] },
    },
    {
      key: "vol", title: "成交量 VOL", hint: "副圖",
      pane: "sub", klineName: "VOL", alertTarget: false, repeatable: false,
      paramSchema: [],
      defaults: { enabled: true, visible: true, params: {} },
    },
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
  ];

  function clone(x) { return JSON.parse(JSON.stringify(x)); }
  function byKey(key) { return REGISTRY.find((e) => e.key === key) || null; }
  function byKlineName(name) { return REGISTRY.find((e) => e.klineName === name) || null; }
  function shortName(key) { const e = byKey(key); return (e && e.klineName) || key; }

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

  function defaultColors(entry) {
    return (entry && entry.lines) ? entry.lines.map((l) => l.default) : [];
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

  // 集中判定「這根指標當前要不要畫」。nakedK（全域裸K）優先蓋掉一切；
  // 其次看使用者是否啟用、是否單獨隱藏（visible===false）；repeatable 需至少一條線。
  function resolveVisibility(entry, conf, nakedK) {
    if (nakedK) return false;
    if (!entry || !conf || !conf.enabled) return false;
    if (conf.visible === false) return false;
    if (entry.repeatable) return (conf.params || []).length > 0;
    return true;
  }

  return { list: REGISTRY, byKey, byKlineName, defaults, merge, alertTargets, calcParams, resolveVisibility, shortName, defaultColors };
});
