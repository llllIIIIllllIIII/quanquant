// 分時均價線（VWAP）。當日成交量加權均價，疊於主圖，供「分時走勢」模式使用。
// 日盤(08:45)、夜盤(15:00)各自重置累積，跨 session 不連續。
// UMD 包裝：瀏覽器掛 window.QQAvgPrice；Node 可 require 做純 VWAP 單測。
(function (root, factory) {
  const api = factory();
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  if (typeof window !== "undefined") window.QQAvgPrice = api;
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  const IND_NAME = "AVG";
  const TPE_OFFSET_MS = 8 * 3600 * 1000; // 台北固定 UTC+8（無日光節約），+8h 後取 UTC 欄位即台北牆鐘
  const DAY_MS = 86400000;
  const DAY_OPEN = 8 * 60 + 45; // 日盤 08:45
  const DAY_CLOSE = 13 * 60 + 45; // 日盤 13:45
  const NIGHT_CLOSE = 5 * 60; // 夜盤次日 05:00

  function isNum(x) {
    return typeof x === "number" && Number.isFinite(x);
  }

  // 該 bar 所屬「交易時段桶」鍵；桶改變即日界、需重置累積。
  // 日盤以曆日為錨（"D"+dayStamp）；夜盤跨午夜，<=05:00 的 bar 歸前一交易日夜盤（"N"+anchor）。
  function sessionKey(ts) {
    if (!isNum(ts)) return null;
    const d = new Date(ts + TPE_OFFSET_MS);
    const minutes = d.getUTCHours() * 60 + d.getUTCMinutes();
    const dayStamp = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
    if (minutes >= DAY_OPEN && minutes <= DAY_CLOSE) return "D" + dayStamp;
    let anchor = dayStamp;
    if (minutes <= NIGHT_CLOSE) anchor -= DAY_MS; // 午夜後 → 前一交易日夜盤
    return "N" + anchor;
  }

  // 純函式：逐根算當日累積 VWAP = Σ(典型價×量)/Σ量，典型價 =(H+L+C)/3。
  // 遇時段桶切換重置；量為 0/NaN 不計入。回傳與 bars 等長的 [{ avg }]。
  function computeVwap(bars) {
    const out = [];
    if (!Array.isArray(bars)) return out;
    let key = null;
    let cumPV = 0;
    let cumV = 0;
    let lastAvg = NaN;
    for (const b of bars) {
      const k = sessionKey(b && b.timestamp);
      if (k !== key) {
        key = k;
        cumPV = 0;
        cumV = 0;
        lastAvg = NaN;
      }
      const tp = (Number(b && b.high) + Number(b && b.low) + Number(b && b.close)) / 3;
      const v = Number(b && b.volume);
      if (isNum(tp) && isNum(v) && v > 0) {
        cumPV += tp * v;
        cumV += v;
        lastAvg = cumPV / cumV;
      } else if (!isNum(lastAvg) && isNum(tp)) {
        lastAvg = tp; // 時段起點量為 0 時，先以典型價墊線，避免斷點
      }
      out.push({ avg: isNum(lastAvg) ? lastAvg : NaN });
    }
    return out;
  }

  let registered = false;

  // 註冊為主圖 price-series 自訂指標（跟隨 candle y 軸，同內建 MA/BOLL）。
  // 線色/線型由 chart.js 的 _applyIndicator 依 registry 的 lines+colors override，
  // 此處只定義 figure 結構。無 klinecharts 時安全 no-op。
  function register() {
    if (registered) return;
    if (typeof klinecharts === "undefined" || !klinecharts.registerIndicator) return;
    klinecharts.registerIndicator({
      name: IND_NAME,
      shortName: IND_NAME,
      series: "price",
      precision: 2,
      figures: [{ key: "avg", title: "均價: ", type: "line" }],
      calc: function (dataList) {
        return computeVwap(dataList);
      },
    });
    registered = true;
  }

  return { IND_NAME, sessionKey, computeVwap, register };
});
