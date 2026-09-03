// Interactivity is handled declaratively:
//   - HTMX: SSE live quote, table swaps, and CRUD requests
//   - Alpine: modal open/close state
//   - HX-Trigger response headers: `closemodal` (close the modal after a save) and
//     `refreshtable` (re-fire the filter form so the table keeps current filters)
//   - hx-disabled-elt on the trade form: disables the submit button in-flight,
//     preventing double submits
//
// No imperative JS is needed today; this file is a placeholder for future glue.

// Theme toggle. Server renders the authoritative data-theme for logged-in
// users; this just flips it live, persists to the API, and lets the chart
// re-skin via the qq:theme-changed event.
window.QQTheme = {
  current() {
    return document.documentElement.getAttribute("data-theme") === "light" ? "light" : "dark";
  },
  apply(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    localStorage.setItem("qq_theme", theme); // login 頁（匿名）預載用
    window.dispatchEvent(new CustomEvent("qq:theme-changed", { detail: theme }));
  },
  toggle() {
    const next = this.current() === "dark" ? "light" : "dark";
    this.apply(next);
    fetch("/api/user/theme", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ theme: next }),
    }).catch(() => { /* 即時已套用；存回失敗僅影響下次載入 */ });
  },
};

// MEDIUM-5（007 收尾，MEDIUM-3 修正）：精簡條（position-strip，見 orders.html
// #pos-strip-box）除了既有 quote-stream 心跳（節流 5 秒，承接報價節奏讓浮損「隨報價
// 即時更新」，見 orders.html 內的取捨說明）之外，額外掛 orders-stream 的 `deal` 事件
// 驅動即時刷新——非本分頁來源的成交（如 agent 端非同步回報）不必再等最多 ~5 秒的節流
// 節奏，改成幾乎即時反映在部位/浮損上；報價節奏本身（PnL 隨報價 tick 更新）維持不動，
// 不受影響。
//
// MEDIUM-3（fresh-context 終審修復）：
//   1. 只橋接 `sse:deal`，不再橋接 `sse:orders-changed`——orders-changed 已經被各頁
//      需要它的元素直接監聽（如 orders.html 的 #agent-status-box：
//      `hx-trigger="... sse:orders-changed"`），這裡再補發一次 `refreshorders` 只是
//      讓同一個 orders-changed ping 造成雙倍請求（該元素自己的直接監聽 + 這裡橋接出的
//      `refreshorders` 又再觸發一次），沒有換到任何新功能。
//   2. `sse:deal` 加 ~300ms debounce——一筆 client_order_id 的市價單滑價分批成交時，
//      RawInboxWorker 會連續 publish_deal 好幾次（一價一事件，見 broker/order_events.py
//      docstring），沒有節流的話一批 5 筆成交會在毫秒內併發出 5 次 `refreshorders`
//      （疊加各元素自己的 hx-get，一次批次可能衝到近 10 次 HTTP）——這正是專案當初把
//      委託/部位表從 `every 2s` 輪詢改成 SSE push 想消滅的那種壓力，只是換了個觸發源。
//      用 clearTimeout/setTimeout 把同一批（300ms 內）的多次 `sse:deal` 合併成一次
//      `refreshorders`，量從 O(成交筆數) 降到 O(1)（每批次一次），不影響「有成交就會
//      刷新」的正確性，只是把時機從「逐筆」改成「批次結束後一次」。
let _dealRefreshTimer = null;
document.addEventListener("sse:deal", () => {
  if (_dealRefreshTimer) clearTimeout(_dealRefreshTimer);
  _dealRefreshTimer = setTimeout(() => {
    _dealRefreshTimer = null;
    if (window.htmx) window.htmx.trigger(document.body, "refreshorders");
  }, 300);
});
