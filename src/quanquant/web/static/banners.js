// 007：下單反饋橫幅（banner-stack）。事件來源兩種，互補不重疊：
//   1. HTTP 同步回應（place/cancel/update）——後端用 HX-Trigger 的 JSON 'order-banner'
//      事件帶 {kind, ok, title, detail} 純文字摘要（見 web/routers/orders.py
//      ::_banner_event），修 P0-4/P0-5/P0-6 的回饋缺口：使用者剛按下送出/取消/改單，
//      同步失敗（如超過風控額度）永遠不會經過 RawInboxWorker，SSE 事件不會發生，只能
//      靠這條路徑。htmx 對 JSON 物件形式的 HX-Trigger 值，直接把該物件當 event.detail
//      （不包一層 .value），從觸發請求的元素冒泡到 document.body。
//   2. SSE `deal`/`order-report`（見 broker/order_events.py，B-1 已交付）——券商端非同步
//      成交/委託狀態變化。各交易頁在既有 `hx-ext="sse" sse-connect="/orders/stream"`
//      作用域內放了 partials/banner_stack.html（`hx-trigger="sse:deal, sse:order-report"`
//      的隱形 sink），htmx-ext-sse 見到具名 sse: trigger 才會對該事件名
//      `source.addEventListener(...)`；收到時呼叫 `htmx.trigger(elt, "sse:<name>",
//      event)`，event 是原始 SSE MessageEvent（`.data` 才是 payload 字串），這個
//      CustomEvent 會冒泡到 document——本檔只在 document 監聽，不另開 EventSource
//      連線、也不對這兩個事件發出任何 HTTP 請求。
(function () {
  "use strict";

  var MAX_BANNERS = 5; // 堆疊上限（原型參數，可調）
  var SUCCESS_MS = 5000; // 成交/成功停留時長
  var FAIL_MS = 8000; // 失敗要比成功停得久
  var LEAVE_MS = 220; // 對齊 qq-banner-out 動畫時長

  var STATUS_LABEL = {
    pending: "待送出", sending: "傳送中", submitted: "已委託",
    partfilled: "部分成交", filled: "全部成交", cancelled: "已取消",
    failed: "失敗", unknown: "狀態不明",
  };
  var ACTION_LABEL = { Buy: "買", Sell: "賣" };

  function stackEl() {
    return document.querySelector(".banner-stack");
  }

  function trim(stack) {
    while (stack.children.length > MAX_BANNERS) {
      stack.removeChild(stack.firstElementChild);
    }
  }

  function dismiss(el) {
    if (!el || !el.parentNode) return;
    el.classList.add("leaving");
    // 不依賴 animationend——prefers-reduced-motion 關掉動畫時該事件永遠不會觸發；
    // 用 timeout 對齊動畫時長，兩種情況都能正確移除。
    window.setTimeout(function () {
      if (el.parentNode) el.parentNode.removeChild(el);
    }, LEAVE_MS);
  }

  function push(opts) {
    var stack = stackEl();
    if (!stack) return; // 本頁沒有 banner-stack 容器（非交易頁）——安靜不做事
    var el = document.createElement("div");
    el.className = "order-banner" + (opts.variant ? " " + opts.variant : "");
    el.setAttribute("role", "status");
    var title = document.createElement("div");
    title.className = "banner-title";
    title.textContent = opts.title || "";
    el.appendChild(title);
    if (opts.detail) {
      var detail = document.createElement("div");
      detail.className = "banner-detail";
      detail.textContent = opts.detail;
      el.appendChild(detail);
    }
    el.addEventListener("click", function () { dismiss(el); }); // 點擊可提前關閉
    stack.appendChild(el);
    trim(stack);
    window.setTimeout(function () { dismiss(el); }, opts.fail ? FAIL_MS : SUCCESS_MS);
  }

  function showHttpBanner(detail) {
    if (!detail || typeof detail !== "object") return;
    push({
      variant: detail.ok ? "" : "fail",
      fail: !detail.ok,
      title: detail.title || (detail.ok ? "委託回報" : "委託失敗"),
      detail: detail.detail || "",
    });
  }

  function showOrderReport(payload) {
    var status = payload.status;
    var fail = status === "failed";
    var actionLabel = ACTION_LABEL[payload.action] || payload.action || "";
    // LOW-6（fresh-context 終審修復）：未成交的 MKT 委託 price 恆為 "0"（字串非空，原本
    // `payload.price ?` 判斷會把它當成有值），顯示出「@ 0」——同 CRITICAL-1
    // （confirm_dialog.html）的病灶。inbox_worker.py::_order_report_event_payload 現在
    // 帶了 price_type，MKT 一律顯示「市價」，不看 price 欄位本身的值。
    var priceText = payload.price_type === "MKT" ? " @ 市價" : (payload.price ? " @ " + payload.price : "");
    var detailLine = (payload.symbol || "") + " " + actionLabel + " " + payload.qty + " 口" + priceText;
    push({
      variant: fail ? "fail" : "",
      fail: fail,
      title: fail ? "委託失敗" : "委託回報：" + (STATUS_LABEL[status] || status),
      detail: detailLine,
    });
  }

  function showDeal(payload) {
    var actionLabel = ACTION_LABEL[payload.action] || payload.action || "";
    // 成交一律成功（Deal 落地代表真的成交了），左側粗邊隨方向色（--rise/--fall）。
    var variant = payload.action === "Sell" ? "fill-short" : "fill-long";
    push({
      variant: variant,
      fail: false,
      title: "成交：" + (payload.symbol || "") + " " + actionLabel,
      detail: payload.qty + " 口 @ " + payload.price,
    });
  }

  function parsePayload(raw) {
    try { return JSON.parse(raw); } catch (e) { return null; }
  }

  document.body.addEventListener("order-banner", function (evt) {
    showHttpBanner(evt.detail);
  });

  document.addEventListener("sse:deal", function (evt) {
    var raw = evt.detail && evt.detail.data;
    var payload = raw ? parsePayload(raw) : null;
    if (payload) showDeal(payload);
  });

  document.addEventListener("sse:order-report", function (evt) {
    var raw = evt.detail && evt.detail.data;
    var payload = raw ? parsePayload(raw) : null;
    if (payload) showOrderReport(payload);
  });

  window.QQBanners = { push: push };
})();
