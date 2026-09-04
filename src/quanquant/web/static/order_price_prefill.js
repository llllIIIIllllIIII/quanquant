// 需求 A（2026-09-05 使用者拍板）：限價單價格欄預填「當下市價」，省去每次手打 5 位數，
// 使用者只需微調。伺服器端（web/routers/orders.py::orders_page 的 prefill_price）負責
// 「頁面初載」這一種情境——見 orders.html 用它設 Alpine `priceValue` 初值＋price input
// 的 `data-qq-prefilled` 起始旗標。這裡只補伺服器端做不到的「之後」兩種情境：
//   ①使用者從 MKT 切回 LMT、且價格欄目前是空的 → 用報價列（#quote）當下顯示的最新價
//     （data-qq-price，隨既有 quote SSE/hx-get 更新，不新增報價來源）補上；
//   ②商品切換（下拉變更、報價列真的換成新商品）→ 若價格欄仍是「上次預填值、使用者
//     沒手改過」就跟著換成新商品的現價；使用者已手改過就不覆寫。
// 兩種情境都只在 price_type 目前是 LMT 時生效——MKT 欄位 readonly 鎖 0（見既有
// x-on:change），不需要也不可以被這裡的邏輯預填。
//
// `data-qq-prefilled` 這個 dataset 旗標分辨「系統填的」vs「使用者手改的」：系統填值時
// 設成 "1"；使用者真的在欄位裡打字/貼上（原生、`evt.isTrusted` 為 true 的 input 事件
// ——用這個瀏覽器內建屬性分辨真人輸入跟本檔自己用 dispatchEvent 合成的同步事件，不必
// 另外維護一個「正在預填中」的旗標）就把旗標清掉，之後任何自動預填都不再覆寫這個欄位。
// 拿不到報價（#quote 尚無 data-qq-price）時一律不填、留空——不可填 0 或假值。
(function () {
  "use strict";

  function priceInput() {
    return document.getElementById("order-price");
  }

  function priceTypeSelect() {
    return document.getElementById("order-price-type");
  }

  function currentQuotePrice() {
    // 報價列由既有 quote SSE/hx-get 維護，直接讀它身上的 data-qq-price——不新增報價
    // 來源、不重複打 /quote。缺席/空字串一律當「拿不到報價」，呼叫端不可填 0 或假值。
    var q = document.querySelector("#quote [data-qq-price]");
    var raw = q ? q.getAttribute("data-qq-price") : null;
    return raw || null;
  }

  function currentQuoteSymbol() {
    var el = document.querySelector("#quote .sym-code");
    return el ? el.textContent : null;
  }

  function applyPrefill(input, price) {
    input.value = price;
    input.dataset.qqPrefilled = "1";
    // 合成事件（isTrusted === false）讓 Alpine 的 x-model 同步這個新值；下面的「清
    // 旗標」監聽器只認 isTrusted 的原生輸入，不會把這次同步誤判成使用者手改。
    input.dispatchEvent(new Event("input", { bubbles: true }));
  }

  // 使用者真的手改欄位 → 清掉「預填」旗標，之後不再自動覆寫（含本檔自己的兩種情境）。
  document.body.addEventListener("input", function (evt) {
    if (evt.isTrusted && evt.target && evt.target.id === "order-price") {
      delete evt.target.dataset.qqPrefilled;
    }
  });

  // ①MKT 切回 LMT、欄位為空 → 用報價列當下的值補上。
  document.body.addEventListener("change", function (evt) {
    var select = priceTypeSelect();
    if (!select || evt.target !== select || select.value !== "LMT") return;
    var input = priceInput();
    // 「空」含 "0"：既有 Alpine handler 在切到 MKT 時會把 priceValue 設成 "0"（非空字串），
    // 切回 LMT 時欄位停在 "0" 而非 ""——只認空字串會讓補值永遠不觸發（終審 MEDIUM-1，
    // Node harness 實證）。LMT 下 0 本來就是非法價（server 端 __post_init__ 拒絕），
    // 把它視同「未填」覆寫成現價不會蓋掉任何有效的使用者輸入。
    if (!input || (input.value !== "" && input.value !== "0")) return;
    var price = currentQuotePrice();
    if (!price) return; // 拿不到報價，留空，不可填 0 或假值
    applyPrefill(input, price);
  });

  // ②商品切換：報價片段（#quote）換成新商品內容、且商品代碼真的變了（排除同商品的
  // 報價心跳重複觸發）→ 若價格欄仍是預填值、使用者沒手改過就跟著換成新現價；已手改過
  // （旗標已被上面的 input 監聽器清掉）一律不覆寫。用商品代碼是否真的改變來判斷，
  // 不依賴事件觸發來源（ajax hx-get 或 sse-swap 心跳都會發 htmx:afterSwap），避免
  // 心跳與商品切換的網路回應互相搶跑造成誤判。
  var lastSeenSymbol = currentQuoteSymbol();
  document.body.addEventListener("htmx:afterSwap", function (evt) {
    if (!evt.target || evt.target.id !== "quote") return;
    var symbol = currentQuoteSymbol();
    var symbolChanged = symbol !== null && lastSeenSymbol !== null && symbol !== lastSeenSymbol;
    lastSeenSymbol = symbol;
    if (!symbolChanged) return;
    var select = priceTypeSelect();
    if (!select || select.value !== "LMT") return; // MKT 不預填
    var input = priceInput();
    if (!input) return;
    if (input.value !== "" && input.dataset.qqPrefilled !== "1") return; // 使用者手改過，不覆寫
    var price = currentQuotePrice();
    if (!price) return;
    applyPrefill(input, price);
  });
})();
