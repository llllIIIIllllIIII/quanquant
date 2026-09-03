// 007：sim 下單確認視窗——送出前預設跳確認（除非使用者已勾過「不再顯示」），內容同
// real 確認框的完整委託摘要；real 完全不吃這個流程，一律走既有的後端強制兩階段確認
// （confirm_dialog.html）。用 htmx 內建的 `htmx:confirm` 事件攔截送出（可 preventDefault
// 後改自己的時機呼叫 `evt.detail.issueRequest(true)` 續送），不新增依賴、不用
// `hx-confirm`（那只能跳原生 window.confirm，秀不出完整摘要與「不再顯示」勾選）。
(function () {
  "use strict";

  var OCTYPE_LABEL = { New: "新倉", Cover: "平倉", Auto: "自動" };

  function fillSummary(form) {
    var fd = new FormData(form);
    var action = fd.get("action");
    var actionLabel = action === "Sell" ? "賣" : "買";
    var symbol = fd.get("symbol") || "";
    var qty = fd.get("qty") || "";
    var priceType = fd.get("price_type");
    var price = priceType === "MKT" ? "市價" : (fd.get("price") || "");
    var octype = fd.get("octype");
    var octypeLabel = OCTYPE_LABEL[octype] || octype || "";
    var summary = document.getElementById("sim-confirm-summary");
    if (summary) {
      summary.textContent = symbol + " " + actionLabel + " " + qty + " 口 @ " + price + "（" + octypeLabel + "）";
    }
  }

  function persistSkipPreference() {
    fetch("/api/user/skip-sim-confirm", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ skip: true }),
    }).catch(function () { /* 即時已套用；存回失敗僅影響下次載入 */ });
  }

  document.body.addEventListener("htmx:confirm", function (evt) {
    var form = evt.target && evt.target.closest ? evt.target.closest("#order-form") : null;
    if (!form) return; // 不是下單表單（例如改單/取消的原生 hx-confirm）——不攔截
    if (form.dataset.simConfirm !== "1") return; // real，或 sim 已勾過「不再顯示」

    var dialog = document.getElementById("sim-confirm-dialog");
    if (!dialog || typeof dialog.showModal !== "function") {
      return; // 保底：找不到 modal 就照舊直接送出，不擋下單
    }
    evt.preventDefault();
    fillSummary(form);
    dialog.showModal();

    var submitBtn = document.getElementById("sim-confirm-submit-btn");
    var cancelBtn = document.getElementById("sim-confirm-cancel-btn");
    var skipBox = document.getElementById("sim-confirm-skip-checkbox");

    // CRITICAL-2（fresh-context 終審修復）：ESC 關閉原生 `<dialog>` 不會經過
    // submitBtn/cancelBtn 的 click——只靠「按鈕 click 時互相移除對方監聽器」清不掉 ESC
    // 這條路徑留下的監聽器。原本的寫法因此會洩漏：使用者開了 modal、按 ESC 關掉、
    // 下一次下單再開一次 modal 又疊上一組新的 click 監聽器；累積 N 次 ESC 後點「確認
    // 送出」會同時觸發 N+1 個 onSubmit，各自呼叫一次 `issueRequest(true)`——等於同一顆
    // client_order_id 同時送出 N+1 個 POST /orders（有競態窗，可能撞 repository 的
    // unique 約束）。
    //
    // 清理契約：submit/cancel 的 click 監聽器都掛 `{ once: true }`（點過一次瀏覽器自動
    // 移除，正常路徑不留尾巴）；`<dialog>` 原生在「ESC 關閉」與「呼叫 .close()」兩種
    // 情況都會 fire 一個 `close` 事件（ESC 會先 fire 可取消的 `cancel`，這裡不攔截，讓
    // 它照瀏覽器預設關閉），故用一個 `{ once: true }` 的 close 監聽器統一收尾——不論是
    // 按鈕點擊關的還是 ESC 關的，`cleanup()` 都保證只執行一次、且一定會執行。
    // `removeEventListener` 對已經因 `once` 自動移除的監聽器呼叫是安全的 no-op，重複
    // 呼叫 cleanup() 不會出錯。
    function cleanup() {
      submitBtn.removeEventListener("click", onSubmit);
      cancelBtn.removeEventListener("click", onCancel);
    }
    function onSubmit() {
      dialog.close(); // 觸發 close 事件 → cleanup()
      if (skipBox && skipBox.checked) {
        form.dataset.simConfirm = "0"; // 這個分頁立即生效，不必等後端回應
        persistSkipPreference();
      }
      evt.detail.issueRequest(true);
    }
    function onCancel() {
      dialog.close(); // 觸發 close 事件 → cleanup()
    }
    submitBtn.addEventListener("click", onSubmit, { once: true });
    cancelBtn.addEventListener("click", onCancel, { once: true });
    dialog.addEventListener("close", cleanup, { once: true });
  });
})();
