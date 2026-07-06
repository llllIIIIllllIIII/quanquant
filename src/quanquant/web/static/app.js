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
