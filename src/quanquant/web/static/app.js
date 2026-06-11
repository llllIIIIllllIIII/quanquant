// Interactivity is handled declaratively:
//   - HTMX: SSE live quote, table swaps, and CRUD requests
//   - Alpine: modal open/close state
//   - HX-Trigger response headers: `closemodal` (close the modal after a save) and
//     `refreshtable` (re-fire the filter form so the table keeps current filters)
//   - hx-disabled-elt on the trade form: disables the submit button in-flight,
//     preventing double submits
//
// No imperative JS is needed today; this file is a placeholder for future glue.
