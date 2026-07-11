// 純 node 斷言（無框架）：node tests/js/chart-guards.test.js
const assert = require("node:assert");
const { isRenderableBar, shouldFreezeQuote } = require("../../src/quanquant/web/static/chart-guards.js");

// --- isRenderableBar ---
const good = { timestamp: 1000, open: 1, high: 2, low: 0.5, close: 1.5 };
assert.strictEqual(isRenderableBar(good, 0), true, "有效 bar、初始 lastBarTs=0 應放行");
assert.strictEqual(isRenderableBar(good, 1000), true, "同 ts（更新當前根）應放行");
assert.strictEqual(isRenderableBar(good, 999), true, "ts 前進應放行");
assert.strictEqual(isRenderableBar(good, 1001), false, "亂序（ts < lastBarTs）應丟棄");
assert.strictEqual(isRenderableBar(null, 0), false, "缺 bar 應丟棄");
assert.strictEqual(isRenderableBar({ timestamp: NaN, open: 1, high: 2, low: 0.5, close: 1.5 }, 0), false, "NaN timestamp 應丟棄");
assert.strictEqual(isRenderableBar({ timestamp: 1000, open: NaN, high: 2, low: 0.5, close: 1.5 }, 0), false, "NaN open 應丟棄");
assert.strictEqual(isRenderableBar({ timestamp: 1000, open: 1, high: Infinity, low: 0.5, close: 1.5 }, 0), false, "Infinity high 應丟棄");
assert.strictEqual(isRenderableBar({ timestamp: 1000, open: 1, high: 2, low: 0.5 }, 0), false, "缺 close 應丟棄");

// --- shouldFreezeQuote ---
assert.strictEqual(shouldFreezeQuote(undefined), false, "meta 缺失不凍結（相容）");
assert.strictEqual(shouldFreezeQuote({ status: "open", fresh: true }), false, "open+fresh 不凍結");
assert.strictEqual(shouldFreezeQuote({ status: "open", fresh: false }), true, "fresh=false 凍結");
assert.strictEqual(shouldFreezeQuote({ status: "suspected_halt", fresh: true }), true, "suspected_halt 凍結");
assert.strictEqual(shouldFreezeQuote({ status: "closed", fresh: true }), true, "closed 凍結");
assert.strictEqual(shouldFreezeQuote({ status: "open" }), false, "只有 status=open、fresh 未定義 不凍結");

console.log("chart-guards: all assertions passed");
