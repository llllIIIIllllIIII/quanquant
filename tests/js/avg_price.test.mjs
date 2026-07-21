import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const AVG = require('../../src/quanquant/web/static/avg_price.js');
const { sessionKey, computeVwap, register } = AVG;

// 台北牆鐘 → epoch-ms（台北固定 UTC+8）。mo 為 0-indexed（7 月 = 6）。
function tpe(y, mo, da, h, mi) {
  return Date.UTC(y, mo, da, h, mi) - 8 * 3600 * 1000;
}
// 造一根 bar。預設 O/H/L/C 同值方便手算；可覆寫 high/low 驗典型價。
function bar(ts, close, volume, extra = {}) {
  return { timestamp: ts, open: close, high: close, low: close, close, volume, ...extra };
}
const near = (a, b) => Math.abs(a - b) < 1e-9;

test('computeVwap 基本：典型價 (H+L+C)/3，量加權累積', () => {
  const bars = [
    bar(tpe(2026, 6, 20, 9, 0), 100, 10, { high: 110, low: 90 }),  // tp=100, v=10
    bar(tpe(2026, 6, 20, 9, 1), 120, 30, { high: 130, low: 110 }), // tp=120, v=30
  ];
  const out = computeVwap(bars);
  assert.equal(out.length, 2);
  assert.ok(near(out[0].avg, 100));           // 1000/10
  assert.ok(near(out[1].avg, 4600 / 40));     // (100*10 + 120*30)/40 = 115
});

test('日界重置：日盤跨日，第二天首根不含前一天', () => {
  const out = computeVwap([
    bar(tpe(2026, 6, 20, 9, 0), 100, 10),
    bar(tpe(2026, 6, 21, 9, 0), 200, 5),
  ]);
  assert.ok(near(out[0].avg, 100));
  assert.ok(near(out[1].avg, 200)); // 新交易日重置
});

test('日界重置：同曆日 日盤→夜盤 各自重置', () => {
  const out = computeVwap([
    bar(tpe(2026, 6, 20, 9, 0), 100, 10),   // 日盤
    bar(tpe(2026, 6, 20, 15, 30), 200, 5),  // 夜盤 → 重置
  ]);
  assert.ok(near(out[0].avg, 100));
  assert.ok(near(out[1].avg, 200));
});

test('夜盤跨午夜不重置：23:00 與次日 01:00 同桶連續累積', () => {
  const out = computeVwap([
    bar(tpe(2026, 6, 20, 23, 0), 100, 10),
    bar(tpe(2026, 6, 21, 1, 0), 200, 10),
  ]);
  assert.ok(near(out[0].avg, 100));
  assert.ok(near(out[1].avg, 150)); // (100*10 + 200*10)/20
});

test('日界重置：夜盤 → 次日日盤 重置', () => {
  const out = computeVwap([
    bar(tpe(2026, 6, 21, 1, 0), 100, 10),   // 夜盤（延續前一交易日）
    bar(tpe(2026, 6, 21, 9, 0), 200, 5),    // 次日日盤 → 重置
  ]);
  assert.ok(near(out[0].avg, 100));
  assert.ok(near(out[1].avg, 200));
});

test('量為 0：不計入，avg 維持前值', () => {
  const out = computeVwap([
    bar(tpe(2026, 6, 20, 9, 0), 100, 10),
    bar(tpe(2026, 6, 20, 9, 1), 200, 0),
  ]);
  assert.ok(near(out[1].avg, 100));
});

test('量為 NaN / 缺 volume：不炸，avg 維持前值', () => {
  const missing = { timestamp: tpe(2026, 6, 20, 9, 1), open: 200, high: 200, low: 200, close: 200 };
  const out = computeVwap([
    bar(tpe(2026, 6, 20, 9, 0), 100, 10),
    missing, // volume 缺 → Number(undefined)=NaN
    bar(tpe(2026, 6, 20, 9, 2), 300, NaN),
  ]);
  assert.ok(near(out[1].avg, 100));
  assert.ok(near(out[2].avg, 100));
});

test('時段起點量為 0：以典型價墊線（非 NaN）', () => {
  const out = computeVwap([bar(tpe(2026, 6, 20, 9, 0), 100, 0, { high: 110, low: 90 })]);
  assert.ok(near(out[0].avg, 100)); // tp=(110+90+100)/3=100
});

test('空 / 非陣列輸入 → []', () => {
  assert.deepEqual(computeVwap([]), []);
  assert.deepEqual(computeVwap(null), []);
  assert.deepEqual(computeVwap(undefined), []);
});

test('sessionKey 邊界：08:44 vs 08:45（進日盤）', () => {
  const before = sessionKey(tpe(2026, 6, 20, 8, 44));
  const open = sessionKey(tpe(2026, 6, 20, 8, 45));
  assert.notEqual(before, open);
  assert.ok(open.startsWith('D'));
});

test('sessionKey 邊界：13:45 vs 13:46（出日盤）', () => {
  const close = sessionKey(tpe(2026, 6, 20, 13, 45));
  const after = sessionKey(tpe(2026, 6, 20, 13, 46));
  assert.ok(close.startsWith('D'));
  assert.notEqual(close, after);
});

test('sessionKey：非數字 timestamp → null', () => {
  assert.equal(sessionKey(undefined), null);
  assert.equal(sessionKey(NaN), null);
  assert.equal(sessionKey('x'), null);
});

test('匯出皆為函式；register 無 klinecharts 安全 no-op', () => {
  assert.equal(typeof sessionKey, 'function');
  assert.equal(typeof computeVwap, 'function');
  assert.equal(typeof register, 'function');
  assert.equal(AVG.IND_NAME, 'AVG');
  assert.doesNotThrow(() => register());
});
