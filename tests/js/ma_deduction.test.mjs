import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { computeLive } = require('../../src/quanquant/web/static/ma_deduction.js');

// 以收盤價序列造 bars（升序，timestamp 每根 +60s）
function bars(closes) {
  return closes.map((c, i) => ({
    timestamp: 1_000_000 + i * 60_000,
    open: c, high: c, low: c, close: c, volume: 1,
  }));
}
const P = (period, color = '#abc') => ({ period, color });

test('upward: 基準價高於扣抵值且超出容忍', () => {
  const [r] = computeLive(bars([100, 100, 110]), [P(2)], 0.1);
  assert.equal(r.period, 2);
  assert.equal(r.deductionIndex, 0);
  assert.equal(r.deductionValue, 100);
  assert.equal(r.basePrice, 110);
  assert.equal(r.status, 'upward');
});

test('downward: 基準價低於扣抵值且超出容忍', () => {
  const [r] = computeLive(bars([110, 110, 100]), [P(2)], 0.1);
  assert.equal(r.status, 'downward');
  assert.equal(r.diff, -10);
});

test('flat: 差距 <= 0.1%', () => {
  const [r] = computeLive(bars([100, 100, 100.05]), [P(2)], 0.1);
  assert.equal(r.status, 'flat');
});

test('insufficient-data: deductionIndex < 0', () => {
  const [r] = computeLive(bars([100, 101, 102]), [P(5)], 0.1);
  assert.equal(r.status, 'insufficient-data');
  assert.equal(r.deductionIndex, -1);
  assert.equal(r.deductionTime, null);
});

test('除以 0: deductionValue 為 0 時 diffPercent 為 null，靠 diff 判方向', () => {
  const [r] = computeLive(bars([0, 50, 60]), [P(2)], 0.1);
  assert.equal(r.deductionValue, 0);
  assert.equal(r.diffPercent, null);
  assert.equal(r.status, 'upward');
});

test('多週期: 同時回傳，短週期計算、長週期資料不足', () => {
  const out = computeLive(bars([100, 105, 110]), [P(2), P(5)], 0.1);
  assert.equal(out.length, 2);
  assert.equal(out[0].status, 'upward');
  assert.equal(out[1].status, 'insufficient-data');
});

test('deductionTime 取扣抵 K 棒的 timestamp', () => {
  const b = bars([100, 100, 110]);
  const [r] = computeLive(b, [P(2)], 0.1);
  assert.equal(r.deductionTime, b[0].timestamp);
});

test('空 bars 回傳空陣列', () => {
  assert.deepEqual(computeLive([], [P(2)], 0.1), []);
});
