import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const QQI = require('../../src/quanquant/web/static/indicators.js');

test('list 含 ma/wr/bias/vol，欄位齊全', () => {
  const keys = QQI.list.map((e) => e.key);
  assert.deepEqual(keys, ['ma', 'wr', 'bias', 'vol', 'macd']);
  const ma = QQI.byKey('ma');
  assert.equal(ma.pane, 'main');
  assert.equal(ma.klineName, 'MA');
  assert.equal(ma.repeatable, true);
  assert.equal(ma.alertTarget, true);
  assert.equal(QQI.byKey('vol').repeatable, false);
  assert.equal(QQI.byKey('nope'), null);
});

test('byKlineName 反查', () => {
  assert.equal(QQI.byKlineName('WR').key, 'wr');
  assert.equal(QQI.byKlineName('ZZZ'), null);
});

test('defaults() 等同舊 DEFAULT_SETTINGS 結構', () => {
  const d = QQI.defaults();
  assert.deepEqual(Object.keys(d), ['ma', 'wr', 'bias', 'vol', 'macd']);
  assert.equal(d.ma.enabled, true);
  assert.equal(d.ma.params.length, 4);
  assert.equal(d.ma.params[0].period, 5);
  assert.equal(d.wr.enabled, false);
  assert.equal(d.vol.enabled, true);
  // 回傳為獨立副本（改一份不影響下一次）
  d.ma.params[0].period = 999;
  assert.equal(QQI.defaults().ma.params[0].period, 5);
});

test('merge: 舊存檔（無新指標）載入後含全部 key 且舊值保留', () => {
  const saved = { ma: { enabled: false, params: [{ period: 7, color: '#111' }] } };
  const m = QQI.merge(saved);
  assert.equal(m.ma.enabled, false);
  assert.equal(m.ma.params[0].period, 7);
  assert.equal(m.wr.enabled, false);       // 未存 → 用 defaults
  assert.deepEqual(Object.keys(m), ['ma', 'wr', 'bias', 'vol', 'macd']);
});

test('merge: 存檔含 registry 已無的 key → 略過', () => {
  const m = QQI.merge({ ghost: { enabled: true } });
  assert.equal(m.ghost, undefined);
});

test('merge: null/非物件輸入 → 回 defaults', () => {
  assert.deepEqual(QQI.merge(null), QQI.defaults());
  assert.deepEqual(QQI.merge('x'), QQI.defaults());
});

test('alertTargets: 僅 alertTarget 指標，不含 price/const/vol', () => {
  const t = QQI.alertTargets();
  assert.deepEqual(t.map((x) => x.code), ['ma', 'wr', 'bias']);
});

test('calcParams repeatable: 各條 period 四捨五入', () => {
  const entry = QQI.byKey('ma');
  const conf = { enabled: true, params: [{ period: 5.4, color: '#a' }, { period: 10.6, color: '#b' }] };
  assert.deepEqual(QQI.calcParams(entry, conf), [5, 11]);
});

test('calcParams fixed: 依 paramSchema number 欄位順序', () => {
  const entry = {
    key: 'x', repeatable: false,
    paramSchema: [
      { field: 'fast', type: 'number' },
      { field: 'slow', type: 'number' },
      { field: 'signal', type: 'number' },
      { field: 'color', type: 'color' },
    ],
  };
  assert.deepEqual(QQI.calcParams(entry, { params: { fast: 12, slow: 26, signal: 9, color: '#a' } }), [12, 26, 9]);
});

test('calcParams: 空/缺 conf 安全回空陣列', () => {
  assert.deepEqual(QQI.calcParams(QQI.byKey('vol'), { params: {} }), []);
  assert.deepEqual(QQI.calcParams(null, null), []);
});

test('macd 已註冊為 fixed 指標，calcParams 為 [fast, slow, signal]', () => {
  const macd = QQI.byKey('macd');
  assert.ok(macd, 'macd 應存在於 registry');
  assert.equal(macd.repeatable, false);
  assert.equal(macd.pane, 'sub');
  assert.equal(macd.klineName, 'MACD');
  assert.deepEqual(QQI.calcParams(macd, macd.defaults), [12, 26, 9]);
});

test('macd 預設不啟用，merge 後既有 key 不受影響', () => {
  const d = QQI.defaults();
  assert.equal(d.macd.enabled, false);
  const m = QQI.merge({ ma: { enabled: false } });
  assert.equal(m.ma.enabled, false);
  assert.equal(m.macd.enabled, false); // 舊存檔無 macd → 用 defaults 補
});

test('defaults() 每指標帶 visible:true', () => {
  const d = QQI.defaults();
  for (const k of ['ma', 'wr', 'bias', 'vol', 'macd']) {
    assert.equal(d[k].visible, true, `${k} 應預設 visible:true`);
  }
});

test('merge 保留使用者存的 visible:false；舊存檔無 visible → 補 true', () => {
  const m = QQI.merge({ ma: { visible: false }, wr: { enabled: true } });
  assert.equal(m.ma.visible, false);   // 使用者關掉的保留
  assert.equal(m.wr.visible, true);    // 舊存檔無 visible → defaults 補
  assert.equal(m.vol.visible, true);
});

test('resolveVisibility: enabled×visible×nakedK 組合', () => {
  const ma = QQI.byKey('ma');       // repeatable
  const vol = QQI.byKey('vol');     // fixed
  const withMA = (o) => ({ enabled: true, visible: true, params: [{ period: 5 }], ...o });
  // 正常顯示
  assert.equal(QQI.resolveVisibility(ma, withMA(), false), true);
  // 裸K → 一律不畫
  assert.equal(QQI.resolveVisibility(ma, withMA(), true), false);
  // 單指標隱藏
  assert.equal(QQI.resolveVisibility(ma, withMA({ visible: false }), false), false);
  // 未啟用
  assert.equal(QQI.resolveVisibility(ma, withMA({ enabled: false }), false), false);
  // repeatable 但無線 → 不畫
  assert.equal(QQI.resolveVisibility(ma, withMA({ params: [] }), false), false);
  // fixed 啟用即畫；visible 預設缺省視為顯示
  assert.equal(QQI.resolveVisibility(vol, { enabled: true, params: {} }, false), true);
  // 缺 conf/entry 安全回 false
  assert.equal(QQI.resolveVisibility(ma, null, false), false);
  assert.equal(QQI.resolveVisibility(null, withMA(), false), false);
});
