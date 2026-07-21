import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const QQI = require('../../src/quanquant/web/static/indicators.js');

test('list 含 ma/wr/bias/vol，欄位齊全', () => {
  const keys = QQI.list.map((e) => e.key);
  assert.deepEqual(keys, ['ma', 'wr', 'bias', 'vol', 'macd', 'boll', 'kdj', 'avg']);
  const ma = QQI.byKey('ma');
  assert.equal(ma.pane, 'main');
  assert.equal(ma.klineName, 'MA');
  assert.equal(ma.repeatable, true);
  assert.equal(ma.alertTarget, true);
  assert.equal(QQI.byKey('vol').repeatable, true);
  assert.equal(QQI.byKey('nope'), null);
});

test('byKlineName 反查', () => {
  assert.equal(QQI.byKlineName('WR').key, 'wr');
  assert.equal(QQI.byKlineName('ZZZ'), null);
});

test('defaults() 等同舊 DEFAULT_SETTINGS 結構', () => {
  const d = QQI.defaults();
  assert.deepEqual(Object.keys(d), ['ma', 'wr', 'bias', 'vol', 'macd', 'boll', 'kdj', 'avg']);
  assert.equal(d.ma.enabled, true);
  assert.equal(d.ma.params.length, 4);
  assert.equal(d.ma.params[0].period, 5);
  assert.equal(d.wr.enabled, false);
  assert.equal(d.vol.enabled, true);
  assert.equal(Array.isArray(d.vol.params), true);
  assert.deepEqual(d.vol.params.map((p) => p.period), [5, 10]);
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
  assert.deepEqual(Object.keys(m), ['ma', 'wr', 'bias', 'vol', 'macd', 'boll', 'kdj', 'avg']);
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
  for (const k of ['ma', 'wr', 'bias', 'vol', 'macd', 'boll', 'kdj', 'avg']) {
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
  const vol = QQI.byKey('vol');     // repeatable+bars
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
  // VOL（bars:true）enabled 即畫，含 0 條 MA 線；visible 預設缺省視為顯示
  assert.equal(QQI.resolveVisibility(vol, { enabled: true, params: {} }, false), true);
  // 缺 conf/entry 安全回 false
  assert.equal(QQI.resolveVisibility(ma, null, false), false);
  assert.equal(QQI.resolveVisibility(null, withMA(), false), false);
});

test('boll 註冊為 main/fixed，calcParams [20, 2]', () => {
  const boll = QQI.byKey('boll');
  assert.ok(boll, 'boll 應存在');
  assert.equal(boll.pane, 'main');
  assert.equal(boll.klineName, 'BOLL');
  assert.equal(boll.repeatable, false);
  assert.equal(boll.alertTarget, false);
  assert.deepEqual(QQI.calcParams(boll, boll.defaults), [20, 2]);
  assert.equal(boll.defaults.visible, true);
});

test('kdj 註冊為 sub/fixed，calcParams [9, 3, 3]', () => {
  const kdj = QQI.byKey('kdj');
  assert.ok(kdj, 'kdj 應存在');
  assert.equal(kdj.pane, 'sub');
  assert.equal(kdj.klineName, 'KDJ');
  assert.equal(kdj.repeatable, false);
  assert.equal(kdj.alertTarget, false);
  assert.deepEqual(QQI.calcParams(kdj, kdj.defaults), [9, 3, 3]);
  assert.equal(kdj.defaults.visible, true);
});

test('alertTargets 不含 boll/kdj（只畫圖不進警示）', () => {
  const codes = QQI.alertTargets().map((x) => x.code);
  assert.deepEqual(codes, ['ma', 'wr', 'bias']);
});

test('shortName: registry 有則回 klineName，未知 key 回原字串', () => {
  assert.equal(QQI.shortName('ma'), 'MA');
  assert.equal(QQI.shortName('bias'), 'BIAS');
  assert.equal(QQI.shortName('kdj'), 'KDJ');
  assert.equal(QQI.shortName('unknown'), 'unknown');
});

test('macd/boll/kdj 有 lines 描述，長度 2/3/3、label 正確', () => {
  assert.deepEqual(QQI.byKey('macd').lines.map((l) => l.label), ['DIF', 'DEA']);
  assert.deepEqual(QQI.byKey('boll').lines.map((l) => l.label), ['上軌', '中軌', '下軌']);
  assert.deepEqual(QQI.byKey('kdj').lines.map((l) => l.label), ['K', 'D', 'J']);
  assert.deepEqual(QQI.byKey('boll').lines.map((l) => l.key), ['up', 'mid', 'dn']);
});

test('defaults() 三者 colors 種子＝各 lines 預設（palette 起算，外觀不變）', () => {
  const d = QQI.defaults();
  assert.deepEqual(d.macd.colors, ['#FF9600', '#935EBD']);
  assert.deepEqual(d.boll.colors, ['#FF9600', '#935EBD', '#1677FF']);
  assert.deepEqual(d.kdj.colors, ['#FF9600', '#935EBD', '#1677FF']);
});

test('defaultColors: 有 lines 回預設陣列、無 lines（ma/vol）回 []', () => {
  assert.deepEqual(QQI.defaultColors(QQI.byKey('boll')), ['#FF9600', '#935EBD', '#1677FF']);
  assert.deepEqual(QQI.defaultColors(QQI.byKey('ma')), []);
  assert.deepEqual(QQI.defaultColors(QQI.byKey('vol')), []);
  assert.deepEqual(QQI.defaultColors(null), []);
  // 與 defaults 種子一致（防漂移）
  assert.deepEqual(QQI.defaults().boll.colors, QQI.defaultColors(QQI.byKey('boll')));
});

test('merge 保留使用者存的 colors；缺 colors → 補預設（通用 shallow-spread）', () => {
  const saved = { boll: { enabled: true, visible: true, params: { period: 20, std: 2 }, colors: ['#111111', '#222222', '#333333'] } };
  const m = QQI.merge(saved);
  assert.deepEqual(m.boll.colors, ['#111111', '#222222', '#333333']);
  assert.deepEqual(m.kdj.colors, ['#FF9600', '#935EBD', '#1677FF']); // 未存 → 預設
});

test('vol 註冊為 repeatable+bars，預設 MA5/MA10', () => {
  const vol = QQI.byKey('vol');
  assert.equal(vol.repeatable, true);
  assert.equal(vol.bars, true);
  assert.equal(vol.pane, 'sub');
  assert.equal(vol.klineName, 'VOL');
  assert.equal(vol.alertTarget, false);
  assert.deepEqual(vol.paramSchema.map((f) => f.field), ['period', 'color']);
  assert.deepEqual(vol.defaults.params, [
    { period: 5, color: '#f0b90b' }, { period: 10, color: '#935EBD' },
  ]);
});

test('resolveVisibility: VOL bars 使 0 條線仍畫；MA 0 條線不畫', () => {
  const vol = QQI.byKey('vol');
  const ma = QQI.byKey('ma');
  // VOL enabled + 0 條 MA 線 → true（量柱恆畫）
  assert.equal(QQI.resolveVisibility(vol, { enabled: true, visible: true, params: [] }, false), true);
  // VOL 舊存檔 params:{} → true（防呆 + bars）
  assert.equal(QQI.resolveVisibility(vol, { enabled: true, visible: true, params: {} }, false), true);
  // MA（無 bars）0 條線 → false
  assert.equal(QQI.resolveVisibility(ma, { enabled: true, visible: true, params: [] }, false), false);
  // VOL visible:false → false；nakedK → false
  assert.equal(QQI.resolveVisibility(vol, { enabled: true, visible: false, params: [{ period: 5 }] }, false), false);
  assert.equal(QQI.resolveVisibility(vol, { enabled: true, visible: true, params: [{ period: 5 }] }, true), false);
});

test('calcParams VOL: 陣列回週期、非陣列（舊 {}）防呆回 []', () => {
  const vol = QQI.byKey('vol');
  assert.deepEqual(QQI.calcParams(vol, { params: [{ period: 5, color: '#a' }, { period: 10, color: '#b' }] }), [5, 10]);
  assert.deepEqual(QQI.calcParams(vol, { params: {} }), []);
  assert.deepEqual(QQI.calcParams(vol, { params: undefined }), []);
});

test('merge: repeatable 指標 params 非陣列一律正規化為 []（VOL 舊存檔遷移）', () => {
  // 既有使用者存檔 vol 為舊 fixed 形態 params:{}
  const m = QQI.merge({ vol: { enabled: true, visible: true, params: {} } });
  assert.equal(Array.isArray(m.vol.params), true);
  assert.deepEqual(m.vol.params, []);            // {} → [] → 只剩量柱、無 MA 線
  assert.equal(m.vol.enabled, true);
  // 未存 VOL → 拿預設 MA5/MA10
  const d = QQI.merge({ ma: { enabled: false } });
  assert.deepEqual(d.vol.params.map((p) => p.period), [5, 10]);
  // 使用者已存陣列 → 原樣保留
  const keep = QQI.merge({ vol: { enabled: true, params: [{ period: 20, color: '#123456' }] } });
  assert.deepEqual(keep.vol.params, [{ period: 20, color: '#123456' }]);
});

test('avg 註冊為 main/fixed 疊線：calcParams []、defaultColors 金色、不進警示', () => {
  const avg = QQI.byKey('avg');
  assert.ok(avg, 'avg 應存在');
  assert.equal(avg.pane, 'main');
  assert.equal(avg.klineName, 'AVG');
  assert.equal(avg.repeatable, false);
  assert.equal(avg.alertTarget, false);
  assert.deepEqual(avg.paramSchema, []);
  assert.deepEqual(QQI.calcParams(avg, avg.defaults), []); // 無 number 參數
  assert.deepEqual(QQI.defaultColors(avg), ['#e8b64c']);
  assert.equal(avg.defaults.enabled, false);
  assert.equal(avg.defaults.visible, true);
  assert.equal(QQI.byKlineName('AVG').key, 'avg');
  assert.ok(!QQI.alertTargets().map((x) => x.code).includes('avg'));
});

test('avg resolveVisibility：enabled 即畫、關閉不畫、裸K 蓋掉', () => {
  const avg = QQI.byKey('avg');
  assert.equal(QQI.resolveVisibility(avg, { enabled: true, visible: true }, false), true);
  assert.equal(QQI.resolveVisibility(avg, { enabled: false, visible: true }, false), false);
  assert.equal(QQI.resolveVisibility(avg, { enabled: true, visible: false }, false), false);
  assert.equal(QQI.resolveVisibility(avg, { enabled: true, visible: true }, true), false);
});
