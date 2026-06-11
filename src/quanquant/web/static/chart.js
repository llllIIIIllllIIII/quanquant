// KLineCharts-based candlestick chart over our own captured TAIFEX data.
//
// Pinned to klinecharts v9.8.12 (umd: dist/umd/klinecharts.min.js). NOTE for future
// upgrades: v10 renamed loadMore -> setLoadMoreDataCallback and changed init options —
// verify before bumping.
//
// Server contract:
//   GET /api/candles?symbol&tf&before&limit  -> {bars asc, hasMore}
//   GET /api/candles/latest?symbol&tf&since  -> {bars}  (re-aggregated tail, 1-2 bars)
//   GET/PUT /api/chart/state                 -> indicator configs + drawings persistence
(function () {
  "use strict";

  const SYMBOL = window.QQ_SYMBOL || "TXF";
  const PAGE_LIMIT = 1000;
  const GROUP_ID = "user-drawings";

  const TFS = [
    { code: "1m", label: "1分" }, { code: "5m", label: "5分" },
    { code: "10m", label: "10分" }, { code: "15m", label: "15分" },
    { code: "20m", label: "20分" }, { code: "30m", label: "30分" },
    { code: "60m", label: "60分" }, { code: "4h", label: "4時" },
    { code: "1d", label: "日" }, { code: "3d", label: "3日" },
    { code: "1w", label: "週" }, { code: "1M", label: "月" },
  ];

  const DEFAULT_SETTINGS = {
    ma: { enabled: true, params: [
      { period: 5, color: "#f0b90b" }, { period: 10, color: "#ff9800" },
      { period: 20, color: "#2196f3" }, { period: 60, color: "#e91e63" },
    ]},
    wr: { enabled: false, params: [
      { period: 14, color: "#f0b90b" }, { period: 28, color: "#2196f3" },
    ]},
    bias: { enabled: false, params: [
      { period: 6, color: "#f0b90b" }, { period: 12, color: "#2196f3" },
      { period: 24, color: "#e91e63" },
    ]},
    vol: { enabled: true },
  };

  const DARK_STYLES = {
    grid: {
      horizontal: { color: "#23232b" },
      vertical: { color: "#23232b" },
    },
    candle: {
      bar: {
        upColor: "#16c784", downColor: "#ea3943", noChangeColor: "#888888",
        upBorderColor: "#16c784", downBorderColor: "#ea3943", noChangeBorderColor: "#888888",
        upWickColor: "#16c784", downWickColor: "#ea3943", noChangeWickColor: "#888888",
      },
      priceMark: { last: { upColor: "#16c784", downColor: "#ea3943" } },
      tooltip: { text: { color: "#b8b8c0" } },
    },
    xAxis: { axisLine: { color: "#33333d" }, tickText: { color: "#b8b8c0" } },
    yAxis: { axisLine: { color: "#33333d" }, tickText: { color: "#b8b8c0" } },
    crosshair: {
      horizontal: { line: { color: "#555560" } },
      vertical: { line: { color: "#555560" } },
    },
    separator: { color: "#33333d" },
  };

  const QQChart = {
    chart: null,
    tf: "1m",
    lastBarTs: 0,
    hasMore: true,
    loadingMore: false,
    polling: false,
    paneIds: { wr: null, bias: null, vol: null, ma: false },
    overlays: new Map(), // id -> {name, points}
    tfCache: new Map(),  // tf -> {bars: Bar[], hasMore} — instant timeframe switching
    settings: JSON.parse(JSON.stringify(DEFAULT_SETTINGS)),

    async init() {
      if (!window.klinecharts) {
        console.error("klinecharts failed to load");
        return;
      }
      this.chart = klinecharts.init("kchart", {
        timezone: "Asia/Taipei",
        locale: "zh-CN", // v9 built-ins: en-US / zh-CN only (zh-TW unregistered)
        styles: DARK_STYLES,
      });
      if (this.chart.setTimezone) this.chart.setTimezone("Asia/Taipei");
      if (this.chart.setPriceVolumePrecision) this.chart.setPriceVolumePrecision(0, 0);

      this.chart.loadMore((ts) => this.loadMore(ts));

      let state = { indicators: null, drawings: null };
      try {
        state = await (await fetch(`/api/chart/state?symbol=${SYMBOL}`)).json();
      } catch (e) { /* defaults */ }
      if (state.indicators) this.settings = this.mergeSettings(state.indicators);

      // load data BEFORE indicators: even if an indicator fails, candles render
      await this.loadInitial();
      await this.applyIndicators();
      this.restoreDrawings(Array.isArray(state.drawings) ? state.drawings : []);
      this._watchdog();

      setInterval(() => this.pollLatest(), 5000);
      window.addEventListener("resize", () => {
        this.chart.resize();
        this._watchdog(); // rapid resizes can wedge the render loop too
      });
      document.addEventListener("visibilitychange", () => {
        if (!document.hidden) this.pollLatest(); // catch up right away
      });
    },

    mergeSettings(saved) {
      const merged = JSON.parse(JSON.stringify(DEFAULT_SETTINGS));
      for (const key of ["ma", "wr", "bias", "vol"]) {
        if (saved[key] && typeof saved[key] === "object") {
          merged[key] = { ...merged[key], ...saved[key] };
        }
      }
      return merged;
    },

    // ---- data ----

    async fetchPage(before) {
      const params = new URLSearchParams({ symbol: SYMBOL, tf: this.tf, limit: PAGE_LIMIT });
      if (before) params.set("before", String(before));
      const resp = await fetch(`/api/candles?${params}`);
      return resp.json();
    },

    _cacheBars(bars, hasMore) {
      this.tfCache.set(this.tf, { bars, hasMore });
    },

    _applyBar(bar) {
      // keep chart + tfCache consistent for one updated/appended bar
      this.chart.updateData(bar);
      const cached = this.tfCache.get(this.tf);
      if (cached && cached.bars.length) {
        const last = cached.bars[cached.bars.length - 1];
        if (last.timestamp === bar.timestamp) cached.bars[cached.bars.length - 1] = bar;
        else if (bar.timestamp > last.timestamp) cached.bars.push(bar);
      }
      if (bar.timestamp > this.lastBarTs) this.lastBarTs = bar.timestamp;
    },

    _snapToLatest() {
      // applyNewData keeps the previous scroll offset; after a TF switch that can
      // leave every bar off-screen (chart looks blank). Always snap to newest.
      if (this.chart.scrollToRealTime) this.chart.scrollToRealTime();
    },

    // NOTE on `tf` guards: every async data path captures the timeframe it was
    // started for and discards its result if the user switched meanwhile.
    // Without this, a stale /latest response can append e.g. a daily bar into a
    // 1m series — the resulting layout math wedges KLineCharts' render loop
    // permanently (blank chart, no exception).

    async loadInitial() {
      const tf = this.tf;
      const data = await this.fetchPage(null);
      if (this.tf !== tf) return; // user switched timeframe mid-flight
      this.hasMore = !!data.hasMore;
      const bars = data.bars || [];
      const empty = document.getElementById("chart-empty");
      if (empty) empty.style.display = bars.length ? "none" : "flex";
      this.chart.applyNewData(bars, this.hasMore);
      this.lastBarTs = bars.length ? bars[bars.length - 1].timestamp : 0;
      this._cacheBars(bars.slice(), this.hasMore);
      this._snapToLatest();
      this._watchdog();
    },

    async loadMore(ts) {
      if (!this.hasMore || this.loadingMore || !ts) return;
      this.loadingMore = true;
      const tf = this.tf;
      try {
        const data = await this.fetchPage(ts);
        if (this.tf !== tf) return; // stale response for a previous timeframe
        this.hasMore = !!data.hasMore;
        const bars = data.bars || [];
        this.chart.applyMoreData(bars, this.hasMore);
        const cached = this.tfCache.get(this.tf);
        if (cached) {
          cached.bars = bars.concat(cached.bars);
          cached.hasMore = this.hasMore;
        }
        this._watchdog();
      } catch (e) { /* next scroll retries */ } finally {
        this.loadingMore = false;
      }
    },

    async pollLatest() {
      if (this.polling || document.hidden) return; // pause in background tabs
      this.polling = true;
      const tf = this.tf;
      try {
        if (!this.lastBarTs) {
          await this.loadInitial(); // nothing yet (e.g. server just started)
          return;
        }
        const params = new URLSearchParams({
          symbol: SYMBOL, tf, since: String(this.lastBarTs),
        });
        const resp = await fetch(`/api/candles/latest?${params}`);
        const data = await resp.json();
        if (this.tf !== tf) return; // stale response for a previous timeframe
        for (const bar of data.bars || []) this._applyBar(bar);
      } catch (e) { /* next poll retries */ } finally {
        this.polling = false;
      }
    },

    async setTf(tf) {
      if (tf === this.tf) return;
      this.tf = tf;

      const cached = this.tfCache.get(tf);
      if (cached && cached.bars.length) {
        // instant switch from client cache, then refresh just the tail
        this.hasMore = cached.hasMore;
        this.chart.applyNewData(cached.bars.slice(), cached.hasMore);
        this.lastBarTs = cached.bars[cached.bars.length - 1].timestamp;
        this._snapToLatest();
        this._watchdog();
        this.pollLatest();
        return;
      }

      this.lastBarTs = 0;
      this.hasMore = true;
      await this.loadInitial();
    },

    // ---- indicators ----

    // KLineCharts ships only 5 default line styles; any line beyond index 4 (or
    // any partially-specified style) must be a COMPLETE style object, otherwise
    // the canvas painter dies silently inside requestAnimationFrame and the
    // whole chart (including candles) stops rendering.
    _lineStyle(color) {
      return { color, size: 1, style: "solid", smooth: false, dashedValue: [2, 2] };
    },

    _nextFrame() {
      return new Promise((r) => requestAnimationFrame(() => r()));
    },

    // Creating several indicator panes in the same tick can wedge KLineCharts
    // v9's render loop (observed intermittently — chart goes fully blank with
    // no exception). Spacing the create calls across animation frames avoids
    // the race, and _watchdog() self-heals if it ever happens anyway.
    async applyIndicators() {
      const s = this.settings;

      // MA overlaid on the candle pane (multi-period, per-line colors)
      try {
        if (s.ma.enabled && s.ma.params.length) {
          const override = {
            name: "MA",
            calcParams: s.ma.params.map((p) => p.period),
            styles: { lines: s.ma.params.map((p) => this._lineStyle(p.color)) },
          };
          if (!this.paneIds.ma) {
            this.chart.createIndicator(override, true, { id: "candle_pane" });
            this.paneIds.ma = true;
          } else {
            this.chart.overrideIndicator(override, "candle_pane");
          }
        } else if (this.paneIds.ma) {
          this.chart.removeIndicator("candle_pane", "MA");
          this.paneIds.ma = false;
        }
      } catch (e) { console.error("MA indicator failed:", e); }

      // sub-pane indicators — one per frame
      await this._nextFrame();
      this.applySubIndicator("WR", "wr", s.wr);
      await this._nextFrame();
      this.applySubIndicator("BIAS", "bias", s.bias);
      await this._nextFrame();

      // volume pane
      try {
        if (s.vol.enabled && !this.paneIds.vol) {
          this.paneIds.vol = this.chart.createIndicator("VOL", false, { height: 90 });
        } else if (!s.vol.enabled && this.paneIds.vol) {
          this.chart.removeIndicator(this.paneIds.vol);
          this.paneIds.vol = null;
        }
      } catch (e) { console.error("VOL indicator failed:", e); }

      // force a clean relayout/repaint after pane changes
      await this._nextFrame();
      this.chart.resize();
    },

    // Self-heal: if the render loop wedged (data present but nothing painted on
    // the main canvas), a resize() rebuilds the layout and revives painting
    // (verified empirically against the wedge). Debounced; scheduled after every
    // data apply / pane change.
    _wdTimers: [],
    _watchdog() {
      this._wdTimers.forEach(clearTimeout);
      const check = () => {
        try {
          if (!this.chart.getDataList().length) return;
          const cv = document.querySelector("#kchart canvas");
          if (!cv || !cv.width) return;
          const ctx = cv.getContext("2d", { willReadFrequently: true });
          const d = ctx.getImageData(0, 0, cv.width, cv.height).data;
          let lit = 0;
          for (let i = 0; i < d.length; i += 64) {
            if (d[i + 3] > 0) { lit++; if (lit > 50) return; } // painted — all good
          }
          console.warn("chart watchdog: blank canvas with data — reviving via resize()");
          this.chart.resize();
          this._snapToLatest();
        } catch (e) { /* heuristic only */ }
      };
      this._wdTimers = [setTimeout(check, 1200), setTimeout(check, 3500)];
    },

    applySubIndicator(name, key, conf) {
      try {
        const active = conf.enabled && conf.params.length;
        if (active) {
          const override = {
            name,
            calcParams: conf.params.map((p) => p.period),
            styles: { lines: conf.params.map((p) => this._lineStyle(p.color)) },
          };
          if (!this.paneIds[key]) {
            this.paneIds[key] = this.chart.createIndicator(override, false, { height: 90 });
          } else {
            this.chart.overrideIndicator(override, this.paneIds[key]);
          }
        } else if (this.paneIds[key]) {
          this.chart.removeIndicator(this.paneIds[key]);
          this.paneIds[key] = null;
        }
      } catch (e) { console.error(name + " indicator failed:", e); }
    },

    async saveIndicators() {
      await this.applyIndicators();
      try {
        await fetch(`/api/chart/state/indicators?symbol=${SYMBOL}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(this.settings),
        });
      } catch (e) { /* non-fatal */ }
    },

    // ---- drawings ----

    overlayEvents() {
      return {
        onDrawEnd: (e) => { this.trackOverlay(e.overlay); return false; },
        onPressedMoveEnd: (e) => { this.trackOverlay(e.overlay); return false; },
        onRemoved: (e) => {
          this.overlays.delete(e.overlay.id);
          this.scheduleSave();
          return false;
        },
      };
    },

    trackOverlay(overlay) {
      if (!overlay || !overlay.points) return;
      this.overlays.set(overlay.id, {
        name: overlay.name,
        points: overlay.points.map((p) => ({ timestamp: p.timestamp, value: p.value })),
      });
      this.scheduleSave();
    },

    draw(name) {
      this.chart.createOverlay({ name, groupId: GROUP_ID, ...this.overlayEvents() });
    },

    restoreDrawings(list) {
      for (const d of list) {
        if (!d || !d.name || !Array.isArray(d.points)) continue;
        const id = this.chart.createOverlay({
          name: d.name, groupId: GROUP_ID, points: d.points, ...this.overlayEvents(),
        });
        if (typeof id === "string") {
          this.overlays.set(id, { name: d.name, points: d.points });
        }
      }
    },

    clearDrawings() {
      this.chart.removeOverlay({ groupId: GROUP_ID });
      this.overlays.clear();
      this.scheduleSave();
    },

    _saveTimer: null,
    scheduleSave() {
      clearTimeout(this._saveTimer);
      this._saveTimer = setTimeout(() => this.saveDrawings(), 1000);
    },

    async saveDrawings() {
      try {
        await fetch(`/api/chart/state/drawings?symbol=${SYMBOL}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
          body: JSON.stringify([...this.overlays.values()]),
        });
      } catch (e) { /* non-fatal */ }
    },
  };

  // ---- Alpine component ----

  window.chartPanel = () => ({
    tfs: TFS,
    tf: "1m",
    settingsOpen: false,
    form: JSON.parse(JSON.stringify(DEFAULT_SETTINGS)),
    indicatorDefs: [
      { key: "ma", title: "均線 MA", hint: "疊於主圖" },
      { key: "wr", title: "威廉指標 WR", hint: "副圖" },
      { key: "bias", title: "乖離率 BIAS", hint: "副圖" },
    ],

    init() { QQChart.init(); },

    async setTf(tf) {
      this.tf = tf;
      await QQChart.setTf(tf);
    },

    draw(name) { QQChart.draw(name); },
    clearDrawings() {
      if (confirm("確定清除所有繪圖？")) QQChart.clearDrawings();
    },

    openSettings() {
      this.form = JSON.parse(JSON.stringify(QQChart.settings)); // edit a copy
      this.settingsOpen = true;
    },

    async saveSettings() {
      // sanitize: positive integer periods only
      for (const key of ["ma", "wr", "bias"]) {
        this.form[key].params = this.form[key].params.filter(
          (p) => Number.isFinite(p.period) && p.period >= 1
        );
        this.form[key].params.forEach((p) => { p.period = Math.round(p.period); });
      }
      QQChart.settings = JSON.parse(JSON.stringify(this.form));
      await QQChart.saveIndicators();
      this.settingsOpen = false;
    },
  });

  window.QQChart = QQChart; // debugging hook
})();
