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

  // Overlays whose position is defined purely by price (a single horizontal
  // level). These translate 1:1 across timeframes — a price is grid-independent
  // — so in "hybrid" scope they render on every timeframe. Everything else is
  // anchored to bar timestamps/geometry and only makes sense on the timeframe
  // it was drawn on (two 1m-apart points collapse onto one daily bar, etc.).
  const CROSS_TF_NAMES = new Set(["horizontalStraightLine", "priceLine"]);

  const TFS = [
    { code: "1m", label: "1分" }, { code: "5m", label: "5分" },
    { code: "10m", label: "10分" }, { code: "15m", label: "15分" },
    { code: "20m", label: "20分" }, { code: "30m", label: "30分" },
    { code: "60m", label: "60分" }, { code: "4h", label: "4時" },
    { code: "1d", label: "日" }, { code: "3d", label: "3日" },
    { code: "1w", label: "週" }, { code: "1M", label: "月" },
  ];

  const OP_LABELS = { gte: "≥", lte: "≤", cross_up: "向上突破", cross_down: "向下突破" };
  function alertLabel(a) {
    const short = (k) => window.QQIndicators.shortName(k);
    const L = a.left_kind === "price" ? "收盤" : `${short(a.left_name)}(${a.left_period})`;
    const R = a.right_kind === "const" ? a.right_value : `${short(a.right_name)}(${a.right_period})`;
    return `${a.timeframe}｜${L} ${OP_LABELS[a.op]} ${R}`;
  }


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

  const LIGHT_STYLES = {
    grid: {
      horizontal: { color: "#e5e9f0" },
      vertical: { color: "#e5e9f0" },
    },
    candle: {
      tooltip: { text: { color: "#475569" } },
    },
    xAxis: { axisLine: { color: "#cbd5e1" }, tickText: { color: "#475569" } },
    yAxis: { axisLine: { color: "#cbd5e1" }, tickText: { color: "#475569" } },
    crosshair: {
      horizontal: { line: { color: "#94a3b8" } },
      vertical: { line: { color: "#94a3b8" } },
    },
    separator: { color: "#cbd5e1" },
  };

  function themeStyles(theme) {
    return theme === "light" ? LIGHT_STYLES : DARK_STYLES;
  }

  function candleColorStyles(scheme) {
    const up = scheme === "red_up" ? "#ea3943" : "#16c784";
    const down = scheme === "red_up" ? "#16c784" : "#ea3943";
    return {
      candle: {
        bar: {
          upColor: up, downColor: down, noChangeColor: "#888888",
          upBorderColor: up, downBorderColor: down, noChangeBorderColor: "#888888",
          upWickColor: up, downWickColor: down, noChangeWickColor: "#888888",
        },
        priceMark: { last: { upColor: up, downColor: down } },
      },
    };
  }

  const QQChart = {
    chart: null,
    tf: "1m",
    session: "all", // "all" | "day" | "night" — intraday price filter
    lastBarTs: 0,
    hasMore: true,
    loadingMore: false,
    polling: false,
    paneIds: { wr: null, bias: null, vol: null, ma: false },
    // Master list of ALL drawings (persisted), each: {key, name, points, tf}.
    // `tf` = timeframe it was drawn on ("1m".."1M"), null for legacy drawings.
    drawings: [],
    _liveIds: new Map(),  // klinecharts overlay id -> drawing key (rendered subset)
    _drawKey: 0,          // monotonic id generator for drawing records
    // "hybrid": horizontal price lines on all TFs, geometric lines on their own
    // TF only. "all": every drawing on every TF (time+price anchored). Set by
    // the Alpine layer from localStorage before init().
    drawScope: "hybrid",
    // Master visibility switch for the whole user-drawing layer. false hides
    // every overlay without touching the master list. Set by the Alpine layer
    // from localStorage before init().
    drawingsVisible: true,
    colorScheme: "green_up",   // 由 Alpine 於 init 前依 window.QQ_COLOR_SCHEME 設定
    _suppressRemoveTracking: false, // true while re-rendering (ignore onRemoved)
    _drawingId: null,    // overlay currently being drawn (ESC cancels it)
    _hoverId: null,      // overlay under the cursor (Delete target)
    _selectedId: null,   // overlay last clicked/selected (sticky Delete target)
    tfCache: new Map(),  // (session|tf) -> {bars, hasMore} — instant switching
    settings: window.QQIndicators.defaults(),
    deductionEnabled: false,   // 均線扣抵三角開關（由 Alpine 依 localStorage 設定）
    nakedK: false,             // 全域裸K：true 時隱藏所有指標與扣抵三角（由 Alpine 依 localStorage 設定）
    onEditIndicator: null,     // (key) => void：點擊指標 tooltip icon → 打開該指標設定
    _deductionSig: "",         // 上次已畫三角的幾何簽章；相同則跳過重畫
    _deductionDrawn: false,    // 目前是否有扣抵三角在圖上（關閉時只清一次）

    async init() {
      if (!window.klinecharts) {
        console.error("klinecharts failed to load");
        return;
      }
      this.colorScheme = window.QQ_COLOR_SCHEME || "green_up";
      const initTheme = document.documentElement.getAttribute("data-theme") === "light" ? "light" : "dark";
      const styles = JSON.parse(JSON.stringify(themeStyles(initTheme)));
      const cc = candleColorStyles(this.colorScheme).candle;
      styles.candle = Object.assign({}, styles.candle, { bar: cc.bar, priceMark: cc.priceMark });
      // v9 內建只有 en-US / zh-CN（zh-CN 的十字游標標籤是簡體）。
      // 註冊繁體 zh-TW 覆寫全部 8 個 i18n key，確保網頁不出現簡體字。
      if (klinecharts.registerLocale) {
        klinecharts.registerLocale("zh-TW", {
          time: "時間：", open: "開：", high: "高：", low: "低：",
          close: "收：", volume: "成交量：", turnover: "成交額：", change: "漲幅：",
        });
      }
      this.chart = klinecharts.init("kchart", {
        timezone: "Asia/Taipei",
        locale: "zh-TW",
        styles,
      });
      if (this.chart.setTimezone) this.chart.setTimezone("Asia/Taipei");
      if (this.chart.setPriceVolumePrecision) this.chart.setPriceVolumePrecision(0, 0);

      // 指標 tooltip 上的「編輯」icon → 打開指標設定 dialog
      this.chart.setStyles({
        indicator: { tooltip: { icons: [{
          id: "qq-edit", position: "middle", marginLeft: 8, marginTop: 6,
          marginRight: 0, marginBottom: 0, paddingLeft: 2, paddingTop: 2,
          paddingRight: 2, paddingBottom: 2, size: 14, color: "#94a3b8",
          activeColor: "#3b82f6", backgroundColor: "transparent",
          activeBackgroundColor: "rgba(59,130,246,0.15)",
          icon: "✎", fontFamily: "sans-serif",
        }] } },
      });
      this.chart.subscribeAction("onTooltipIconClick", (data) => {
        if (!data || data.iconId !== "qq-edit" || !this.onEditIndicator) return;
        const klineName = (data.indicator && data.indicator.name) || data.indicatorName || null;
        const entry = klineName ? window.QQIndicators.byKlineName(klineName) : null;
        this.onEditIndicator(entry ? entry.key : undefined);
      });

      this.session = localStorage.getItem("qq_session") || "all";
      this.chart.loadMore((ts) => this.loadMore(ts));
      window.addEventListener("qq:theme-changed", (e) => this.applyTheme(e.detail));

      let state = { indicators: null, drawings: null };
      try {
        state = await (await fetch(`/api/chart/state?symbol=${SYMBOL}`)).json();
      } catch (e) { /* defaults */ }
      if (state.indicators) this.settings = this.mergeSettings(state.indicators);

      // load data BEFORE indicators: even if an indicator fails, candles render
      if (window.MADeduction) window.MADeduction.register();
      await this.loadInitial();
      await this.applyIndicators();
      this.restoreDrawings(Array.isArray(state.drawings) ? state.drawings : []);
      this._watchdog();

      // pick up the final container width after the flex layout (left rail) settles
      requestAnimationFrame(() => this.chart.resize());

      setInterval(() => this.pollLatest(), 5000);
      this._bindQuoteSync();
      this._bindDrawingKeys();
      window.addEventListener("resize", () => {
        this.chart.resize();
        this._watchdog(); // rapid resizes can wedge the render loop too
      });
      document.addEventListener("visibilitychange", () => {
        if (!document.hidden) this.pollLatest(); // catch up right away
      });
    },

    mergeSettings(saved) {
      return window.QQIndicators.merge(saved);
    },

    // ---- data ----

    _cacheKey(tf) { return this.session + "|" + (tf || this.tf); },

    async fetchPage(before) {
      const params = new URLSearchParams({ symbol: SYMBOL, tf: this.tf, limit: PAGE_LIMIT });
      if (before) params.set("before", String(before));
      if (this.session !== "all") params.set("session", this.session);
      const resp = await fetch(`/api/candles?${params}`);
      return resp.json();
    },

    _cacheBars(bars, hasMore) {
      this.tfCache.set(this._cacheKey(), { bars, hasMore });
    },

    _applyBar(bar) {
      // 資料防呆：OHLC + timestamp 必須有限；亂序（早於已見最後一根）直接丟棄，
      // 避免休市/來源異常造成的 NaN 破圖與「K 線亂跳」。
      if (!window.QQChartGuards.isRenderableBar(bar, this.lastBarTs)) return;

      // keep chart + tfCache consistent for one updated/appended bar
      this.chart.updateData(bar);
      const cached = this.tfCache.get(this._cacheKey());
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

    // NOTE on guards: every async data path captures the (tf, session) it was
    // started for and discards its result if the user switched meanwhile.
    // Without this, a stale response can append e.g. a daily bar into a 1m series
    // — the resulting layout math wedges KLineCharts' render loop (blank chart).

    async loadInitial() {
      const tf = this.tf, sess = this.session;
      const data = await this.fetchPage(null);
      if (this.tf !== tf || this.session !== sess) return; // switched mid-flight
      this.hasMore = !!data.hasMore;
      const bars = data.bars || [];
      const empty = document.getElementById("chart-empty");
      if (empty) empty.style.display = bars.length ? "none" : "flex";
      this.chart.applyNewData(bars, this.hasMore);
      this.lastBarTs = bars.length ? bars[bars.length - 1].timestamp : 0;
      this._cacheBars(bars.slice(), this.hasMore);
      this._snapToLatest();
      this._watchdog();
      this.refreshDeduction();
    },

    async loadMore(ts) {
      if (!this.hasMore || this.loadingMore || !ts) return;
      this.loadingMore = true;
      const tf = this.tf, sess = this.session;
      try {
        const data = await this.fetchPage(ts);
        if (this.tf !== tf || this.session !== sess) return; // stale response
        this.hasMore = !!data.hasMore;
        const bars = data.bars || [];
        this.chart.applyMoreData(bars, this.hasMore);
        const cached = this.tfCache.get(this._cacheKey());
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
      const tf = this.tf, sess = this.session;
      try {
        if (!this.lastBarTs) {
          await this.loadInitial(); // nothing yet (e.g. server just started)
          return;
        }
        const params = new URLSearchParams({
          symbol: SYMBOL, tf, since: String(this.lastBarTs),
        });
        if (sess !== "all") params.set("session", sess);
        const resp = await fetch(`/api/candles/latest?${params}`);
        const data = await resp.json();
        if (this.tf !== tf || this.session !== sess) return; // stale response
        for (const bar of data.bars || []) this._applyBar(bar);
        if ((data.bars || []).length) this.refreshDeduction();
      } catch (e) { /* next poll retries */ } finally {
        this.polling = false;
      }
    },

    // Keep the chart's last-price marker in lockstep with the big quote display.
    // Both are the same value (CLastPrice of the live snapshot), but the quote is
    // server-pushed over SSE on every 5s poll while the chart's own pollLatest()
    // runs on an independent 5s timer — so the chart can visibly trail the quote
    // by up to a poll cycle. The quote partial swaps into #quote (htmx SSE) and
    // carries the raw price in data-qq-price; on each swap we nudge the in-progress
    // bar's close immediately, with zero server round-trip. pollLatest() still
    // reconciles volume / new buckets on its timer.
    _bindQuoteSync() {
      const el = document.getElementById("quote");
      if (!el) return;
      const sync = () => {
        const node = el.querySelector("[data-qq-price]");
        if (!node) return;
        this.onQuote(parseFloat(node.dataset.qqPrice), node.dataset.qqSession, {
          status: node.dataset.qqMarketStatus,
          fresh: node.dataset.qqFresh === "true",
        });
      };
      // htmx fires afterSwap on the target after each SSE message is swapped in.
      document.body.addEventListener("htmx:afterSwap", (e) => {
        if (e.target && e.target.id === "quote") sync();
      });
      sync(); // pick up the first quote already rendered via hx-get on load
    },

    onQuote(price, quoteSession, meta) {
      if (!Number.isFinite(price)) return;
      // 休市／停滯（非 open 或非 fresh）時，不可用凍結報價覆寫最後一根真棒。
      if (window.QQChartGuards.shouldFreezeQuote(meta)) return;
      // A day/night-filtered chart intentionally shows that session's last bar; a
      // live quote from the other session must not overwrite it. Only sync when
      // the chart shows the combined series (matches the quote's contract).
      if (this.session !== "all") return;
      if (!this.chart) return;
      const list = this.chart.getDataList();
      if (!list || !list.length) return;
      const last = list[list.length - 1];
      if (price === last.close) return;
      this._applyBar({
        timestamp: last.timestamp,
        open: last.open,
        high: Math.max(last.high, price),
        low: Math.min(last.low, price),
        close: price,
        volume: last.volume,
      });
      this.refreshDeduction();
    },

    _switchFromCacheOrLoad() {
      const cached = this.tfCache.get(this._cacheKey());
      if (cached && cached.bars.length) {
        // instant switch from client cache, then refresh just the tail
        this.hasMore = cached.hasMore;
        this.chart.applyNewData(cached.bars.slice(), cached.hasMore);
        this.lastBarTs = cached.bars[cached.bars.length - 1].timestamp;
        this._snapToLatest();
        this._watchdog();
        this.refreshDeduction();
        this.pollLatest();
        return true;
      }
      this.lastBarTs = 0;
      this.hasMore = true;
      return false;
    },

    async setTf(tf) {
      if (tf === this.tf) return;
      this.tf = tf;
      if (!this._switchFromCacheOrLoad()) await this.loadInitial();
      this._renderDrawings(); // re-anchor overlays to the new timeframe grid
    },

    async setSession(mode) {
      if (mode === this.session) return;
      this.session = mode;
      localStorage.setItem("qq_session", mode);
      if (!this._switchFromCacheOrLoad()) await this.loadInitial();
      this._renderDrawings(); // data grid changed → rebuild overlays cleanly
    },

    // ---- indicators ----

    // KLineCharts ships only 5 default line styles; any line beyond index 4 (or
    // any partially-specified style) must be a COMPLETE style object, otherwise
    // the canvas painter dies silently inside requestAnimationFrame and the
    // whole chart (including candles) stops rendering.
    _lineStyle(color) {
      return { color, size: 1, style: "solid", smooth: false, dashedValue: [2, 2] };
    },

    applyColorScheme(scheme) {
      this.colorScheme = scheme;
      // quote panel rise/fall colour follows the scheme via html[data-scheme]
      document.documentElement.setAttribute("data-scheme", scheme);
      if (this.chart && this.chart.setStyles) {
        this.chart.setStyles(candleColorStyles(scheme));
      }
    },

    applyTheme(theme) {
      if (this.chart && this.chart.setStyles) {
        this.chart.setStyles(themeStyles(theme));
        // K 棒漲跌色由紅漲/綠漲偏好控制，主題切換後重套一次以免被覆蓋
        this.chart.setStyles(candleColorStyles(this.colorScheme));
      }
    },

    _nextFrame() {
      return new Promise((r) => requestAnimationFrame(() => r()));
    },

    // Creating several indicator panes in the same tick can wedge KLineCharts
    // v9's render loop (observed intermittently — chart goes fully blank with
    // no exception). Spacing the create calls across animation frames avoids
    // the race, and _watchdog() self-heals if it ever happens anyway.
    async applyIndicators() {
      for (const entry of window.QQIndicators.list) {
        this._applyIndicator(entry, this.settings[entry.key]);
        if (entry.pane === "sub") await this._nextFrame(); // 逐格建立，避免同 tick 建多 pane 卡渲染
      }
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

    _applyIndicator(entry, conf) {
      const key = entry.key;
      const onMain = entry.pane === "main";
      try {
        // 可見性集中判定（含裸K/單指標隱藏/repeatable 空線）——見 indicators.js
        const active = window.QQIndicators.resolveVisibility(entry, conf, this.nakedK);
        if (active) {
          const override = {
            name: entry.klineName,
            calcParams: window.QQIndicators.calcParams(entry, conf),
          };
          if (entry.repeatable) {
            override.styles = { lines: conf.params.map((p) => this._lineStyle(p.color)) };
          }
          if (!this.paneIds[key]) {
            if (onMain) {
              this.chart.createIndicator(override, true, { id: "candle_pane" });
              this.paneIds[key] = true;
            } else {
              this.paneIds[key] = this.chart.createIndicator(override, false, { height: 90 });
            }
          } else if (onMain) {
            this.chart.overrideIndicator(override, "candle_pane");
          } else {
            this.chart.overrideIndicator(override, this.paneIds[key]);
          }
        } else if (this.paneIds[key]) {
          if (onMain) {
            this.chart.removeIndicator("candle_pane", entry.klineName);
            this.paneIds[key] = false;
          } else {
            this.chart.removeIndicator(this.paneIds[key]);
            this.paneIds[key] = null;
          }
        }
      } catch (e) { console.error(entry.klineName + " indicator failed:", e); }
    },

    async saveIndicators() {
      await this.applyIndicators();
      this.refreshDeduction();
      try {
        await fetch(`/api/chart/state/indicators?symbol=${SYMBOL}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(this.settings),
        });
      } catch (e) { /* non-fatal */ }
    },

    // ---- MA deduction (live mode) ----

    setDeduction(on) {
      this.deductionEnabled = !!on;
      this.refreshDeduction();
    },

    // 全域裸K開關：重套所有指標（resolveVisibility 依 nakedK 決定畫或移除）並刷新扣抵。
    setNakedK(on) {
      this.nakedK = !!on;
      this.applyIndicators();
      this.refreshDeduction();
    },

    // 以最新 K 棒重算各啟用 MA 的扣抵；狀態列每 tick 更新，三角僅在扣抵 K 棒
    // 組合改變（新棒）時重畫，避免每 5 秒輪詢重建造成閃爍與 hover 文字斷裂。
    refreshDeduction() {
      if (!this.chart || !window.MADeduction) return;
      if (!this.deductionEnabled || this.nakedK) {
        if (this._deductionDrawn) {
          window.MADeduction.clear(this.chart);
          this._deductionDrawn = false;
          this._deductionSig = "";
        }
        return;
      }
      const bars = this.chart.getDataList() || [];
      // 只對「已顯示」的 MA 線算扣抵：MA 指標關閉時不顯示三角。
      const params = (this.settings.ma && this.settings.ma.enabled && this.settings.ma.params) || [];
      const results = window.MADeduction.computeLive(bars, params); // 用模組預設容忍值 0.1
      const sig = results
        .map((r) => r.period + ":" + r.deductionTime + ":" + r.deductionValue + ":" + r.color)
        .join("|");
      if (sig !== this._deductionSig) {
        window.MADeduction.draw(this.chart, results); // 三角幾何改變才重畫
        this._deductionSig = sig;
        this._deductionDrawn = true;
      }
    },

    // ---- drawings ----

    overlayEvents() {
      return {
        // freshly drawn overlay → create a new record tagged with the current TF
        onDrawEnd: (e) => { this._drawingId = null; this._registerNewOverlay(e.overlay); return false; },
        // dragged an existing overlay → update its record's points
        onPressedMoveEnd: (e) => { this._updateOverlayPoints(e.overlay); return false; },
        // track the cursor/click target so keyboard Delete knows what to remove
        onMouseEnter: (e) => { this._hoverId = e.overlay.id; return false; },
        onMouseLeave: (e) => { if (this._hoverId === e.overlay.id) this._hoverId = null; return false; },
        onClick: (e) => { this._selectedId = e.overlay.id; return false; },
        onRemoved: (e) => {
          if (this._suppressRemoveTracking) return false; // re-render churn, not a user delete
          const key = this._liveIds.get(e.overlay.id);
          this._liveIds.delete(e.overlay.id);
          if (key != null) this.drawings = this.drawings.filter((r) => r.key !== key);
          if (this._hoverId === e.overlay.id) this._hoverId = null;
          if (this._selectedId === e.overlay.id) this._selectedId = null;
          this.scheduleSave();
          return false;
        },
      };
    },

    _pointsOf(overlay) {
      // Keep dataIndex alongside timestamp: klinecharts positions x by timestamp
      // when present, else falls back to dataIndex. Points drawn past the last
      // bar (the empty "future" zone) have NO timestamp — without dataIndex they
      // recreate at convertToPixel(undefined) === NaN and the drawing breaks.
      return (overlay.points || []).map((p) => ({
        timestamp: p.timestamp, dataIndex: p.dataIndex, value: p.value,
      }));
    },

    _registerNewOverlay(overlay) {
      if (!overlay || !overlay.points) return;
      const key = ++this._drawKey;
      this.drawings.push({ key, name: overlay.name, points: this._pointsOf(overlay), tf: this.tf });
      this._liveIds.set(overlay.id, key);
      this.scheduleSave();
    },

    _updateOverlayPoints(overlay) {
      const key = this._liveIds.get(overlay.id);
      if (key == null) return;
      const rec = this.drawings.find((r) => r.key === key);
      if (!rec) return;
      rec.points = this._pointsOf(overlay);
      this.scheduleSave();
    },

    draw(name) {
      // remember the id while it's being drawn so ESC can cancel it mid-draw
      this._drawingId = this.chart.createOverlay({ name, groupId: GROUP_ID, ...this.overlayEvents() });
    },

    // Which drawings are visible for the current (scope, timeframe).
    _isCrossTf(name) { return CROSS_TF_NAMES.has(name); },
    // Every point must have an x anchor (timestamp OR dataIndex); a point with
    // neither renders at convertToPixel(undefined) === NaN and sprawls across the
    // whole chart. Skip such records (e.g. drawings saved by the buggy build that
    // lost their anchor) instead of misrendering them.
    _anchorable(rec) {
      return (rec.points || []).length > 0 && rec.points.every(
        (p) => Number.isFinite(p.timestamp) || Number.isFinite(p.dataIndex));
    },
    _shouldShow(rec) {
      if (this.drawScope === "all") return true;          // show everything, everywhere
      if (this._isCrossTf(rec.name)) return true;         // horizontal price levels: all TFs
      if (rec.tf == null) return true;                    // legacy (untagged): keep visible
      return rec.tf === this.tf;                           // geometric: native TF only
    },

    // Rebuild the on-chart overlays from the master list for the current TF/scope.
    // Wipes the live layer first; the group-remove fires onRemoved per overlay,
    // so guard it to avoid mutating the master list.
    _renderDrawings() {
      if (!this.chart) return;
      this._suppressRemoveTracking = true;
      this.chart.removeOverlay({ groupId: GROUP_ID });
      this._suppressRemoveTracking = false;
      this._liveIds.clear();
      this._hoverId = null;
      this._selectedId = null;
      if (!this.drawingsVisible) return; // master switch off: keep the layer wiped
      for (const rec of this.drawings) {
        if (!this._shouldShow(rec)) continue;
        if (!this._anchorable(rec)) { console.warn("skip unanchorable drawing", rec); continue; }
        const id = this.chart.createOverlay({
          name: rec.name, groupId: GROUP_ID, points: rec.points, ...this.overlayEvents(),
        });
        if (typeof id === "string") this._liveIds.set(id, rec.key);
      }
    },

    setDrawScope(scope) {
      this.drawScope = scope === "all" ? "all" : "hybrid";
      localStorage.setItem("qq_draw_scope", this.drawScope);
      this._renderDrawings();
    },

    // Show/hide the entire user-drawing layer. The master list is untouched, so
    // toggling back on re-renders every drawing for the current TF/scope.
    setDrawingsVisible(visible) {
      this.drawingsVisible = !!visible;
      localStorage.setItem("qq_draw_visible", this.drawingsVisible ? "1" : "0");
      this._renderDrawings();
    },

    // TradingView-style keyboard UX: ESC cancels a half-drawn overlay (or clears
    // the selection); Delete/Backspace removes the hovered/selected overlay.
    // Skipped while typing in a field or while a settings/alerts dialog is open.
    _bindDrawingKeys() {
      document.addEventListener("keydown", (e) => {
        const t = e.target;
        if (t && (t.tagName === "INPUT" || t.tagName === "SELECT" ||
                  t.tagName === "TEXTAREA" || t.isContentEditable)) return;
        if (document.querySelector("dialog[open]")) return;

        if (e.key === "Escape") {
          if (this._drawingId) {
            this.chart.removeOverlay(this._drawingId); // cancel mid-draw
            this._drawingId = null;
            e.preventDefault();
          } else {
            this._selectedId = null; // just clear the sticky selection
          }
        } else if (e.key === "Delete" || e.key === "Backspace") {
          const id = this._selectedId || this._hoverId;
          if (id) {
            this.chart.removeOverlay(id); // onRemoved clears tracking + persists
            this._selectedId = null;
            this._hoverId = null;
            e.preventDefault();
          }
        }
      });
    },

    restoreDrawings(list) {
      this.drawings = [];
      for (const d of list) {
        if (!d || !d.name || !Array.isArray(d.points)) continue;
        this.drawings.push({
          key: ++this._drawKey,
          name: d.name,
          points: d.points,
          tf: typeof d.tf === "string" ? d.tf : null, // legacy records have no tf
        });
      }
      this._renderDrawings();
    },

    clearDrawings() {
      this._suppressRemoveTracking = true;
      this.chart.removeOverlay({ groupId: GROUP_ID }); // wipe all TFs, not just visible
      this._suppressRemoveTracking = false;
      this.drawings = [];
      this._liveIds.clear();
      this._hoverId = null;
      this._selectedId = null;
      this.scheduleSave();
    },

    _saveTimer: null,
    scheduleSave() {
      clearTimeout(this._saveTimer);
      this._saveTimer = setTimeout(() => this.saveDrawings(), 1000);
    },

    async saveDrawings() {
      const payload = this.drawings.map((r) => ({ name: r.name, points: r.points, tf: r.tf }));
      try {
        await fetch(`/api/chart/state/drawings?symbol=${SYMBOL}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(payload),
        });
      } catch (e) { /* non-fatal */ }
    },
  };

  // ---- Alpine component ----

  window.chartPanel = () => ({
    tfs: TFS,
    tf: "1m",
    session: "all",
    sessions: [
      { code: "all", label: "全部" },
      { code: "day", label: "只日盤" },
      { code: "night", label: "只夜盤" },
    ],
    settingsOpen: false,
    moreOpen: false,
    fullscreen: false,
    deductionOn: false,
    nakedK: false,       // 全域裸K（此裝置看盤模式，存 localStorage）
    drawScope: "hybrid", // "hybrid" | "all" — cross-timeframe drawing visibility
    drawingsVisible: true, // master show/hide switch for the whole drawing layer
    colorScheme: window.QQ_COLOR_SCHEME || "green_up",
    form: window.QQIndicators.defaults(),
    indicators: window.QQIndicators.list,   // registry（主從式清單/目錄的來源）
    activeIndicator: null,                  // 右欄正在編輯的指標 key
    catalogOpen: false,                     // 「新增指標」目錄是否展開

    // ---- alerts ----
    alertsOpen: false,
    alerts: [],
    alertEvents: [],
    toasts: [],
    _toastId: 0,
    indTargets: [{ code: "price", label: "收盤價" }, ...window.QQIndicators.alertTargets()],
    rightTargets: [{ code: "const", label: "固定值" }, ...window.QQIndicators.alertTargets()],
    ops: [
      { code: "cross_up", label: "向上突破" }, { code: "cross_down", label: "向下突破" },
      { code: "gte", label: "≥ 大於等於" }, { code: "lte", label: "≤ 小於等於" },
    ],
    alertForm: {
      timeframe: "1m", leftTarget: "price", leftPeriod: 20, op: "cross_up",
      rightTarget: "const", rightValue: 18000, rightPeriod: 60, fireOnce: false,
    },

    alertLabel(a) { return alertLabel(a); },

    init() {
      this.session = localStorage.getItem("qq_session") || "all";
      this.deductionOn = localStorage.getItem("qq_ma_deduction") === "1";
      this.nakedK = localStorage.getItem("qq_naked_k") === "1";
      this.drawScope = localStorage.getItem("qq_draw_scope") || "hybrid";
      this.drawingsVisible = localStorage.getItem("qq_draw_visible") !== "0";
      QQChart.onEditIndicator = (key) => this.openSettings(key);
      QQChart.deductionEnabled = this.deductionOn;
      QQChart.nakedK = this.nakedK;
      QQChart.drawScope = this.drawScope;
      QQChart.drawingsVisible = this.drawingsVisible;
      QQChart.colorScheme = this.colorScheme;
      QQChart.init();
      this._initAlerts();
      window.addEventListener("keydown", (e) => {
        if (document.querySelector("dialog[open]")) return;
        if (QQChart._drawingId) return;
        if (e.key === "Escape" && this.fullscreen) this.toggleFullscreen();
      });
    },

    _initAlerts() {
      this.loadAlerts();
      this.loadEvents();
      try {
        const es = new EventSource("/alerts/stream");
        es.onmessage = (e) => {
          try { this._onAlert(JSON.parse(e.data)); } catch (err) { /* ignore */ }
        };
      } catch (e) { /* SSE unsupported */ }
    },

    _onAlert(data) {
      const id = ++this._toastId;
      this.toasts.push({ id, body: data.body });
      setTimeout(() => { this.toasts = this.toasts.filter((t) => t.id !== id); }, 9000);
      this.loadEvents();
    },

    async openAlerts() {
      this.alertForm.timeframe = this.tf;
      await this.loadAlerts();
      await this.loadEvents();
      this.alertsOpen = true;
    },

    async loadAlerts() {
      try { this.alerts = await (await fetch(`/api/alerts?symbol=${SYMBOL}`)).json(); }
      catch (e) { /* offline */ }
    },

    async loadEvents() {
      try { this.alertEvents = await (await fetch("/api/alerts/events?limit=20")).json(); }
      catch (e) { /* offline */ }
    },

    async submitAlert() {
      const f = this.alertForm;
      const leftInd = f.leftTarget !== "price";
      const rightConst = f.rightTarget === "const";
      const payload = {
        symbol: SYMBOL, timeframe: f.timeframe,
        left_kind: leftInd ? "indicator" : "price",
        left_name: leftInd ? f.leftTarget : null,
        left_period: leftInd ? Math.round(f.leftPeriod) : null,
        op: f.op,
        right_kind: rightConst ? "const" : "indicator",
        right_value: rightConst ? Number(f.rightValue) : null,
        right_name: rightConst ? null : f.rightTarget,
        right_period: rightConst ? null : Math.round(f.rightPeriod),
        fire_once: f.fireOnce,
      };
      try {
        const r = await fetch("/api/alerts", {
          method: "POST", headers: { "content-type": "application/json" },
          body: JSON.stringify(payload),
        });
        if (r.ok) await this.loadAlerts();
        else window.alert("警示建立失敗，請檢查欄位設定。");
      } catch (e) { /* offline */ }
    },

    async toggleAlert(a) {
      try {
        const r = await fetch(`/api/alerts/${a.id}`, {
          method: "PATCH", headers: { "content-type": "application/json" },
          body: JSON.stringify({ enabled: !a.enabled }),
        });
        if (r.ok) await this.loadAlerts();
      } catch (e) { /* offline */ }
    },

    async deleteAlert(id) {
      try { await fetch(`/api/alerts/${id}`, { method: "DELETE" }); await this.loadAlerts(); }
      catch (e) { /* offline */ }
    },

    async setTf(tf) {
      this.tf = tf;
      await QQChart.setTf(tf);
    },

    async setSession(mode) {
      this.session = mode;
      await QQChart.setSession(mode);
    },

    draw(name) { QQChart.draw(name); },
    clearDrawings() {
      if (confirm("確定清除所有繪圖？")) QQChart.clearDrawings();
    },

    toggleDeduction() {
      this.deductionOn = !this.deductionOn;
      localStorage.setItem("qq_ma_deduction", this.deductionOn ? "1" : "0");
      QQChart.setDeduction(this.deductionOn);
    },

    toggleNakedK() {
      this.nakedK = !this.nakedK;
      localStorage.setItem("qq_naked_k", this.nakedK ? "1" : "0");
      QQChart.setNakedK(this.nakedK);
    },

    toggleDrawScope() {
      this.drawScope = this.drawScope === "hybrid" ? "all" : "hybrid";
      QQChart.setDrawScope(this.drawScope);
    },

    toggleDrawingsVisible() {
      this.drawingsVisible = !this.drawingsVisible;
      QQChart.setDrawingsVisible(this.drawingsVisible);
    },

    toggleFullscreen() {
      this.fullscreen = !this.fullscreen;
      document.body.classList.toggle("chart-fullscreen", this.fullscreen);
      // relayout after the CSS takes effect
      requestAnimationFrame(() => { if (QQChart.chart) QQChart.chart.resize(); });
    },

    toggleColorScheme() {
      this.colorScheme = this.colorScheme === "red_up" ? "green_up" : "red_up";
      QQChart.applyColorScheme(this.colorScheme);
      fetch("/api/user/color-scheme", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ scheme: this.colorScheme }),
      }).catch(() => { /* 即時已套用；存回失敗僅影響下次載入 */ });
    },

    openSettings(key) {
      this.form = JSON.parse(JSON.stringify(QQChart.settings)); // edit a copy
      // 預設選取：帶入的 key → 否則第一個已啟用指標 → 否則第一個
      const enabledKeys = this.indicators.filter((e) => this.form[e.key] && this.form[e.key].enabled).map((e) => e.key);
      this.activeIndicator = key || enabledKeys[0] || this.indicators[0]?.key || null;
      this.catalogOpen = false;
      this.settingsOpen = true;
    },

    async saveSettings() {
      for (const entry of this.indicators) {
        const conf = this.form[entry.key];
        if (!conf) continue;
        if (entry.repeatable) {
          conf.params = (conf.params || []).filter((p) => Number.isFinite(p.period) && p.period >= 1);
          conf.params.forEach((p) => { p.period = Math.round(p.period); });
        } else {
          for (const f of entry.paramSchema) {
            if (f.type !== "number") continue;
            const v = Math.round(conf.params[f.field]);
            conf.params[f.field] = Number.isFinite(v) && v >= 1 ? v : (entry.defaults.params[f.field] || 1);
          }
        }
      }
      QQChart.settings = JSON.parse(JSON.stringify(this.form));
      await QQChart.saveIndicators();
      this.settingsOpen = false;
    },
  });

  window.QQChart = QQChart; // debugging hook
})();
