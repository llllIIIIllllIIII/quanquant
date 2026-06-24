/* Market Pulse v0.1 — price-velocity audio cue (front-end).

   The backend PulseEngine computes the Velocity Level and stamps it on the live
   quote SSE as data-qq-pulse-level. This self-contained module turns level >= 2
   into short synthesized "唧" beeps, gated by an on/off toggle (heart-beat icon),
   per-level cooldown and a global max audio rate. No audio files — Web Audio
   oscillator only. Decoupled from chart.js. */
(function () {
  "use strict";

  var STORE_KEY = "qq_pulse";

  // Per-level beep pattern: beep count, frequency (Hz), gap + duration (s), gain.
  var PATTERNS = {
    2: { beeps: 1, freq: 880, gap: 0.0, dur: 0.06, gain: 0.12 },
    3: { beeps: 2, freq: 1100, gap: 0.07, dur: 0.05, gain: 0.16 },
    4: { beeps: 3, freq: 1320, gap: 0.05, dur: 0.045, gain: 0.2 },
  };
  var COOLDOWN = { 2: 2000, 3: 1000, 4: 500 }; // per-level cooldown ms (spec §11.3)
  var MAX_RATE = 5; // global max plays/sec (spec §11.4)
  var RATE_WINDOW = 1000;

  var ctx = null;
  var enabled = false;
  var lastPlay = { 2: 0, 3: 0, 4: 0 };
  var recent = []; // timestamps of recent plays, for the max-rate cap
  var btn = null;

  function now() {
    return window.performance && performance.now ? performance.now() : Date.now();
  }

  function ensureCtx() {
    if (ctx) return ctx;
    var AC = window.AudioContext || window.webkitAudioContext;
    if (!AC) return null;
    ctx = new AC();
    return ctx;
  }

  function unlock() {
    var c = ensureCtx();
    if (c && c.state === "suspended") c.resume();
  }

  function beep(freq, startAt, dur, gain) {
    var c = ensureCtx();
    if (!c) return;
    var osc = c.createOscillator();
    var g = c.createGain();
    osc.type = "square";
    osc.frequency.value = freq;
    // tiny attack/decay envelope so each "唧" is clean (no click)
    g.gain.setValueAtTime(0.0001, startAt);
    g.gain.exponentialRampToValueAtTime(gain, startAt + 0.005);
    g.gain.exponentialRampToValueAtTime(0.0001, startAt + dur);
    osc.connect(g);
    g.connect(c.destination);
    osc.start(startAt);
    osc.stop(startAt + dur + 0.02);
  }

  function playPattern(level) {
    var c = ensureCtx();
    var p = PATTERNS[level];
    if (!c || !p) return;
    var t0 = c.currentTime + 0.01;
    for (var i = 0; i < p.beeps; i++) {
      beep(p.freq, t0 + i * (p.dur + p.gap), p.dur, p.gain);
    }
  }

  function rateOk(t) {
    while (recent.length && t - recent[0] > RATE_WINDOW) recent.shift();
    return recent.length < MAX_RATE;
  }

  function maybePlay(level) {
    if (!enabled || level < 2 || !PATTERNS[level]) return;
    var t = now();
    if (t - lastPlay[level] < COOLDOWN[level]) return;
    if (!rateOk(t)) return;
    lastPlay[level] = t;
    recent.push(t);
    playPattern(level);
  }

  function readLevel(target) {
    var el =
      target && target.querySelector
        ? target.querySelector("[data-qq-pulse-level]") || target
        : target;
    var raw = el && el.dataset ? el.dataset.qqPulseLevel : null;
    var n = parseInt(raw, 10);
    return isNaN(n) ? 0 : n;
  }

  function onSwap(e) {
    if (!e || !e.target || e.target.id !== "quote") return;
    maybePlay(readLevel(e.target));
  }

  function reflect() {
    if (!btn) return;
    btn.classList.toggle("active", enabled);
    btn.setAttribute("aria-pressed", enabled ? "true" : "false");
    btn.title = enabled ? "市場脈搏音效：開（點擊關閉）" : "市場脈搏音效：關（點擊開啟）";
  }

  function setEnabled(v) {
    enabled = !!v;
    try {
      localStorage.setItem(STORE_KEY, enabled ? "1" : "0");
    } catch (_) {}
    if (enabled) {
      unlock();
      playPattern(2); // brief confirmation beep so the user knows it's on
    }
    reflect();
  }

  function toggle() {
    setEnabled(!enabled);
  }

  function init() {
    try {
      enabled = localStorage.getItem(STORE_KEY) === "1";
    } catch (_) {
      enabled = false;
    }
    btn = document.getElementById("pulse-toggle");
    if (btn) btn.addEventListener("click", toggle);
    document.body.addEventListener("htmx:afterSwap", onSwap);
    reflect();
  }

  // Console QA helper: hear a level's pattern without waiting for a real burst.
  window.QQPulse = {
    setEnabled: setEnabled,
    toggle: toggle,
    isEnabled: function () {
      return enabled;
    },
    test: function (level) {
      unlock();
      playPattern(level || 2);
    },
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
