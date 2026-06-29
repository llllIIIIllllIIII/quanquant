/* Market Pulse v0.1 — price-velocity audio cue (front-end).

   The backend PulseEngine computes the Velocity Level and stamps it on the live
   quote SSE as data-qq-pulse-level. This self-contained module turns level >= 2
   into short synthesized "唧" beeps, gated by an on/off toggle (heart-beat icon),
   per-level cooldown and a global max audio rate. No audio files — Web Audio
   oscillator only. Decoupled from chart.js. */
(function () {
  "use strict";

  var STORE_KEY = "qq_pulse";

  // Express level by hit DENSITY, not pitch: one dull base tone, more beeps.
  var FREQ = 520; // low-ish; a low-pass keeps the timbre blunt, not piercing
  var LOWPASS = 4000; // cut highs (3–5kHz) so it reads as soft taps
  var PATTERNS = {
    2: { beeps: 1, gap: 0.0, dur: 0.05, gain: 0.05 },
    3: { beeps: 2, gap: 0.09, dur: 0.05, gain: 0.07 },
    4: { beeps: 3, gap: 0.07, dur: 0.045, gain: 0.1 },
  };
  var COOLDOWN = { 2: 3000, 3: 2000, 4: 1500 }; // per-level cooldown ms (1.5–3s)
  var MAX_RATE = 2; // global max plays/sec
  var RATE_WINDOW = 1000;

  var ctx = null;
  var enabled = false;
  var lastPlay = { 2: 0, 3: 0, 4: 0 };
  var recent = []; // timestamps of recent plays, for the max-rate cap
  var seenLevel = 0; // last level read from the SSE
  var confirmedLevel = 0; // committed level (upgrades need 2 consecutive reads)
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

  function beep(startAt, dur, gain) {
    var c = ensureCtx();
    if (!c) return;
    var osc = c.createOscillator();
    var g = c.createGain();
    var lp = c.createBiquadFilter();
    osc.type = "triangle"; // blunt, less piercing than a square wave
    osc.frequency.value = FREQ;
    lp.type = "lowpass";
    lp.frequency.value = LOWPASS;
    // fast attack + fast decay → a soft tap, not a sharp beep
    g.gain.setValueAtTime(0.0001, startAt);
    g.gain.exponentialRampToValueAtTime(gain, startAt + 0.004);
    g.gain.exponentialRampToValueAtTime(0.0001, startAt + dur);
    osc.connect(g);
    g.connect(lp);
    lp.connect(c.destination);
    osc.start(startAt);
    osc.stop(startAt + dur + 0.02);
  }

  function playPattern(level) {
    var c = ensureCtx();
    var p = PATTERNS[level];
    if (!c || !p) return;
    var t0 = c.currentTime + 0.01;
    for (var i = 0; i < p.beeps; i++) {
      beep(t0 + i * (p.dur + p.gap), p.dur, p.gain);
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
    var lvl = readLevel(e.target);
    // Upgrades need 2 consecutive confirmations (a 1-tick blip never escalates);
    // downgrades apply immediately so it goes quiet fast.
    if (lvl > confirmedLevel) {
      if (lvl === seenLevel) confirmedLevel = lvl;
    } else {
      confirmedLevel = lvl;
    }
    seenLevel = lvl;
    maybePlay(confirmedLevel);
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
