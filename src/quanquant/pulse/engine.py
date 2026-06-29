"""Market Pulse streaming engine.

`PulseEngine` subscribes to the shared poller's tick stream (the un-coalesced
path — every published tick, unlike the ≤4/s coalesced SSE), maintains a rolling
``(ts, price)`` buffer, and classifies the current Velocity Level on demand.

Audio playback, per-level cooldown and the on/off toggle all live in the browser
(`static/pulse.js`); this engine only computes the level (stamped onto the quote
SSE) and fires a Telegram push when the market *enters* the configured high level
(edge-triggered + cooldown), reusing the existing TelegramNotifier.
"""
import asyncio
import logging
import time
from collections import deque
from datetime import timedelta, timezone

from quanquant.pulse import metrics

log = logging.getLogger(__name__)

_CST = timezone(timedelta(hours=8))


def _metrics_str(m: dict) -> str:
    """Compact one-line metric dump for the audit log / simulator."""
    return "T=%d M=%.1f R=%.1f D=%s" % (
        m["tick_count_1s"],
        m["price_move_3s"],
        m["velocity_ratio"],
        "true" if m["directional_continuity_3s"] else "false",
    )


def format_pulse_message(symbol: str, state: str, price: float, m: dict, when) -> str:
    """Compact zh Telegram body for a Market Pulse high-velocity event."""
    label = metrics.STATE_LABELS.get(
        next((k for k, v in metrics.STATE_NAMES.items() if v == state), 0), state
    )
    t = m.get("tick_count_1s", 0)
    r = m.get("velocity_ratio", 0.0)
    move = m.get("price_move_3s", 0.0)
    clock = when.astimezone(_CST).strftime("%H:%M:%S") if when is not None else "--:--:--"
    return "\n".join([
        "🚀 Market Pulse — 急速行情",
        "",
        f"📊 商品：{symbol}",
        f"⚡ 狀態：{state.capitalize()}（{label}）",
        f"💲 價位：{price:,.0f}",
        f"📈 速度比：{r:.1f}x（1s 跳動 {t} 次）",
        f"📐 3s 位移：{move:.0f} ticks",
        f"🕐 {clock} CST",
    ])


class PulseEngine:
    """Rolling tick buffer + velocity classifier + Telegram edge-notifier."""

    def __init__(
        self,
        symbol: str,
        telegram=None,
        *,
        telegram_level: int = 4,
        telegram_cooldown: float = 60.0,
        telegram_enabled: bool = True,
        clock=time.monotonic,
    ) -> None:
        self._symbol = symbol
        self._telegram = telegram
        self._tg_level = telegram_level
        self._tg_cooldown = telegram_cooldown
        self.telegram_enabled = telegram_enabled  # runtime toggle (web /api/pulse/telegram)
        self._clock = clock
        self._buf: deque[tuple[float, float]] = deque()
        self._prev_level = 0
        self._last_tg: float | None = None

    def on_event(self, snapshot) -> None:
        """Ingest one tick (no-op on error events). Cheap; called per tick."""
        if snapshot is None:
            return
        try:
            price = float(snapshot.price)
        except (TypeError, ValueError):
            return
        now = self._clock()
        self._buf.append((now, price))
        self._prune(now)
        level, state, m = metrics.classify(self._buf, now)
        if level != self._prev_level:
            # Audit trail: INFO for changes crossing a sounding level (>=2),
            # DEBUG for the quiet 0<->1 flapping. Lets you confirm every real
            # beep / Telegram against the metrics that produced it.
            lvl = logging.INFO if max(level, self._prev_level) >= 2 else logging.DEBUG
            log.log(lvl, "pulse %s level %d→%d (%s) %s price=%.0f",
                    self._symbol, self._prev_level, level, state, _metrics_str(m), price)
        self._maybe_notify(level, state, snapshot, price, m, now)
        self._prev_level = level

    def current(self, now: float | None = None) -> tuple[int, str, dict]:
        """(level, state_code, metrics) as of `now` — prunes stale ticks first so a
        quiet market correctly decays to Silent even without new events. Used by the
        simulator to read the metrics behind a classification."""
        now = self._clock() if now is None else now
        self._prune(now)
        return metrics.classify(self._buf, now)

    def level_at(self, now: float | None = None) -> tuple[int, str]:
        """(level, state_code) as of `now` — the quote SSE stamps this."""
        level, state, _ = self.current(now)
        return level, state

    def _prune(self, now: float) -> None:
        cutoff = now - metrics.BASELINE_WINDOW
        b = self._buf
        while b and b[0][0] < cutoff:
            b.popleft()

    def _maybe_notify(self, level, state, snapshot, price, m, now) -> None:
        if self._telegram is None or not self.telegram_enabled:
            return
        entered = level >= self._tg_level and self._prev_level < self._tg_level
        if not entered:
            return
        if self._last_tg is not None and (now - self._last_tg) < self._tg_cooldown:
            return
        self._last_tg = now
        when = getattr(snapshot, "fetched_at", None)
        text = format_pulse_message(self._symbol, state, price, m, when)
        log.info("pulse %s TELEGRAM ▶ entered L%d (%s) %s price=%.0f",
                 self._symbol, level, state, _metrics_str(m), price)
        self._dispatch(text)

    def _dispatch(self, text: str) -> None:
        """Fire-and-forget the Telegram send so a 10s HTTP timeout never stalls the
        tick loop. The notifier already swallows its own errors."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return  # no event loop (shouldn't happen in production)
        loop.create_task(self._telegram.send_text(text))


async def run_pulse_engine(poller, engine: PulseEngine) -> None:
    """Lifespan task: feed every published tick into the engine (no throttle —
    we must count each tick). Errors never kill the engine."""
    queue = poller.subscribe()
    try:
        while True:
            event = await queue.get()
            try:
                engine.on_event(event.snapshot)
            except Exception:
                log.exception("pulse: on_event failed")
    finally:
        poller.unsubscribe(queue)
