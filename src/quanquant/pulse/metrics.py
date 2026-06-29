"""Pure price-velocity math for Market Pulse v0.1.

Input is a time-ordered buffer of ``(ts, price)`` samples (ts in seconds from a
monotonic clock, price as float). Every function is pure and side-effect free so
the thresholds are trivially unit-testable. Tunable thresholds are the constants
below — first values come straight from the spec (Market Pulse v1.docx); adjust
here after live tuning (no DB, no config bloat).

Trigger semantics: velocity-gated. The velocity ratio R is the necessary driver
for any sound; the 3s move M is a confirmation floor that keeps in-place
flickering (high tick count, ~zero displacement) silent. See `classify`.

TXF tick size = 1 index point, so a price move in points equals a move in ticks.
"""
from collections.abc import Sequence

Buffer = Sequence[tuple[float, float]]

# --- Tunable thresholds (revised parameter table; edit here to retune) -------
TICK_SIZE = 1.0          # TXF: 1 tick = 1 index point

# Silence / anti-false gates.
MIN_TICKS = 3            # T < this → silent (kills single/double-tick noise)
MIN_TICKS_SOUND = 5      # T < this → capped at Watch (idle market, one trade spiking R)

# Velocity-ratio thresholds per level (R = now-speed vs 30s baseline speed).
R_L1 = 1.5
R_L2 = 3.0               # raised from 2.5 to filter common small wobble
R_L3 = 4.5
R_L4 = 6.0

# Move thresholds per level (in ticks). L2 needs EITHER (R≥R_L2 and M≥M_L2)
# OR a pure real move M≥M_L2_PURE — so a high ratio with a tiny move stays quiet.
M_L2 = 2.0               # move floor when ratio-driven
M_L2_PURE = 4.0          # pure-move trigger (ratio-independent)
M_L3 = 6.0
M_L4 = 8.0

WINDOW_TICK = 1.0        # tick_count window (seconds)
WINDOW_MOVE = 3.0        # price-move / continuity window (seconds)
BASELINE_WINDOW = 30.0   # rolling baseline window (seconds)
BASELINE_FLOOR = 1.0     # treat baseline as ≥1 change/s (avoid div-zero & ratio blow-up)
BASELINE_WARMUP = 5.0    # need this many seconds of history before allowing L2+
CONTINUITY_RATIO = 0.6   # |net move| / |total move| ≥ this ⇒ directional (informational)

STATE_NAMES = {0: "silent", 1: "watch", 2: "active", 3: "fast", 4: "extreme"}
STATE_LABELS = {0: "無行情", 1: "輕微活躍", 2: "開始變快", 3: "明顯快速", 4: "急速行情"}


def _change_count(buf: Buffer, now: float, window: float) -> int:
    """Number of price *changes* (price != previous sample) whose timestamp falls
    in the last `window` seconds. The comparison reaches one sample before the
    window so a change exactly at the boundary is still detected."""
    cutoff = now - window
    count = 0
    prev: float | None = None
    for ts, price in buf:
        if prev is not None and price != prev and ts > cutoff:
            count += 1
        prev = price
    return count


def tick_count_1s(buf: Buffer, now: float) -> int:
    """T — price changes in the last 1 second."""
    return _change_count(buf, now, WINDOW_TICK)


def price_move_3s(buf: Buffer, now: float) -> float:
    """M — high-low price range over the last 3 seconds, in ticks."""
    cutoff = now - WINDOW_MOVE
    prices = [p for ts, p in buf if ts > cutoff]
    if len(prices) < 2:
        return 0.0
    return (max(prices) - min(prices)) / TICK_SIZE


def velocity_ratio(buf: Buffer, now: float) -> float:
    """R — current 1s change rate ÷ average per-second change rate over 30s."""
    t = tick_count_1s(buf, now)
    base_per_sec = _change_count(buf, now, BASELINE_WINDOW) / BASELINE_WINDOW
    return t / max(base_per_sec, BASELINE_FLOOR)


def directional_continuity_3s(buf: Buffer, now: float) -> bool:
    """D — whether the 3s net displacement dominates the total path (one-way push
    vs in-place oscillation). v0.1: computed/logged only, not a level gate."""
    cutoff = now - WINDOW_MOVE
    win = [p for ts, p in buf if ts > cutoff]
    if len(win) < 2:
        return False
    net = abs(win[-1] - win[0])
    total = sum(abs(win[i] - win[i - 1]) for i in range(1, len(win)))
    if total == 0:
        return False
    return (net / total) >= CONTINUITY_RATIO


def _warmed(buf: Buffer, now: float) -> bool:
    """True once we hold ≥ BASELINE_WARMUP seconds of history — guards against a
    falsely-high ratio right after open when the baseline is still empty."""
    return bool(buf) and (now - buf[0][0]) >= BASELINE_WARMUP


def classify(buf: Buffer, now: float) -> tuple[int, str, dict]:
    """Velocity-gated descending classification → (level, state_code, metrics).

        T < MIN_TICKS (3)                       → 0 Silent  (single/double-tick noise)
        T < MIN_TICKS_SOUND (5)                 → ≤1 Watch  (idle market false R spike)
        R≥R_L4 and M≥M_L4                       → 4 Extreme
        R≥R_L3 and M≥M_L3                       → 3 Fast
        (R≥R_L2 and M≥M_L2) or M≥M_L2_PURE      → 2 Active
        R≥R_L1                                  → 1 Watch
        else                                    → 0 Silent

    Levels 2+ require ≥MIN_TICKS_SOUND ticks AND a warmed baseline (else ≤Watch).
    """
    t = tick_count_1s(buf, now)
    m = price_move_3s(buf, now)
    r = velocity_ratio(buf, now)
    d = directional_continuity_3s(buf, now)
    metrics = {
        "tick_count_1s": t,
        "price_move_3s": m,
        "velocity_ratio": r,
        "directional_continuity_3s": d,
    }

    if t < MIN_TICKS:
        return 0, STATE_NAMES[0], metrics

    # Anti-false-signal gate: too few ticks/s to sound → at most Watch.
    if t < MIN_TICKS_SOUND:
        level = 1 if r >= R_L1 else 0
        return level, STATE_NAMES[level], metrics

    warm = _warmed(buf, now)
    if warm and r >= R_L4 and m >= M_L4:
        level = 4
    elif warm and r >= R_L3 and m >= M_L3:
        level = 3
    elif warm and ((r >= R_L2 and m >= M_L2) or m >= M_L2_PURE):
        level = 2
    elif r >= R_L1:
        level = 1
    else:
        level = 0
    return level, STATE_NAMES[level], metrics
