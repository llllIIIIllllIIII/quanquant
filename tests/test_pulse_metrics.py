"""Pure metric / classification tests for Market Pulse v0.1."""
from quanquant.pulse import metrics
from quanquant.pulse.metrics import (
    classify,
    directional_continuity_3s,
    price_move_3s,
    tick_count_1s,
    velocity_ratio,
)


def _slow_baseline(seconds, start=0.0, base=100.0):
    """One small ±1 price change per second (≈1 change/s baseline)."""
    return [(start + i, base + (i % 2)) for i in range(seconds)]


def test_tick_count_counts_changes_in_last_second():
    buf = [(i / 10, 100 + (i % 2)) for i in range(10)]  # ts 0.0..0.9, alternating
    assert tick_count_1s(buf, 0.9) == 9


def test_tick_count_ignores_repeats_and_old_ticks():
    buf = [(0.0, 100), (0.2, 100), (0.4, 101), (5.0, 101), (5.2, 102)]
    assert tick_count_1s(buf, 5.3) == 1  # only the 101->102 change is in the last 1s


def test_price_move_is_high_low_range_in_ticks():
    buf = [(0.0, 100), (1.0, 103), (2.0, 99), (2.9, 101)]
    assert price_move_3s(buf, 3.0) == 4.0  # 103 - 99


def test_velocity_ratio_floored_baseline_no_div_zero():
    buf = [(0.0, 100), (0.5, 101)]
    assert velocity_ratio(buf, 1.0) == 1 / metrics.BASELINE_FLOOR


def test_directional_continuity_true_for_one_way_push():
    buf = [(0.0, 100), (1.0, 102), (2.0, 104), (2.9, 106)]
    assert directional_continuity_3s(buf, 3.0) is True


def test_directional_continuity_false_for_in_place_oscillation():
    buf = [(0.0, 100), (1.0, 102), (2.0, 100), (2.9, 102)]
    assert directional_continuity_3s(buf, 3.0) is False


def test_gate_silent_when_below_activity():
    buf = [(0.0, 100), (0.5, 101)]
    assert classify(buf, 1.0)[:2] == (0, "silent")


def test_single_tick_noise_stays_silent():
    buf = _slow_baseline(20) + [(20.5, 130)]  # one lone large jump
    assert classify(buf, 20.6)[0] <= 1  # velocity-gated: 1 tick never sounds


def test_sustained_burst_is_extreme():
    buf = _slow_baseline(26)  # 26s warmed baseline (~1 change/s)
    for k in range(9):  # 9-tick rising burst over the last ~0.9s, +8 ticks move
        buf.append((29.0 + 0.1 * k, 110 + k))
    level, state, m = classify(buf, 30.0)
    assert (level, state) == (4, "extreme")
    assert m["price_move_3s"] >= metrics.M_L4
    assert m["velocity_ratio"] >= metrics.R_L4


def test_warmup_caps_level_when_history_too_short():
    buf = [(0.1 * k, 110 + k) for k in range(9)]  # same burst, <1s of history
    assert classify(buf, 0.9)[0] <= 1


def test_few_ticks_capped_at_watch():
    buf = _slow_baseline(26)
    for k in range(4):  # only 4 ticks (< MIN_TICKS_SOUND) but a big move
        buf.append((29.1 + 0.2 * k, 110 + 3 * k))
    level, _, m = classify(buf, 30.0)
    assert m["tick_count_1s"] == 4
    assert level <= 1  # anti-false gate caps at Watch despite the move


def test_high_ratio_tiny_move_stays_watch():
    buf = _slow_baseline(26)
    for k in range(7):  # fast in-place flicker: many ticks, ~no displacement
        buf.append((29.0 + 0.1 * k, 100 + (k % 2)))
    level, _, m = classify(buf, 30.0)
    assert m["tick_count_1s"] >= 5
    assert level == 1  # R high, but M below both L2 move thresholds → no sound


def test_pure_move_triggers_active_even_when_ratio_low():
    buf = [(i / 3.0, 100 + (i % 2)) for i in range(90)]  # dense ~3 changes/s baseline
    for k in range(5):  # real move (M ≥ M_L2_PURE) but a low ratio
        buf.append((29.7 + 0.02 * k, 104 + k))
    level, _, m = classify(buf, 30.0)
    assert m["velocity_ratio"] < metrics.R_L2
    assert level == 2  # pure-move path still sounds Active
