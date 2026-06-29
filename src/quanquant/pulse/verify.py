"""Market Pulse trigger verifier — a deterministic scenario simulator.

Runs a battery of named price scenarios through the REAL PulseEngine (injected
clock + recording Telegram) and prints, for each, the metrics (T/M/R/D), the
classified Velocity Level, whether audio would sound, and whether Telegram would
fire — with a PASS/FAIL against the expected outcome.

This lets you confirm the heart-beat / Telegram notification fires under the
correct conditions WITHOUT waiting for a real market burst. The thresholds it
checks against are the live constants in `metrics.py`, so it also catches a
mis-tuned threshold.

    uv run quanquant-pulse-verify        # exit 0 = all scenarios behaved as expected
"""
import asyncio
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Callable

from quanquant.pulse import metrics as M
from quanquant.pulse.engine import PulseEngine

_TG_LEVEL = 4
_TG_COOLDOWN = 60.0


class _Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


class _RecordingTelegram:
    def __init__(self):
        self.calls = []

    async def send_text(self, text):
        self.calls.append(text)


def _snap(price):
    return SimpleNamespace(price=price, fetched_at=datetime(2026, 1, 1, 4, 0, tzinfo=timezone.utc))


# --- scenario feed builders (ts in seconds from the injected clock) ----------
def _baseline(eng, clock, start, secs=26, base=100):
    """~1 price change/s of ±1 — a quiet, warmed baseline."""
    for i in range(secs):
        clock.t = start + i
        eng.on_event(_snap(base + (i % 2)))


def _burst(eng, clock, start, n, base=110):
    """n rising ticks over ~0.1s spacing → +(n-1) ticks of move in ~1s."""
    for k in range(n):
        clock.t = start + 0.1 * k
        eng.on_event(_snap(base + k))


def _episode(eng, clock, start, n):
    _baseline(eng, clock, start)
    _burst(eng, clock, start + 29, n)


def _quiet(eng, clock):
    _baseline(eng, clock, 0)


def _flicker(eng, clock):
    _baseline(eng, clock, 0)
    for k in range(9):  # fast but in-place: 100<->101, big tick count, ~0 move
        clock.t = 29 + 0.1 * k
        eng.on_event(_snap(100 + (k % 2)))


def _single_spike(eng, clock):
    _baseline(eng, clock, 0)
    clock.t = 29.0
    eng.on_event(_snap(130))  # one lone big jump


def _slow_drift(eng, clock):
    _baseline(eng, clock, 0)
    for k in range(6):  # +1/s — large total move, low tick frequency
        clock.t = 29 + k
        eng.on_event(_snap(102 + k))


def _idle_spike(eng, clock):
    _baseline(eng, clock, 0)
    for k in range(4):  # only 4 ticks (< 5) → anti-false gate caps at Watch
        clock.t = 29 + 0.1 * k
        eng.on_event(_snap(110 + 3 * k))


@dataclass
class Scenario:
    name: str
    feed: Callable
    level: int    # expected peak level
    exact: bool   # True: peak == level; False: peak <= level
    tg: int       # expected Telegram send count


SCENARIOS = [
    Scenario("安靜無行情", _quiet, 0, False, 0),
    Scenario("原地高頻震盪", _flicker, 1, False, 0),
    Scenario("單筆暴衝(雜訊)", _single_spike, 1, False, 0),
    Scenario("慢慢走一段(低頻)", _slow_drift, 1, False, 0),
    Scenario("靜止盤微量(T<5)", _idle_spike, 1, False, 0),
    Scenario("開始變快 Active", lambda e, c: _episode(e, c, 0, 5), 2, True, 0),
    Scenario("明顯快速 Fast", lambda e, c: _episode(e, c, 0, 7), 3, True, 0),
    Scenario("持續急速 Extreme", lambda e, c: _episode(e, c, 0, 9), 4, True, 1),
    Scenario("急→落→60s內再急", lambda e, c: (_episode(e, c, 0, 9), _episode(e, c, 40, 9)), 4, True, 1),
    Scenario("急→落→60s後再急", lambda e, c: (_episode(e, c, 0, 9), _episode(e, c, 80, 9)), 4, True, 2),
]


def _w(s):
    return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in s)


def _pad(s, width):
    return s + " " * max(0, width - _w(s))


async def _run() -> int:
    print("Market Pulse 觸發驗證（情境模擬器）\n")
    print(
        f"門檻：T<{M.MIN_TICKS}靜音、T<{M.MIN_TICKS_SOUND}最多L1｜"
        f"L2 (R≥{M.R_L2}&M≥{M.M_L2:.0f})或M≥{M.M_L2_PURE:.0f}｜L3 R≥{M.R_L3}&M≥{M.M_L3:.0f}｜"
        f"L4 R≥{M.R_L4}&M≥{M.M_L4:.0f}｜Telegram=進入L{_TG_LEVEL}+{_TG_COOLDOWN:.0f}s冷卻\n"
    )
    head = (
        _pad("情境", 20) + _pad("T", 4) + _pad("M", 6) + _pad("R", 6) + _pad("D", 7)
        + _pad("等級", 12) + _pad("音效", 6) + _pad("TG", 5) + _pad("預期", 10) + "結果"
    )
    print(head)
    print("-" * 84)

    all_ok = True
    for sc in SCENARIOS:
        clock, tg = _Clock(), _RecordingTelegram()
        eng = PulseEngine("TXF", telegram=tg, telegram_level=_TG_LEVEL,
                          telegram_cooldown=_TG_COOLDOWN, clock=clock)
        sc.feed(eng, clock)
        await asyncio.sleep(0)  # flush fire-and-forget Telegram tasks
        level, state, m = eng.current()
        tg_n = len(tg.calls)

        ok = (level == sc.level if sc.exact else level <= sc.level) and tg_n == sc.tg
        all_ok = all_ok and ok
        expect = f"{'=' if sc.exact else '≤'}{sc.level} TG{sc.tg}"
        row = (
            _pad(sc.name, 20)
            + _pad(str(m["tick_count_1s"]), 4)
            + _pad(f"{m['price_move_3s']:.0f}", 6)
            + _pad(f"{m['velocity_ratio']:.1f}", 6)
            + _pad("yes" if m["directional_continuity_3s"] else "no", 7)
            + _pad(f"{level} {state}", 12)
            + _pad("響" if level >= 2 else "—", 6)
            + _pad(f"✓{tg_n}" if tg_n else "—", 5)
            + _pad(expect, 10)
            + ("PASS" if ok else "FAIL ✗")
        )
        print(row)

    print("-" * 84)
    print(("全部 PASS ✓" if all_ok else "有情境 FAIL ✗ — 觸發條件與預期不符") + f"（{len(SCENARIOS)} 情境）")
    return 0 if all_ok else 1


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
