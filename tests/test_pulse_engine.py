"""PulseEngine streaming + Telegram edge-notify tests (injected clock + fake TG)."""
import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from quanquant.pulse.engine import PulseEngine, format_pulse_message


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


class FakeTelegram:
    def __init__(self):
        self.calls = []

    async def send_text(self, text):
        self.calls.append(text)


def _snap(price):
    return SimpleNamespace(price=price, fetched_at=datetime(2026, 1, 1, 4, 0, tzinfo=timezone.utc))


async def _feed_burst(eng, clock, start, n_burst):
    """26s slow baseline then an n_burst-tick rising run in the last ~1s."""
    for i in range(26):
        clock.t = start + i
        eng.on_event(_snap(100 + (i % 2)))
    for k in range(n_burst):
        clock.t = start + 29 + 0.1 * k
        eng.on_event(_snap(110 + k))
    await asyncio.sleep(0)  # let fire-and-forget create_task run


async def test_entering_extreme_pushes_telegram_once():
    clock, tg = Clock(), FakeTelegram()
    eng = PulseEngine("TXF", telegram=tg, telegram_level=4, telegram_cooldown=60, clock=clock)
    await _feed_burst(eng, clock, 0, n_burst=9)
    assert len(tg.calls) == 1
    assert eng.level_at()[0] == 4


async def test_below_threshold_does_not_push():
    clock, tg = Clock(), FakeTelegram()
    eng = PulseEngine("TXF", telegram=tg, telegram_level=4, telegram_cooldown=60, clock=clock)
    await _feed_burst(eng, clock, 0, n_burst=6)  # tops out at Fast (move < 8 ticks)
    assert tg.calls == []
    assert eng.level_at()[0] == 3


async def test_cooldown_blocks_re_entry_then_allows():
    clock, tg = Clock(), FakeTelegram()
    eng = PulseEngine("TXF", telegram=tg, telegram_level=2, telegram_cooldown=60, clock=clock)
    await _feed_burst(eng, clock, 0, n_burst=9)
    assert len(tg.calls) == 1
    await _feed_burst(eng, clock, 40, n_burst=9)   # +40s — inside cooldown
    assert len(tg.calls) == 1
    await _feed_burst(eng, clock, 80, n_burst=9)   # +80s from first — cooldown elapsed
    assert len(tg.calls) == 2


async def test_no_telegram_configured_never_crashes():
    clock = Clock()
    eng = PulseEngine("TXF", telegram=None, clock=clock)
    await _feed_burst(eng, clock, 0, n_burst=9)  # must not raise
    assert eng.level_at()[0] == 4


def test_format_pulse_message_contains_symbol_and_state():
    msg = format_pulse_message(
        "TXF", "extreme", 23150.0,
        {"tick_count_1s": 8, "velocity_ratio": 6.3, "price_move_3s": 9.0},
        datetime(2026, 1, 1, 6, 30, 5, tzinfo=timezone.utc),
    )
    assert "TXF" in msg and "急速行情" in msg
