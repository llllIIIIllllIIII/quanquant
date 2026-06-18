"""Unit tests for the Shioaji streamer — pure conversion + selection logic.

These never log in or import the native `shioaji` client (that lives behind a
lazy import in `_connect`); they exercise tick→snapshot mapping, the simtrade
guard, and front-contract selection with fakes.
"""
from decimal import Decimal
from types import SimpleNamespace

from quanquant.sources.shioaji_stream import ShioajiStreamer


class _RecordingLoop:
    """Stand-in for the asyncio loop: runs the scheduled callback inline."""

    def __init__(self):
        self.calls = []

    def call_soon_threadsafe(self, fn, *args):
        self.calls.append(args)
        fn(*args)


class _RecordingHub:
    def __init__(self):
        self.published = []

    def publish(self, event):
        self.published.append(event)


def _streamer(hub=None, loop=None):
    return ShioajiStreamer(
        "key", "secret", "TXF", hub or _RecordingHub(), loop or _RecordingLoop()
    )


def _tick(**over):
    base = dict(
        close=18500, price_chg=12, pct_chg=0.065, total_volume=1234,
        open=18488, high=18510, low=18480, simtrade=0,
    )
    base.update(over)
    return SimpleNamespace(**base)


def test_to_event_maps_tick_fields():
    s = _streamer()
    s._contract_code = "TXF202607"
    event = s._to_event(_tick())
    snap = event.snapshot
    assert snap.price == Decimal("18500")
    assert snap.change == Decimal("12")
    assert snap.change_pct == 0.065
    assert snap.volume == 1234  # cumulative total_volume (builder diffs it)
    assert snap.high_price == Decimal("18510")
    assert snap.low_price == Decimal("18480")
    assert snap.data_date == ""  # skips the day-replay staleness net
    assert snap.contract_month == "TXF202607"


def test_on_tick_publishes_via_loop():
    hub, loop = _RecordingHub(), _RecordingLoop()
    s = _streamer(hub, loop)
    s._on_tick(_tick(close=18600))
    assert len(hub.published) == 1
    assert hub.published[0].snapshot.price == Decimal("18600")
    assert s._seconds_since_tick() < 1.0  # liveness timestamp updated


def test_on_tick_drops_simtrade():
    hub = _RecordingHub()
    s = _streamer(hub)
    s._on_tick(_tick(simtrade=1))
    assert hub.published == []  # simulated tick ignored — not a real price


def test_front_contract_picks_nearest_nonexpired_and_skips_continuous():
    contracts = [
        SimpleNamespace(code="TXF200001", delivery_date="2000/01/19"),  # expired
        SimpleNamespace(code="TXF209902", delivery_date="2099/02/18"),
        SimpleNamespace(code="TXF209901", delivery_date="2099/01/15"),  # nearest active
        SimpleNamespace(code="TXFR1", delivery_date="2099/01/15"),      # continuous → skip
        SimpleNamespace(code="TXFR2", delivery_date="2099/02/18"),      # continuous → skip
    ]
    api = SimpleNamespace(Contracts=SimpleNamespace(Futures=SimpleNamespace(TXF=contracts)))
    chosen = _streamer()._front_contract(api)
    assert chosen.code == "TXF209901"


def test_front_contract_falls_back_to_nearest_when_all_expired():
    contracts = [
        SimpleNamespace(code="TXF200001", delivery_date="2000/01/19"),
        SimpleNamespace(code="TXF200002", delivery_date="2000/02/16"),
    ]
    api = SimpleNamespace(Contracts=SimpleNamespace(Futures=SimpleNamespace(TXF=contracts)))
    chosen = _streamer()._front_contract(api)
    assert chosen.code == "TXF200001"  # min delivery_date among all when none active
