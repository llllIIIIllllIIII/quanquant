"""T0.3 盤中報價停滯 watchdog：僅在交易時段（session_open_fn 回 True）且 poller 報價停滯
逾 threshold 時才發 feed_stale 告警；休市（session_open_fn 回 False）即使 stale 也不發，
避免每晚對著關閉的市場狂噴告警。判定抽成 `_feed_stale_check` 單次函式，不必真跑無限 loop。"""
from quanquant.web.app import _feed_stale_check


class _FakePoller:
    def __init__(self, *, stale: bool, age: float):
        self._stale = stale
        self._age = age

    def is_stale(self, threshold: float) -> bool:
        return self._stale

    def seconds_since_snapshot(self) -> float:
        return self._age


class _RecordingAlerter:
    def __init__(self):
        self.feed_stale_calls = []

    def feed_stale(self, *, age_seconds: float) -> None:
        self.feed_stale_calls.append(age_seconds)


def test_alerts_when_session_open_and_stale():
    poller = _FakePoller(stale=True, age=123.0)
    alerter = _RecordingAlerter()
    _feed_stale_check(poller, alerter, threshold=90.0, session_open_fn=lambda: True)
    assert alerter.feed_stale_calls == [123.0]


def test_does_not_alert_when_market_closed_even_if_stale():
    """驗收重點：休市（session_open_fn 回 False）即使 stale 也不得發告警。"""
    poller = _FakePoller(stale=True, age=9999.0)
    alerter = _RecordingAlerter()
    _feed_stale_check(poller, alerter, threshold=90.0, session_open_fn=lambda: False)
    assert alerter.feed_stale_calls == []


def test_does_not_alert_when_session_open_but_not_stale():
    poller = _FakePoller(stale=False, age=5.0)
    alerter = _RecordingAlerter()
    _feed_stale_check(poller, alerter, threshold=90.0, session_open_fn=lambda: True)
    assert alerter.feed_stale_calls == []


def test_noop_when_poller_or_alerter_missing():
    alerter = _RecordingAlerter()
    _feed_stale_check(None, alerter, threshold=90.0, session_open_fn=lambda: True)
    assert alerter.feed_stale_calls == []
    poller = _FakePoller(stale=True, age=1.0)
    _feed_stale_check(poller, None, threshold=90.0, session_open_fn=lambda: True)  # 不得 raise
