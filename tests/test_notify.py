import json

import pytest
import respx
from httpx import Response

from quanquant.notify.base import Notification
from quanquant.notify.browser import BrowserNotifier
from quanquant.notify.telegram import TelegramNotifier


def _n() -> Notification:
    return Notification(
        symbol="TXF", timeframe="1m", tf_label="1分",
        condition="收盤 ≥ 18000", left_value=18012.0, right_value=None,
        right_is_indicator=False, alert_id=1, bar_ts=123,
        body="TXF 1m 收盤 ≥ 18000（18012）",
    )


@pytest.mark.asyncio
async def test_browser_notifier_pubsub():
    b = BrowserNotifier()
    q = b.subscribe()
    await b.send(_n())
    payload = json.loads(q.get_nowait())
    assert payload["symbol"] == "TXF" and payload["alertId"] == 1
    b.unsubscribe(q)


@pytest.mark.asyncio
@respx.mock
async def test_telegram_noop_when_unconfigured():
    route = respx.post(url__regex=r"api\.telegram\.org/.*").mock(return_value=Response(200))
    await TelegramNotifier("", "").send(_n())          # no token
    await TelegramNotifier("TOK", "").send(_n())       # no chat_id
    assert not route.called


@pytest.mark.asyncio
@respx.mock
async def test_telegram_sends_when_configured():
    route = respx.post(url__regex=r"api\.telegram\.org/bot.+/sendMessage").mock(
        return_value=Response(200, json={"ok": True})
    )
    await TelegramNotifier("TOK", "CHAT").send(_n())
    assert route.called
    body = json.loads(route.calls.last.request.content)
    assert body["chat_id"] == "CHAT"
    assert "TXF" in body["text"]
