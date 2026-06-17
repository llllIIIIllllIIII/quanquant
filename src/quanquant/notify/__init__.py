"""Notify facade: fan a Notification out to all channels."""
from quanquant.notify.base import Notification, Notifier
from quanquant.notify.browser import BrowserNotifier
from quanquant.notify.telegram import TelegramNotifier

__all__ = ["Notification", "Notifier", "BrowserNotifier", "TelegramNotifier", "Notify", "build_notify"]


class Notify:
    def __init__(self, browser: BrowserNotifier, telegram: TelegramNotifier) -> None:
        self.browser = browser
        self._telegram = telegram

    async def send(self, n: Notification) -> None:
        await self.browser.send(n)   # always (SSE toast for open dashboards)
        await self._telegram.send(n)  # only if configured


def build_notify(settings) -> Notify:
    return Notify(
        BrowserNotifier(),
        TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id),
    )
