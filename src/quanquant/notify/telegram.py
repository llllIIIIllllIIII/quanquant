"""Telegram Bot API notifier. No-op when unconfigured; never blocks/raises.

Standard message format makes every alert state which condition fired:

    🔔 QuanQuant 警示觸發

    📊 商品：TXF
    🕐 週期：5分
    🎯 條件：收盤 向上突破 MA(20)
    💲 結算值：18,012
    📐 對象值：18,000        (only when the right side is an indicator)
    🗓 K棒：2026-06-16 10:35（收盤結算）
"""
import logging
from datetime import datetime, timedelta, timezone

import httpx

from quanquant.notify.base import Notification

log = logging.getLogger(__name__)
_CST = timezone(timedelta(hours=8))


def _fmt(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{round(v):,}" if abs(v - round(v)) < 1e-6 else f"{v:,.2f}"


def format_alert(n: Notification) -> str:
    bar_time = datetime.fromtimestamp(n.bar_ts / 1000, tz=_CST).strftime("%Y-%m-%d %H:%M")
    lines = [
        "🔔 QuanQuant 警示觸發",
        "",
    ]
    if n.owner_name:
        lines.append(f"👤 擁有者：{n.owner_name}")
    lines += [
        f"📊 商品：{n.symbol}",
        f"🕐 週期：{n.tf_label}",
        f"🎯 條件：{n.condition}",
        f"💲 結算值：{_fmt(n.left_value)}",
    ]
    if n.right_is_indicator:
        lines.append(f"📐 對象值：{_fmt(n.right_value)}")
    lines.append(f"🗓 K棒：{bar_time}（收盤結算）")
    return "\n".join(lines)


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str) -> None:
        self._token = token
        self._chat_id = chat_id

    @property
    def configured(self) -> bool:
        return bool(self._token and self._chat_id)

    async def send(self, n: Notification) -> None:
        await self.send_text(format_alert(n))

    async def send_text(self, text: str) -> None:
        """Send a plain message. Market Pulse pushes through here directly; alerts
        go via `send` (format then delegate). No-op when unconfigured; never raises."""
        if not self.configured:
            return  # browser-only when no token/chat_id set
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                await client.post(
                    f"https://api.telegram.org/bot{self._token}/sendMessage",
                    json={"chat_id": self._chat_id, "text": text},
                )
        except Exception:
            # Telegram 中斷絕不能阻塞 alerts / poll loop，但也不再靜默——記一次 warning，
            # 否則告警投遞失敗完全不可見（Tier0 C#4，與告警機制疊加放大）。
            log.warning("Telegram 告警投遞失敗（已略過，不阻塞 alerts/poll loop）", exc_info=True)
