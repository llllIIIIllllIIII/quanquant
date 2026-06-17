"""Notification abstraction — alerts fan out to browser SSE + Telegram."""
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class Notification:
    symbol: str
    timeframe: str          # code, e.g. "5m"
    tf_label: str           # zh label, e.g. "5分"
    condition: str          # "收盤 向上突破 MA(20)" — what triggered
    left_value: float | None   # settled value of the left operand at bar close
    right_value: float | None  # right operand value (None for constants)
    right_is_indicator: bool
    alert_id: int
    bar_ts: int             # closed bar epoch ms UTC
    body: str               # compact one-line summary (browser toast / DB log)


class Notifier(Protocol):
    async def send(self, n: Notification) -> None: ...
