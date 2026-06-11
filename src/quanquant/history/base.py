"""Abstract interface for historical candle providers.

A future Shioaji minute-level provider is a new subclass with timeframe="1m" —
the candles pipeline needs no changes (HistoricalCandle rows map 1:1 to Candle).
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import ClassVar


@dataclass(frozen=True, slots=True)
class HistoricalCandle:
    ts: int                  # bucket start, epoch ms UTC (1d: trading date 08:45 CST)
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    trading_date: str | None  # "YYYY-MM-DD" CST (None for night-session 1m bars)
    session: str              # "day" | "night"
    source: str               # provider name, e.g. "finmind" / "finmind_tick"


class HistoryProvider(ABC):
    """Fetches historical candles for one timeframe from an external service."""

    timeframe: ClassVar[str]  # "1d" (FinMind) | "1m" (future Shioaji)

    @abstractmethod
    async def fetch_candles(self, symbol: str, start: date, end: date) -> list[HistoricalCandle]:
        """Return candles for [start, end] ascending by ts."""

    @abstractmethod
    async def close(self) -> None:
        """Release any persistent connections."""

    async def __aenter__(self) -> "HistoryProvider":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()
