"""Server-side technical indicators for alert evaluation.

Pure functions over float price series, matching docs/requirements.md §5.1 and
KLineCharts' built-in MA/WR/BIAS so alerts agree with the chart the user sees.
Each returns the value at the LAST bar, or None on insufficient data.
"""
from collections.abc import Sequence

Bar = dict  # {"timestamp", "open", "high", "low", "close", "volume"}


def sma(closes: Sequence[float], n: int) -> float | None:
    """SMA(n) = Σ(close, n) / n."""
    if n <= 0 or len(closes) < n:
        return None
    return sum(closes[-n:]) / n


def wr(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], n: int) -> float | None:
    """Williams %R(n) = (HHV − C) / (HHV − LLV) × −100, range −100…0."""
    if n <= 0 or len(closes) < n:
        return None
    hhv = max(highs[-n:])
    llv = min(lows[-n:])
    if hhv == llv:  # flat range — avoid div-by-zero (KLineCharts yields 0)
        return 0.0
    return (hhv - closes[-1]) / (hhv - llv) * -100.0


def bias(closes: Sequence[float], n: int) -> float | None:
    """BIAS(n) = (C − SMA(n)) / SMA(n) × 100%."""
    m = sma(closes, n)
    if m is None or m == 0:
        return None
    return (closes[-1] - m) / m * 100.0


def value_at(name: str, period: int, bars: Sequence[Bar]) -> float | None:
    """Indicator value at the last bar. name ∈ {"ma","wr","bias"}."""
    closes = [b["close"] for b in bars]
    if name == "ma":
        return sma(closes, period)
    if name == "bias":
        return bias(closes, period)
    if name == "wr":
        return wr([b["high"] for b in bars], [b["low"] for b in bars], closes, period)
    return None
