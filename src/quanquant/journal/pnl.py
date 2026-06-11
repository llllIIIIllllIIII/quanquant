"""Profit & loss math — pure functions, no DB or framework dependency.

P&L = direction-adjusted point move * size * point_value - fee.
For TXF (大台) point_value is NT$200/point; MTX (小台) is NT$50.
"""
from decimal import Decimal


def compute_pnl(
    direction: str,
    entry_price: Decimal,
    exit_price: Decimal | None,
    size: int,
    point_value: Decimal,
    fee: Decimal | None = None,
) -> Decimal | None:
    """Realized P&L of a closed trade, or None if the position is still open."""
    if exit_price is None:
        return None
    move = (exit_price - entry_price) if direction == "long" else (entry_price - exit_price)
    return move * Decimal(size) * point_value - (fee or Decimal(0))


def unrealized_pnl(
    direction: str,
    entry_price: Decimal,
    mark_price: Decimal,
    size: int,
    point_value: Decimal,
    fee: Decimal | None = None,
) -> Decimal:
    """Mark-to-market P&L for an open position against the current price."""
    result = compute_pnl(direction, entry_price, mark_price, size, point_value, fee)
    assert result is not None  # mark_price is never None
    return result
