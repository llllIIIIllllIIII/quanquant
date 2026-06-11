"""Performance statistics — pure functions over a list of closed trades.

A "trade" here is any object exposing `.pnl` (Decimal | None), `.tags` (comma str
or None), `.symbol`, `.exit_time`, `.entry_time`. Filtering (symbol/tag/date) is
done in the repository before these run, so this module stays a pure function of
a trade list and is trivially unit-testable.
"""
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal

from quanquant.journal.schemas import split_tags

_ZERO = Decimal(0)


@dataclass(frozen=True, slots=True)
class Metrics:
    count: int
    total_pnl: Decimal       # 總損益
    wins: int
    losses: int
    breakeven: int
    win_rate: float          # 勝率 (0..1)
    avg_win: Decimal | None  # 平均獲利
    avg_loss: Decimal | None # 平均虧損 (負值)
    max_win: Decimal         # 最大單筆獲利
    max_loss: Decimal        # 最大單筆虧損 (負值或 0)
    profit_factor: float | None
    max_drawdown: Decimal    # 最大回撤 (正值, NT$)


@dataclass(frozen=True, slots=True)
class StatsResult:
    overall: Metrics
    by_tag: dict[str, Metrics]
    by_symbol: dict[str, Metrics]


def max_drawdown(trades_sorted_by_exit: list) -> Decimal:
    """Largest peak-to-trough drop of cumulative realized equity (starting at 0)."""
    equity = peak = max_dd = _ZERO
    for t in trades_sorted_by_exit:
        if t.pnl is None:
            continue
        equity += t.pnl
        if equity > peak:
            peak = equity
        drop = peak - equity
        if drop > max_dd:
            max_dd = drop
    return max_dd


def metrics_for(trades: list) -> Metrics:
    ordered = sorted(trades, key=lambda t: (t.exit_time or t.entry_time))
    pnls = [t.pnl for t in ordered if t.pnl is not None]
    count = len(pnls)

    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    breakeven = count - len(wins) - len(losses)

    total = sum(pnls, _ZERO)
    sum_win = sum(wins, _ZERO)
    sum_loss = sum(losses, _ZERO)

    return Metrics(
        count=count,
        total_pnl=total,
        wins=len(wins),
        losses=len(losses),
        breakeven=breakeven,
        win_rate=(len(wins) / count) if count else 0.0,
        avg_win=(sum_win / len(wins)) if wins else None,
        avg_loss=(sum_loss / len(losses)) if losses else None,
        max_win=max(pnls, default=_ZERO) if pnls else _ZERO,
        max_loss=min(pnls, default=_ZERO) if pnls else _ZERO,
        profit_factor=(float(sum_win / -sum_loss)) if sum_loss < 0 else None,
        max_drawdown=max_drawdown(ordered),
    )


def compute_stats(trades: list) -> StatsResult:
    by_tag_groups: dict[str, list] = defaultdict(list)
    by_symbol_groups: dict[str, list] = defaultdict(list)

    for t in trades:
        for tag in split_tags(t.tags):
            by_tag_groups[tag].append(t)
        by_symbol_groups[t.symbol].append(t)

    return StatsResult(
        overall=metrics_for(trades),
        by_tag={k: metrics_for(v) for k, v in sorted(by_tag_groups.items())},
        by_symbol={k: metrics_for(v) for k, v in sorted(by_symbol_groups.items())},
    )
