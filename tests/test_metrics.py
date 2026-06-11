import datetime as dt
from decimal import Decimal
from types import SimpleNamespace

from quanquant.stats.metrics import compute_stats, max_drawdown, metrics_for


def mk(pnl, day=1, symbol="TXF", tags=None):
    return SimpleNamespace(
        pnl=(Decimal(pnl) if pnl is not None else None),
        symbol=symbol,
        tags=(",".join(tags) if tags else None),
        entry_time=dt.datetime(2026, 6, day, 9, 0),
        exit_time=dt.datetime(2026, 6, day, 10, 0),
    )


def test_empty_has_no_zero_division():
    m = metrics_for([])
    assert m.count == 0
    assert m.win_rate == 0.0
    assert m.total_pnl == Decimal(0)
    assert m.max_drawdown == Decimal(0)
    assert m.avg_win is None and m.avg_loss is None


def test_win_rate_and_averages():
    m = metrics_for([mk("100", 1), mk("-50", 2), mk("200", 3)])
    assert m.count == 3 and m.wins == 2 and m.losses == 1
    assert abs(m.win_rate - 2 / 3) < 1e-9
    assert m.avg_win == Decimal("150")
    assert m.avg_loss == Decimal("-50")
    assert m.max_win == Decimal("200")
    assert m.max_loss == Decimal("-50")


def test_breakeven_excluded_from_win_loss():
    m = metrics_for([mk("0", 1), mk("100", 2)])
    assert m.breakeven == 1 and m.wins == 1 and m.losses == 0
    assert m.win_rate == 0.5


def test_max_drawdown_monotonic_up_is_zero():
    assert max_drawdown([mk("100", 1), mk("50", 2), mk("30", 3)]) == Decimal(0)


def test_max_drawdown_peak_then_trough():
    # equity curve: 100, 60, 160, 110 -> drawdowns 40 then 50 -> max 50
    assert max_drawdown([mk("100", 1), mk("-40", 2), mk("100", 3), mk("-50", 4)]) == Decimal("50")


def test_metrics_sorts_by_exit_for_drawdown():
    unordered = [mk("-50", 4), mk("100", 1), mk("-40", 2), mk("100", 3)]
    assert metrics_for(unordered).max_drawdown == Decimal("50")


def test_profit_factor():
    m = metrics_for([mk("100", 1), mk("-50", 2)])
    assert abs(m.profit_factor - 2.0) < 1e-9


def test_profit_factor_none_when_no_losses():
    assert metrics_for([mk("100", 1)]).profit_factor is None


def test_compute_stats_groups_by_tag_and_symbol():
    trades = [
        mk("100", 1, "TXF", ["突破"]),
        mk("-50", 2, "MTX", ["消息面"]),
        mk("200", 3, "TXF", ["突破", "均線"]),
    ]
    st = compute_stats(trades)
    assert st.overall.total_pnl == Decimal("250")
    assert st.by_tag["突破"].count == 2
    assert set(st.by_symbol) == {"TXF", "MTX"}
    assert st.by_symbol["TXF"].total_pnl == Decimal("300")
