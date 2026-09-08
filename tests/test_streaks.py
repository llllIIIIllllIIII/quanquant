"""010：最長連續虧損筆數——純函式邊界測試（⑨ 空/全贏/交錯）。"""
from decimal import Decimal as D

from quanquant.stats.streaks import max_losing_streak, max_losing_streak_for_trades


def test_empty_list_is_zero():
    assert max_losing_streak([]) == 0


def test_all_wins_is_zero():
    assert max_losing_streak([D(100), D(200), D(50)]) == 0


def test_all_losses_counts_all():
    assert max_losing_streak([D(-100), D(-50), D(-1)]) == 3


def test_interleaved_finds_longest_run():
    # win, loss, loss, loss, win, loss -> longest run = 3
    pnls = [D(100), D(-10), D(-20), D(-30), D(50), D(-5)]
    assert max_losing_streak(pnls) == 3


def test_breakeven_zero_breaks_the_streak():
    pnls = [D(-10), D(-20), D(0), D(-5)]
    assert max_losing_streak(pnls) == 2


def test_none_pnl_breaks_the_streak():
    """未平倉/尚無損益 (None) 不算虧損，也中止連續。"""
    pnls = [D(-10), D(-20), None, D(-5)]
    assert max_losing_streak(pnls) == 2


def test_losing_streak_at_the_end():
    pnls = [D(100), D(-10), D(-20)]
    assert max_losing_streak(pnls) == 2


class _T:
    def __init__(self, pnl):
        self.pnl = pnl


def test_for_trades_wrapper_reads_pnl_attribute():
    trades = [_T(D(-1)), _T(D(-2)), _T(D(5))]
    assert max_losing_streak_for_trades(trades) == 2
