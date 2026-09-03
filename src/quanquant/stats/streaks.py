"""最長連續虧損筆數——純函式，不動 `stats/metrics.py`（010 明訂不動該檔）。

輸入是按時間排序（ascending）後的 pnl 序列；`None`（未平倉/尚無損益）與 0（打平）
一律中止連續，只有嚴格 < 0 才算虧損。
"""


def max_losing_streak(pnls: list) -> int:
    """由已排序（依平倉時間 ascending）的 pnl 序列算最長連續虧損筆數。"""
    best = current = 0
    for pnl in pnls:
        if pnl is not None and pnl < 0:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def max_losing_streak_for_trades(trades_sorted_by_exit) -> int:
    """便利包裝：直接吃已依平倉時間排序的 Trade-like 物件列表。"""
    return max_losing_streak([t.pnl for t in trades_sorted_by_exit])
