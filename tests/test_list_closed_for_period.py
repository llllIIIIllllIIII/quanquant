"""008：`list_closed_for_period`——已平倉逐筆明細查詢（trading_day 區間＋來源／結果篩選）。

④ 結果篩選／含手動補記 WHERE 正確；①（DB 層）trading_day 含夜盤跨日的成員資格。
"""
import datetime as dt
from decimal import Decimal

from quanquant.journal import repository as repo
from quanquant.journal.schemas import TradeCreate

TODAY = dt.date(2026, 9, 2)  # 週三


def _closed(session, user_id, *, exit_time, symbol="TXF", pnl_sign=1, tags=None,
            source="shioaji", mode="real", entry_time=None):
    entry_time = entry_time or (exit_time - dt.timedelta(hours=1))
    entry_price = Decimal("18000")
    exit_price = entry_price + (Decimal("100") if pnl_sign >= 0 else Decimal("-100"))
    return repo.create_trade(
        session,
        TradeCreate(
            symbol=symbol, direction="long", entry_time=entry_time, entry_price=entry_price,
            exit_time=exit_time, exit_price=exit_price, size=1, point_value=Decimal("200"),
            tags=tags or [], mode=mode, source=source,
        ),
        user_id=user_id,
    )


def test_default_excludes_manual_source(session, user):
    _closed(session, user.id, exit_time=dt.datetime(2026, 9, 2, 10, 0), source="shioaji")
    _closed(session, user.id, exit_time=dt.datetime(2026, 9, 2, 11, 0), source="manual")

    only_auto = repo.list_closed_for_period(session, user_id=user.id, mode="real",
                                             date_from=TODAY, date_to=TODAY)
    assert len(only_auto) == 1
    assert only_auto[0].source == "shioaji"

    with_manual = repo.list_closed_for_period(session, user_id=user.id, mode="real",
                                               date_from=TODAY, date_to=TODAY, include_manual=True)
    assert len(with_manual) == 2


def test_result_filter_win_and_loss(session, user):
    _closed(session, user.id, exit_time=dt.datetime(2026, 9, 2, 9, 0), pnl_sign=1)
    _closed(session, user.id, exit_time=dt.datetime(2026, 9, 2, 10, 0), pnl_sign=-1)

    wins = repo.list_closed_for_period(session, user_id=user.id, mode="real",
                                        date_from=TODAY, date_to=TODAY, result="win")
    losses = repo.list_closed_for_period(session, user_id=user.id, mode="real",
                                          date_from=TODAY, date_to=TODAY, result="loss")
    assert len(wins) == 1 and wins[0].pnl > 0
    assert len(losses) == 1 and losses[0].pnl < 0


def test_symbol_and_tag_filters(session, user):
    _closed(session, user.id, exit_time=dt.datetime(2026, 9, 2, 9, 0), symbol="TXF", tags=["突破"])
    _closed(session, user.id, exit_time=dt.datetime(2026, 9, 2, 10, 0), symbol="MTX", tags=["當沖"])

    assert len(repo.list_closed_for_period(
        session, user_id=user.id, mode="real", date_from=TODAY, date_to=TODAY, symbol="TXF"
    )) == 1
    assert len(repo.list_closed_for_period(
        session, user_id=user.id, mode="real", date_from=TODAY, date_to=TODAY, tag="當沖"
    )) == 1


def test_mode_scoping(session, user):
    _closed(session, user.id, exit_time=dt.datetime(2026, 9, 2, 9, 0), mode="real")
    _closed(session, user.id, exit_time=dt.datetime(2026, 9, 2, 10, 0), mode="sim")

    assert len(repo.list_closed_for_period(
        session, user_id=user.id, mode="real", date_from=TODAY, date_to=TODAY
    )) == 1
    assert len(repo.list_closed_for_period(
        session, user_id=user.id, mode="sim", date_from=TODAY, date_to=TODAY
    )) == 1


def test_previous_night_session_counts_as_today(session, user):
    """① 核心案例：昨晚 23:06 的夜盤成交，query(date_from=date_to=今日) 應涵蓋。"""
    _closed(session, user.id, exit_time=dt.datetime(2026, 9, 1, 23, 6, 40))

    rows = repo.list_closed_for_period(session, user_id=user.id, mode="real",
                                        date_from=TODAY, date_to=TODAY)
    assert len(rows) == 1


def test_own_night_session_excluded_from_today_belongs_to_next_day(session, user):
    """今天自己的夜盤（15:00 後）屬於下一個交易日，不該落在「今日」查詢裡。"""
    _closed(session, user.id, exit_time=dt.datetime(2026, 9, 2, 20, 0, 0))

    rows = repo.list_closed_for_period(session, user_id=user.id, mode="real",
                                        date_from=TODAY, date_to=TODAY)
    assert len(rows) == 0

    next_day = dt.date(2026, 9, 3)
    rows_next = repo.list_closed_for_period(session, user_id=user.id, mode="real",
                                             date_from=next_day, date_to=next_day)
    assert len(rows_next) == 1


def test_open_trades_excluded(session, user):
    repo.create_trade(session, TradeCreate(
        symbol="TXF", direction="long", entry_time=dt.datetime(2026, 9, 2, 9, 0),
        entry_price=Decimal("18000"), size=1, point_value=Decimal("200"), source="shioaji",
    ), user_id=user.id)
    rows = repo.list_closed_for_period(session, user_id=user.id, mode="real",
                                        date_from=TODAY, date_to=TODAY)
    assert rows == []


def test_sorted_descending_by_exit_time(session, user):
    _closed(session, user.id, exit_time=dt.datetime(2026, 9, 2, 9, 0))
    _closed(session, user.id, exit_time=dt.datetime(2026, 9, 2, 14, 0))
    rows = repo.list_closed_for_period(session, user_id=user.id, mode="real",
                                        date_from=TODAY, date_to=TODAY)
    assert [r.exit_time.hour for r in rows] == [14, 9]
