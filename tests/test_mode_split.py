"""模擬/正式分流：同一 user 混存 sim+real，統計互不相加。"""
import datetime as dt
from decimal import Decimal

from quanquant.journal import repository as repo
from quanquant.stats.metrics import compute_stats

from tests.conftest import make_create


def _closed(session, user_id, *, mode, exit_price, day):
    repo.create_trade(
        session,
        make_create(
            mode=mode,
            exit_time=dt.datetime(2026, 6, day, 10, 0),
            exit_price=Decimal(exit_price),
        ),
        user_id=user_id,
    )


def test_stats_never_aggregate_across_mode(session, user):
    # real: 一筆 +20000；sim: 兩筆 (+20000, -20000)
    _closed(session, user.id, mode="real", exit_price="18100", day=1)
    _closed(session, user.id, mode="sim", exit_price="18100", day=2)
    _closed(session, user.id, mode="sim", exit_price="17900", day=3)

    real = compute_stats(repo.list_for_stats(session, user_id=user.id, mode="real"))
    sim = compute_stats(repo.list_for_stats(session, user_id=user.id, mode="sim"))

    assert real.overall.count == 1
    assert real.overall.total_pnl == Decimal("20000")
    assert sim.overall.count == 2
    assert sim.overall.total_pnl == Decimal("0")   # +20000 - 20000，未含 real 的 20000


def test_list_trades_mode_isolated(session, user):
    repo.create_trade(session, make_create(symbol="TXF"), user_id=user.id)               # real
    repo.create_trade(session, make_create(symbol="MXF", mode="sim"), user_id=user.id)   # sim
    assert len(repo.list_trades(session, user_id=user.id, mode="real")) == 1
    assert len(repo.list_trades(session, user_id=user.id, mode="sim")) == 1
