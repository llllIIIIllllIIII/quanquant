import datetime as dt
from decimal import Decimal

from quanquant.journal import repository as repo
from quanquant.journal.schemas import TradeUpdate, split_tags

from tests.conftest import make_create


def test_open_trade_has_no_pnl(session):
    t = repo.create_trade(session, make_create())
    assert t.pnl is None and t.exit_time is None


def test_closed_trade_pnl_computed(session):
    t = repo.create_trade(
        session, make_create(exit_time=dt.datetime(2026, 6, 1, 10, 0), exit_price=Decimal("18100"))
    )
    assert t.pnl == Decimal("20000") and t.pnl_is_manual is False


def test_manual_pnl_override_preserved(session):
    t = repo.create_trade(
        session,
        make_create(exit_time=dt.datetime(2026, 6, 1, 10, 0), exit_price=Decimal("18100"), pnl=Decimal("12345")),
    )
    assert t.pnl == Decimal("12345") and t.pnl_is_manual is True


def test_update_recomputes_pnl(session):
    t = repo.create_trade(
        session, make_create(exit_time=dt.datetime(2026, 6, 1, 10, 0), exit_price=Decimal("18100"))
    )
    repo.update_trade(session, t.id, TradeUpdate(exit_price=Decimal("18200")))
    assert repo.get_trade(session, t.id).pnl == Decimal("40000")


def test_tags_round_trip(session):
    t = repo.create_trade(session, make_create(tags=["突破", "均線"]))
    assert split_tags(repo.get_trade(session, t.id).tags) == ["突破", "均線"]


def test_delete(session):
    t = repo.create_trade(session, make_create())
    assert repo.delete_trade(session, t.id) is True
    assert repo.get_trade(session, t.id) is None


def test_filters(session):
    repo.create_trade(
        session,
        make_create(symbol="TXF", exit_time=dt.datetime(2026, 6, 1, 10, 0), exit_price=Decimal("18100"), tags=["突破"]),
    )
    repo.create_trade(session, make_create(symbol="MTX"))  # open

    assert len(repo.list_trades(session, status="open")) == 1
    assert len(repo.list_trades(session, status="closed")) == 1
    assert len(repo.list_trades(session, symbol="MTX")) == 1
    assert len(repo.list_trades(session, tag="突破")) == 1
    assert len(repo.list_for_stats(session)) == 1
    assert repo.list_symbols(session) == ["MTX", "TXF"]
    assert repo.list_all_tags(session) == ["突破"]
