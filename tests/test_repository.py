import datetime as dt
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from quanquant.journal import repository as repo
from quanquant.journal.schemas import TradeUpdate, split_tags

from tests.conftest import make_create


def test_open_trade_has_no_pnl(session, user):
    t = repo.create_trade(session, make_create(), user_id=user.id)
    assert t.pnl is None and t.exit_time is None


def test_closed_trade_pnl_computed(session, user):
    t = repo.create_trade(
        session, make_create(exit_time=dt.datetime(2026, 6, 1, 10, 0), exit_price=Decimal("18100")),
        user_id=user.id,
    )
    assert t.pnl == Decimal("20000") and t.pnl_is_manual is False


def test_manual_pnl_override_preserved(session, user):
    t = repo.create_trade(
        session,
        make_create(exit_time=dt.datetime(2026, 6, 1, 10, 0), exit_price=Decimal("18100"), pnl=Decimal("12345")),
        user_id=user.id,
    )
    assert t.pnl == Decimal("12345") and t.pnl_is_manual is True


def test_update_recomputes_pnl(session, user):
    t = repo.create_trade(
        session, make_create(exit_time=dt.datetime(2026, 6, 1, 10, 0), exit_price=Decimal("18100")),
        user_id=user.id,
    )
    repo.update_trade(session, t.id, TradeUpdate(exit_price=Decimal("18200")), user_id=user.id)
    assert repo.get_trade(session, t.id, user_id=user.id).pnl == Decimal("40000")


def test_tags_round_trip(session, user):
    t = repo.create_trade(session, make_create(tags=["突破", "均線"]), user_id=user.id)
    assert split_tags(repo.get_trade(session, t.id, user_id=user.id).tags) == ["突破", "均線"]


def test_delete(session, user):
    t = repo.create_trade(session, make_create(), user_id=user.id)
    assert repo.delete_trade(session, t.id, user_id=user.id) is True
    assert repo.get_trade(session, t.id, user_id=user.id) is None


def test_filters(session, user):
    repo.create_trade(
        session,
        make_create(symbol="TXF", exit_time=dt.datetime(2026, 6, 1, 10, 0), exit_price=Decimal("18100"), tags=["突破"]),
        user_id=user.id,
    )
    repo.create_trade(session, make_create(symbol="MTX"), user_id=user.id)  # open

    assert len(repo.list_trades(session, user_id=user.id, status="open")) == 1
    assert len(repo.list_trades(session, user_id=user.id, status="closed")) == 1
    assert len(repo.list_trades(session, user_id=user.id, symbol="MTX")) == 1
    assert len(repo.list_trades(session, user_id=user.id, tag="突破")) == 1
    assert len(repo.list_for_stats(session, user_id=user.id)) == 1
    assert repo.list_symbols(session, user_id=user.id) == ["MTX", "TXF"]
    assert repo.list_all_tags(session, user_id=user.id) == ["突破"]


def test_mode_and_source_default(session, user):
    t = repo.create_trade(session, make_create(), user_id=user.id)
    assert t.mode == "real" and t.source == "manual"


def test_mode_round_trip_sim(session, user):
    t = repo.create_trade(session, make_create(mode="sim"), user_id=user.id)
    assert repo.get_trade(session, t.id, user_id=user.id).mode == "sim"


def test_source_round_trip_shioaji(session, user):
    t = repo.create_trade(session, make_create(source="shioaji"), user_id=user.id)
    assert repo.get_trade(session, t.id, user_id=user.id).source == "shioaji"


def test_list_trades_scoped_by_mode(session, user):
    repo.create_trade(session, make_create(symbol="TXF"), user_id=user.id)              # real
    repo.create_trade(session, make_create(symbol="MXF", mode="sim"), user_id=user.id)  # sim
    real = repo.list_trades(session, user_id=user.id, mode="real")
    sim = repo.list_trades(session, user_id=user.id, mode="sim")
    assert [t.symbol for t in real] == ["TXF"]
    assert [t.symbol for t in sim] == ["MXF"]


# --- C2：fresh DB（create_all）也要有 mode/source CHECK，不只靠遷移 ALTER ---

_INSERT_TRADE_SQL = (
    "INSERT INTO trades (symbol, direction, entry_time, entry_price, size, point_value, mode, source) "
    "VALUES ('TXF', 'long', '2026-06-01 09:00:00', '18000', 1, '200', {mode}, {source})"
)


def test_fresh_db_rejects_illegal_mode(engine):
    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(text(_INSERT_TRADE_SQL.format(mode="'paper'", source="'manual'")))


def test_fresh_db_rejects_illegal_source(engine):
    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(text(_INSERT_TRADE_SQL.format(mode="'real'", source="'auto'")))


def test_fresh_db_rejects_null_mode(engine):
    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(text(_INSERT_TRADE_SQL.format(mode="NULL", source="'manual'")))
