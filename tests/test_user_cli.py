"""bootstrap: first admin + claiming pre-account data."""
import argparse
import datetime as dt
from decimal import Decimal

import pytest
from sqlmodel import select

from quanquant.db.models import Alert, ChartState, Trade, UserChartState
from quanquant.user_cli import _prompt_password, bootstrap_admin


@pytest.fixture
def legacy_data(session):
    session.add(Trade(
        symbol="TXF", direction="long", entry_time=dt.datetime(2026, 6, 1, 9, 0),
        entry_price=Decimal("18000"), size=1, point_value=Decimal("200"),
    ))
    session.add(Alert(
        symbol="TXF", timeframe="5m", left_kind="price", op="gte",
        right_kind="const", right_value=Decimal("18000"),
    ))
    session.add(ChartState(symbol="TXF", kind="indicators", payload='{"ma":[5]}'))
    session.add(ChartState(symbol="TXF", kind="pulse", payload='{"telegramEnabled":true}'))
    session.commit()


def test_bootstrap_creates_admin_and_claims(session, legacy_data):
    admin = bootstrap_admin(session, "henry", "pw12345")
    assert admin.role == "admin"
    assert session.exec(select(Trade)).first().user_id == admin.id
    assert session.exec(select(Alert)).first().user_id == admin.id
    copied = session.exec(select(UserChartState)).all()
    assert len(copied) == 1  # indicators copied; kind="pulse" stays system-level
    assert copied[0].user_id == admin.id and copied[0].kind == "indicators"
    # legacy chart_states rows untouched (rollback safety)
    assert len(session.exec(select(ChartState)).all()) == 2


def test_bootstrap_refuses_second_run(session):
    bootstrap_admin(session, "henry", "pw12345")
    with pytest.raises(SystemExit):
        bootstrap_admin(session, "again", "pw")


def test_bootstrap_claims_only_orphans(session, legacy_data, user):
    # a row that already has an owner must keep it
    owned = Trade(
        symbol="TXF", direction="long", entry_time=dt.datetime(2026, 6, 2, 9, 0),
        entry_price=Decimal("18000"), size=1, point_value=Decimal("200"), user_id=user.id,
    )
    session.add(owned)
    session.commit()
    # `user` fixture is admin — bootstrap refuses; demote first to test claiming
    user_row = session.get(type(user), user.id)
    user_row.role = "user"
    session.add(user_row)
    session.commit()

    admin = bootstrap_admin(session, "henry", "pw12345")
    session.refresh(owned)
    assert owned.user_id == user.id  # untouched


def test_prompt_password_rejects_empty(monkeypatch):
    monkeypatch.setattr("quanquant.user_cli.getpass.getpass", lambda _: "")
    with pytest.raises(SystemExit):
        _prompt_password(argparse.Namespace(password=None))


def test_prompt_password_accepts_nonempty(monkeypatch):
    monkeypatch.setattr("quanquant.user_cli.getpass.getpass", lambda _: "pw12345")
    assert _prompt_password(argparse.Namespace(password=None)) == "pw12345"
