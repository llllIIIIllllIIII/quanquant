"""Cross-user isolation: A must not see or touch B's data."""
import datetime as dt
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from quanquant.auth import service as auth_service
from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.journal import repository as repo
from quanquant.journal.schemas import TradeCreate

from .conftest import _build_app


@pytest.fixture
def user_b(engine):
    with Session(engine) as s:
        return auth_service.create_user(s, "bob", "bob-pw", display_name="Bob")


@pytest.fixture
def client_b(engine, user_b):
    c = TestClient(_build_app(engine))
    c.cookies.set(SESSION_COOKIE, sign_session(user_b.id, user_b.token_version))
    return c


def _trade(session, user_id, price="18000"):
    return repo.create_trade(
        session,
        TradeCreate(
            symbol="TXF", direction="long", entry_time=dt.datetime(2026, 6, 1, 9, 0),
            entry_price=Decimal(price), size=1, point_value=Decimal("200"),
        ),
        user_id=user_id,
    )


def test_repo_list_filters_by_user(session, user, user_b):
    _trade(session, user.id)
    _trade(session, user_b.id)
    assert len(repo.list_trades(session, user_id=user.id)) == 1
    assert len(repo.list_trades(session, user_id=user_b.id)) == 1


def test_repo_get_update_delete_scoped(session, user, user_b):
    t = _trade(session, user.id)
    assert repo.get_trade(session, t.id, user_id=user_b.id) is None
    assert repo.delete_trade(session, t.id, user_id=user_b.id) is False
    assert repo.get_trade(session, t.id, user_id=user.id) is not None


def test_journal_page_shows_only_own(client, client_b, session, user, user_b):
    _trade(session, user.id, price="11111")
    _trade(session, user_b.id, price="22222")
    assert "11,111" in client.get("/journal").text
    assert "22,222" not in client.get("/journal").text
    assert "11,111" not in client_b.get("/journal").text
    assert "22,222" in client_b.get("/journal").text


def test_cannot_delete_others_trade_via_route(client_b, session, user):
    t = _trade(session, user.id)
    client_b.delete(f"/trades/{t.id}")
    assert repo.get_trade(session, t.id, user_id=user.id) is not None


def test_stats_only_own(client, client_b, session, user, user_b):
    repo.create_trade(
        session,
        TradeCreate(
            symbol="TXF", direction="long", entry_time=dt.datetime(2026, 6, 1, 9, 0),
            entry_price=Decimal("18000"), exit_time=dt.datetime(2026, 6, 1, 10, 0),
            exit_price=Decimal("18100"), size=1, point_value=Decimal("200"),
        ),
        user_id=user.id,
    )
    own = client.get("/stats/data").json()
    other = client_b.get("/stats/data").json()
    # 鍵名以 quanquant/stats/metrics.py compute_stats 實際回傳為準（實作前先讀該檔確認）
    assert own != other
    # Verify user B (no trades) sees zero trades, user A sees their trade
    assert own["overall"]["count"] == 1
    assert other["overall"]["count"] == 0


def test_chart_state_isolated(client, client_b):
    client.put("/api/chart/state/indicators", json={"ma": [5, 10]})
    assert client.get("/api/chart/state").json()["indicators"] == {"ma": [5, 10]}
    assert client_b.get("/api/chart/state").json()["indicators"] is None


def test_chart_state_upsert_per_user(client, client_b):
    client.put("/api/chart/state/drawings", json=[{"type": "line"}])
    client_b.put("/api/chart/state/drawings", json=[{"type": "rect"}])
    assert client.get("/api/chart/state").json()["drawings"] == [{"type": "line"}]
    assert client_b.get("/api/chart/state").json()["drawings"] == [{"type": "rect"}]


_ALERT_BODY = {
    "symbol": "TXF", "timeframe": "5m", "left_kind": "price",
    "op": "gte", "right_kind": "const", "right_value": "18000",
}


def test_alerts_isolated(client, client_b):
    client.post("/api/alerts", json=_ALERT_BODY)
    assert len(client.get("/api/alerts").json()) == 1
    assert len(client_b.get("/api/alerts").json()) == 0


def test_others_alert_404(client, client_b):
    aid = client.post("/api/alerts", json=_ALERT_BODY).json()["id"]
    assert client_b.patch(f"/api/alerts/{aid}", json={"enabled": False}).status_code == 404
    assert client_b.delete(f"/api/alerts/{aid}").status_code == 404
    assert client.get("/api/alerts").json()[0]["enabled"] is True  # owner unaffected


def test_alert_events_scoped(client, client_b, session):
    from quanquant.db.models import AlertEvent

    aid = client.post("/api/alerts", json=_ALERT_BODY).json()["id"]
    session.add(AlertEvent(alert_id=aid, bar_ts=0, message="fired"))
    session.commit()
    assert len(client.get("/api/alerts/events").json()) == 1
    assert len(client_b.get("/api/alerts/events").json()) == 0
