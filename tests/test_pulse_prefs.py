"""Market Pulse Telegram-toggle persistence + web endpoint."""
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from quanquant.auth import service as auth_service
from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.pulse.prefs import load_telegram_enabled, save_telegram_enabled

from .conftest import _build_app


@pytest.fixture
def plain_client(engine):
    with Session(engine) as s:
        u = auth_service.create_user(s, "plain", "pw", role="user")
    c = TestClient(_build_app(engine))
    c.cookies.set(SESSION_COOKIE, sign_session(u.id, u.token_version))
    return c


def test_telegram_pref_defaults_to_none(session):
    assert load_telegram_enabled(session, "TXF") is None  # use config default


def test_telegram_pref_roundtrip(session):
    save_telegram_enabled(session, "TXF", True)
    assert load_telegram_enabled(session, "TXF") is True
    save_telegram_enabled(session, "TXF", False)  # upsert, not duplicate
    assert load_telegram_enabled(session, "TXF") is False


def test_pulse_telegram_endpoint(client):
    assert client.get("/api/pulse/state").json() == {"telegramEnabled": False}
    r = client.put("/api/pulse/telegram", json={"enabled": True})
    assert r.status_code == 200
    assert r.json() == {"telegramEnabled": True}


def test_pulse_telegram_non_admin_403(plain_client, client):
    """Non-admin must get 403; admin (client fixture) must still succeed."""
    assert plain_client.put("/api/pulse/telegram", json={"enabled": True}).status_code == 403
    assert client.put("/api/pulse/telegram", json={"enabled": True}).status_code == 200
