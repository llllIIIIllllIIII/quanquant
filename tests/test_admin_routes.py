"""/admin/users: admin-only management surface."""
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from quanquant.auth import service as auth_service
from quanquant.auth.tokens import SESSION_COOKIE, sign_session

from .conftest import _build_app


@pytest.fixture
def plain_client(engine):
    with Session(engine) as s:
        u = auth_service.create_user(s, "plain", "pw", role="user")
    c = TestClient(_build_app(engine))
    c.cookies.set(SESSION_COOKIE, sign_session(u.id, u.token_version))
    return c


def _amy(engine):
    with Session(engine) as s:
        return auth_service.get_by_username(s, "amy")


def test_non_admin_403(plain_client):
    assert plain_client.get("/admin/users").status_code == 403


def test_page_lists_users(client):
    r = client.get("/admin/users")
    assert r.status_code == 200 and "tester" in r.text


def test_create_user(client, engine):
    r = client.post(
        "/admin/users",
        data={"username": "amy", "display_name": "Amy", "password": "amy-pw", "role": "user"},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert _amy(engine) is not None


def test_create_duplicate_shows_error(client, engine):
    body = {"username": "amy", "display_name": "Amy", "password": "pw", "role": "user"}
    client.post("/admin/users", data=body)
    r = client.post("/admin/users", data=body)
    assert "already exists" in r.text


def test_toggle_active(client, engine):
    client.post("/admin/users", data={"username": "amy", "display_name": "Amy",
                                      "password": "pw", "role": "user"})
    client.post(f"/admin/users/{_amy(engine).id}/toggle-active")
    assert _amy(engine).is_active is False


def test_reset_password_bumps_tv(client, engine):
    client.post("/admin/users", data={"username": "amy", "display_name": "Amy",
                                      "password": "pw", "role": "user"})
    amy = _amy(engine)
    client.post(f"/admin/users/{amy.id}/reset-password", data={"password": "new-pw"})
    assert _amy(engine).token_version == amy.token_version + 1
