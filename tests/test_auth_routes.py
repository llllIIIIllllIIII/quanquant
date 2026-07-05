"""Login/logout flow. Router protection is asserted in test_route_protection.py (Task 5)."""
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from quanquant.auth import service
from quanquant.auth.tokens import SESSION_COOKIE
from quanquant.web.app import create_app
from quanquant.web.deps import get_poller, get_session


@pytest.fixture(autouse=True)
def _fresh_lockout():
    service.clear_failures()
    yield
    service.clear_failures()


@pytest.fixture
def anon_client(engine):
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    return TestClient(app)


@pytest.fixture
def henry(engine):
    with Session(engine) as s:
        return service.create_user(s, "henry", "pw12345", display_name="Henry")


def test_login_page_renders(anon_client):
    r = anon_client.get("/login")
    assert r.status_code == 200
    assert "password" in r.text


def test_login_success_sets_cookie_and_redirects(anon_client, henry):
    r = anon_client.post(
        "/login", data={"username": "henry", "password": "pw12345"}, follow_redirects=False
    )
    assert r.status_code == 303
    assert r.headers["location"] == "/"
    assert SESSION_COOKIE in r.cookies


def test_login_failure_shows_error_no_cookie(anon_client, henry):
    r = anon_client.post("/login", data={"username": "henry", "password": "bad"})
    assert r.status_code == 200
    assert SESSION_COOKIE not in r.cookies
    assert "帳號或密碼錯誤" in r.text


def test_logout_clears_cookie(anon_client, henry):
    anon_client.post("/login", data={"username": "henry", "password": "pw12345"})
    r = anon_client.post("/logout", follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"] == "/login"
    assert anon_client.cookies.get(SESSION_COOKIE) is None
