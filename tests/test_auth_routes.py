"""Login/logout flow. Router protection is asserted in test_route_protection.py (Task 5)."""
import pytest
from sqlmodel import Session

from quanquant.auth import service
from quanquant.auth.tokens import SESSION_COOKIE


@pytest.fixture(autouse=True)
def _fresh_lockout():
    service.clear_failures()
    yield
    service.clear_failures()


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


def test_change_password_flow(client):
    # wrong old password → error page
    r = client.post("/account/password",
                    data={"old_password": "WRONG", "new_password": "brand-new"})
    assert "舊密碼錯誤" in r.text

    r = client.post("/account/password",
                    data={"old_password": "test-pw", "new_password": "brand-new"},
                    follow_redirects=False)
    assert r.status_code == 303
    # a fresh cookie was issued for this device — protected pages still work
    assert client.get("/journal").status_code == 200


def test_navbar_shows_user_menu(client):
    body = client.get("/").text
    assert "Tester" in body           # display_name
    assert "/logout" in body
