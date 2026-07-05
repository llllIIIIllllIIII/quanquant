"""Every app surface requires login; /healthz and /login stay public."""
import pytest

PROTECTED_PAGES = ["/", "/journal", "/stats"]
PROTECTED_APIS = ["/api/candles?tf=1m", "/api/alerts", "/api/chart/state", "/api/pulse/state"]


@pytest.mark.parametrize("path", PROTECTED_PAGES)
def test_pages_redirect_anonymous(anon_client, path):
    r = anon_client.get(path, follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"] == "/login"


@pytest.mark.parametrize("path", PROTECTED_APIS)
def test_apis_401_anonymous(anon_client, path):
    r = anon_client.get(path, follow_redirects=False)
    assert r.status_code == 401
    assert r.headers.get("hx-redirect") == "/login"


def test_healthz_public(anon_client):
    assert anon_client.get("/healthz").status_code == 200


def test_login_public(anon_client):
    assert anon_client.get("/login").status_code == 200


def test_logged_in_client_passes(client):
    assert client.get("/").status_code == 200
