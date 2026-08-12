import httpx
import pytest

from quanquant.agent.gui.security import GUI_SESSION_COOKIE, GuiSecurityState, bootstrap_url, install_security_headers, require_gui_session
from fastapi import Depends, FastAPI


def _build_app(state):
    app = FastAPI()
    install_security_headers(app)

    @app.get("/bootstrap")
    def bootstrap(secret: str, response):
        ...  # 實作時完成；測試只驗證下面兩支

    @app.get("/protected", dependencies=[Depends(require_gui_session)])
    def protected():
        return {"ok": True}

    app.state.gui_security = state
    return app


def test_bootstrap_url_contains_secret_once():
    state = GuiSecurityState(port=54321)
    url = bootstrap_url(state)
    assert f"127.0.0.1:{54321}" in url and state.bootstrap_secret in url


async def test_protected_endpoint_rejects_missing_session_cookie():
    state = GuiSecurityState(port=54321)
    app = _build_app(state)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:54321") as client:
        resp = await client.get("/protected", headers={"Host": "127.0.0.1:54321"})
    assert resp.status_code == 403


async def test_protected_endpoint_rejects_wrong_host_header():
    state = GuiSecurityState(port=54321)
    state.session_token = "s3cr3t"
    app = _build_app(state)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:54321",
                                  cookies={GUI_SESSION_COOKIE: "s3cr3t"}) as client:
        resp = await client.get("/protected", headers={"Host": "evil.example:54321"})
    assert resp.status_code == 403


async def test_protected_endpoint_allows_matching_session_and_host():
    state = GuiSecurityState(port=54321)
    state.session_token = "s3cr3t"
    app = _build_app(state)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:54321",
                                  cookies={GUI_SESSION_COOKIE: "s3cr3t"}) as client:
        resp = await client.get("/protected", headers={"Host": "127.0.0.1:54321"})
    assert resp.status_code == 200


async def test_responses_carry_no_store_and_csp_headers():
    state = GuiSecurityState(port=54321)
    app = _build_app(state)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:54321") as client:
        resp = await client.get("/bootstrap", params={"secret": "x"}, headers={"Host": "127.0.0.1:54321"})
    assert resp.headers.get("cache-control") == "no-store"
    assert "default-src 'self'" in resp.headers.get("content-security-policy", "")
