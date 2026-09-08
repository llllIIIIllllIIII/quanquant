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


async def test_protected_endpoint_allows_origin_null_from_no_referrer_form_post():
    # 回歸：`Referrer-Policy: no-referrer` 下瀏覽器對同源 form POST 送 `Origin: null`
    # （真瀏覽器手動測試在 /setup/step1/start 撞到的 403），不得誤擋。
    state = GuiSecurityState(port=54321)
    state.session_token = "s3cr3t"
    app = _build_app(state)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:54321",
                                  cookies={GUI_SESSION_COOKIE: "s3cr3t"}) as client:
        resp = await client.get("/protected", headers={"Host": "127.0.0.1:54321",
                                                        "Origin": "null", "Sec-Fetch-Site": "same-origin"})
    assert resp.status_code == 200


async def test_protected_endpoint_rejects_real_cross_site_origin():
    # 放行 "null" 不得順帶放行真正的跨站 Origin。
    state = GuiSecurityState(port=54321)
    state.session_token = "s3cr3t"
    app = _build_app(state)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:54321",
                                  cookies={GUI_SESSION_COOKIE: "s3cr3t"}) as client:
        resp = await client.get("/protected", headers={"Host": "127.0.0.1:54321",
                                                        "Origin": "http://evil.example"})
    assert resp.status_code == 403


async def test_responses_carry_no_store_and_csp_headers():
    state = GuiSecurityState(port=54321)
    app = _build_app(state)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:54321") as client:
        resp = await client.get("/bootstrap", params={"secret": "x"}, headers={"Host": "127.0.0.1:54321"})
    assert resp.headers.get("cache-control") == "no-store"
    csp = resp.headers.get("content-security-policy", "")
    assert "default-src 'self'" in csp
    # 精靈/狀態頁的 head inline `<style>` 必須放行，否則錯誤橫幅無樣式、訊息被忽略
    # （使用者實測：launch 失敗「沒反應」）。script/connect 仍鎖 default-src 'self'。
    assert "style-src 'self' 'unsafe-inline'" in csp
