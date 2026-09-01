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
    # color_scheme must still be present and a radio pre-checked (regression fix)
    assert 'value="green_up" checked' in r.text

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


def test_dashboard_wires_data_scheme_from_color_scheme(client):
    # the quote panel's rise/fall colour flips with the K-line colour scheme
    # via html[data-scheme]; the dashboard must seed it from QQ_COLOR_SCHEME
    # on a persistent ancestor (survives the periodic HTMX quote swap).
    body = client.get("/").text
    assert 'documentElement.setAttribute("data-scheme"' in body


def test_html_root_carries_data_scheme_from_user_preference_on_every_page(client):
    """001：漲跌配色偏好在下單頁根本沒有生效——data-scheme 移到 base.html 的 <html> 上，
    全站頁面（不只儀表板）都要拿到，未設定時 fallback green_up。"""
    for path in ("/", "/orders", "/journal"):
        body = client.get(path).text
        assert 'data-scheme="green_up"' in body, path


def test_html_root_data_scheme_follows_saved_preference(client):
    r = client.put("/api/user/color-scheme", json={"scheme": "red_up"})
    assert r.status_code == 204
    body = client.get("/orders").text
    assert 'data-scheme="red_up"' in body


def test_dashboard_chart_title_uses_symbol_label_with_session_suffix(client):
    """003：儀表板圖表標題顯示中文顯示名，後綴隨時段選單變（全/日/夜為 Alpine x-text，
    這裡驗證伺服器端已把 symbol_label 值嵌入表達式，而非裸代碼）。"""
    body = client.get("/").text
    assert "'台指近'" in body
    assert "session === 'day' ? '日' : session === 'night' ? '夜' : '全'" in body


def test_account_page_shows_color_scheme_radio(client):
    body = client.get("/account").text
    assert 'name="scheme"' in body
    assert "green_up" in body and "red_up" in body


def test_set_color_scheme_via_account_form(client):
    r = client.post("/account/color-scheme", data={"scheme": "red_up"},
                    follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"] == "/account"
    # 重新載入帳戶頁，red_up 被預選
    body = client.get("/account").text
    assert 'value="red_up" checked' in body


def test_set_color_scheme_invalid_shows_error(client):
    r = client.post("/account/color-scheme", data={"scheme": "bad"})
    assert r.status_code == 200
    assert "配色設定無效" in r.text


def test_put_color_scheme_api_persists(client):
    r = client.put("/api/user/color-scheme", json={"scheme": "red_up"})
    assert r.status_code == 204
    # 帳戶頁反映新值（同一欄位）
    assert 'value="red_up" checked' in client.get("/account").text


def test_put_color_scheme_api_rejects_invalid(client):
    r = client.put("/api/user/color-scheme", json={"scheme": "nope"})
    assert r.status_code == 422


def test_put_color_scheme_api_requires_auth(anon_client):
    r = anon_client.put("/api/user/color-scheme", json={"scheme": "red_up"},
                        follow_redirects=False)
    assert r.status_code in (401, 303, 307)


def test_put_color_scheme_api_bad_body(client):
    r = client.put("/api/user/color-scheme", content=b"not json",
                   headers={"Content-Type": "application/json"})
    assert r.status_code == 422


def test_navbar_brand_links_home(client):
    r = client.get("/account")  # 任一套用 base.html 的頁面
    assert r.status_code == 200
    assert '<a href="/" class="brand"' in r.text


def test_base_loads_tokens_css(client):
    r = client.get("/account")
    assert '/static/tokens.css' in r.text


def test_put_theme_api_persists(client):
    r = client.put("/api/user/theme", json={"theme": "light"})
    assert r.status_code == 204


def test_put_theme_api_rejects_invalid(client):
    r = client.put("/api/user/theme", json={"theme": "neon"})
    assert r.status_code == 422


def test_put_theme_api_requires_auth(anon_client):
    r = anon_client.put("/api/user/theme", json={"theme": "light"},
                        follow_redirects=False)
    assert r.status_code in (401, 303, 307)


def test_put_theme_api_bad_body(client):
    r = client.put("/api/user/theme", content=b"not json",
                   headers={"Content-Type": "application/json"})
    assert r.status_code == 422


def test_base_renders_user_theme(client):
    client.put("/api/user/theme", json={"theme": "light"})
    r = client.get("/account")
    assert 'data-theme="light"' in r.text


def test_base_theme_defaults_dark(client):
    r = client.get("/account")
    assert 'data-theme="dark"' in r.text


def test_dashboard_loads_indicators_module(client):
    body = client.get("/").text
    assert "/static/indicators.js" in body


def test_indicator_dialog_is_master_detail(client):
    body = client.get("/").text
    # 主從式版面容器 + 由 registry 衍生（不再有寫死的 indicatorDefs 迴圈）
    assert 'class="ind-split"' in body
    assert 'x-text="e.title"' in body
