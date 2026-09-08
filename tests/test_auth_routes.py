"""Login/logout flow. Router protection is asserted in test_route_protection.py (Task 5)."""
import re

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


class _FakeRiskGuard:
    """R2-4：帳戶頁的 Agent Token 卡片沿用下單頁原本的 owner-only 判定
    （`risk_guard.is_owner`／簽發端點的 `risk_guard.assert_owner`）；這裡只需要最小的
    假物件，不需要 kill switch/cooldown 那些下單子系統的行為。"""

    def __init__(self, owner_ids):
        self._owner_ids = set(owner_ids)

    def is_owner(self, user_id) -> bool:
        return user_id in self._owner_ids

    def assert_owner(self, user_id) -> None:
        from quanquant.broker.base import AuthorizationError

        if user_id not in self._owner_ids:
            raise AuthorizationError("not owner")


@pytest.fixture
def owner_client(client, user):
    """`client` fixture 預設沒有 `app.state.order_risk_guard`（get_order_risk_guard 回
    None），比照下單子系統停用；owner_client 額外把當前登入的 `user` 標成 owner，供
    R2-4 帳戶頁 Agent Token 卡片的測試使用。"""
    client.app.state.order_risk_guard = _FakeRiskGuard({user.id})
    return client


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


# ---------------------------------------------------------------------------
# R2-4：Agent Token 產生/重置整塊 UI 從下單頁搬到帳戶設定頁；下單頁對應的「已搬走」
# 驗收見 test_orders_routes.py::test_orders_page_no_longer_shows_agent_token_control。
#
# 2026-09-05（使用者拍板）：手動產生/重置改 admin-only——原本「owner-only」的授權在此
# 變更為「owner 且 admin」兩者皆要。`client`/`owner_client` 底下的 `user` 均沿用
# conftest 預設 role=admin，故既有「owner 可見卡片」測試在新語意下天然也是
# 「admin+owner 可見」，不必改內容；新增的測試才需要額外把 role 降回一般 user，驗證
# 「owner 但非 admin」這個新邊界。
# ---------------------------------------------------------------------------

def test_account_page_shows_agent_token_card_for_admin_owner(owner_client):
    """admin 且 owner（`owner_client` 預設狀態，`user` 角色沿用 conftest 的 role=admin）
    → 卡片顯示。"""
    text = owner_client.get("/account").text
    assert 'hx-post="/orders/agent-token"' in text
    assert "尚未產生 agent token" in text


def test_account_page_hides_agent_token_card_for_non_owner(client):
    """`client` fixture 未設 `order_risk_guard`（比照下單子系統停用/一般登入者非
    owner）——帳戶頁不應顯示 Agent Token 卡片。"""
    text = client.get("/account").text
    assert 'hx-post="/orders/agent-token"' not in text
    assert "Agent Token" not in text


def test_account_page_hides_agent_token_card_for_owner_who_is_not_admin(owner_client, session, user):
    """2026-09-05（使用者拍板）：owner 身份不足以看到卡片——`owner_client` 預設
    admin+owner，這裡把角色降回一般 user（owner 身份不變），卡片必須跟著消失，證明
    gate 真的是「owner 且 admin」而非單看 owner。"""
    user.role = "user"
    session.add(user)
    session.commit()
    text = owner_client.get("/account").text
    assert 'hx-post="/orders/agent-token"' not in text
    assert "Agent Token" not in text


def test_account_page_agent_token_post_403_for_owner_who_is_not_admin(owner_client, session, user):
    """卡片被藏起來不代表端點本身安全——server 端仍要擋（UI 隱藏≠授權，2026-08-27
    b9f2511 的教訓）：owner 但非 admin 直接 POST 端點一樣 403。"""
    user.role = "user"
    session.add(user)
    session.commit()
    resp = owner_client.post("/orders/agent-token")
    assert resp.status_code == 403


def test_account_page_agent_token_issue_shows_plaintext_once(owner_client, session, user):
    resp = owner_client.post("/orders/agent-token")
    assert resp.status_code == 200
    assert "<code" in resp.text
    assert "尚未產生" not in resp.text


def test_account_page_reload_does_not_leak_plaintext_after_issue(owner_client):
    """R2-4 搬家後的等效驗收（原本在 /orders 頁的同名測試已隨 Agent Token 卡片搬到這裡）：
    簽發當下的回應才看得到明文；重新整理 /account 頁只看得到 expires_at/last_used_at，
    看不到明文（DB 本就只存 hash，route 只在簽發那次回應塞 raw_token）。"""
    issue_resp = owner_client.post("/orders/agent-token")
    plaintext = re.search(r"<code[^>]*>([^<]+)</code>", issue_resp.text).group(1)

    reload_text = owner_client.get("/account").text
    assert plaintext not in reload_text
    assert "尚未使用" in reload_text  # 剛簽發、還沒被 WS 握手用過


def test_account_page_error_paths_still_show_agent_token_card_for_owner(owner_client):
    """搬家副作用檢查：色調/密碼表單驗證失敗時重繪 account.html，Agent Token 卡片不能
    因為新 context 沒補齊而憑空消失（見 auth.py::_account_context 的三處共用）。"""
    resp = owner_client.post("/account/color-scheme", data={"scheme": "bad"})
    assert resp.status_code == 200
    assert 'hx-post="/orders/agent-token"' in resp.text


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


# ---------------------------------------------------------------------------
# 007：sim 下單確認視窗偏好——skip_sim_confirm（⑤ 勾選後 User 欄位落 DB／⑦ 帳戶頁可改回）
# ---------------------------------------------------------------------------

def test_account_page_shows_sim_confirm_checkbox_unchecked_by_default(client):
    r = client.get("/account")
    assert r.status_code == 200
    assert 'name="skip_sim_confirm"' in r.text
    assert 'name="skip_sim_confirm" value="1" checked' not in r.text


def test_put_skip_sim_confirm_api_persists_to_user_row(client, engine, user):
    """⑤：勾選後（前端的「不再顯示」checkbox 觸發這支 API）User 欄位要真的落 DB，不是只
    在畫面上假裝生效。"""
    r = client.put("/api/user/skip-sim-confirm", json={"skip": True})
    assert r.status_code == 204
    with Session(engine) as s:
        from quanquant.db.models import User

        row = s.get(User, user.id)
        assert row.skip_sim_confirm is True


def test_put_skip_sim_confirm_api_rejects_non_bool(client):
    r = client.put("/api/user/skip-sim-confirm", json={"skip": "yes"})
    assert r.status_code == 422


def test_put_skip_sim_confirm_api_requires_auth(anon_client):
    r = anon_client.put("/api/user/skip-sim-confirm", json={"skip": True}, follow_redirects=False)
    assert r.status_code in (401, 303, 307)


def test_put_skip_sim_confirm_api_bad_body(client):
    r = client.put("/api/user/skip-sim-confirm", content=b"not json",
                   headers={"Content-Type": "application/json"})
    assert r.status_code == 422


def test_account_page_sim_confirm_form_can_turn_preference_back_on_and_off(client, engine, user):
    """⑦：帳戶頁可改回——勾選存成 True 之後，再用未勾選（表單不送這個欄位）的請求送出，
    要能改回 False。"""
    r_on = client.post("/account/sim-confirm", data={"skip_sim_confirm": "1"}, follow_redirects=False)
    assert r_on.status_code == 303
    with Session(engine) as s:
        from quanquant.db.models import User

        assert s.get(User, user.id).skip_sim_confirm is True
    assert 'name="skip_sim_confirm" value="1" checked' in client.get("/account").text

    r_off = client.post("/account/sim-confirm", data={}, follow_redirects=False)  # 未勾選：不送欄位
    assert r_off.status_code == 303
    with Session(engine) as s:
        from quanquant.db.models import User

        assert s.get(User, user.id).skip_sim_confirm is False
    assert 'name="skip_sim_confirm" value="1" checked' not in client.get("/account").text


def test_dashboard_loads_indicators_module(client):
    body = client.get("/").text
    assert "/static/indicators.js" in body


def test_indicator_dialog_is_master_detail(client):
    body = client.get("/").text
    # 主從式版面容器 + 由 registry 衍生（不再有寫死的 indicatorDefs 迴圈）
    assert 'class="ind-split"' in body
    assert 'x-text="e.title"' in body
