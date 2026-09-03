"""`/agent/authorize` 核准頁。round4（reviewer Critical 修正）：`app.state.order_risk_guard`
未接線（`risk_guard is None`）是可達的生產狀態（`ORDER_CHANNEL` 未配置／子系統停用／
`connect()` 失敗），router 本身無條件掛載（只走 `dependencies=protected`＝需登入），故不
能把「沒設定 risk_guard」當成「不限制」——那等於任何登入使用者都能在子系統未接線時核准/
拒絕任意裝置代碼。原本 7 支只用 conftest 的 `client`（完全不 wire `order_risk_guard`）
測「查碼/核准成功」的測試，改接下面的 `owner_client`（wire 一個判定使用者是 owner 的
fake risk_guard，比照 `tests/test_orders_routes.py` 的 `_FakeRiskGuard`/`order_client`
手法）——查碼/核准/拒絕的正常流程語意不變，只是不再假裝『沒設定 risk_guard』代表
『無限制放行』。新增：`non_owner_client`（owner 名單不含目前使用者）與繼續使用 `client`
（risk_guard 未接線）分別驗證 fail-closed 的兩種情境。"""
import hashlib
from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from quanquant.auth.device_flow import create_device_code
from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.web.app import create_app
from quanquant.web.deps import get_poller, get_session


def _pending_code(session, *, verifier="v" * 43):
    challenge = hashlib.sha256(verifier.encode()).hexdigest()
    raw, row = create_device_code(session, request_ip="203.0.113.5", code_challenge=challenge)
    return raw, row


class _FakeRiskGuard:
    """比照 `tests/test_orders_routes.py` 的 `_FakeRiskGuard`；這裡只需要 `is_owner`——
    round4 修正後的 `_owner_status` 只呼叫這一個方法（不再用 `assert_owner` 例外路徑）。"""

    def __init__(self, owner_ids):
        self._owner_ids = set(owner_ids)

    def is_owner(self, user_id) -> bool:
        return user_id in self._owner_ids


def _wire_client(engine, user, *, risk_guard) -> TestClient:
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    app.state.order_risk_guard = risk_guard
    c = TestClient(app)
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    return c


@pytest.fixture
def owner_client(engine, user) -> TestClient:
    """已登入且 risk_guard 判定為 owner（正常操作路徑）。"""
    return _wire_client(engine, user, risk_guard=_FakeRiskGuard(owner_ids={user.id}))


@pytest.fixture
def non_owner_client(engine, user) -> TestClient:
    """已登入，但 risk_guard 已接線且判定這個使用者不在 owner 名單內。"""
    return _wire_client(engine, user, risk_guard=_FakeRiskGuard(owner_ids={user.id + 999}))


# ---------------------------------------------------------------------------
# 登入前提
# ---------------------------------------------------------------------------

def test_get_authorize_page_requires_login(anon_client):
    resp = anon_client.get("/agent/authorize", follow_redirects=False)
    assert resp.status_code in (303, 401)


# ---------------------------------------------------------------------------
# owner-only：GET 頁面
# ---------------------------------------------------------------------------

def test_get_authorize_page_succeeds_for_owner(owner_client):
    resp = owner_client.get("/agent/authorize")
    assert resp.status_code == 200
    assert 'name="user_code"' in resp.text


def test_get_authorize_page_forbidden_for_non_owner(non_owner_client):
    resp = non_owner_client.get("/agent/authorize")
    assert resp.status_code == 403


def test_get_authorize_page_shows_disabled_when_subsystem_not_wired(client):
    """`risk_guard is None`：頁面顯示「未啟用」資訊，且不得含有查碼表單——不是裝飾性
    文案掛在旁邊，是真的不能操作（斷言表單欄位不存在，不只是斷言訊息存在）。"""
    resp = client.get("/agent/authorize")
    assert resp.status_code == 200
    assert "未啟用" in resp.text
    assert 'name="user_code"' not in resp.text


# ---------------------------------------------------------------------------
# 查碼（POST /agent/authorize）
# ---------------------------------------------------------------------------

def test_lookup_shows_confirmation_for_valid_pending_code(owner_client, session):
    raw, row = _pending_code(session)
    get_resp = owner_client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = owner_client.post("/agent/authorize", data={"user_code": row.user_code, "csrf_token": csrf})
    assert resp.status_code == 200 and row.user_code in resp.text
    assert "核准" in resp.text and "拒絕" in resp.text


def test_lookup_confirmation_shows_created_at_in_taiwan_local_not_utc(owner_client, session):
    """NEW-2：安全比對畫面（核對裝置代碼），`created_at` 是 naive-UTC，改用 `dt_cst`
    後應顯示台灣本地時間，不是裸印的 UTC 原始值（差 8 小時會誤導核對）。"""
    raw, row = _pending_code(session)
    row.created_at = datetime(2026, 1, 1, 10, 0, 0)  # naive-UTC
    session.add(row)
    session.commit()
    get_resp = owner_client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = owner_client.post("/agent/authorize", data={"user_code": row.user_code, "csrf_token": csrf})
    assert resp.status_code == 200
    assert "2026-01-01 18:00:00" in resp.text  # CST = UTC+8
    assert "2026-01-01 10:00" not in resp.text  # 不是裸印的 UTC 原始值


def test_lookup_unknown_code_shows_generic_error(owner_client):
    get_resp = owner_client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = owner_client.post("/agent/authorize", data={"user_code": "ZZZZ-0000", "csrf_token": csrf})
    assert resp.status_code == 200 and "找不到" in resp.text


def test_lookup_forbidden_for_non_owner(non_owner_client, session):
    raw, row = _pending_code(session)
    resp = non_owner_client.post(
        "/agent/authorize", data={"user_code": row.user_code, "csrf_token": "irrelevant"}
    )
    assert resp.status_code == 403


def test_lookup_rejected_when_subsystem_not_wired(client, session):
    raw, row = _pending_code(session)
    resp = client.post("/agent/authorize", data={"user_code": row.user_code, "csrf_token": "irrelevant"})
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# 核准/拒絕（POST /agent/authorize/decide）
# ---------------------------------------------------------------------------

def test_decide_without_valid_csrf_rejected(owner_client, session):
    raw, row = _pending_code(session)
    resp = owner_client.post("/agent/authorize/decide",
                        data={"device_code_id": row.id, "decision": "approve", "csrf_token": "forged"})
    assert resp.status_code == 403
    session.refresh(row)
    assert row.status == "pending"


def test_decide_approve_binds_current_user_and_conditional_update(owner_client, session, user):
    raw, row = _pending_code(session)
    get_resp = owner_client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = owner_client.post("/agent/authorize/decide",
                        data={"device_code_id": row.id, "decision": "approve", "csrf_token": csrf})
    assert resp.status_code == 200
    session.refresh(row)
    assert row.status == "approved" and row.user_id == user.id


def test_decide_deny_sets_denied(owner_client, session):
    raw, row = _pending_code(session)
    get_resp = owner_client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = owner_client.post("/agent/authorize/decide",
                        data={"device_code_id": row.id, "decision": "deny", "csrf_token": csrf})
    assert resp.status_code == 200
    session.refresh(row)
    assert row.status == "denied"


def test_decide_already_processed_is_rejected_not_overwritten(owner_client, session, user):
    raw, row = _pending_code(session)
    row.status, row.user_id = "approved", user.id
    session.add(row); session.commit()
    get_resp = owner_client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = owner_client.post("/agent/authorize/decide",
                        data={"device_code_id": row.id, "decision": "deny", "csrf_token": csrf})
    assert resp.status_code == 200 and "已處理" in resp.text
    session.refresh(row)
    assert row.status == "approved"  # 沒被 deny 蓋掉


def test_decide_forbidden_for_non_owner(non_owner_client, session):
    raw, row = _pending_code(session)
    resp = non_owner_client.post(
        "/agent/authorize/decide",
        data={"device_code_id": row.id, "decision": "approve", "csrf_token": "irrelevant"},
    )
    assert resp.status_code == 403
    session.refresh(row)
    assert row.status == "pending"


def test_decide_rejected_when_subsystem_not_wired(client, session):
    raw, row = _pending_code(session)
    resp = client.post(
        "/agent/authorize/decide",
        data={"device_code_id": row.id, "decision": "approve", "csrf_token": "irrelevant"},
    )
    assert resp.status_code == 403
    session.refresh(row)
    assert row.status == "pending"
