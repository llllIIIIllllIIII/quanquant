"""/admin/users: admin-only management surface."""
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from quanquant.auth import service as auth_service
from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.broker import repository as brepo
from quanquant.db.models import Cooldown

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


# ---- 冷靜期管理（/admin/cooling-off）：admin-only 列出 active + 提前解除 ----

def _seed_cooldown(engine, user_id: int):
    now = brepo.now_epoch_ms()
    with Session(engine) as s:
        s.add(Cooldown(user_id=user_id, until_ts=now + 3_600_000, created_ts=now))
        s.commit()


def test_cooling_off_page_lists_active_user(client, engine, user):
    _seed_cooldown(engine, user.id)
    r = client.get("/admin/cooling-off")
    assert r.status_code == 200
    assert "tester" in r.text  # 冷靜期中的 user（conftest 的 tester）列出


def test_cooling_off_page_empty_when_none_active(client):
    r = client.get("/admin/cooling-off")
    assert r.status_code == 200
    assert "目前沒有使用者處於冷靜期" in r.text


def test_cooling_off_lift_removes_active(client, engine, user):
    _seed_cooldown(engine, user.id)
    r = client.post(f"/admin/cooling-off/{user.id}/lift", follow_redirects=False)
    assert r.status_code == 303
    with Session(engine) as s:
        assert brepo.active_cooldown(s, user_id=user.id, now_ms=brepo.now_epoch_ms()) is None


def test_cooling_off_page_non_admin_403(plain_client):
    assert plain_client.get("/admin/cooling-off").status_code == 403


def test_cooling_off_lift_non_admin_403(plain_client, engine, user):
    _seed_cooldown(engine, user.id)
    r = plain_client.post(f"/admin/cooling-off/{user.id}/lift")
    assert r.status_code == 403
    with Session(engine) as s:  # 未被解除
        assert brepo.active_cooldown(s, user_id=user.id, now_ms=brepo.now_epoch_ms()) is not None
