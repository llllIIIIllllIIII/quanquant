"""authenticate() + user management + login-failure lockout."""
import pytest

from quanquant.auth import service
from quanquant.db.models import User


@pytest.fixture(autouse=True)
def _fresh_lockout():
    service.clear_failures()
    yield
    service.clear_failures()


@pytest.fixture
def henry(session):
    return service.create_user(session, "henry", "pw12345", display_name="Henry", role="admin")


def test_create_and_authenticate(session, henry):
    assert henry.id is not None
    u = service.authenticate(session, "henry", "pw12345")
    assert u is not None and u.username == "henry"


def test_wrong_password_rejected(session, henry):
    assert service.authenticate(session, "henry", "nope") is None


def test_unknown_user_rejected(session):
    assert service.authenticate(session, "ghost", "pw") is None


def test_duplicate_username_raises(session, henry):
    with pytest.raises(ValueError):
        service.create_user(session, "henry", "other")


def test_inactive_user_rejected(session, henry):
    service.set_active(session, henry, False)
    assert service.authenticate(session, "henry", "pw12345") is None


def test_lockout_after_5_failures(session, henry):
    for _ in range(5):
        assert service.authenticate(session, "henry", "bad", now=100.0) is None
    # locked: even the CORRECT password fails inside the 60s window
    assert service.authenticate(session, "henry", "pw12345", now=130.0) is None
    # after the window it works again
    assert service.authenticate(session, "henry", "pw12345", now=161.0) is not None


def test_success_clears_failures(session, henry):
    for _ in range(4):
        service.authenticate(session, "henry", "bad", now=100.0)
    assert service.authenticate(session, "henry", "pw12345", now=101.0) is not None
    # counter reset — 4 more failures still below the threshold
    for _ in range(4):
        service.authenticate(session, "henry", "bad", now=102.0)
    assert service.authenticate(session, "henry", "pw12345", now=103.0) is not None


def test_reset_password_bumps_token_version(session, henry):
    old_tv = henry.token_version
    service.reset_password(session, henry, "newpw999")
    assert henry.token_version == old_tv + 1
    assert service.authenticate(session, "henry", "newpw999") is not None


def test_change_password_needs_old(session, henry):
    assert service.change_password(session, henry, "WRONG", "x") is False
    assert service.change_password(session, henry, "pw12345", "newpw999") is True
    assert service.authenticate(session, "henry", "newpw999") is not None


def test_list_users_sorted(session, henry):
    service.create_user(session, "amy", "pw")
    names = [u.username for u in service.list_users(session)]
    assert names == ["amy", "henry"]


def test_set_color_scheme_valid_persists(session, henry):
    assert service.set_color_scheme(session, henry, "red_up") is True
    reloaded = service.get_by_username(session, "henry")
    assert reloaded.chart_color_scheme == "red_up"


def test_set_color_scheme_rejects_invalid(session, henry):
    assert service.set_color_scheme(session, henry, "rainbow") is False
    reloaded = service.get_by_username(session, "henry")
    assert reloaded.chart_color_scheme is None  # 未寫入


def test_set_color_scheme_does_not_bump_token_version(session, henry):
    before = henry.token_version
    service.set_color_scheme(session, henry, "green_up")
    assert henry.token_version == before


def test_set_theme_valid_persists(session, henry):
    assert service.set_theme(session, henry, "light") is True
    reloaded = session.get(User, henry.id)
    assert reloaded.theme == "light"


def test_set_theme_rejects_invalid(session, henry):
    assert service.set_theme(session, henry, "neon") is False
    reloaded = session.get(User, henry.id)
    assert reloaded.theme is None  # 未寫入


def test_set_theme_does_not_bump_token_version(session, henry):
    before = henry.token_version
    service.set_theme(session, henry, "dark")
    assert session.get(User, henry.id).token_version == before
