import datetime as dt
from datetime import timedelta

from sqlmodel import Session, select

from quanquant.auth import service as auth_service
from quanquant.auth.agent_tokens import get_active_token, issue_token, validate_token
from quanquant.db.models import AgentToken


def _make_user(engine, username="owner"):
    with Session(engine) as s:
        return auth_service.create_user(s, username, "pw", role="admin")


def test_issue_token_returns_opaque_plaintext_and_persists_hash_only(engine):
    user = _make_user(engine)
    with Session(engine) as s:
        raw = issue_token(s, user_id=user.id, ttl_days=30)
    assert isinstance(raw, str) and len(raw) >= 32
    with Session(engine) as s:
        row = s.exec(select(AgentToken).where(AgentToken.user_id == user.id)).one()
        assert row.token_hash != raw           # 明文不落地
        assert row.revoked_at is None
        assert row.last_used_at is None


def test_validate_token_success_updates_last_used_at(engine):
    user = _make_user(engine)
    with Session(engine) as s:
        raw = issue_token(s, user_id=user.id, ttl_days=30)
    with Session(engine) as s:
        row = validate_token(s, raw=raw)
    assert row is not None
    assert row.user_id == user.id
    assert row.last_used_at is not None


def test_validate_token_unknown_raw_returns_none(engine):
    with Session(engine) as s:
        assert validate_token(s, raw="not-a-real-token") is None


def test_validate_token_empty_raw_returns_none(engine):
    with Session(engine) as s:
        assert validate_token(s, raw="") is None


def test_validate_token_expired_returns_none(engine):
    user = _make_user(engine)
    with Session(engine) as s:
        raw = issue_token(s, user_id=user.id, ttl_days=30)
        row = s.exec(select(AgentToken).where(AgentToken.user_id == user.id)).one()
        row.expires_at = dt.datetime.utcnow() - timedelta(seconds=1)
        s.add(row)
        s.commit()
    with Session(engine) as s:
        assert validate_token(s, raw=raw) is None


def test_validate_token_revoked_returns_none(engine):
    user = _make_user(engine)
    with Session(engine) as s:
        raw = issue_token(s, user_id=user.id, ttl_days=30)
        row = s.exec(select(AgentToken).where(AgentToken.user_id == user.id)).one()
        row.revoked_at = dt.datetime.utcnow()
        s.add(row)
        s.commit()
    with Session(engine) as s:
        assert validate_token(s, raw=raw) is None


def test_issue_token_rotation_invalidates_previous_and_keeps_new_valid(engine):
    user = _make_user(engine)
    with Session(engine) as s:
        raw1 = issue_token(s, user_id=user.id, ttl_days=30)
    with Session(engine) as s:
        raw2 = issue_token(s, user_id=user.id, ttl_days=30)
    assert raw1 != raw2
    with Session(engine) as s:
        assert validate_token(s, raw=raw1) is None
    with Session(engine) as s:
        assert validate_token(s, raw=raw2) is not None


def test_same_user_only_one_active_row_after_multiple_rotations(engine):
    user = _make_user(engine)
    with Session(engine) as s:
        issue_token(s, user_id=user.id, ttl_days=30)
        issue_token(s, user_id=user.id, ttl_days=30)
        issue_token(s, user_id=user.id, ttl_days=30)
    with Session(engine) as s:
        active = s.exec(
            select(AgentToken).where(AgentToken.user_id == user.id, AgentToken.revoked_at.is_(None))
        ).all()
        assert len(active) == 1
        all_rows = s.exec(select(AgentToken).where(AgentToken.user_id == user.id)).all()
        assert len(all_rows) == 3   # 舊列保留（撤銷紀錄），非刪除


def test_rotation_does_not_affect_other_users_tokens(engine):
    user_a = _make_user(engine, "a")
    user_b = _make_user(engine, "b")
    with Session(engine) as s:
        raw_a = issue_token(s, user_id=user_a.id, ttl_days=30)
        raw_b = issue_token(s, user_id=user_b.id, ttl_days=30)
    with Session(engine) as s:
        # 再幫 a rotation 一次，b 的 token 不受影響。
        issue_token(s, user_id=user_a.id, ttl_days=30)
    with Session(engine) as s:
        assert validate_token(s, raw=raw_a) is None
    with Session(engine) as s:
        assert validate_token(s, raw=raw_b) is not None


def test_get_active_token_returns_latest_non_revoked(engine):
    user = _make_user(engine)
    with Session(engine) as s:
        assert get_active_token(s, user_id=user.id) is None
        issue_token(s, user_id=user.id, ttl_days=30)
    with Session(engine) as s:
        row = get_active_token(s, user_id=user.id)
        assert row is not None and row.revoked_at is None
