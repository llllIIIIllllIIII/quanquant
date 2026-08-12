import datetime as dt
import hashlib

import pytest
from sqlmodel import select

from quanquant.auth import service as auth_service
from quanquant.auth.agent_tokens import validate_token
from quanquant.auth.device_flow import claim_and_issue_device_token, create_device_code, poll_device_token
from quanquant.db.models import AgentDeviceCode, _utcnow


def _approved_row(session, user, *, verifier="v" * 43, interval=5):
    challenge = hashlib.sha256(verifier.encode()).hexdigest()
    raw, row = create_device_code(session, request_ip="203.0.113.5", code_challenge=challenge)
    row.status, row.user_id, row.current_interval = "approved", user.id, interval
    session.add(row)
    session.commit()
    return raw, row, verifier


def test_poll_wrong_verifier_returns_invalid_and_zero_side_effects(session, user):
    raw, row, _ = _approved_row(session, user)
    before = row.consecutive_violations
    result = poll_device_token(session, device_code=raw, code_verifier="wrong", ttl_days=30)
    assert result == {"state": "invalid"}
    session.refresh(row)
    assert row.consecutive_violations == before and row.last_polled_at is None
    assert row.consumed_at is None and row.status == "approved"


def test_poll_unknown_device_code_returns_invalid(session):
    assert poll_device_token(session, device_code="nope", code_verifier="v", ttl_days=30) == {"state": "invalid"}


def test_poll_pending_row_returns_pending_with_interval(session, user):
    challenge = hashlib.sha256(b"v").hexdigest()
    raw, row = create_device_code(session, request_ip="203.0.113.5", code_challenge=challenge)
    result = poll_device_token(session, device_code=raw, code_verifier="v", ttl_days=30)
    assert result == {"state": "pending", "interval": 5}


def test_poll_approved_row_claims_token_and_reports_metadata(session, user):
    raw, row, verifier = _approved_row(session, user, interval=0)
    result = poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30)
    assert result["state"] == "approved"
    assert result["profile_id"] == str(user.id)
    assert result["username"] == user.username
    assert validate_token(session, raw=result["token"]) is not None


def test_poll_after_consumed_is_readonly_idempotent(session, user):
    raw, row, verifier = _approved_row(session, user, interval=0)
    first = poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30)
    assert first["state"] == "approved"
    second = poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30)
    third = poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30)
    assert second == {"state": "consumed"} == third


def test_poll_expired_terminal_state(session, user):
    raw, row, verifier = _approved_row(session, user, interval=0)
    row.expires_at = _utcnow() - dt.timedelta(seconds=1)
    session.add(row); session.commit()
    assert poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30) == {"state": "expired"}


def test_poll_denied_terminal_state(session, user):
    raw, row, verifier = _approved_row(session, user, interval=0)
    row.status = "denied"
    session.add(row); session.commit()
    assert poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30) == {"state": "denied"}


def test_slow_down_escalates_interval_and_blocks_after_five_violations(session, user):
    raw, row, verifier = _approved_row(session, user, interval=30)  # 30s 起跳，之後每次都算太快
    for i in range(5):
        result = poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30)
        assert result["state"] == "slow_down"
    session.refresh(row)
    assert row.blocked_until is not None and row.blocked_until > _utcnow()
    assert row.consecutive_violations == 0  # 達 5 次後歸零
    blocked = poll_device_token(session, device_code=raw, code_verifier=verifier, ttl_days=30)
    assert blocked["state"] == "slow_down" and "blocked_until" in blocked


def test_concurrent_claim_only_one_winner(session, user):
    """R3-1 精神的併發搶占測試：兩個獨立 Session 對同一列同時呼叫 claim，只有一方成功。"""
    raw, row, verifier = _approved_row(session, user, interval=0)
    from sqlmodel import Session
    with Session(session.get_bind()) as s2:
        r1 = claim_and_issue_device_token(session, device_code_id=row.id, user_id=user.id, ttl_days=30)
        r2 = claim_and_issue_device_token(s2, device_code_id=row.id, user_id=user.id, ttl_days=30)
    assert (r1 is None) != (r2 is None)  # 恰一方贏
