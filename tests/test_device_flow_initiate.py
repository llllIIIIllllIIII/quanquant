import datetime as dt

import pytest
from sqlmodel import select

from quanquant.auth.device_flow import (
    count_active_pending_for_ip, count_active_pending_global,
    create_device_code, generate_user_code,
)
from quanquant.db.models import AgentDeviceCode, _utcnow


def test_generate_user_code_format_excludes_confusing_chars():
    for _ in range(200):
        code = generate_user_code()
        assert len(code) == 9 and code[4] == "-"
        assert not (set(code) & set("0O1I"))


def test_create_device_code_persists_hash_not_plaintext(session):
    raw, row = create_device_code(session, request_ip="203.0.113.5", code_challenge="c" * 64)
    assert row.device_code_hash != raw and len(row.device_code_hash) == 64
    assert row.status == "pending" and row.request_ip == "203.0.113.5"
    assert row.code_challenge == "c" * 64


def test_create_device_code_deletes_stale_expired_rows(session):
    stale = AgentDeviceCode(
        device_code_hash="s" * 64, code_challenge="c" * 64, user_code="STAL-EONE",
        request_ip="203.0.113.5", expires_at=_utcnow() - dt.timedelta(days=2),
    )
    session.add(stale)
    session.commit()
    create_device_code(session, request_ip="203.0.113.5", code_challenge="c" * 64)
    remaining = session.exec(select(AgentDeviceCode).where(AgentDeviceCode.user_code == "STAL-EONE")).first()
    assert remaining is None


def test_count_active_pending_for_ip_only_counts_unexpired_pending(session):
    create_device_code(session, request_ip="203.0.113.5", code_challenge="c" * 64)
    expired = AgentDeviceCode(
        device_code_hash="e" * 64, code_challenge="c" * 64, user_code="EXPI-REDX",
        request_ip="203.0.113.5", expires_at=_utcnow() - dt.timedelta(seconds=1),
    )
    session.add(expired)
    session.commit()
    assert count_active_pending_for_ip(session, request_ip="203.0.113.5") == 1
    assert count_active_pending_for_ip(session, request_ip="198.51.100.1") == 0
    assert count_active_pending_global(session) == 1
