import pytest
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.schema import CreateTable
from sqlmodel import select

from quanquant.db.models import AgentDeviceCode


def test_agent_device_code_ddl_compiles_on_both_dialects_with_status_check():
    for dialect in (sqlite.dialect(), postgresql.dialect()):
        ddl = str(CreateTable(AgentDeviceCode.__table__).compile(dialect=dialect))
        assert "agent_device_codes" in ddl
        assert "ck_agent_device_codes_status" in ddl


def test_agent_device_code_defaults_and_roundtrip(session):
    import datetime as dt

    row = AgentDeviceCode(
        device_code_hash="h" * 64, code_challenge="c" * 64, user_code="ABCD-EFGH",
        request_ip="203.0.113.5", expires_at=dt.datetime(2099, 1, 1),
    )
    session.add(row)
    session.commit()
    got = session.exec(select(AgentDeviceCode)).one()
    assert got.status == "pending"
    assert got.current_interval == 5
    assert got.consecutive_violations == 0
    assert got.user_id is None and got.consumed_at is None and got.blocked_until is None


def test_agent_device_code_rejects_invalid_status_value(session):
    import datetime as dt
    from sqlalchemy.exc import IntegrityError

    row = AgentDeviceCode(
        device_code_hash="x" * 64, code_challenge="c" * 64, user_code="ZZZZ-9999",
        request_ip="127.0.0.1", status="bogus", expires_at=dt.datetime(2099, 1, 1),
    )
    session.add(row)
    with pytest.raises(IntegrityError):
        session.commit()
