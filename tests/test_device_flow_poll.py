import datetime as dt
import hashlib
import threading

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from quanquant.auth.agent_tokens import validate_token
from quanquant.auth.device_flow import claim_and_issue_device_token, create_device_code, poll_device_token
from quanquant.db.models import AgentDeviceCode, AgentToken, User, _utcnow


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


def test_sequential_claim_second_call_after_commit_is_rejected(session, user):
    """注意：這裡兩次 claim 呼叫是在同一個 StaticPool 底層連線上**依序**（非真併發）
    執行——它驗證的是 conditional UPDATE 對『已 commit 狀態』的冪等拒絕語意（第二次呼叫
    時 consumed_at 已非 NULL，WHERE 條件不命中、rowcount=0，直接回 None），不是 in-flight
    race-safety 的實證。真執行緒級併發搶占測試見下方
    test_concurrent_claim_only_one_winner_thread_level_race（file-based SQLite 兩條真連線
    ＋threading.Barrier 對齊起跑）。"""
    raw, row, verifier = _approved_row(session, user, interval=0)
    with Session(session.get_bind()) as s2:
        r1 = claim_and_issue_device_token(session, device_code_id=row.id, user_id=user.id, ttl_days=30)
        r2 = claim_and_issue_device_token(s2, device_code_id=row.id, user_id=user.id, ttl_days=30)
    assert (r1 is None) != (r2 is None)  # 恰一方贏


@pytest.mark.parametrize("attempt", range(8))
def test_concurrent_claim_only_one_winner_thread_level_race(tmp_path, attempt):
    """R3-1 的真執行緒級併發搶占證明：file-based SQLite（非 conftest 的 StaticPool 共用
    單連線——這裡兩個 thread 各自開獨立 Session、各自一條真連線）＋busy_timeout（sqlite
    connect `timeout`，秒）避免兩條連線互撞鎖時被誤判成 'database is locked' 而非真正的
    搶占失敗；threading.Barrier 讓兩個 thread 對齊起跑點，盡量逼近同時呼叫
    claim_and_issue_device_token。斷言：恰一方拿到 (raw, token)（相當於 approved 終態）、
    另一方拿到 None（conditional UPDATE 的 WHERE consumed_at IS NULL AND status='approved'
    不命中，相當於 consumed 終態）；DB 裡該 user 的 active token（revoked_at IS NULL）
    恰一枚，device code 列 consumed_at 已寫入。parametrize 8 次覆蓋排程順序的隨機性。"""
    engine = create_engine(f"sqlite:///{tmp_path / 'race.db'}", connect_args={"timeout": 30})
    SQLModel.metadata.create_all(engine)

    with Session(engine) as setup:
        user = User(username="race-user", display_name="Race", password_hash="x")
        setup.add(user)
        setup.commit()
        setup.refresh(user)
        user_id = user.id

        verifier = "v" * 43
        challenge = hashlib.sha256(verifier.encode()).hexdigest()
        raw, row = create_device_code(setup, request_ip="203.0.113.9", code_challenge=challenge)
        row.status, row.user_id, row.current_interval = "approved", user_id, 0
        setup.add(row)
        setup.commit()
        device_code_id = row.id

    barrier = threading.Barrier(2)
    results = [None, None]
    errors = []

    def _worker(idx):
        try:
            with Session(engine) as s:
                barrier.wait(timeout=5)
                results[idx] = claim_and_issue_device_token(
                    s, device_code_id=device_code_id, user_id=user_id, ttl_days=30,
                )
        except Exception as exc:  # 意外例外要浮現，不能被 thread 靜靜吞掉
            errors.append(exc)

    threads = [threading.Thread(target=_worker, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert not errors, f"worker thread(s) raised: {errors}"
    winners = [r for r in results if r is not None]
    losers = [r for r in results if r is None]
    assert len(winners) == 1 and len(losers) == 1  # 恰一方贏，另一方拿到終態拒絕（None）

    with Session(engine) as check:
        active_tokens = check.exec(
            select(AgentToken).where(AgentToken.user_id == user_id, AgentToken.revoked_at.is_(None))
        ).all()
        assert len(active_tokens) == 1  # 只有贏家那次 stage_rotation 真的 commit 了

        final_row = check.get(AgentDeviceCode, device_code_id)
        assert final_row.consumed_at is not None
