import datetime as dt
import hashlib
import threading
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


# ---- C10（LOW，codex 終審）：併發 rotation 撞
# `uq_agent_tokens_active_per_user`——`issue_token` 撞鍵安全重試一次，最終仍恰一枚有效
# token。----


def test_issue_token_concurrent_rotation_retries_and_leaves_exactly_one_active(tmp_path):
    """真雙連線＋`threading.Barrier` 逼近併發 rotation——兩個獨立 session 幾乎同時對同一個
    user 呼叫 `issue_token()`，`uq_agent_tokens_active_per_user` partial unique index 讓
    其中一個在 commit 時撞鍵，`issue_token` 內建的撞鍵安全重試（重讀 active rows、把贏家
    的新 token 也一併 revoke、再 insert 自己這枚）必須讓最終結果恰有一枚有效 token（不是
    兩枚，也不是撞鍵後就地放棄拋出例外）。改用檔案 SQLite（獨立連線）而非 in-memory：
    StaticPool 共用單一底層連線會讓兩執行緒同時 flush 產生與本測試無關的 identity-map
    交錯錯誤（比照 `test_agent_commands.py::test_update_singleflight_concurrent_two_
    writers_exactly_one_wins` 既有理由）。"""
    from sqlmodel import SQLModel, create_engine

    db_path = tmp_path / "token_rotation_concurrency.db"
    eng = create_engine(f"sqlite:///{db_path}", connect_args={"timeout": 30})
    SQLModel.metadata.create_all(eng)

    with Session(eng) as s:
        user = auth_service.create_user(s, "concurrent-owner", "pw", role="admin")
        user_id = user.id

    results: list[str] = []
    barrier = threading.Barrier(2)

    def _attempt() -> None:
        with Session(eng) as s:
            barrier.wait(timeout=5)
            raw = issue_token(s, user_id=user_id, ttl_days=30)
            results.append(raw)

    t1 = threading.Thread(target=_attempt)
    t2 = threading.Thread(target=_attempt)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert len(results) == 2 and results[0] != results[1]  # 兩次呼叫都成功回傳、各自不同明文
    with Session(eng) as s:
        active = s.exec(
            select(AgentToken).where(AgentToken.user_id == user_id, AgentToken.revoked_at.is_(None))
        ).all()
        assert len(active) == 1  # 恰一枚有效——沒有因為併發撞鍵而留下兩枚同時有效的 token
        active_hash = active[0].token_hash
        assert active_hash in {hashlib.sha256(r.encode("utf-8")).hexdigest() for r in results}


def test_get_active_token_returns_latest_non_revoked(engine):
    user = _make_user(engine)
    with Session(engine) as s:
        assert get_active_token(s, user_id=user.id) is None
        issue_token(s, user_id=user.id, ttl_days=30)
    with Session(engine) as s:
        row = get_active_token(s, user_id=user.id)
        assert row is not None and row.revoked_at is None


def test_stage_rotation_does_not_commit(session, user):
    from quanquant.auth.agent_tokens import get_active_token, stage_rotation

    raw, token = stage_rotation(session, user_id=user.id, ttl_days=30)
    assert raw and token.token_hash
    session.rollback()
    # rollback 撤銷了尚未 commit 的 insert——沒有任何 active token 留下
    assert get_active_token(session, user_id=user.id) is None


def test_issue_token_still_rotates_via_stage_rotation(session, user):
    from quanquant.auth.agent_tokens import issue_token, validate_token

    first = issue_token(session, user_id=user.id, ttl_days=30)
    second = issue_token(session, user_id=user.id, ttl_days=30)
    assert first != second
    assert validate_token(session, raw=first) is None       # 舊枚已撤銷
    assert validate_token(session, raw=second) is not None  # 新枚有效
