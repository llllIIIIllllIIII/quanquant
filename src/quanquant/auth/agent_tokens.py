"""Per-user 本機 broker agent WS 憑證（D2）：DB opaque token，取代 Inc0 的全站共用靜態
密鑰 `AGENT_WS_TOKEN`。

明文（`secrets.token_urlsafe(32)`）只在 `issue_token` 回傳當下存在一次——呼叫端（route）
負責把它顯示給使用者，之後永遠拿不回來；DB 只存 `sha256` hash（見 `AgentToken.token_hash`，
`db/models.py`），查表比對用 hash 相等即可（不需要 `secrets.compare_digest`——這是 DB 索引
等值查詢，不是 Python 記憶體內字串比對，沒有 timing side-channel 的同一類疑慮）。

rotation 語意（每 user 同時只有一枚有效 token）：`issue_token` 簽發新枚前，把該 user
「目前仍有效」（`revoked_at IS NULL`）的舊列全部標 `revoked_at=now`——已經撤銷過的列維持
原本的撤銷時間不動（不重寫歷史），實務上這個集合任何時刻最多只有一列（不變量本身保證）。
"""
import hashlib
import secrets
from datetime import timedelta

from sqlmodel import Session, select

from quanquant.db.models import AgentToken, _utcnow


def _hash(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def issue_token(session: Session, *, user_id: int, ttl_days: int) -> str:
    """簽發（或 rotation）agent token：回傳明文（只有這一次），DB 只存 hash。"""
    now = _utcnow()
    active_rows = session.exec(
        select(AgentToken).where(
            AgentToken.user_id == user_id,
            AgentToken.revoked_at.is_(None),
        )
    ).all()
    for row in active_rows:
        row.revoked_at = now
        session.add(row)
    raw = secrets.token_urlsafe(32)
    token = AgentToken(
        user_id=user_id, token_hash=_hash(raw), expires_at=now + timedelta(days=ttl_days),
    )
    session.add(token)
    session.commit()
    session.refresh(token)
    return raw


def validate_token(session: Session, *, raw: str) -> AgentToken | None:
    """sha256 查表：查無/過期/撤銷一律回 None；命中則更新 `last_used_at`（同一交易 commit）。"""
    if not raw:
        return None
    token = session.exec(
        select(AgentToken).where(AgentToken.token_hash == _hash(raw))
    ).first()
    if token is None:
        return None
    now = _utcnow()
    if token.revoked_at is not None or token.expires_at <= now:
        return None
    token.last_used_at = now
    session.add(token)
    session.commit()
    session.refresh(token)
    return token


def get_active_token(session: Session, *, user_id: int) -> AgentToken | None:
    """供 UI（orders 頁 token 管理段）顯示現況用：該 user 目前未撤銷的那一枚（可能已過期——
    過期與否交給呼叫端以 `expires_at` 自行判斷、顯示「請重新產生」，這裡不因過期就消失，
    讓 owner 在畫面上仍看得到「上一枚何時到期」）。"""
    return session.exec(
        select(AgentToken)
        .where(AgentToken.user_id == user_id, AgentToken.revoked_at.is_(None))
        .order_by(AgentToken.created_at.desc())
    ).first()
