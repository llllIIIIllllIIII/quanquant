"""Per-user 本機 broker agent WS 憑證（D2）：DB opaque token，取代 Inc0 的全站共用靜態
密鑰 `AGENT_WS_TOKEN`。

明文（`secrets.token_urlsafe(32)`）只在 `issue_token` 回傳當下存在一次——呼叫端（route）
負責把它顯示給使用者，之後永遠拿不回來；DB 只存 `sha256` hash（見 `AgentToken.token_hash`，
`db/models.py`），查表比對用 hash 相等即可（不需要 `secrets.compare_digest`——這是 DB 索引
等值查詢，不是 Python 記憶體內字串比對，沒有 timing side-channel 的同一類疑慮）。

rotation 語意（每 user 同時只有一枚有效 token）：`issue_token` 簽發新枚前，把該 user
「目前仍有效」（`revoked_at IS NULL`）的舊列全部標 `revoked_at=now`——已經撤銷過的列維持
原本的撤銷時間不動（不重寫歷史）。

C10（LOW，codex 終審）：這個不變量過去**只**靠應用層「先 revoke 再 insert」維持——併發
rotation（同一 user 兩個請求幾乎同時呼叫 `issue_token`）下，兩邊都可能在對方尚未 commit
前讀到「目前無 active row」，各自 insert 一筆，DB 端（`token_hash unique` 之外）沒有任何
約束擋下，會產生兩枚同時有效的 token。修復雙管齊下：(1) `db/models.py` 加
`uq_agent_tokens_active_per_user` partial unique index（`user_id WHERE revoked_at IS
NULL`），DB 層強制恰一筆；(2) 這裡撞鍵安全重試一次——輸家 `IntegrityError` 後重讀 active
rows（此刻贏家的新 token 已可見）、把它也 revoke 掉、再 insert 自己這枚，語意等同「最後
commit 的呼叫者贏得 rotation」（rotation 本就是主觀上的「最新一次操作生效」，不是需要
公平排序的資源分配）。
"""
import hashlib
import secrets
from datetime import timedelta

from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from quanquant.db.models import AgentToken, _utcnow


def _hash(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def issue_token(session: Session, *, user_id: int, ttl_days: int) -> str:
    """簽發（或 rotation）agent token：回傳明文（只有這一次），DB 只存 hash。撞
    `uq_agent_tokens_active_per_user`（併發 rotation 輸家）安全重試一次，見模組頂部 C10
    說明；重試仍撞鍵（理論上不該發生——重試前已把當下所有 active row 一併 revoke）就原樣
    往外拋，不無限重試。"""
    for attempt in range(2):
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
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            if attempt == 1:
                raise
            continue
        session.refresh(token)
        return raw
    raise AssertionError("unreachable")  # pragma: no cover


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
