"""010：每日復盤（`DailyReview`）CRUD。

Create-or-update 語意（快照凍結，008/010 驗收條件⑧）：`save_review` 第一次呼叫（該
`(user_id, mode, trading_day)` 尚無資料列）才把 `snapshot` 寫進 DB；之後任何一次呼叫
只更新三個主觀欄位與 `updated_at`，**完全不碰**已存在列的 `snapshot_*` 欄位——即使呼叫端
傳入不同的 `snapshot`，也會被忽略。這是刻意設計，不是遺漏：避免「事後補單／改單」讓
歷史復盤的客觀數字被悄悄改寫。

競態處理（終審 MEDIUM-3，2026-09-04）：`get_review` 判定「不存在」到真正 INSERT 之間
有 TOCTOU 窗口——同一使用者連點兩次「儲存」（或兩個分頁）可能都判定「不存在」而都嘗試
INSERT，其中一個會撞 `uq_daily_reviews_user_mode_day` 唯一鍵。比照
`broker/repository.py` 既有慣例：INSERT 包 try/except `IntegrityError` → rollback →
重新查詢確認撞到的正是這個 `(user_id, mode, trading_day)` 唯一鍵，才改走 UPDATE 分支
（不可讓使用者看到 500）；查無則代表另有原因（如 mode CHECK），重新拋出、不吞。
"""
from datetime import datetime, timezone

from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from quanquant.db.models import DailyReview


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def get_review(session: Session, *, user_id: int, mode: str, trading_day: str) -> DailyReview | None:
    stmt = select(DailyReview).where(
        DailyReview.user_id == user_id,
        DailyReview.mode == mode,
        DailyReview.trading_day == trading_day,
    )
    return session.exec(stmt).first()


def _apply_subjective(
    review: DailyReview, *, discipline_note: str | None, emotion_note: str | None, tomorrow_focus: str | None,
) -> DailyReview:
    review.discipline_note = discipline_note
    review.emotion_note = emotion_note
    review.tomorrow_focus = tomorrow_focus
    review.updated_at = _utcnow()
    # snapshot_* 刻意不動——見模組頂部快照凍結說明。
    return review


def _update_existing(session: Session, review: DailyReview, **subjective) -> DailyReview:
    session.add(_apply_subjective(review, **subjective))
    session.commit()
    session.refresh(review)
    return review


def save_review(
    session: Session,
    *,
    user_id: int,
    mode: str,
    trading_day: str,
    discipline_note: str | None,
    emotion_note: str | None,
    tomorrow_focus: str | None,
    snapshot: dict,
) -> DailyReview:
    """建立或更新（見模組頂部的快照凍結語意與競態處理）。`snapshot` 鍵：`pnl`/
    `trade_count`/`daily_quota`/`win_rate`/`max_losing_streak`/`kill_switch_count`
    （皆可為 None，`kill_switch_count=None` 代表查無資料源，見 db/models.py::DailyReview
    類別註解）。"""
    subjective = {
        "discipline_note": discipline_note, "emotion_note": emotion_note, "tomorrow_focus": tomorrow_focus,
    }
    existing = get_review(session, user_id=user_id, mode=mode, trading_day=trading_day)
    if existing is not None:
        return _update_existing(session, existing, **subjective)

    review = DailyReview(
        user_id=user_id,
        mode=mode,
        trading_day=trading_day,
        discipline_note=discipline_note,
        emotion_note=emotion_note,
        tomorrow_focus=tomorrow_focus,
        snapshot_pnl=snapshot.get("pnl"),
        snapshot_trade_count=snapshot.get("trade_count"),
        snapshot_daily_quota=snapshot.get("daily_quota"),
        snapshot_win_rate=snapshot.get("win_rate"),
        snapshot_max_losing_streak=snapshot.get("max_losing_streak"),
        snapshot_kill_switch_count=snapshot.get("kill_switch_count"),
    )
    session.add(review)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        raced = get_review(session, user_id=user_id, mode=mode, trading_day=trading_day)
        if raced is None:
            raise  # 不是唯一鍵撞號（如 mode CHECK 之類）——另有原因，不吞
        return _update_existing(session, raced, **subjective)
    session.refresh(review)
    return review


def list_reviews(
    session: Session, *, user_id: int, mode: str = "real",
    date_from: str | None = None, date_to: str | None = None,
) -> list[DailyReview]:
    """交易日記頁讀入口：依 trading_day 新到舊排序。"""
    stmt = select(DailyReview).where(DailyReview.user_id == user_id, DailyReview.mode == mode)
    if date_from:
        stmt = stmt.where(DailyReview.trading_day >= date_from)
    if date_to:
        stmt = stmt.where(DailyReview.trading_day <= date_to)
    stmt = stmt.order_by(DailyReview.trading_day.desc())  # type: ignore[union-attr]
    return list(session.exec(stmt))
