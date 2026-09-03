"""010：DailyReview repository——⑦ 唯一鍵與 mode 分離、⑧ 快照凍結。"""
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from quanquant.journal import review_repository as reviews

_SNAPSHOT_A = {
    "pnl": Decimal("12400"), "trade_count": 8, "daily_quota": 20,
    "win_rate": 0.625, "max_losing_streak": 1, "kill_switch_count": 0,
}
_SNAPSHOT_B = {
    "pnl": Decimal("99999"), "trade_count": 99, "daily_quota": 999,
    "win_rate": 0.1, "max_losing_streak": 9, "kill_switch_count": 9,
}


def test_get_review_missing_returns_none(session, user):
    assert reviews.get_review(session, user_id=user.id, mode="real", trading_day="2026-09-02") is None


def test_save_review_creates_new_row(session, user):
    row = reviews.save_review(
        session, user_id=user.id, mode="real", trading_day="2026-09-02",
        discipline_note="照計畫", emotion_note="穩", tomorrow_focus="少凹單",
        snapshot=_SNAPSHOT_A,
    )
    assert row.id is not None
    assert row.snapshot_pnl == Decimal("12400")
    assert row.snapshot_trade_count == 8
    assert row.snapshot_kill_switch_count == 0


def test_kill_switch_count_none_means_no_data_source(session, user):
    """驗收條件：查無資料源顯示「—」，不可顯示假 0——用 None 表示。"""
    snap = dict(_SNAPSHOT_A, kill_switch_count=None)
    row = reviews.save_review(
        session, user_id=user.id, mode="real", trading_day="2026-09-02",
        discipline_note=None, emotion_note=None, tomorrow_focus=None, snapshot=snap,
    )
    assert row.snapshot_kill_switch_count is None


# ---- ⑦ 唯一鍵與 mode 分離 ----

def test_unique_per_user_mode_trading_day_upserts_not_duplicates(session, user):
    reviews.save_review(session, user_id=user.id, mode="real", trading_day="2026-09-02",
                         discipline_note="A", emotion_note=None, tomorrow_focus=None, snapshot=_SNAPSHOT_A)
    reviews.save_review(session, user_id=user.id, mode="real", trading_day="2026-09-02",
                         discipline_note="B", emotion_note=None, tomorrow_focus=None, snapshot=_SNAPSHOT_B)

    rows = reviews.list_reviews(session, user_id=user.id, mode="real")
    assert len(rows) == 1
    assert rows[0].discipline_note == "B"


def test_sim_and_real_are_separate_rows_for_same_trading_day(session, user):
    reviews.save_review(session, user_id=user.id, mode="real", trading_day="2026-09-02",
                         discipline_note="real-note", emotion_note=None, tomorrow_focus=None,
                         snapshot=_SNAPSHOT_A)
    reviews.save_review(session, user_id=user.id, mode="sim", trading_day="2026-09-02",
                         discipline_note="sim-note", emotion_note=None, tomorrow_focus=None,
                         snapshot=_SNAPSHOT_B)

    real_row = reviews.get_review(session, user_id=user.id, mode="real", trading_day="2026-09-02")
    sim_row = reviews.get_review(session, user_id=user.id, mode="sim", trading_day="2026-09-02")
    assert real_row.discipline_note == "real-note"
    assert sim_row.discipline_note == "sim-note"
    assert real_row.id != sim_row.id


def test_different_trading_days_are_separate_rows(session, user):
    reviews.save_review(session, user_id=user.id, mode="real", trading_day="2026-09-01",
                         discipline_note=None, emotion_note=None, tomorrow_focus=None, snapshot=_SNAPSHOT_A)
    reviews.save_review(session, user_id=user.id, mode="real", trading_day="2026-09-02",
                         discipline_note=None, emotion_note=None, tomorrow_focus=None, snapshot=_SNAPSHOT_A)
    assert len(reviews.list_reviews(session, user_id=user.id, mode="real")) == 2


def test_fresh_db_enforces_unique_constraint_at_db_layer(engine, user):
    """C2 慣例：fresh DB（create_all）也要有唯一鍵防呆，不能只靠 app 層 create-or-update。"""
    sql = (
        "INSERT INTO daily_reviews (user_id, mode, trading_day, created_at, updated_at) "
        "VALUES ({uid}, 'real', '2026-09-02', '2026-09-02 00:00:00', '2026-09-02 00:00:00')"
    )
    with engine.begin() as conn:
        conn.execute(text(sql.format(uid=user.id)))
    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(text(sql.format(uid=user.id)))


def test_fresh_db_rejects_illegal_mode(engine, user):
    sql = (
        "INSERT INTO daily_reviews (user_id, mode, trading_day, created_at, updated_at) "
        f"VALUES ({user.id}, 'paper', '2026-09-02', '2026-09-02 00:00:00', '2026-09-02 00:00:00')"
    )
    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(text(sql))


# ---- ⑧ 快照凍結：編輯主觀欄位不改動客觀快照 ----

def test_second_save_does_not_overwrite_snapshot(session, user):
    reviews.save_review(session, user_id=user.id, mode="real", trading_day="2026-09-02",
                         discipline_note="第一次", emotion_note=None, tomorrow_focus=None,
                         snapshot=_SNAPSHOT_A)
    updated = reviews.save_review(
        session, user_id=user.id, mode="real", trading_day="2026-09-02",
        discipline_note="改過了", emotion_note="也改了", tomorrow_focus="調整這個",
        snapshot=_SNAPSHOT_B,  # 傳入不同的快照，預期被忽略
    )
    assert updated.discipline_note == "改過了"
    assert updated.emotion_note == "也改了"
    assert updated.tomorrow_focus == "調整這個"
    # 客觀快照仍是第一次儲存時凍結的 A，不是這次傳入的 B
    assert updated.snapshot_pnl == _SNAPSHOT_A["pnl"]
    assert updated.snapshot_trade_count == _SNAPSHOT_A["trade_count"]
    assert updated.snapshot_daily_quota == _SNAPSHOT_A["daily_quota"]
    assert updated.snapshot_win_rate == _SNAPSHOT_A["win_rate"]
    assert updated.snapshot_max_losing_streak == _SNAPSHOT_A["max_losing_streak"]
    assert updated.snapshot_kill_switch_count == _SNAPSHOT_A["kill_switch_count"]


# ---- 終審 MEDIUM-3：save_review 的 check-then-insert 競態——INSERT 撞唯一鍵要 fallback
# 成 UPDATE，不可 500 ----

def test_concurrent_first_save_race_falls_back_to_update_not_500(session, user, monkeypatch):
    """模擬兩個請求同時判定「今天還沒有復盤」都想 INSERT：先讓真正的一列存在於 DB，
    再讓 `save_review` 內部第一次呼叫 `get_review` 假裝查無（重現 TOCTOU 窗口），逼它真的
    走到 INSERT → 撞 `uq_daily_reviews_user_mode_day` → IntegrityError → rollback →
    重新查詢 → 改走 UPDATE 分支。"""
    reviews.save_review(
        session, user_id=user.id, mode="real", trading_day="2026-09-02",
        discipline_note="第一個請求先進來", emotion_note=None, tomorrow_focus=None,
        snapshot=_SNAPSHOT_A,
    )

    original_get_review = reviews.get_review
    calls = {"n": 0}

    def _fake_get_review(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return None  # 第一次查詢時假裝「還沒有」，逼近真實碰撞路徑
        return original_get_review(*args, **kwargs)

    monkeypatch.setattr(reviews, "get_review", _fake_get_review)

    result = reviews.save_review(
        session, user_id=user.id, mode="real", trading_day="2026-09-02",
        discipline_note="第二個請求（撞號）", emotion_note=None, tomorrow_focus=None,
        snapshot=_SNAPSHOT_B,
    )

    assert result.discipline_note == "第二個請求（撞號）"
    # 快照仍是第一個真正建立時凍結的 A，不是撞號那次帶的 B（fallback 走 UPDATE 分支，
    # 不動 snapshot_*，與一般的第二次儲存同一套凍結語意）。
    assert result.snapshot_pnl == _SNAPSHOT_A["pnl"]
    assert result.snapshot_trade_count == _SNAPSHOT_A["trade_count"]
    # 沒有變成兩列
    assert len(reviews.list_reviews(session, user_id=user.id, mode="real")) == 1


def test_list_reviews_ordered_newest_trading_day_first(session, user):
    reviews.save_review(session, user_id=user.id, mode="real", trading_day="2026-09-01",
                         discipline_note=None, emotion_note=None, tomorrow_focus=None, snapshot=_SNAPSHOT_A)
    reviews.save_review(session, user_id=user.id, mode="real", trading_day="2026-09-03",
                         discipline_note=None, emotion_note=None, tomorrow_focus=None, snapshot=_SNAPSHOT_A)
    reviews.save_review(session, user_id=user.id, mode="real", trading_day="2026-09-02",
                         discipline_note=None, emotion_note=None, tomorrow_focus=None, snapshot=_SNAPSHOT_A)
    rows = reviews.list_reviews(session, user_id=user.id, mode="real")
    assert [r.trading_day for r in rows] == ["2026-09-03", "2026-09-02", "2026-09-01"]
