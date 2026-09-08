"""010：每日復盤——寫入口（交易績效頁，POST /stats/review）＋讀入口（交易日記頁）＋
快照凍結／緊急停止次數無資料源顯示「—」的端對端驗收。"""
import datetime as dt
from decimal import Decimal

from quanquant.journal import repository as repo
from quanquant.journal.schemas import TradeCreate
from quanquant.journal.trading_day import today_trading_day


def _closed(session, user_id, *, exit_time, pnl_sign=1, mode="real"):
    entry_time = exit_time - dt.timedelta(hours=1)
    entry_price = Decimal("18000")
    exit_price = entry_price + (Decimal("100") if pnl_sign >= 0 else Decimal("-100"))
    return repo.create_trade(
        session,
        TradeCreate(
            symbol="TXF", direction="long", entry_time=entry_time, entry_price=entry_price,
            exit_time=exit_time, exit_price=exit_price, size=1, point_value=Decimal("200"),
            mode=mode, source="shioaji",
        ),
        user_id=user_id,
    )


def test_kill_switch_count_shows_dash_not_fake_zero_when_unsaved(client):
    """驗收條件：查無資料源（kill switch 啟動次數目前無持久化紀錄）顯示「—」，不可顯示假 0。"""
    today = today_trading_day().isoformat()
    resp = client.get(f"/stats?date_from={today}&date_to={today}")
    assert "尚未填寫" in resp.text
    # 「系統帶入」列裡緊急停止那一格是 "—"，不是假 0
    assert "緊急停止 <b>—</b>" in resp.text
    assert "緊急停止 <b>0</b>" not in resp.text


def test_review_objective_no_longer_conflates_trade_count_with_order_quota(client):
    """終審 MEDIUM-2：已平倉 round-trip 筆數與委託次數日配額口徑不同（trading_day vs
    日曆日），不可塞進同一個「N 筆（日配額 M）」括號。"""
    today = today_trading_day().isoformat()
    resp = client.get(f"/stats?date_from={today}&date_to={today}")
    assert "日配額" not in resp.text
    assert "已平倉 <b>0</b> 筆" in resp.text


def test_save_review_then_page_shows_saved_state(client, session, user):
    today = today_trading_day()
    _closed(session, user.id, exit_time=dt.datetime.combine(today, dt.time(9, 0)))
    qs = f"date_from={today.isoformat()}&date_to={today.isoformat()}"

    resp = client.post("/stats/review", data={
        "mode": "real", "trading_day": today.isoformat(),
        "discipline_note": "照計畫", "emotion_note": "穩", "tomorrow_focus": "少凹單",
    })
    assert resp.status_code in (200, 303)

    page = client.get(f"/stats?{qs}")
    assert "已填寫" in page.text
    assert "照計畫" in page.text
    assert "少凹單" in page.text


def test_second_save_updates_subjective_but_freezes_snapshot(client, session, user):
    today = today_trading_day()
    _closed(session, user.id, exit_time=dt.datetime.combine(today, dt.time(9, 0)), pnl_sign=1)

    client.post("/stats/review", data={
        "mode": "real", "trading_day": today.isoformat(),
        "discipline_note": "第一次", "emotion_note": "", "tomorrow_focus": "",
    })

    # 儲存後才發生的一筆新交易（模擬事後補單）——不應該讓已凍結的快照跟著變動。
    _closed(session, user.id, exit_time=dt.datetime.combine(today, dt.time(11, 0)), pnl_sign=-1)

    client.post("/stats/review", data={
        "mode": "real", "trading_day": today.isoformat(),
        "discipline_note": "改過了", "emotion_note": "", "tomorrow_focus": "",
    })

    qs = f"date_from={today.isoformat()}&date_to={today.isoformat()}"
    page = client.get(f"/stats?{qs}")
    assert "改過了" in page.text
    assert "已平倉 <b>1</b> 筆" in page.text  # 快照仍是第一次儲存時的 1 筆，不是後來變成的 2 筆


def test_sim_and_real_reviews_are_independent(client, session, user):
    today = today_trading_day()
    _closed(session, user.id, exit_time=dt.datetime.combine(today, dt.time(9, 0)), mode="real")
    _closed(session, user.id, exit_time=dt.datetime.combine(today, dt.time(9, 0)), mode="sim")

    client.post("/stats/review", data={
        "mode": "real", "trading_day": today.isoformat(),
        "discipline_note": "real 復盤", "emotion_note": "", "tomorrow_focus": "",
    })

    qs = f"date_from={today.isoformat()}&date_to={today.isoformat()}"
    real_page = client.get(f"/stats?mode=real&{qs}")
    sim_page = client.get(f"/stats?mode=sim&{qs}")
    assert "已填寫" in real_page.text
    assert "尚未填寫" in sim_page.text


# ---- 終審 MEDIUM-3：POST /stats/review 輸入約束 ----

def test_save_review_rejects_note_over_length_cap(client):
    today = today_trading_day().isoformat()
    resp = client.post("/stats/review", data={
        "mode": "real", "trading_day": today,
        "discipline_note": "x" * 2001, "emotion_note": "", "tomorrow_focus": "",
    })
    assert resp.status_code == 400


def test_save_review_accepts_note_at_length_cap(client):
    today = today_trading_day().isoformat()
    resp = client.post("/stats/review", data={
        "mode": "real", "trading_day": today,
        "discipline_note": "x" * 2000, "emotion_note": "", "tomorrow_focus": "",
    })
    assert resp.status_code in (200, 303)


def test_save_review_rejects_future_trading_day(client):
    future = (today_trading_day() + dt.timedelta(days=30)).isoformat()
    resp = client.post("/stats/review", data={
        "mode": "real", "trading_day": future,
        "discipline_note": "", "emotion_note": "", "tomorrow_focus": "",
    })
    assert resp.status_code == 400


def test_save_review_accepts_today_and_past_trading_day(client, session, user):
    today = today_trading_day()
    past = today - dt.timedelta(days=7)
    for day in (today, past):
        resp = client.post("/stats/review", data={
            "mode": "real", "trading_day": day.isoformat(),
            "discipline_note": "", "emotion_note": "", "tomorrow_focus": "",
        })
        assert resp.status_code in (200, 303), day


# ---- 終審 LOW-6：日期格式錯誤回 400，不 500 ----

def test_stats_page_with_garbage_date_returns_400_not_500(client):
    resp = client.get("/stats?date_from=garbage&date_to=2026-09-02")
    assert resp.status_code == 400


def test_journal_page_shows_saved_review_alongside_trades(client, session, user):
    today = today_trading_day()
    _closed(session, user.id, exit_time=dt.datetime.combine(today, dt.time(9, 0)))
    client.post("/stats/review", data={
        "mode": "real", "trading_day": today.isoformat(),
        "discipline_note": "日記頁可以看到這句話", "emotion_note": "", "tomorrow_focus": "",
    })

    journal = client.get("/journal?mode=real").text
    assert "每日復盤" in journal
    assert "日記頁可以看到這句話" in journal
    # 既有逐筆列表與手動補記表單不動：新增按鈕與交易表仍在
    assert "+ 新增交易" in journal
    assert 'id="trade-tbody"' in journal


# ---- 終審 MEDIUM-R1：過去的非交易日不得寫復盤、也不給入口 ----

def _last_sunday() -> dt.date:
    """最近一個已過去的週日（保證是過去日、且必非交易日）。"""
    today = today_trading_day()
    return today - dt.timedelta(days=today.weekday() + 1)


def test_save_review_rejects_past_non_trading_day(client):
    """MEDIUM-R1：未來日檢查擋不住過去的週末——沒有 is_trading_day 檢查時，
    POST 過去的週日會寫進一則永遠不被任何「今日」視圖叫出的孤兒 DailyReview。"""
    from quanquant.candles.market_calendar import is_trading_day

    sunday = _last_sunday()
    assert not is_trading_day(sunday)  # sanity：測試對象確實是非交易日
    resp = client.post("/stats/review", data={
        "mode": "real", "trading_day": sunday.isoformat(),
        "discipline_note": "非交易日寫的", "emotion_note": "", "tomorrow_focus": "",
    })
    assert resp.status_code == 400
    assert "交易日" in resp.text


def test_stats_page_past_non_trading_single_day_hides_review_block(client):
    """MEDIUM-R1 對稱面：自訂區間選到過去的週日（frm==to 成立但非交易日）時，
    復盤區塊整段不出現——畫面一開始就不給會被寫入端拒絕的入口。"""
    sunday = _last_sunday().isoformat()
    resp = client.get(f"/stats?date_from={sunday}&date_to={sunday}")
    assert resp.status_code == 200
    assert "今日復盤" not in resp.text
    assert "儲存今日復盤" not in resp.text
