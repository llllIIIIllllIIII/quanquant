"""008：交易績效頁（/stats）——單頁合併驗收：預設今日／摺疊初始態／分頁不動統計／
篩選 WHERE／匯出範圍／導覽退場。"""
import datetime as dt
import re
from decimal import Decimal

from quanquant.journal import repository as repo
from quanquant.journal.schemas import TradeCreate
from quanquant.journal.trading_day import today_trading_day, trading_day_bounds


def _closed(session, user_id, *, exit_time, pnl_sign=1, source="shioaji", mode="real", symbol="TXF"):
    entry_time = exit_time - dt.timedelta(hours=1)
    entry_price = Decimal("18000")
    exit_price = entry_price + (Decimal("100") if pnl_sign >= 0 else Decimal("-100"))
    return repo.create_trade(
        session,
        TradeCreate(
            symbol=symbol, direction="long", entry_time=entry_time, entry_price=entry_price,
            exit_time=exit_time, exit_price=exit_price, size=1, point_value=Decimal("200"),
            mode=mode, source=source,
        ),
        user_id=user_id,
    )


# ---- ① 預設今日＋trading_day 含夜盤 ----

def test_default_range_is_today_trading_day(client):
    resp = client.get("/stats")
    today = today_trading_day().isoformat()
    assert resp.status_code == 200
    # 篩選列的日期欄位（隱藏套用結果）應等於今天的 trading_day
    assert f'value="{today}"' in resp.text


def test_default_range_includes_previous_night_session_trade(client, session, user):
    today = today_trading_day()
    lower, upper = trading_day_bounds(today, today)
    _closed(session, user.id, exit_time=lower + dt.timedelta(hours=2))  # 昨晚夜盤延續
    _closed(session, user.id, exit_time=upper + dt.timedelta(hours=1))  # 今天自己的夜盤（屬次一交易日）

    # R3-1：`client` fixture 未設 app.state.order_service，`/stats` 未帶 ?mode= 現在預設
    # fallback 回 "sim"（見 web/deps.py::resolve_mode）；這裡驗的是日期範圍邏輯，資料是
    # `_closed` 預設的 mode="real"，故明確帶 ?mode=real 讓兩者對得上，不受預設值變動影響。
    resp = client.get("/stats?mode=real")
    assert "共 1 筆" in resp.text


# ---- ② 摺疊初始態隨區間 ----

def test_more_metrics_collapsed_by_default_when_range_is_today(client):
    today = today_trading_day().isoformat()
    resp = client.get(f"/stats?date_from={today}&date_to={today}")
    assert '<details class="more-metrics" >' in resp.text or '<details class="more-metrics">' in resp.text
    assert '<details class="more-metrics" open>' not in resp.text


def test_more_metrics_expanded_by_default_when_range_spans_multiple_days(client):
    today = today_trading_day()
    week_ago = (today - dt.timedelta(days=7)).isoformat()
    resp = client.get(f"/stats?date_from={week_ago}&date_to={today.isoformat()}")
    assert '<details class="more-metrics" open>' in resp.text


# ---- ③ 翻頁統計不變／⑤ 分頁計數與頁碼 ----

def _make_many_closed_trades(session, user_id, n, *, day):
    lower, _upper = trading_day_bounds(day, day)
    for i in range(n):
        _closed(session, user_id, exit_time=lower + dt.timedelta(minutes=i + 1), pnl_sign=1 if i % 2 else -1)


def _extract_total_pnl(html: str) -> str:
    m = re.search(r'class="big (?:up|down)">([^<]+)</div>', html)
    assert m, "找不到總損益數字"
    return m.group(1)


def test_pagination_page_count_and_labels(client, session, user):
    today = today_trading_day()
    _make_many_closed_trades(session, user.id, 55, day=today)
    # R3-1：資料是 `_closed` 預設的 mode="real"，明確帶 ?mode=real（見上方註解）。
    resp = client.get(f"/stats?date_from={today.isoformat()}&date_to={today.isoformat()}&mode=real")
    assert "共 55 筆、第 1 / 2 頁" in resp.text


def test_page_2_total_pnl_matches_page_1(client, session, user):
    """陷阱測試（規格明訂）：第 2 頁的總損益與第 1 頁相同——彙總必須涵蓋整個篩選區間，
    不是只算當前頁。"""
    today = today_trading_day()
    _make_many_closed_trades(session, user.id, 55, day=today)
    qs = f"date_from={today.isoformat()}&date_to={today.isoformat()}&mode=real"
    page1 = client.get(f"/stats?{qs}&page=1")
    page2 = client.get(f"/stats?{qs}&page=2")
    assert _extract_total_pnl(page1.text) == _extract_total_pnl(page2.text)
    assert "第 2 / 2 頁" in page2.text
    # 兩頁顯示的逐筆列不同（第 2 頁不是第 1 頁的複製）
    assert page1.text != page2.text


def test_page_out_of_range_clamped_not_500(client, session, user):
    today = today_trading_day()
    _make_many_closed_trades(session, user.id, 3, day=today)
    resp = client.get(f"/stats?date_from={today.isoformat()}&date_to={today.isoformat()}&mode=real&page=999")
    assert resp.status_code == 200
    assert "第 1 / 1 頁" in resp.text


# ---- ④ 結果篩選／含手動補記 WHERE 正確 ----

def test_result_filter_only_losses(client, session, user):
    today = today_trading_day()
    _closed(session, user.id, exit_time=dt.datetime.combine(today, dt.time(9, 0)), pnl_sign=1)
    _closed(session, user.id, exit_time=dt.datetime.combine(today, dt.time(10, 0)), pnl_sign=-1)
    qs = f"date_from={today.isoformat()}&date_to={today.isoformat()}&mode=real"
    resp = client.get(f"/stats?{qs}&result=loss")
    assert "共 1 筆" in resp.text


def test_include_manual_toggle_changes_count(client, session, user):
    today = today_trading_day()
    _closed(session, user.id, exit_time=dt.datetime.combine(today, dt.time(9, 0)), source="shioaji")
    _closed(session, user.id, exit_time=dt.datetime.combine(today, dt.time(10, 0)), source="manual")
    qs = f"date_from={today.isoformat()}&date_to={today.isoformat()}&mode=real"
    default_resp = client.get(f"/stats?{qs}")
    with_manual_resp = client.get(f"/stats?{qs}&include_manual=1")
    assert "共 1 筆" in default_resp.text
    assert "共 2 筆" in with_manual_resp.text


# ---- ⑥ 匯出範圍＝篩選非分頁 ----

def test_export_csv_includes_all_filtered_rows_not_just_one_page(client, session, user):
    today = today_trading_day()
    _make_many_closed_trades(session, user.id, 55, day=today)
    qs = f"date_from={today.isoformat()}&date_to={today.isoformat()}&mode=real"
    csv_resp = client.get(f"/stats/export.csv?{qs}")
    lines = csv_resp.text.strip().splitlines()
    assert len(lines) - 1 == 55  # header + 55 rows，不受單頁 50 筆上限截斷


def test_export_scope_respects_result_filter(client, session, user):
    today = today_trading_day()
    _closed(session, user.id, exit_time=dt.datetime.combine(today, dt.time(9, 0)), pnl_sign=1)
    _closed(session, user.id, exit_time=dt.datetime.combine(today, dt.time(10, 0)), pnl_sign=-1)
    qs = f"date_from={today.isoformat()}&date_to={today.isoformat()}&mode=real&result=win"
    csv_resp = client.get(f"/stats/export.csv?{qs}")
    lines = csv_resp.text.strip().splitlines()
    assert len(lines) - 1 == 1


# ---- ⑩ 跨日區間不顯示復盤區塊 ----

def test_review_block_shown_for_single_day(client):
    today = today_trading_day().isoformat()
    resp = client.get(f"/stats?date_from={today}&date_to={today}")
    assert "今日復盤" in resp.text


def test_review_block_hidden_for_multi_day_range(client):
    today = today_trading_day()
    week_ago = (today - dt.timedelta(days=7)).isoformat()
    resp = client.get(f"/stats?date_from={week_ago}&date_to={today.isoformat()}")
    assert "今日復盤" not in resp.text


# ---- 終審 LOW-5：匯出鈕旁補口徑註腳（手動補記單預設不計入） ----

def test_export_buttons_have_manual_source_footnote(client):
    html = client.get("/stats").text
    idx = html.index("匯出 Excel")
    assert "含手動補記" in html[idx: idx + 200]


# ---- ⑪ 舊「績效統計」導覽退場＋新頁在交易下拉 ----

def test_old_standalone_stats_nav_link_is_gone(client):
    html = client.get("/").text
    assert ">績效統計<" not in html


def test_new_stats_link_lives_inside_trade_dropdown(client):
    html = client.get("/").text
    dropdown = re.search(r'<details class="dropdown nav-trade">.*?</details>', html, re.DOTALL).group(0)
    assert 'href="/stats"' in dropdown
    assert "交易績效" in dropdown


def test_trade_dropdown_marked_active_on_stats_page(client):
    html = client.get("/stats").text
    assert '<summary class="active">交易</summary>' in html
