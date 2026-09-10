"""R3-1（2026-09-10）：`/stats`（含 `/stats/export.csv`／`/stats/export.xlsx`）與 `/journal`
未帶 `?mode=` 時，預設要跟隨 server 執行模式（`app.state.order_service.mode`），與既有的
`/orders/queue`、`/orders/deals`、`/orders/holdings` 一致——修前這兩頁硬編 `mode: str =
Query("real")`，同一個導覽下拉切頁會靜默換模式（見 `web/deps.py::resolve_mode`）。

`?mode=` 一律優先；`service` 為 `None`（下單子系統停用／非 owner）時 fallback 回 `"sim"`。
"""
import datetime as dt
from decimal import Decimal
from types import SimpleNamespace

from quanquant.journal import repository as repo
from quanquant.journal.schemas import TradeCreate
from quanquant.journal.trading_day import today_trading_day

_TODAY = today_trading_day()


def _closed_trade(session, user_id, *, mode):
    """收盤時間固定落在今天的 trading_day——`/stats` 未帶 date_from/date_to 時預設篩選
    範圍就是「今日」（見 stats.py::_resolve_range），用今天的交易才不會被日期範圍先濾掉、
    干擾這裡真正要驗的 mode 預設行為。"""
    entry = dt.datetime.combine(_TODAY, dt.time(9, 0))
    exit_ = dt.datetime.combine(_TODAY, dt.time(10, 0))
    return repo.create_trade(
        session,
        TradeCreate(
            symbol="TXF", direction="long", entry_time=entry,
            entry_price=Decimal("18000"), exit_time=exit_,
            exit_price=Decimal("18100"), size=1, point_value=Decimal("200"), mode=mode,
            # 008 的「含手動補記」預設不勾選（include_manual=False）會濾掉 source="manual"
            # 的預設值——標成 shioaji 來源，這裡才驗得到 mode 篩選本身的效果。
            source="shioaji",
        ),
        user_id=user_id,
    )


def _selected_tab(html: str, label: str) -> bool:
    tag = html.split(label)[0].rsplit("<a", 1)[-1]
    return "is-selected" in tag


# ---------------------------------------------------------------------------
# 預設跟隨 service.mode（sim／real 兩側都驗）
# ---------------------------------------------------------------------------

def test_stats_page_default_follows_sim_service_mode(client):
    client.app.state.order_service = SimpleNamespace(mode="sim")
    html = client.get("/stats").text
    assert _selected_tab(html, "模擬") is True
    assert _selected_tab(html, "正式") is False


def test_stats_page_default_follows_real_service_mode(client):
    client.app.state.order_service = SimpleNamespace(mode="real")
    html = client.get("/stats").text
    assert _selected_tab(html, "正式") is True
    assert _selected_tab(html, "模擬") is False


def test_journal_page_default_follows_sim_service_mode(client):
    client.app.state.order_service = SimpleNamespace(mode="sim")
    html = client.get("/journal").text
    assert _selected_tab(html, "模擬") is True
    assert _selected_tab(html, "正式") is False


def test_journal_page_default_follows_real_service_mode(client):
    client.app.state.order_service = SimpleNamespace(mode="real")
    html = client.get("/journal").text
    assert _selected_tab(html, "正式") is True
    assert _selected_tab(html, "模擬") is False


def test_stats_page_shows_only_sim_data_by_default_when_service_is_sim(client, session, user):
    client.app.state.order_service = SimpleNamespace(mode="sim")
    _closed_trade(session, user.id, mode="sim")
    _closed_trade(session, user.id, mode="real")
    html = client.get("/stats").text
    assert "共 1 筆" in html


def test_journal_page_shows_only_sim_data_by_default_when_service_is_sim(client, session, user):
    client.app.state.order_service = SimpleNamespace(mode="sim")
    _closed_trade(session, user.id, mode="sim")
    _closed_trade(session, user.id, mode="real")
    html = client.get("/journal").text
    assert 'id="trade-tbody"' in html


# ---------------------------------------------------------------------------
# ?mode= 一律優先，不受 service.mode 影響
# ---------------------------------------------------------------------------

def test_explicit_mode_query_overrides_sim_service_mode_on_stats(client):
    client.app.state.order_service = SimpleNamespace(mode="sim")
    html = client.get("/stats?mode=real").text
    assert _selected_tab(html, "正式") is True
    assert _selected_tab(html, "模擬") is False


def test_explicit_mode_query_overrides_real_service_mode_on_journal(client):
    client.app.state.order_service = SimpleNamespace(mode="real")
    html = client.get("/journal?mode=sim").text
    assert _selected_tab(html, "模擬") is True
    assert _selected_tab(html, "正式") is False


# ---------------------------------------------------------------------------
# service 為 None（下單子系統停用／非 owner）時 fallback 明確、不爆——回 "sim"
# ---------------------------------------------------------------------------

def test_service_none_fallback_defaults_to_sim_on_stats(client, session, user):
    # `client` fixture 從不設 app.state.order_service（lifespan 未跑）——get_order_service
    # 天然回 None，正是「下單子系統停用」情境。
    _closed_trade(session, user.id, mode="sim")
    _closed_trade(session, user.id, mode="real")
    html = client.get("/stats").text
    assert "共 1 筆" in html  # fallback "sim"，不是舊行為的 "real"


def test_service_none_fallback_defaults_to_sim_on_journal(client):
    html = client.get("/journal").text
    assert _selected_tab(html, "模擬") is True
    assert _selected_tab(html, "正式") is False


def test_service_none_does_not_500(client):
    assert client.get("/stats").status_code == 200
    assert client.get("/journal").status_code == 200


# ---------------------------------------------------------------------------
# 匯出端點同一預設；/stats/data 維持舊行為（硬編 real）不動
# ---------------------------------------------------------------------------

def test_export_csv_default_follows_service_mode(client, session, user):
    client.app.state.order_service = SimpleNamespace(mode="sim")
    _closed_trade(session, user.id, mode="sim")
    _closed_trade(session, user.id, mode="real")
    lines = client.get("/stats/export.csv").text.strip().splitlines()
    assert len(lines) - 1 == 1  # header + 1 筆（sim 那筆）


def test_export_csv_explicit_mode_still_overrides(client, session, user):
    client.app.state.order_service = SimpleNamespace(mode="sim")
    _closed_trade(session, user.id, mode="sim")
    _closed_trade(session, user.id, mode="real")
    lines = client.get("/stats/export.csv?mode=real").text.strip().splitlines()
    assert len(lines) - 1 == 1  # header + 1 筆（real 那筆，因為 ?mode= 優先）


def test_export_xlsx_default_follows_service_mode_not_500(client, session, user):
    client.app.state.order_service = SimpleNamespace(mode="sim")
    _closed_trade(session, user.id, mode="sim")
    resp = client.get("/stats/export.xlsx")
    assert resp.status_code == 200


def test_stats_data_keeps_hardcoded_real_default_unchanged(client, session, user):
    """`/stats/data` 刻意不動（見 stats.py 檔頭註解）——即使 service.mode 是 sim，
    未帶 `?mode=` 仍應維持舊行為的 real 預設。"""
    client.app.state.order_service = SimpleNamespace(mode="sim")
    _closed_trade(session, user.id, mode="real")
    data = client.get("/stats/data").json()
    assert data["overall"]["count"] == 1


# ---------------------------------------------------------------------------
# 五個有 mode 分頁的頁面互跳不換模式
# ---------------------------------------------------------------------------

def test_five_mode_pages_stay_consistent_without_explicit_mode(client):
    client.app.state.order_service = SimpleNamespace(mode="sim")
    for path in ("/orders/queue", "/orders/deals", "/orders/holdings", "/stats", "/journal"):
        html = client.get(path).text
        assert _selected_tab(html, "模擬") is True, path
        assert _selected_tab(html, "正式") is False, path


def test_five_mode_pages_stay_consistent_when_service_is_real(client):
    client.app.state.order_service = SimpleNamespace(mode="real")
    for path in ("/orders/queue", "/orders/deals", "/orders/holdings", "/stats", "/journal"):
        html = client.get(path).text
        assert _selected_tab(html, "正式") is True, path
        assert _selected_tab(html, "模擬") is False, path
