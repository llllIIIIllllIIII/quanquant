import json
from datetime import datetime
from decimal import Decimal

import pytest
from sqlmodel import Session

from quanquant.candles.repo import upsert_candles
from quanquant.db.models import Candle
from quanquant.market_hours import CST


def ms(y, m, d, hh, mm) -> int:
    return int(datetime(y, m, d, hh, mm, tzinfo=CST).timestamp() * 1000)


def mk_1m(ts, price, vol=10, session="day", trading_date=None):
    p = Decimal(price)
    return Candle(
        symbol="TXF", timeframe="1m", ts=ts, open=p, high=p + 5, low=p - 5, close=p + 1,
        volume=vol, source="live", session=session, trading_date=trading_date,
    )


def mk_1d(date_str, price, vol=1000):
    y, m, d = (int(x) for x in date_str.split("-"))
    p = Decimal(price)
    return Candle(
        symbol="TXF", timeframe="1d", ts=ms(y, m, d, 8, 45), open=p, high=p + 50,
        low=p - 50, close=p + 10, volume=vol, source="finmind", session="day",
        trading_date=date_str,
    )


@pytest.fixture
def seeded(engine):
    """30 day-session 1m bars (09:00–09:29 on 2026-06-10) + 5 stored 日K."""
    with Session(engine) as s:
        rows = [
            mk_1m(ms(2026, 6, 10, 9, i), "18000", vol=10, trading_date="2026-06-10")
            for i in range(30)
        ]
        rows += [
            mk_1d(d, "17900")
            for d in ("2026-06-03", "2026-06-04", "2026-06-05", "2026-06-08", "2026-06-09")
        ]
        upsert_candles(s, rows)
    return engine


def test_page_shape_and_order(client, seeded):
    r = client.get("/api/candles", params={"symbol": "TXF", "tf": "1m", "limit": 10})
    assert r.status_code == 200
    data = r.json()
    assert data["symbol"] == "TXF" and data["tf"] == "1m"
    bars = data["bars"]
    assert len(bars) == 10
    assert bars == sorted(bars, key=lambda b: b["timestamp"])  # ascending
    assert bars[-1]["timestamp"] == ms(2026, 6, 10, 9, 29)
    assert data["hasMore"] is True  # 20 older 1m bars exist


def test_before_pagination_and_has_more_exact(client, seeded):
    r1 = client.get("/api/candles", params={"tf": "1m", "limit": 20})
    oldest = r1.json()["bars"][0]["timestamp"]
    r2 = client.get("/api/candles", params={"tf": "1m", "limit": 20, "before": oldest})
    bars2 = r2.json()["bars"]
    assert len(bars2) == 10  # only 10 remain
    assert bars2[-1]["timestamp"] < oldest
    assert r2.json()["hasMore"] is False


def test_derived_5m_aggregation(client, seeded):
    r = client.get("/api/candles", params={"tf": "5m", "limit": 100})
    bars = r.json()["bars"]
    assert len(bars) == 6  # 30 minutes -> 6 buckets
    assert bars[0]["timestamp"] == ms(2026, 6, 10, 9, 0)
    assert bars[0]["volume"] == 50  # 5 x vol 10


def test_daily_includes_synthetic_today(client, seeded):
    r = client.get("/api/candles", params={"tf": "1d", "limit": 100})
    bars = r.json()["bars"]
    # 5 stored days + 1 synthetic from 2026-06-10's day-session 1m data
    assert len(bars) == 6
    assert bars[-1]["timestamp"] == ms(2026, 6, 10, 8, 45)
    assert bars[-1]["volume"] == 300  # 30 x vol 10


def test_weekly_aggregates_from_daily(client, seeded):
    r = client.get("/api/candles", params={"tf": "1w", "limit": 100})
    bars = r.json()["bars"]
    # 06-03..05 (W23) and 06-08..10 incl. synthetic (W24)
    assert len(bars) == 2


def test_latest_since_filtering(client, seeded):
    since = ms(2026, 6, 10, 9, 25)
    r = client.get("/api/candles/latest", params={"tf": "5m", "since": since})
    bars = r.json()["bars"]
    assert len(bars) == 1
    assert bars[0]["timestamp"] == since
    assert bars[0]["volume"] == 50


def test_unknown_tf_422(client):
    assert client.get("/api/candles", params={"tf": "7m"}).status_code == 422
    assert (
        client.get("/api/candles/latest", params={"tf": "7m", "since": 0}).status_code == 422
    )


# --- chart state ---


def test_chart_state_roundtrip(client):
    empty = client.get("/api/chart/state").json()
    assert empty == {"indicators": None, "drawings": None}

    ind = {"ma": {"enabled": True, "params": [{"period": 5, "color": "#f0b90b"}]}}
    assert client.put("/api/chart/state/indicators", json=ind).status_code == 204
    drawings = [{"name": "segment", "points": [{"timestamp": 1, "value": 2}]}]
    assert client.put("/api/chart/state/drawings", json=drawings).status_code == 204

    state = client.get("/api/chart/state").json()
    assert state["indicators"] == ind
    assert state["drawings"] == drawings

    ind2 = {"ma": {"enabled": False, "params": []}}
    assert client.put("/api/chart/state/indicators", json=ind2).status_code == 204
    assert client.get("/api/chart/state").json()["indicators"] == ind2  # upserted


def test_chart_state_preserves_visible_field(client):
    # 眼睛隱藏（visible:false）跟帳號：payload 以原始 JSON 整包存取，
    # 任意欄位（含 visible）須原樣往返，不被 schema 過濾掉。
    ind = {
        "ma": {"enabled": True, "visible": False, "params": [{"period": 5, "color": "#f0b90b"}]},
        "vol": {"enabled": True, "visible": True, "params": {}},
    }
    assert client.put("/api/chart/state/indicators", json=ind).status_code == 204
    got = client.get("/api/chart/state").json()["indicators"]
    assert got == ind
    assert got["ma"]["visible"] is False


def test_chart_state_preserves_colors_field(client):
    # 子線顏色（colors 陣列）跟帳號：payload 整包 JSON 存取，colors 須原樣往返。
    ind = {
        "boll": {"enabled": True, "visible": True, "params": {"period": 20, "std": 2},
                 "colors": ["#111111", "#222222", "#333333"]},
        "macd": {"enabled": True, "visible": True, "params": {"fast": 12, "slow": 26, "signal": 9},
                 "colors": ["#aabbcc", "#ddeeff"]},
    }
    assert client.put("/api/chart/state/indicators", json=ind).status_code == 204
    got = client.get("/api/chart/state").json()["indicators"]
    assert got == ind
    assert got["boll"]["colors"] == ["#111111", "#222222", "#333333"]


def test_chart_state_preserves_vol_ma_params(client):
    # VOL 量能均線（params 為 {period,color} 陣列）跟帳號：整包 JSON 存取，
    # 陣列須原樣往返，不被 schema 過濾。
    ind = {
        "vol": {"enabled": True, "visible": True,
                "params": [{"period": 5, "color": "#f0b90b"},
                           {"period": 10, "color": "#935EBD"}]},
    }
    assert client.put("/api/chart/state/indicators", json=ind).status_code == 204
    got = client.get("/api/chart/state").json()["indicators"]
    assert got == ind
    assert [p["period"] for p in got["vol"]["params"]] == [5, 10]


def test_chart_state_unknown_kind_404(client):
    assert client.put("/api/chart/state/nope", json={}).status_code == 404


def test_chart_state_oversize_413(client):
    huge = json.dumps({"x": "a" * (260 * 1024)})
    r = client.put(
        "/api/chart/state/drawings", content=huge,
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 413


def test_healthz(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_healthz_order_subsystem_none_when_lifespan_not_run(client):
    """TestClient（未用 `with` context）不會跑真正的 lifespan，app.state.order_session_state
    自然不存在——/healthz 對此情況回傳 order_subsystem=None，不是錯誤。"""
    r = client.get("/healthz")
    assert r.json()["order_subsystem"] is None


def test_healthz_reflects_order_session_state_when_present(client):
    from quanquant.broker.session_state import OrderSessionState
    from quanquant.web.deps import get_order_session_state

    state = OrderSessionState()
    state.mark_unhealthy("connect 失敗，下單子系統停用: boom")
    state.reconnect_attempts = 3
    client.app.dependency_overrides[get_order_session_state] = lambda: state

    r = client.get("/healthz")
    assert r.status_code == 503  # T0.3：unhealthy（非 disabled）→ /healthz 回 503
    body = r.json()["order_subsystem"]
    assert body == {
        "status": "unhealthy",  # T0.3：新增健康分級（disabled/ready/unhealthy）
        "ready": False, "last_error": "connect 失敗，下單子系統停用: boom", "reconnect_attempts": 3,
    }

    del client.app.dependency_overrides[get_order_session_state]
