"""006（批次 B-1）：GET /orders/position-strip — 下單頁「未平倉/浮動損益/可動用保證金」
精簡條資料源。symbol-scoped（跟著商品選擇器走，未持有該商品時部位/浮損兩段整段不顯示，
不是顯示 0）；浮動損益重用 `journal/pnl.py::unrealized_pnl`；可動用保證金 sim 固定中性
占位「—」，real 走尚未實作的 provider 縫（目前也回 None → 占位）；損益色 class 隨紅綠
偏好既有慣例（`.pnl.up/.pnl.down`，同 partials/trade_table.html）。
"""
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.broker.types import Position
from quanquant.models import FuturesSnapshot
from quanquant.web.app import create_app
from quanquant.web.deps import get_poller, get_session


def _snap(price: str) -> FuturesSnapshot:
    p = Decimal(price)
    return FuturesSnapshot(
        symbol="TXF", price=p, change=Decimal(0), change_pct=0.0, volume=1000,
        open_price=p, high_price=p, low_price=p,
        fetched_at=datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc),
        data_date="2026-06-16", contract_month="TXFF6-F",
    )


class _FakePoller:
    def __init__(self, price: str | None):
        self.last = _snap(price) if price is not None else None


class _FakeService:
    def __init__(self, *, mode="sim", symbol="TXF", positions=None):
        self.mode = mode
        self.symbol = symbol
        self.account = "F1"
        self._positions = positions if positions is not None else []
        self._deny_user = None

    def positions_snapshot(self, *, actor_user_id):
        from quanquant.broker.base import AuthorizationError

        if self._deny_user == actor_user_id:
            raise AuthorizationError("not owner")
        return self._positions


@pytest.fixture
def strip_client(engine, user):
    def _make(*, service=None, poller=None):
        def _session_override():
            with Session(engine) as s:
                yield s

        app = create_app()
        app.dependency_overrides[get_session] = _session_override
        app.dependency_overrides[get_poller] = lambda: poller
        app.state.order_service = service
        c = TestClient(app)
        c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
        return c

    return _make


# ---------------------------------------------------------------------------
# ⑤ 有倉三段齊
# ---------------------------------------------------------------------------

def test_holding_position_renders_all_three_segments(strip_client):
    service = _FakeService(
        mode="real",
        positions=[Position(symbol="TXF", direction="long", qty=2, avg_price=Decimal("18000"))],
    )
    client = strip_client(service=service, poller=_FakePoller("18100"))
    resp = client.get("/orders/position-strip?symbol=TXF")
    assert resp.status_code == 200
    text = resp.text
    assert "未平倉" in text
    assert "多" in text
    assert "2 口" in text
    assert "18,000" in text  # 均價
    assert "浮動損益" in text
    assert "可動用保證金" in text


def test_unrealized_pnl_matches_pure_function_and_uses_symbol_point_value(strip_client):
    """浮損重用 unrealized_pnl，不另寫數學：多單 18000 進場、現價 18100，TXF 點值 200，
    2 口 → (18100-18000)*2*200 = 40000。"""
    service = _FakeService(
        mode="real",
        positions=[Position(symbol="TXF", direction="long", qty=2, avg_price=Decimal("18000"))],
    )
    client = strip_client(service=service, poller=_FakePoller("18100"))
    resp = client.get("/orders/position-strip?symbol=TXF")
    assert "40,000" in resp.text


# ---------------------------------------------------------------------------
# ⑥ 無倉隱藏兩段
# ---------------------------------------------------------------------------

def test_no_position_for_symbol_hides_position_and_pnl_segments_entirely(strip_client):
    """無持倉整段不 render（不是顯示 0）——切換到沒有部位的商品時，畫面上完全找不到
    「未平倉」「浮動損益」這兩段文字，只剩保證金段。"""
    service = _FakeService(mode="real", positions=[])
    client = strip_client(service=service, poller=_FakePoller("18100"))
    resp = client.get("/orders/position-strip?symbol=TXF")
    assert resp.status_code == 200
    assert "未平倉" not in resp.text
    assert "浮動損益" not in resp.text
    assert "可動用保證金" in resp.text  # 保證金段仍在


def test_symbol_not_matching_service_symbol_treated_as_no_position(strip_client):
    """symbol-scoped 邊界（比照 002 quote 的 unknown_symbol）：請求的 symbol 與 service
    追蹤的商品不同，一律視為未持有，不可誤植別檔的部位——即使 service.symbol 上確實有倉。"""
    service = _FakeService(
        mode="real", symbol="TXF",
        positions=[Position(symbol="TXF", direction="long", qty=1, avg_price=Decimal("18000"))],
    )
    client = strip_client(service=service, poller=_FakePoller("18100"))
    resp = client.get("/orders/position-strip?symbol=MXF")  # 不是 service.symbol
    assert resp.status_code == 200
    assert "未平倉" not in resp.text
    assert "浮動損益" not in resp.text


def test_order_subsystem_disabled_shows_only_margin_placeholder(strip_client):
    """service is None（下單子系統未啟用）：不 500，部位/浮損不顯示，保證金顯示占位。"""
    client = strip_client(service=None, poller=None)
    resp = client.get("/orders/position-strip?symbol=TXF")
    assert resp.status_code == 200
    assert "未平倉" not in resp.text
    assert "—" in resp.text


# ---------------------------------------------------------------------------
# ⑦ sim 保證金占位
# ---------------------------------------------------------------------------

def test_sim_mode_margin_shows_neutral_placeholder_not_zero_or_error(strip_client):
    service = _FakeService(mode="sim", positions=[])
    client = strip_client(service=service, poller=_FakePoller("18100"))
    resp = client.get("/orders/position-strip?symbol=TXF")
    assert resp.status_code == 200
    normalized = " ".join(resp.text.split())
    assert "可動用保證金 —" in normalized
    assert "可動用保證金 0" not in normalized  # 不可顯示 0


def test_real_mode_margin_also_shows_placeholder_pending_f4_provider(strip_client):
    """real provider 縫目前尚未實作（待 F4），一樣回 None → 占位，不可顯示錯誤數字。"""
    service = _FakeService(mode="real", positions=[])
    client = strip_client(service=service, poller=_FakePoller("18100"))
    resp = client.get("/orders/position-strip?symbol=TXF")
    assert "—" in resp.text


# ---------------------------------------------------------------------------
# ⑧ 損益色 class 隨偏好（既有 .pnl.up/.pnl.down 慣例，隨 --rise/--fall 翻轉）
# ---------------------------------------------------------------------------

def test_profitable_long_position_uses_pnl_up_class(strip_client):
    service = _FakeService(
        mode="real",
        positions=[Position(symbol="TXF", direction="long", qty=1, avg_price=Decimal("18000"))],
    )
    client = strip_client(service=service, poller=_FakePoller("18100"))  # 多單、現價更高 → 賺
    resp = client.get("/orders/position-strip?symbol=TXF")
    assert 'class="pnl up"' in resp.text


def test_losing_long_position_uses_pnl_down_class(strip_client):
    service = _FakeService(
        mode="real",
        positions=[Position(symbol="TXF", direction="long", qty=1, avg_price=Decimal("18000"))],
    )
    client = strip_client(service=service, poller=_FakePoller("17900"))  # 多單、現價更低 → 賠
    resp = client.get("/orders/position-strip?symbol=TXF")
    assert 'class="pnl down"' in resp.text


def test_no_mark_price_shows_dash_with_neutral_class_not_pnl_down(strip_client):
    """終審必修 LOW-6：有部位但無現價（poller 無報價）時，浮動損益顯示「—」；不可落到
    `.pnl.down`（賠錢的警示色）——那是「不知道」，不是「賠錢」。"""
    service = _FakeService(
        mode="real",
        positions=[Position(symbol="TXF", direction="long", qty=1, avg_price=Decimal("18000"))],
    )
    client = strip_client(service=service, poller=_FakePoller(None))
    resp = client.get("/orders/position-strip?symbol=TXF")
    assert 'class="pnl"' in resp.text  # 無方向 class，中性
    assert 'class="pnl down"' not in resp.text
    assert 'class="pnl up"' not in resp.text


def test_short_position_direction_class_uses_existing_dir_convention(strip_client):
    service = _FakeService(
        mode="real",
        positions=[Position(symbol="TXF", direction="short", qty=1, avg_price=Decimal("18000"))],
    )
    client = strip_client(service=service, poller=_FakePoller("18100"))
    resp = client.get("/orders/position-strip?symbol=TXF")
    assert 'class="dir short"' in resp.text
    assert "空" in resp.text


def test_non_owner_gets_403_not_silently_empty_strip(strip_client, user):
    service = _FakeService(mode="real", positions=[])
    service._deny_user = user.id
    client = strip_client(service=service, poller=_FakePoller("18100"))
    resp = client.get("/orders/position-strip?symbol=TXF")
    assert resp.status_code == 403
