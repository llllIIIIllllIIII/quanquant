from datetime import datetime, timezone
from decimal import Decimal

from fastapi.testclient import TestClient
from sqlmodel import Session

from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.models import FuturesSnapshot
from quanquant.poller import QuoteEvent, QuotePoller
from quanquant.sources.base import DataSource
from quanquant.web.app import create_app
from quanquant.web.deps import get_poller, get_session
from quanquant.web.routers.dashboard import _quote_context
from quanquant.web.templating import _symbol_label, render_partial


def _snap(is_fresh=True):
    p = Decimal("18000")
    return FuturesSnapshot(
        symbol="TXF", price=p, change=Decimal(0), change_pct=0.0, volume=1000,
        open_price=p, high_price=p, low_price=p,
        fetched_at=datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc),
        data_date="2026-06-16", contract_month="TXFF6-F", is_fresh=is_fresh,
    )


def test_quote_context_has_market_status():
    ctx = _quote_context(_snap(), None, poller=None)
    assert "market_status" in ctx
    assert ctx["market_status"] in {"open", "closed", "suspected_halt"}
    assert ctx["session"] in {"day", "night", "closed"}


def test_quote_partial_renders_status_attrs():
    ctx = _quote_context(_snap(is_fresh=True), None, poller=None)
    html = render_partial("partials/quote.html", **ctx)
    assert "data-qq-market-status" in html
    assert "data-qq-fresh" in html


def test_symbol_label_known_codes():
    assert _symbol_label("TXF") == "台指"
    assert _symbol_label("MXF") == "小台"
    assert _symbol_label("TMF") == "微台"


def test_symbol_label_unknown_code_falls_back_to_code_itself():
    """003：未知／未對映商品代碼，顯示代碼本身，不可顯示錯誤中文名或空白。"""
    assert _symbol_label("ZZZ") == "ZZZ"
    assert _symbol_label(None) == ""


def test_quote_partial_shows_sym_code_and_sym_name_split():
    """003：報價列在商品選擇器與現價之間顯示中文顯示名＋期別（不含契約月，使用者定案）。"""
    ctx = _quote_context(_snap(), None, poller=None)
    html = render_partial("partials/quote.html", **ctx)
    assert '<span class="sym-code">TXF</span>' in html
    assert '<span class="sym-name">台指近</span>' in html
    assert "2609" not in html  # 契約月不顯示（使用者定案）


def test_flash_class_only_when_flag_set():
    # 預設不帶 flash（心跳/同價重發不閃）
    ctx = _quote_context(_snap(), None, poller=None)
    assert ctx["flash"] is False
    assert "flash" not in render_partial("partials/quote.html", **ctx)
    # 價格有變動時 flash=True → fragment 帶 flash class
    ctx = _quote_context(_snap(), None, poller=None, flash=True)
    assert 'class="quote up flash"' in render_partial("partials/quote.html", **ctx)


# ---------------------------------------------------------------------------
# 002：GET /quote、GET /quote/stream 的 symbol-scoped 驗收（路由層，真 QuotePoller）
# ---------------------------------------------------------------------------

class _NullSource(DataSource):
    """測試用假來源：QuotePoller 建構需要一個 DataSource，但測試只手動 publish()，
    從不呼叫 run()，故 fetch_snapshot 永遠不會真的被呼叫。"""

    async def fetch_snapshot(self, symbol: str = "TXF") -> FuturesSnapshot:
        raise NotImplementedError

    async def close(self) -> None:
        pass


def _seeded_poller(symbol: str = "TXF") -> QuotePoller:
    poller = QuotePoller(_NullSource(), symbol, 5.0)
    poller.publish(QuoteEvent(snapshot=_snap(), error=None, at=datetime.now(timezone.utc)))
    return poller


def _quote_client(engine, user, poller):
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: poller
    c = TestClient(app)
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    return c


def test_quote_route_known_symbol_matching_poller_returns_price(engine, user):
    """symbol 與 poller 追蹤商品相符 → 照常回報價（單一商品現況下行為不變）。"""
    client = _quote_client(engine, user, _seeded_poller("TXF"))
    resp = client.get("/quote?symbol=TXF")
    assert resp.status_code == 200
    assert "sym-code" in resp.text
    assert "無報價" not in resp.text


def test_quote_route_unknown_symbol_shows_no_quote_never_last_price(engine, user):
    """未知/非 poller 追蹤商品 → 明確顯示「無報價」，絕不可繼續顯示其他商品的價格。"""
    client = _quote_client(engine, user, _seeded_poller("TXF"))
    resp = client.get("/quote?symbol=MXF")
    assert resp.status_code == 200
    assert "MXF 無報價" in resp.text
    assert "18000" not in resp.text  # 不是殘留 TXF 的價


def test_quote_route_without_symbol_param_keeps_existing_behavior(engine, user):
    """symbol 參數缺席＝既有單一商品行為不變。"""
    client = _quote_client(engine, user, _seeded_poller("TXF"))
    resp = client.get("/quote")
    assert resp.status_code == 200
    assert "18000" in resp.text


def test_quote_stream_unknown_symbol_returns_empty_stream_not_last_price(engine, user):
    """SSE 對未知商品同樣不推別檔的價——回空 stream。"""
    client = _quote_client(engine, user, _seeded_poller("TXF"))
    resp = client.get("/quote/stream?symbol=MXF")
    assert resp.status_code == 200
    assert "18000" not in resp.text
