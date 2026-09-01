from datetime import datetime, timezone
from decimal import Decimal

from quanquant.models import FuturesSnapshot
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
