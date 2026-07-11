from datetime import datetime, timezone
from decimal import Decimal

from quanquant.models import FuturesSnapshot
from quanquant.web.routers.dashboard import _quote_context
from quanquant.web.templating import render_partial


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
