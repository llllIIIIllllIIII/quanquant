from decimal import Decimal

from quanquant.journal.pnl import compute_pnl, unrealized_pnl


def test_long_profit_with_fee():
    # (18100-18000)*2*200 - 50
    assert compute_pnl("long", Decimal("18000"), Decimal("18100"), 2, Decimal("200"), Decimal("50")) == Decimal("39950")


def test_short_profit():
    assert compute_pnl("short", Decimal("18000"), Decimal("17900"), 1, Decimal("200")) == Decimal("20000")


def test_long_loss():
    assert compute_pnl("long", Decimal("18000"), Decimal("17950"), 1, Decimal("200")) == Decimal("-10000")


def test_short_loss():
    assert compute_pnl("short", Decimal("18000"), Decimal("18050"), 1, Decimal("50")) == Decimal("-2500")


def test_mtx_point_value():
    assert compute_pnl("long", Decimal("18000"), Decimal("18100"), 1, Decimal("50")) == Decimal("5000")


def test_open_returns_none():
    assert compute_pnl("long", Decimal("18000"), None, 1, Decimal("200")) is None


def test_fee_none_treated_as_zero():
    assert compute_pnl("long", Decimal("18000"), Decimal("18010"), 1, Decimal("200"), None) == Decimal("2000")


def test_decimal_is_exact():
    result = compute_pnl("long", Decimal("18000.25"), Decimal("18000.75"), 1, Decimal("200"))
    assert result == Decimal("100.00")
    assert isinstance(result, Decimal)


def test_unrealized_pnl():
    assert unrealized_pnl("long", Decimal("18000"), Decimal("18050"), 1, Decimal("200")) == Decimal("10000")
