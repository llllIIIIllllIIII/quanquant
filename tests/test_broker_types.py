"""broker 純型別：欄位齊全、OrderRequest 無 mode（server-side 決定）、
fail-closed 驗證（qty/price/枚舉）、Fill 帶 mode/user_id/broker_order_id、Protocol 可被 duck-typed、
canonical_payload_hash 涵蓋全部可執行欄位且逐欄敏感（HIGH#5 的型別層防線）。"""
from decimal import Decimal

import pytest

from quanquant.broker.base import AuthorizationError, OrderError, OrderService, RiskError
from quanquant.broker.types import Fill, OrderAck, OrderRequest, Position, RiskDecision, canonical_payload_hash


def _req(**over):
    base = dict(
        client_order_id="C1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", user_id=7,
    )
    base.update(over)
    return OrderRequest(**base)


def _hash_kwargs(**over):
    base = dict(
        symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", account="F1", mode="sim",
    )
    base.update(over)
    return base


def test_order_request_has_no_mode_field():
    req = _req()
    assert not hasattr(req, "mode")  # mode 只由 OrderService.mode（server-side）決定


def test_order_request_fields():
    req = _req()
    assert req.user_id == 7 and req.octype == "New" and req.client_order_id == "C1"


@pytest.mark.parametrize("bad", [dict(qty=0), dict(qty=-1)])
def test_order_request_rejects_nonpositive_qty(bad):
    with pytest.raises(ValueError):
        _req(**bad)


@pytest.mark.parametrize("bad", [dict(price=Decimal("0")), dict(price=Decimal("-1"))])
def test_order_request_rejects_nonpositive_price(bad):
    with pytest.raises(ValueError):
        _req(**bad)


@pytest.mark.parametrize("field,bad", [
    ("action", "Hold"), ("price_type", "XYZ"), ("order_type", "GTC"), ("octype", "Reverse"),
])
def test_order_request_rejects_illegal_enums(field, bad):
    with pytest.raises(ValueError):
        _req(**{field: bad})


# ---- bug 2：price 驗證改成 price_type 感知——MKT 不需要價格 ----

def test_order_request_allows_zero_price_for_mkt_order():
    """MKT（市價單）price 一律視為 0，不再套用 LMT 的 price>0 規則。"""
    req = _req(price=Decimal("0"), price_type="MKT", order_type="IOC")
    assert req.price == Decimal("0")


def test_order_request_rejects_negative_price_even_for_mkt():
    """MKT 放寬 price==0，但仍不可為負——不是完全不驗證。"""
    with pytest.raises(ValueError):
        _req(price=Decimal("-1"), price_type="MKT", order_type="IOC")


def test_order_request_rejects_mkt_with_rod_order_type():
    """TAIFEX 市價單不接受 ROD，只能搭配 IOC/FOK。"""
    with pytest.raises(ValueError):
        _req(price=Decimal("0"), price_type="MKT", order_type="ROD")


def test_order_request_still_rejects_lmt_zero_price():
    """既有規則不得因為 MKT 的放寬而跟著弱化：LMT 仍要求 price>0。"""
    with pytest.raises(ValueError):
        _req(price=Decimal("0"), price_type="LMT")


def test_fill_carries_user_mode_and_broker_order_id():
    f = Fill(
        broker="shioaji", fill_id="F1", ordno="O1", broker_order_id="B1", symbol="TXF", action="Sell",
        price=Decimal("18100"), qty=1, fee=Decimal("50"), octype="Cover",
        ts=1_780_000_000_000, account="F123", mode="sim", user_id=7,
    )
    assert f.user_id == 7 and f.mode == "sim" and f.action == "Sell" and f.broker_order_id == "B1"


def test_fill_rejects_illegal_mode():
    with pytest.raises(ValueError):
        Fill(
            broker="shioaji", fill_id="F1", ordno="O1", broker_order_id=None, symbol="TXF", action="Sell",
            price=Decimal("1"), qty=1, fee=None, octype="Cover",
            ts=1, account="F123", mode="paper", user_id=7,
        )


def _fill(**over):
    base = dict(
        broker="shioaji", fill_id="F1", ordno="O1", broker_order_id="B1", symbol="TXF", action="Sell",
        price=Decimal("18100"), qty=1, fee=Decimal("50"), octype="Cover",
        ts=1_780_000_000_000, account="F123", mode="sim", user_id=7,
    )
    base.update(over)
    return Fill(**base)


@pytest.mark.parametrize("bad", [dict(qty=0), dict(qty=-1)])
def test_fill_rejects_nonpositive_qty(bad):
    with pytest.raises(ValueError):
        _fill(**bad)


@pytest.mark.parametrize("bad", [dict(price=Decimal("0")), dict(price=Decimal("-1"))])
def test_fill_rejects_nonpositive_price(bad):
    with pytest.raises(ValueError):
        _fill(**bad)


@pytest.mark.parametrize("field,bad", [("action", "Hold"), ("octype", "Reverse")])
def test_fill_rejects_illegal_enums(field, bad):
    """HIGH#8：非法 callback 不得被當合法值頂替（不猜測成 Auto/空字串）。"""
    with pytest.raises(ValueError):
        _fill(**{field: bad})


@pytest.mark.parametrize("bad", [dict(fill_id=""), dict(account=""), dict(ts=0), dict(ts=-1)])
def test_fill_rejects_missing_fill_id_account_or_nonpositive_ts(bad):
    with pytest.raises(ValueError):
        _fill(**bad)


def test_order_ack_position_and_risk_decision():
    ack = OrderAck(client_order_id="C1", broker_order_id="B1", ordno="O1", status="submitted")
    pos = Position(symbol="TXF", direction="long", qty=2, avg_price=Decimal("18000"))
    dec = RiskDecision(allowed=False, reason="kill switch", needs_confirm=False)
    assert ack.status == "submitted" and pos.qty == 2 and dec.allowed is False


def test_exceptions_are_distinct():
    assert issubclass(RiskError, Exception) and issubclass(OrderError, Exception)
    assert issubclass(AuthorizationError, Exception)
    assert len({RiskError, OrderError, AuthorizationError}) == 3


def test_protocol_is_runtime_checkable_duck():
    class _Impl:
        mode = "sim"

        async def place(self, req, *, actor_user_id, confirm_token=None): ...
        async def cancel(self, broker_order_id, *, actor_user_id): ...
        async def update(self, broker_order_id, *, actor_user_id, price=None, qty=None, confirm_token=None): ...
        async def positions(self, *, actor_user_id): ...
        def on_fill(self, handler): ...

    assert isinstance(_Impl(), OrderService)


def test_canonical_payload_hash_is_deterministic():
    a = canonical_payload_hash(**_hash_kwargs())
    b = canonical_payload_hash(**_hash_kwargs())
    assert a == b and isinstance(a, str) and len(a) == 64  # sha256 hex


@pytest.mark.parametrize("field,new_value", [
    ("price", Decimal("18500")), ("qty", 2), ("price_type", "MKT"),
    ("order_type", "IOC"), ("octype", "Cover"), ("account", "F2"), ("mode", "real"),
    ("action", "Sell"), ("symbol", "MXF"),
])
def test_canonical_payload_hash_sensitive_to_every_executable_field(field, new_value):
    """HIGH#5：逐欄 mutation 必令 hash 改變，否則確認 LMT/ROD 後可篡改成 MKT/IOC/FOK 送出。"""
    base = canonical_payload_hash(**_hash_kwargs())
    mutated = canonical_payload_hash(**_hash_kwargs(**{field: new_value}))
    assert base != mutated


def test_canonical_payload_hash_ignores_decimal_string_formatting_noise():
    # Decimal("18000") 與 Decimal("18000.00") 是同一個可執行內容，正規化後應同 hash。
    a = canonical_payload_hash(**_hash_kwargs(price=Decimal("18000")))
    b = canonical_payload_hash(**_hash_kwargs(price=Decimal("18000.00")))
    assert a == b


def test_canonical_payload_hash_ignores_decimal_zero_padding_and_scientific_notation():
    """round3 HIGH#5：零值與科學記號陷阱——Decimal('0') vs Decimal('0.00')、
    Decimal('1E+1') vs Decimal('10') 正規化後須同 hash，且輸出不得含 'E' 科學記號。"""
    a = canonical_payload_hash(**_hash_kwargs(price=Decimal("10"), qty=1))
    b = canonical_payload_hash(**_hash_kwargs(price=Decimal("1E+1"), qty=1))
    assert a == b
