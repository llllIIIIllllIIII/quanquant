"""orders 路由：GET /orders 首次渲染生成 hidden client_order_id；sim place 直接成功；
real place 缺 token 回確認框、帶 token 重送成功；AuthorizationError→403；委託列表依 mode
隔離；service 未啟用時表單顯示停用訊息（不是 500）。

round3 修正的路由層驗收（用假 service/guard 測 HTTP 映射；用「真 RiskGuard + 真
ShioajiAdapter（注入 _FakeApi，不連真網路）」測 round3 #1 的關鍵修正——PUT /orders/{id}
的 real 兩階段確認 round-trip 不再因為 route 端 hash 算法與 adapter 不一致而永遠鎖死）：
- #1：real update round-trip（只改價／只改量／價量都改）——route 先讀既有 Order 合併欄位，
  用與 ShioajiAdapter.update 完全相同的合併演算法算 canonical hash，token 才驗得過。
- #7：client_order_id 首次渲染生成、同表單重送同一個值。
- #9：委託列表除取消鈕外另有「改單」控制（GET .../edit 開表單，PUT 送出，real 兩步確認）。
- #6：positions 非 owner 403（非 200 空表）；place/update/cancel 非 owner 一律 403。
"""
import re
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, OrderError, RiskError
from quanquant.broker.risk import RiskGuard
from quanquant.broker.shioaji_adapter import ShioajiAdapter
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import OrderAck, Position
from quanquant.web.app import create_app
from quanquant.web.deps import get_poller, get_session


def _hidden(text: str, name: str) -> str | None:
    """抓 `<input name="X" ... value="Y">` 的 value（不假設 value 緊接在 name 後面——
    可見欄位如 `order_edit_form.html` 的 qty/price input 中間還夾了 type/min 等屬性）。"""
    m = re.search(rf'name="{name}"[^>]*?value="([^"]*)"', text)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Part 1：假 service/guard——只測路由層 HTTP 映射（403/確認框觸發/停用訊息等）
# ---------------------------------------------------------------------------

class _FakeService:
    def __init__(self, mode="sim"):
        self.mode = mode
        self.account = "F1"
        self.placed = []
        self._deny_user = None

    async def place(self, req, *, actor_user_id, confirm_token=None):
        if self._deny_user == actor_user_id:
            raise AuthorizationError("not owner")
        if self.mode == "real" and not confirm_token:
            raise RiskError("需要確認", needs_confirm=True)
        self.placed.append(req)
        return OrderAck(client_order_id=req.client_order_id, broker_order_id="B1", ordno="O1", status="submitted")

    async def cancel(self, broker_order_id, *, actor_user_id):
        if self._deny_user == actor_user_id:
            raise AuthorizationError("not owner")
        return OrderAck(client_order_id="", broker_order_id=broker_order_id, ordno="O1", status="cancelled")

    async def update(self, broker_order_id, *, actor_user_id, price=None, qty=None, confirm_token=None):
        if self._deny_user == actor_user_id:
            raise AuthorizationError("not owner")
        return OrderAck(client_order_id="", broker_order_id=broker_order_id, ordno="O1", status="submitted")

    async def positions(self, *, actor_user_id):
        if self._deny_user == actor_user_id:
            raise AuthorizationError("not owner")
        return [Position(symbol="TXF", direction="long", qty=1, avg_price=Decimal("18000"))]

    def on_fill(self, handler):
        pass


class _FakeRiskGuard:
    def __init__(self):
        self.kill_switch = False

    def set_kill_switch(self, value):
        self.kill_switch = value

    def assert_owner(self, actor_user_id):
        pass

    def issue_confirm_token(self, session, *, actor_user_id, payload_hash):
        return f"TOKEN-{payload_hash}"


@pytest.fixture
def fake_service():
    return _FakeService()


@pytest.fixture
def fake_guard():
    return _FakeRiskGuard()


@pytest.fixture
def order_client(engine, user, fake_service, fake_guard):
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    app.state.order_service = fake_service
    app.state.order_risk_guard = fake_guard
    c = TestClient(app)
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    return c


def test_orders_page_renders_with_hidden_client_order_id(order_client):
    resp = order_client.get("/orders")
    assert resp.status_code == 200
    assert re.search(r'name="client_order_id" value="([0-9a-f-]{36})"', resp.text) is not None


def test_two_get_requests_generate_different_client_order_ids(order_client):
    """每次 GET /orders 是新的表單渲染，各自生新 id；穩定性測的是同一次渲染內的 HTTP retry。"""
    a = _hidden(order_client.get("/orders").text, "client_order_id")
    b = _hidden(order_client.get("/orders").text, "client_order_id")
    assert a != b


def test_place_order_sim_sends_directly(order_client, fake_service, user):
    resp = order_client.post("/orders", data={
        "client_order_id": "C1", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert resp.status_code == 200
    assert len(fake_service.placed) == 1


def test_place_order_real_two_step_confirm_round_trip(order_client, fake_service, user):
    fake_service.mode = "real"
    form = {
        "client_order_id": "C1", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    }
    first = order_client.post("/orders", data=form)
    assert first.status_code == 200
    token = _hidden(first.text, "confirm_token")
    assert token is not None  # 回了確認框，不是直接失敗

    second = order_client.post("/orders", data={**form, "confirm_token": token})
    assert second.status_code == 200
    assert len(fake_service.placed) == 1  # 帶 token 那次真的送出去了


# ---- bug 2：市價單（MKT）不再因 price 而報 decimal.ConversionSyntax ----

def test_place_order_mkt_with_empty_price_succeeds_defaults_to_zero(order_client, fake_service, user):
    """MKT（市價單）price 欄位留白（Alpine 停用時瀏覽器也不會送出這個欄位）不應報
    decimal.ConversionSyntax，應直接視為 price=0 成功送出。"""
    resp = order_client.post("/orders", data={
        "client_order_id": "C-MKT", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "",
        "price_type": "MKT", "order_type": "IOC", "octype": "New",
    })
    assert resp.status_code == 200
    assert len(fake_service.placed) == 1
    assert fake_service.placed[0].price == Decimal("0")


def test_place_order_mkt_without_price_field_at_all_succeeds(order_client, fake_service, user):
    """price 欄位整個缺席（HTML disabled input 不會被送出）也要一樣成功，不是只處理空字串。"""
    form = {
        "client_order_id": "C-MKT2", "symbol": "TXF", "action": "Buy", "qty": "1",
        "price_type": "MKT", "order_type": "IOC", "octype": "New",
    }
    resp = order_client.post("/orders", data=form)
    assert resp.status_code == 200
    assert len(fake_service.placed) == 1
    assert fake_service.placed[0].price == Decimal("0")


def test_place_order_lmt_with_empty_price_shows_form_error_not_500(order_client, fake_service, user):
    """LMT（限價單）留白仍要求價格——不強制歸零，只是不再拋出未經處理的 ConversionSyntax
    原始例外訊息，而是走既有的表單錯誤流程（200 + 錯誤訊息，不是 500）。"""
    resp = order_client.post("/orders", data={
        "client_order_id": "C-LMT-EMPTY", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert resp.status_code == 200
    assert len(fake_service.placed) == 0


def test_place_order_mkt_with_rod_rejected(order_client, fake_service, user):
    """TAIFEX 市價單不接受 ROD，只能搭配 IOC/FOK。"""
    resp = order_client.post("/orders", data={
        "client_order_id": "C-MKTROD", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "",
        "price_type": "MKT", "order_type": "ROD", "octype": "New",
    })
    assert resp.status_code == 200
    assert len(fake_service.placed) == 0


def test_place_order_without_service_shows_disabled_message(engine, user):
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    c = TestClient(app)
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    resp = c.post("/orders", data={
        "client_order_id": "C1", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert resp.status_code == 200  # 不是 500
    assert "未啟用" in resp.text


def test_place_order_authorization_error_maps_to_403(order_client, fake_service, user):
    fake_service._deny_user = user.id
    resp = order_client.post("/orders", data={
        "client_order_id": "C1", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert resp.status_code == 403


def test_update_order_authorization_error_maps_to_403(order_client, fake_service, user):
    """round3 #6：改單所有權由 service 內部驗證，route 只負責映射 403。"""
    fake_service._deny_user = user.id
    resp = order_client.put("/orders/B1", data={"qty": "2"})
    assert resp.status_code == 403


def test_positions_non_owner_gets_403(order_client, fake_service, user):
    fake_service._deny_user = user.id
    resp = order_client.get("/orders/positions")
    assert resp.status_code == 403


def test_cancel_order_authorization_error_maps_to_403(order_client, fake_service, user):
    fake_service._deny_user = user.id
    resp = order_client.delete("/orders/B1")
    assert resp.status_code == 403


def test_orders_list_scoped_by_mode(order_client, session, user):
    brepo.create_order(session, client_order_id="R1", request_hash="H1", user_id=user.id, mode="real",
                       broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
                       price=Decimal("18000"), price_type="LMT", order_type="ROD", octype="New",
                       trading_day="2026-06-16")
    brepo.create_order(session, client_order_id="S1", request_hash="H2", user_id=user.id, mode="sim",
                       broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
                       price=Decimal("18000"), price_type="LMT", order_type="ROD", octype="New",
                       trading_day="2026-06-16")
    session.commit()
    sim = order_client.get("/orders/list?mode=sim").text
    real = order_client.get("/orders/list?mode=real").text
    assert sim.count("<tr") <= 2 and real.count("<tr") <= 2  # header + 至多一筆資料列


def test_edit_order_form_without_service_shows_disabled_message(engine, user):
    """round3 #9：改單控制的 GET 端點，service 未啟用時同樣回可讀訊息，不是 500。"""
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    c = TestClient(app)
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    resp = c.get("/orders/B1/edit")
    assert resp.status_code == 200
    assert "未啟用" in resp.text


# ---------------------------------------------------------------------------
# Part 2：真 RiskGuard + 真 ShioajiAdapter（注入 _FakeApi，不連真網路）——
# round3 #1 的關鍵驗收：route → RiskGuard → adapter 的完整 real update round-trip。
# ---------------------------------------------------------------------------

class _FakeOrderHandle:
    def __init__(self, id_, seqno):
        self.id = id_
        self.seqno = seqno


class _FakeTrade:
    def __init__(self, id_, seqno):
        self.order = _FakeOrderHandle(id_, seqno)


class _FakeApi:
    """假 shioaji client：不連真網路，place_order/update_order 同步回傳並記錄呼叫參數。"""

    def __init__(self):
        self.futopt_account = type("Acc", (), {"account_id": "F1"})()
        self._seq = 0
        self.updated = []  # [(ordno, price, qty), ...]

    def Order(self, **kw):
        return kw

    def place_order(self, contract, order):
        self._seq += 1
        return _FakeTrade(f"ORD{self._seq}", f"SEQ{self._seq}")

    def cancel_order(self, ordno):
        return _FakeTrade(ordno, f"SEQ-{ordno}")

    def update_order(self, ordno, **kw):
        self.updated.append((ordno, kw.get("price"), kw.get("qty")))
        return _FakeTrade(ordno, f"SEQ-{ordno}")

    def logout(self):
        pass


def _real_guard(engine, *, owner_user_ids):
    return RiskGuard(
        session_factory=lambda: Session(engine), secret="s", owner_user_ids=owner_user_ids,
        symbol_whitelist=frozenset({"TXF"}), max_qty_per_order=20, max_qty_per_day=200,
        max_orders_per_day=200,
    )


def _real_adapter(engine, guard, *, mode="real"):
    adapter = ShioajiAdapter(
        api_key="k", secret_key="s", ca_path=None, ca_passwd=None, person_id=None,
        symbol="TXF", mode=mode, session_factory=lambda: Session(engine),
        supervisor=BrokerSupervisor(), risk_guard=guard,
    )
    adapter._api = _FakeApi()  # 跳過 connect()，直接注入假 client
    adapter._contract = object()
    adapter.account = "F1"
    return adapter


def _real_client(engine, user, adapter, guard):
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    app.state.order_service = adapter
    app.state.order_risk_guard = guard
    c = TestClient(app)
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    return c


def _place_real_order(client, engine, *, client_order_id="C1", **fields) -> str:
    """走完整兩步確認把一張 real 委託送出，回傳 broker_order_id 供後續改單測試用。"""
    form = {
        "client_order_id": client_order_id, "symbol": "TXF", "action": "Buy", "qty": "2",
        "price": "18000", "price_type": "LMT", "order_type": "ROD", "octype": "New",
    }
    form.update(fields)
    first = client.post("/orders", data=form)
    assert first.status_code == 200
    token = _hidden(first.text, "confirm_token")
    assert token is not None, f"預期回確認框，實際: {first.text}"
    second = client.post("/orders", data={**form, "confirm_token": token})
    assert second.status_code == 200
    with Session(engine) as s:
        order = brepo.find_order_by_client_order_id(s, client_order_id)
        assert order is not None and order.status == "submitted", order.status if order else None
        return order.broker_order_id


def test_real_place_two_step_confirm_round_trip_creates_order(engine, user):
    guard = _real_guard(engine, owner_user_ids=frozenset({user.id}))
    adapter = _real_adapter(engine, guard)
    client = _real_client(engine, user, adapter, guard)

    broker_order_id = _place_real_order(client, engine)
    assert broker_order_id  # 真的送出去了（adapter._api.place_order 有回 ordno/seqno）


def test_real_place_mkt_order_with_empty_price_succeeds_through_full_pipeline(engine, user):
    """bug 2 端到端驗收：表單→OrderRequest→RiskGuard→adapter 全走真實元件（sim 模式跳過
    兩階段確認），MKT + 空/0 價格 + IOC 不再報 decimal.ConversionSyntax，也不被
    price>0 檢查擋下。"""
    guard = _real_guard(engine, owner_user_ids=frozenset({user.id}))
    adapter = _real_adapter(engine, guard, mode="sim")
    client = _real_client(engine, user, adapter, guard)

    resp = client.post("/orders", data={
        "client_order_id": "C-MKT-REAL", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "",
        "price_type": "MKT", "order_type": "IOC", "octype": "New",
    })
    assert resp.status_code == 200
    with Session(engine) as s:
        order = brepo.find_order_by_client_order_id(s, "C-MKT-REAL")
        assert order is not None and order.status == "submitted"
        assert order.price == Decimal("0")


def test_real_update_round_trip_price_only_does_not_deadlock(engine, user):
    """round3 #1 核心驗收：只改價（qty 沿用既有值），round-trip 不鎖死。"""
    guard = _real_guard(engine, owner_user_ids=frozenset({user.id}))
    adapter = _real_adapter(engine, guard)
    client = _real_client(engine, user, adapter, guard)
    broker_order_id = _place_real_order(client, engine, client_order_id="C-PRICE", qty="2", price="18000")

    first = client.put(f"/orders/{broker_order_id}", data={"price": "18500"})
    assert first.status_code == 200
    token = _hidden(first.text, "confirm_token")
    assert token is not None, f"預期改單回確認框，實際: {first.text}"
    dialog_qty = _hidden(first.text, "qty")
    dialog_price = _hidden(first.text, "price")
    assert dialog_qty == "2"  # 合併後沿用既有口數，不是被 route 端瞎猜的值蓋掉
    assert dialog_price == "18500"

    second = client.put(f"/orders/{broker_order_id}", data={
        "price": dialog_price, "qty": dialog_qty, "confirm_token": token,
    })
    assert second.status_code == 200

    with Session(engine) as s:
        order = brepo.find_order_by_broker_id(s, broker="shioaji", account="F1", mode="real",
                                              broker_order_id=broker_order_id)
        assert order.status == "submitted"
        assert order.price == Decimal("18500")
        assert order.qty == 2
    assert adapter._api.updated[-1][1] == 18500.0
    assert adapter._api.updated[-1][2] == 2


def test_real_update_round_trip_qty_only_does_not_deadlock(engine, user):
    """round3 #1 核心驗收：只改量（price 沿用既有值），round-trip 不鎖死。"""
    guard = _real_guard(engine, owner_user_ids=frozenset({user.id}))
    adapter = _real_adapter(engine, guard)
    client = _real_client(engine, user, adapter, guard)
    broker_order_id = _place_real_order(client, engine, client_order_id="C-QTY", qty="2", price="18000")

    first = client.put(f"/orders/{broker_order_id}", data={"qty": "5"})
    assert first.status_code == 200
    token = _hidden(first.text, "confirm_token")
    assert token is not None, f"預期改單回確認框，實際: {first.text}"
    dialog_qty = _hidden(first.text, "qty")
    dialog_price = _hidden(first.text, "price")
    assert dialog_qty == "5"
    assert dialog_price == "18000"  # 合併後沿用既有價格，不是被 route 端瞎猜的值蓋掉

    second = client.put(f"/orders/{broker_order_id}", data={
        "price": dialog_price, "qty": dialog_qty, "confirm_token": token,
    })
    assert second.status_code == 200

    with Session(engine) as s:
        order = brepo.find_order_by_broker_id(s, broker="shioaji", account="F1", mode="real",
                                              broker_order_id=broker_order_id)
        assert order.status == "submitted"
        assert order.qty == 5
        assert order.price == Decimal("18000")
    assert adapter._api.updated[-1][2] == 5


def test_real_update_round_trip_price_and_qty_together_does_not_deadlock(engine, user):
    """round3 #1：價量都改的一般情形也要 round-trip 成功（合併規則對兩者皆顯式提供時等價於直接採用新值）。"""
    guard = _real_guard(engine, owner_user_ids=frozenset({user.id}))
    adapter = _real_adapter(engine, guard)
    client = _real_client(engine, user, adapter, guard)
    broker_order_id = _place_real_order(client, engine, client_order_id="C-BOTH", qty="2", price="18000")

    first = client.put(f"/orders/{broker_order_id}", data={"qty": "3", "price": "18200"})
    assert first.status_code == 200
    token = _hidden(first.text, "confirm_token")
    assert token is not None

    second = client.put(f"/orders/{broker_order_id}", data={"price": "18200", "qty": "3", "confirm_token": token})
    assert second.status_code == 200

    with Session(engine) as s:
        order = brepo.find_order_by_broker_id(s, broker="shioaji", account="F1", mode="real",
                                              broker_order_id=broker_order_id)
        assert order.status == "submitted"
        assert order.qty == 3
        assert order.price == Decimal("18200")


def test_real_update_wrong_confirm_token_stays_needs_confirm_not_silently_accepted(engine, user):
    """反向驗證：token 與實際送出內容不符時，round-trip 不會「意外通過」——證明前面幾個
    round-trip 測試的成功不是因為 route/adapter 對 token 內容照單全收。"""
    guard = _real_guard(engine, owner_user_ids=frozenset({user.id}))
    adapter = _real_adapter(engine, guard)
    client = _real_client(engine, user, adapter, guard)
    broker_order_id = _place_real_order(client, engine, client_order_id="C-BADTOKEN", qty="2", price="18000")

    first = client.put(f"/orders/{broker_order_id}", data={"qty": "5"})
    token = _hidden(first.text, "confirm_token")
    assert token is not None

    # 帶著「改量為 5」簽出的 token，卻改送不同的量（9）——payload_hash 對不上，應再次卡在確認框。
    tampered = client.put(f"/orders/{broker_order_id}", data={"qty": "9", "confirm_token": token})
    assert tampered.status_code == 200
    assert _hidden(tampered.text, "confirm_token") is not None  # 又是一個新的確認框，不是靜默通過
    with Session(engine) as s:
        order = brepo.find_order_by_broker_id(s, broker="shioaji", account="F1", mode="real",
                                              broker_order_id=broker_order_id)
        assert order.qty == 2  # 沒有被竄改的請求改動


def test_real_update_non_owner_rejected_with_403(engine, user):
    """round3 #6：改單所有權由 adapter/RiskGuard 內部驗證，route 映射 403（即使對方也是
    owner allowlist 內的合法 owner——是「別人的委託」這件事本身要擋，比照
    test_shioaji_adapter.py::test_cancel_rejects_non_owner_of_order_even_when_actor_is_a_different_owner）。"""
    from quanquant.auth import service as auth_service

    with Session(engine) as s:
        other_user = auth_service.create_user(s, "other", "test-pw", display_name="Other", role="admin")

    guard = _real_guard(engine, owner_user_ids=frozenset({user.id, other_user.id}))
    adapter = _real_adapter(engine, guard)
    client = _real_client(engine, user, adapter, guard)
    broker_order_id = _place_real_order(client, engine, client_order_id="C-OWNER", qty="2", price="18000")

    def _session_override():
        with Session(engine) as s:
            yield s

    other_app = create_app()
    other_app.dependency_overrides[get_session] = _session_override
    other_app.dependency_overrides[get_poller] = lambda: None
    other_app.state.order_service = adapter
    other_app.state.order_risk_guard = guard
    other_client = TestClient(other_app)
    other_client.cookies.set(SESSION_COOKIE, sign_session(other_user.id, other_user.token_version))

    resp = other_client.put(f"/orders/{broker_order_id}", data={"qty": "5"})
    assert resp.status_code == 403


def test_edit_order_form_prefills_existing_qty_and_price(engine, user):
    """round3 #9：改單 GET 端點回傳的表單要帶現有 qty/price 預填。"""
    guard = _real_guard(engine, owner_user_ids=frozenset({user.id}))
    adapter = _real_adapter(engine, guard)
    client = _real_client(engine, user, adapter, guard)
    broker_order_id = _place_real_order(client, engine, client_order_id="C-EDIT", qty="4", price="18300")

    resp = client.get(f"/orders/{broker_order_id}/edit")
    assert resp.status_code == 200
    assert _hidden(resp.text, "qty") == "4"
    assert _hidden(resp.text, "price") == "18300"
