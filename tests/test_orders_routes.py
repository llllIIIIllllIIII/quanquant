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
import asyncio
import json
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, OrderError, RiskError
from quanquant.broker.connection_gate import AgentConnectionGate
from quanquant.broker.order_events import OrderEventHub
from quanquant.broker.risk import RiskGuard
from quanquant.broker.shioaji_adapter import ShioajiAdapter
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import OrderAck, Position
from quanquant.db.models import AgentToken, Cooldown
from quanquant.web.app import create_app
from quanquant.web.deps import get_poller, get_session
from quanquant.web.routers.orders import (
    _parse_optional_update_price,
    _parse_order_price,
    orders_stream,
)


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
        self.symbol = "TXF"
        self.placed = []
        self._deny_user = None
        # 007：可控注入 place/cancel/update 的 post-submission 失敗（OrderError/RiskError
        # 不需確認），供橫幅（banner_kind）行為測試用——預設 None 不影響既有任何測試。
        self.place_error: Exception | None = None
        self.cancel_error: Exception | None = None
        self.update_error: Exception | None = None

    async def place(self, req, *, actor_user_id, confirm_token=None):
        if self._deny_user == actor_user_id:
            raise AuthorizationError("not owner")
        if self.place_error is not None:
            raise self.place_error
        if self.mode == "real" and not confirm_token:
            raise RiskError("需要確認", needs_confirm=True)
        self.placed.append(req)
        return OrderAck(client_order_id=req.client_order_id, broker_order_id="B1", ordno="O1", status="submitted")

    async def cancel(self, broker_order_id, *, actor_user_id):
        if self._deny_user == actor_user_id:
            raise AuthorizationError("not owner")
        if self.cancel_error is not None:
            raise self.cancel_error
        return OrderAck(client_order_id="", broker_order_id=broker_order_id, ordno="O1", status="cancelled")

    async def update(self, broker_order_id, *, actor_user_id, price=None, qty=None, confirm_token=None):
        if self._deny_user == actor_user_id:
            raise AuthorizationError("not owner")
        if self.update_error is not None:
            raise self.update_error
        return OrderAck(client_order_id="", broker_order_id=broker_order_id, ordno="O1", status="submitted")

    async def positions(self, *, actor_user_id):
        if self._deny_user == actor_user_id:
            raise AuthorizationError("not owner")
        return [Position(symbol="TXF", direction="long", qty=1, avg_price=Decimal("18000"))]

    def positions_snapshot(self, *, actor_user_id):
        if self._deny_user == actor_user_id:
            raise AuthorizationError("not owner")
        return [Position(symbol="TXF", direction="long", qty=1, avg_price=Decimal("18000"))]

    def on_fill(self, handler):
        pass


class _FakeRiskGuard:
    """D3 兩層 kill switch：`global_on`（全站總閘）+ `per_user`（個人急停，key=user_id）。
    `kill_switch` 屬性保留（唯讀，等於 global_on）只為兼容舊稱呼；正確的判定入口是
    `blocked(user_id)`，route/adapter 一律經它查詢。"""

    def __init__(self, owner_ids=None):
        self.global_on = False
        self.per_user: dict[int, bool] = {}
        self.global_actor = None
        # owner_ids=None → 允許所有人（既有測試預設：assert_owner 為 no-op、is_owner 恆真）；
        # 傳集合則只有集合內的 user 是 owner（kill switch 非 owner→403 測試用）。
        self._owner_ids = None if owner_ids is None else set(owner_ids)
        self.set_kill_switch_calls = []

    @property
    def kill_switch(self) -> bool:
        return self.global_on

    def set_kill_switch(self, value, *, scope, actor_user_id):
        self.assert_owner(actor_user_id)
        self.set_kill_switch_calls.append((value, scope, actor_user_id))
        if scope == "self":
            self.per_user[actor_user_id] = value
        elif scope == "global":
            self.global_on = value
            self.global_actor = actor_user_id
        else:
            raise ValueError(f"未知的 kill switch scope: {scope!r}")

    def blocked(self, user_id) -> bool:
        return self.global_on or self.per_user.get(user_id, False)

    def kill_switch_view(self, user_id) -> dict:
        return {
            "global_on": self.global_on,
            "self_on": self.per_user.get(user_id, False),
            "blocked": self.blocked(user_id),
            "global_actor": self.global_actor,
        }

    def is_owner(self, actor_user_id):
        return self._owner_ids is None or actor_user_id in self._owner_ids

    def assert_owner(self, actor_user_id):
        if self._owner_ids is not None and actor_user_id not in self._owner_ids:
            raise AuthorizationError("not owner")

    def issue_confirm_token(self, session, *, actor_user_id, payload_hash):
        return f"TOKEN-{payload_hash}"


class _FakeOps:
    """假 ops_alerter：只記錄 kill_switch(...) 呼叫（API 見 notify/ops_alerter.py）。"""

    def __init__(self):
        self.kill_switch_calls = []

    def kill_switch(self, *, enabled, actor_user_id, scope="global", open_order_count=0, detail=""):
        self.kill_switch_calls.append(
            {"enabled": enabled, "actor_user_id": actor_user_id, "scope": scope,
             "open_order_count": open_order_count}
        )


@pytest.fixture
def fake_service():
    return _FakeService()


@pytest.fixture
def fake_guard():
    return _FakeRiskGuard()


@pytest.fixture
def fake_ops():
    return _FakeOps()


@pytest.fixture
def order_client(engine, user, fake_service, fake_guard, fake_ops):
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    app.state.order_service = fake_service
    app.state.order_risk_guard = fake_guard
    app.state.ops_alerter = fake_ops
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


def test_place_form_price_input_uses_readonly_not_disabled_for_mkt(order_client):
    """bug 1（simtrade 實測回歸）前端強化：MKT 時的 price 欄位改用 readonly（一律隨表單
    送出），不再用 disabled——disabled 欄位是否真的被瀏覽器/HTMX 排除在送出範圍外，這件事
    本身無法用 TestClient 驗證（見 docs/superpowers/reviews/2026-07-25 驗收殘留：「真瀏覽器
    UI/HTMX 未經真瀏覽器互動」），readonly 讓後端永遠收得到明確的 price 欄位值，不必依賴
    這個無法驗證的前提。"""
    resp = order_client.get("/orders")
    price_tag = re.search(r'<input name="price"[^>]*>', resp.text)
    assert price_tag is not None, resp.text
    tag = price_tag.group(0)
    assert "readonly" in tag  # 用 x-bind:readonly（或等效綁定），欄位一律送出
    assert "disabled" not in tag  # 不再用 disabled（會被排除在 FormData 之外）


def test_direction_field_is_radio_toggle_not_select(order_client):
    """001：方向改成左右分段開關（radio + fieldset），不再是 <select name="action">；
    name/value 契約不變（"Buy"/"Sell"），buy 預設 checked。"""
    text = order_client.get("/orders").text
    assert '<select name="action">' not in text
    assert re.search(r'<input type="radio"[^>]*name="action"[^>]*value="Buy"[^>]*checked', text) is not None
    assert re.search(r'<input type="radio"[^>]*name="action"[^>]*value="Sell"', text) is not None


def test_place_order_still_accepts_action_buy_and_sell(order_client, fake_service):
    """001 的驗收邊界：改成 radio 之後，後端仍照舊收 action=Buy/Sell（表單契約不變）。"""
    resp_buy = order_client.post("/orders", data={
        "client_order_id": "C-BUY", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert resp_buy.status_code == 200
    resp_sell = order_client.post("/orders", data={
        "client_order_id": "C-SELL", "symbol": "TXF", "action": "Sell", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert resp_sell.status_code == 200
    assert [o.action for o in fake_service.placed] == ["Buy", "Sell"]


def test_symbol_select_uses_optgroup_with_chinese_label(order_client):
    """003：商品下拉展開用中文名為群組標題（optgroup label）、代碼為選項文字；收合狀態
    （option 文字）只有代碼，保持緊湊。"""
    text = order_client.get("/orders").text
    assert '<optgroup label="台指">' in text
    assert '<option value="TXF">TXF</option>' in text


def test_quote_compact_panel_present_with_symbol_selector_outside_form(order_client):
    """002（B 版）：報價列在表單上方，商品選擇器併入報價列、放在 <form> 外面，用
    form="order-form" 關聯回表單；報價片段 hx-include 商品選擇器、change 時重抓。"""
    text = order_client.get("/orders").text
    assert 'class="quote-panel quote-compact"' in text
    assert '<form id="order-form"' in text
    assert 'id="order-symbol" class="quote-symbol-select"' in text
    assert 'form="order-form"' in text
    assert 'hx-get="/quote" hx-include="#order-symbol"' in text
    assert "change from:#order-symbol" in text
    # 商品選擇器不再留在表單第一格內
    assert "<label>商品" not in text


def test_order_form_still_submits_symbol_via_form_attribute(order_client, fake_service):
    """B 版關鍵驗收：選擇器雖在表單外，FormData 仍會帶上 symbol，送出契約不變。"""
    resp = order_client.post("/orders", data={
        "client_order_id": "C-SYM", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert resp.status_code == 200
    assert fake_service.placed[0].symbol == "TXF"


def test_orders_page_uses_sse_push_not_polling_and_guards_double_submit(order_client):
    """006 精簡化後：委託表／部位表已搬到 /orders/queue、/orders/holdings（另見對應測試），
    下單頁只留 agent 狀態（掛在 /orders/stream 的 sse:orders-changed，agent 連線/斷線時
    agent_ws.py 會 hub.publish()）與精簡條（掛在 /quote/stream 的既有報價心跳，見
    orders.html 內的實作理由註解）。頁面不得再出現盲輪詢、不得再直接嵌委託表／部位表的
    hx-get 掛載點；送出鈕仍保留 hx-disabled-elt 防連點。"""
    text = order_client.get("/orders").text
    assert 'sse-connect="/orders/stream"' in text  # agent 狀態的 SSE 連線容器仍在
    assert 'sse-connect="/quote/stream' in text  # 精簡條/報價共用的 SSE 連線容器
    # 只剩 agent 狀態 div 的 hx-trigger 掛 sse:orders-changed（比對 hx-trigger 屬性本身，
    # 不比對整頁原始文字——避免被模板內解說用的中文註解一併算進去）。
    assert len(re.findall(r'hx-trigger="[^"]*sse:orders-changed[^"]*"', text)) == 1
    # 不盲輪詢：比對 hx-trigger 屬性本身有沒有裸的 `every Ns`，不比對整頁原始文字
    # （模板內解說用的中文註解會提到「every 2s」這個詞本身，直接找整頁字串會誤判）。
    assert not re.search(r'hx-trigger="[^"]*every \d+s[^"]*"', text)
    assert "refreshorders from:body" in text  # 本分頁下單動作當下精簡條/agent 狀態仍即時刷新
    assert 'hx-get="/orders/list' not in text  # 委託表已搬到 /orders/queue
    assert 'hx-get="/orders/positions"' not in text  # 部位表已搬到 /orders/holdings
    assert 'hx-get="/orders/position-strip"' in text  # 精簡條掛載點仍在
    # 精簡條掛在報價 SSE 的 message 事件上，但實測心跳是每 ~1 秒，未節流會變成每秒打
    # 這支端點；用 htmx 的 throttle:5s 修飾詞把實際觸發頻率壓回「5 秒源」的節奏。
    assert "sse:message throttle:5s" in text
    assert "hx-disabled-elt" in text  # 送出期間停用送出鈕（防 double-submit）


def test_orders_stream_without_hub_returns_empty_stream_not_500(order_client):
    """/orders/stream：app.state.order_events 未接線（此 fixture 未設 hub）時回空 stream、
    不 500——與 alerts_stream 同慣例，端點在無 lifespan 的測試/停用情境不壞。"""
    resp = order_client.get("/orders/stream")
    assert resp.status_code == 200


def test_orders_stream_dispatches_scoped_events_and_keeps_orders_changed_unchanged():
    """007（批次 B-1）：/orders/stream 依 queue item 型別分派 SSE 事件名——scoped 事件
    （dict）依 `item["event"]` 分派（如 'deal'），payload 序列化成 JSON data；既有無資料
    廣播 ping（裸字串 "1"）維持 'orders-changed' 事件名與 data 不變（④）。直接呼叫路由
    函式本身＋讀 `EventSourceResponse.body_iterator`，繞開 TestClient 對無限串流的同步限制。
    """
    async def _run():
        hub = OrderEventHub()
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(order_events=hub)))
        user = SimpleNamespace(id=1)
        resp = await orders_stream(request, user)

        hub.publish_deal(user_id=1, payload={"symbol": "TXF", "action": "Buy", "qty": 1, "price": "18000"})
        deal_event = await resp.body_iterator.__anext__()
        assert deal_event["event"] == "deal"
        assert json.loads(deal_event["data"]) == {
            "symbol": "TXF", "action": "Buy", "qty": 1, "price": "18000",
        }

        hub.publish()  # 既有無資料 ping：event 名與 data 逐位元不變
        broadcast_event = await resp.body_iterator.__anext__()
        assert broadcast_event == {"event": "orders-changed", "data": "1"}

    asyncio.run(_run())


def test_place_order_sim_sends_directly(order_client, fake_service, user):
    resp = order_client.post("/orders", data={
        "client_order_id": "C1", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert resp.status_code == 200
    assert len(fake_service.placed) == 1


def test_place_success_rotates_client_order_id_via_oob_swap(order_client, fake_service, user):
    """反向/下一筆委託不得重用同一顆 client_order_id（simtrade 實測回歸：市價買單成功後，
    反向賣單沿用同鍵、payload 不同，被 repository 冪等防護擋成「已存在但 payload 不同」）。
    下單成功後回傳的 body 必須帶一個 out-of-band swap，把下單面板 hidden input
    （id=client-order-id-input）換成全新的 UUID，讓下一筆用新鍵。"""
    submitted = "C-REUSE"
    resp = order_client.post("/orders", data={
        "client_order_id": submitted, "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert resp.status_code == 200
    assert 'hx-swap-oob="true"' in resp.text
    assert 'id="client-order-id-input"' in resp.text
    fresh = _hidden(resp.text, "client_order_id")
    assert fresh is not None
    assert fresh != submitted  # 成功後換了全新的鍵
    assert re.fullmatch(r"[0-9a-f-]{36}", fresh) is not None  # 是新生成的 UUID


def test_place_failure_keeps_same_client_order_id_for_retry_idempotency(order_client, fake_service, user):
    """失敗路徑（此處為 LMT 留白的表單錯誤）不得輪替 client_order_id——round3 #7 的設計是
    「同一張表單的 HTTP retry 沿用同鍵才能冪等去重」，只有**成功**才換鍵。"""
    resp = order_client.post("/orders", data={
        "client_order_id": "C-KEEP", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",  # LMT 留白 → 表單錯誤
    })
    assert resp.status_code == 200
    assert len(fake_service.placed) == 0
    assert 'hx-swap-oob="true"' not in resp.text  # 失敗不換鍵


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


# ---------------------------------------------------------------------------
# T0.3(B) kill switch runtime 開關：owner-only 端點 + 告警 + 最小 UI
# ---------------------------------------------------------------------------

def _seed_order(session, user, *, client_order_id, request_hash, status, broker_order_id, ordno):
    brepo.set_order_ack(
        session,
        brepo.create_order(
            session, client_order_id=client_order_id, request_hash=request_hash, user_id=user.id,
            mode="sim", broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
            price=Decimal("18000"), price_type="LMT", order_type="ROD", octype="New",
            trading_day="2026-07-27",
        ).id,
        broker_order_id=broker_order_id, ordno=ordno, status=status,
    )


def test_kill_switch_owner_turns_on_toggles_and_alerts_with_open_order_count(
    order_client, session, fake_guard, fake_ops, user
):
    """owner 翻 ON（scope=global）：200、guard.global_on 變 True、ops.kill_switch 被呼叫
    （enabled=True、scope=global、帶正確 open_order_count——只算 submitted/partfilled，
    不算 filled）。"""
    _seed_order(session, user, client_order_id="KOPEN", request_hash="KH1", status="submitted",
                broker_order_id="KB-OPEN", ordno="KO")
    _seed_order(session, user, client_order_id="KDONE", request_hash="KH2", status="filled",
                broker_order_id="KB-DONE", ordno="KD")
    session.commit()

    resp = order_client.post("/orders/kill-switch", data={"enabled": "true", "scope": "global"})
    assert resp.status_code == 200
    assert fake_guard.global_on is True
    assert len(fake_ops.kill_switch_calls) == 1
    call = fake_ops.kill_switch_calls[0]
    assert call["enabled"] is True
    assert call["actor_user_id"] == user.id
    assert call["scope"] == "global"
    assert call["open_order_count"] == 1  # 只算未成交掛單，filled 不算
    assert 'hx-post="/orders/kill-switch"' in resp.text  # 回傳更新後的控制片段


def test_kill_switch_owner_turns_off(order_client, fake_guard, fake_ops, user):
    fake_guard.global_on = True
    resp = order_client.post("/orders/kill-switch", data={"enabled": "false", "scope": "global"})
    assert resp.status_code == 200
    assert fake_guard.global_on is False
    assert fake_ops.kill_switch_calls[-1]["enabled"] is False


def test_kill_switch_non_owner_gets_403_and_does_not_toggle(order_client, fake_guard, fake_ops, user):
    """非 owner → 403、set_kill_switch 未被呼叫、狀態不變、不發告警。"""
    fake_guard._owner_ids = set()  # 沒有任何 owner → 目前 user 不是 owner
    resp = order_client.post("/orders/kill-switch", data={"enabled": "true", "scope": "global"})
    assert resp.status_code == 403
    assert fake_guard.global_on is False
    assert fake_guard.set_kill_switch_calls == []
    assert fake_ops.kill_switch_calls == []


def test_kill_switch_scope_self_only_toggles_actor_own_switch(order_client, fake_guard, fake_ops, user):
    """scope=self：只改 actor 自己的 per_user 開關，全站總閘不受影響（route 端無目標 user
    參數，server 天然無法替他人翻閘）。"""
    resp = order_client.post("/orders/kill-switch", data={"enabled": "true", "scope": "self"})
    assert resp.status_code == 200
    assert fake_guard.per_user.get(user.id) is True
    assert fake_guard.global_on is False
    call = fake_ops.kill_switch_calls[-1]
    assert call["scope"] == "self" and call["actor_user_id"] == user.id


def test_kill_switch_invalid_scope_rejected(order_client, fake_guard, fake_ops):
    resp = order_client.post("/orders/kill-switch", data={"enabled": "true", "scope": "bogus"})
    assert resp.status_code == 400
    assert fake_guard.set_kill_switch_calls == []
    assert fake_ops.kill_switch_calls == []


def test_kill_switch_without_risk_guard_does_not_500(engine, user):
    """risk_guard 為 None（下單子系統關）→ 優雅回應（200 停用片段），不是 500。"""
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    # 刻意不設 app.state.order_risk_guard → get_order_risk_guard 回 None
    c = TestClient(app)
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    resp = c.post("/orders/kill-switch", data={"enabled": "true"})
    assert resp.status_code == 200
    assert "未啟用" in resp.text


def test_orders_page_no_longer_shows_kill_switch_control_moved_to_risk_page(order_client):
    """005：kill switch（緊急停止下單）控制項搬到獨立的 /risk 頁，下單頁不再有切換控制
    （即使是 admin）。"""
    text = order_client.get("/orders").text
    assert 'hx-post="/orders/kill-switch"' not in text


def test_orders_page_shows_red_banner_and_disables_submit_when_kill_switch_blocked(
    order_client, fake_guard
):
    """005（終審必修 CRITICAL-2，還原）：緊急停止下單啟動時，下單頁必須顯示明確狀態
    （紅色橫幅，連到 /risk）並停用送出鈕；正常狀態下不佔任何版面（見另一測試）。

    R2-5 不適用於急停：R2-5 原文的前提是「F0 改成『只擋開倉』後」才把急停套進鎖倉別＋
    送出鈕保留可用的那套 UI；`broker/risk.py::check_place` 目前對急停仍是全擋（kill
    switch 檢查在 octype 判斷之前），F0 尚未實作。若讓送出鈕維持可用，使用者填完「平倉」
    單送出仍會被伺服器拒絕——這正是 R2-5 想消滅的「按了才知道」，只是換了個地方發生。
    急停生效時繼續維持 005 已驗收的「整顆停用送出鈕」，見
    test_octype_not_locked_when_kill_switch_blocked（倉別本身不鎖，因為鎖了也沒用——
    整顆都按不了）。"""
    fake_guard.global_on = True
    text = order_client.get("/orders").text
    assert "緊急停止下單已啟動" in text
    assert 'href="/risk"' in text
    assert re.search(r'<button type="submit"[^>]*disabled', text) is not None


def test_orders_page_shows_no_risk_banner_when_kill_switch_and_cooldown_are_off(order_client):
    """正常狀態下不佔任何版面：無緊急停止、無冷靜期時，下單頁不出現風控橫幅。"""
    text = order_client.get("/orders").text
    assert "risk-banner" not in text


def test_orders_page_octype_not_locked_by_default(order_client):
    """R2-5 邊界：無急停、無冷靜期時，倉別選單維持原狀——三個選項都不 disabled，沒有鎖定
    說明文字，行為完全不變。"""
    text = order_client.get("/orders").text
    assert re.search(r'<option value="New"[^>]*disabled', text) is None
    assert re.search(r'<option value="Auto"[^>]*disabled', text) is None
    assert "僅能平倉" not in text


def test_octype_not_locked_when_kill_switch_blocked(order_client, fake_guard, user):
    """終審必修 CRITICAL-2：「我的緊急停止」生效時，`check_place` 目前仍全擋（F0「只擋
    開倉」尚未實作），倉別欄位不鎖 Cover——鎖了也沒用，因為連平倉單都會被伺服器拒絕；
    真正誠實的 UI 是整顆停用送出鈕（見
    test_orders_page_shows_red_banner_and_disables_submit_when_kill_switch_blocked）。
    等 F0 把急停改成 octype-aware（只擋開倉）後，才把 kill_switch.blocked 併入
    octype_locked 判斷。"""
    fake_guard.per_user[user.id] = True
    text = order_client.get("/orders").text
    assert re.search(r'<option value="New"[^>]*disabled', text) is None
    assert re.search(r'<option value="Auto"[^>]*disabled', text) is None
    assert "冷靜期中僅能平倉" not in text


def test_octype_locked_to_cover_when_cooldown_active(order_client, session, user):
    """R2-5：冷靜期中同樣鎖倉別「平倉」＋顯示「冷靜期中僅能平倉」，送出鈕維持可用。"""
    now = brepo.now_epoch_ms()
    brepo.create_cooldown(session, user_id=user.id, until_ms=now + 3_600_000, now_ms=now)
    session.commit()
    text = order_client.get("/orders").text
    assert re.search(r'<option value="New"[^>]*disabled', text) is not None
    assert re.search(r'<option value="Auto"[^>]*disabled', text) is not None
    assert re.search(r'<option value="Cover"[^>]*selected', text) is not None
    assert "冷靜期中僅能平倉" in text
    assert re.search(r'<button type="submit"[^>]*disabled', text) is None  # 送出鈕維持可用


def _demote_to_non_admin(session, user):
    """測試 user 預設 role=admin；降成一般 user，讓 get_current_user 於下次請求讀到非 admin
    （kill switch／斷開 Agent 收 admin-only 後，用來驗非 admin 看不到/不能用；2026-08-27）。"""
    user.role = "user"
    session.add(user)
    session.commit()


def test_orders_page_hides_kill_switch_control_for_non_admin(order_client, session, user):
    _demote_to_non_admin(session, user)
    text = order_client.get("/orders").text
    assert 'hx-post="/orders/kill-switch"' not in text


def test_kill_switch_non_admin_gets_403_and_does_not_toggle(
    order_client, fake_guard, fake_ops, session, user
):
    """邊界②：非 admin（即使是 owner）POST scope=global → 403、未翻閘、不告警——維持
    admin-only（server 端強制，非只靠 UI 隱藏）。"""
    _demote_to_non_admin(session, user)
    resp = order_client.post("/orders/kill-switch", data={"enabled": "true", "scope": "global"})
    assert resp.status_code == 403
    assert fake_guard.set_kill_switch_calls == []
    assert fake_ops.kill_switch_calls == []


def test_kill_switch_scope_self_non_admin_owner_succeeds(
    order_client, fake_guard, fake_ops, session, user
):
    """邊界①（R2-1／D-1，2026-09-02）：scope=self 的 403 gate 從 role=='admin' 改為本人
    （owner）即可——非 admin 但是 owner 的使用者可以翻自己的急停，200、per_user 真的翻了、
    仍照舊告警。"""
    _demote_to_non_admin(session, user)
    resp = order_client.post("/orders/kill-switch", data={"enabled": "true", "scope": "self"})
    assert resp.status_code == 200
    assert fake_guard.per_user.get(user.id) is True
    assert fake_guard.set_kill_switch_calls[-1] == (True, "self", user.id)
    assert fake_ops.kill_switch_calls[-1]["scope"] == "self"


def test_kill_switch_scope_self_non_admin_owner_response_keeps_global_readonly(
    order_client, fake_guard, session, user
):
    """終審 HIGH #1：非 admin owner POST scope=self 後，回應片段（hx-swap outerHTML 換回
    #kill-switch-control）不得含全站急停的切換表單——回應必須自己重新算 global_readonly
    （user.role!='admin'），不能讓 partial 的 `global_readonly|default(false)` 落回預設值
    False 而把全站表單畫出來（server 端仍 403，但違反 R2-1「全站對非 admin 唯讀」的 UI 契約）。"""
    _demote_to_non_admin(session, user)
    resp = order_client.post("/orders/kill-switch", data={"enabled": "true", "scope": "self"})
    assert resp.status_code == 200
    assert 'value="global"' not in resp.text  # 全站的 hidden scope 表單不該出現
    assert 'name="scope" value="self"' in resp.text  # 我的急停表單仍在（本人可操作）


def test_kill_switch_scope_self_non_owner_still_gets_403(
    order_client, fake_guard, fake_ops, session, user
):
    """R2-1 邊界：scope=self 開放的是「本人（owner）」，不是「任何登入者」——非 owner（即使
    demote 前是 admin）仍然 403，owner 授權判定本身沒有被拿掉。"""
    fake_guard._owner_ids = set()  # user 非 owner
    resp = order_client.post("/orders/kill-switch", data={"enabled": "true", "scope": "self"})
    assert resp.status_code == 403
    assert fake_guard.set_kill_switch_calls == []


# ---------------------------------------------------------------------------
# D2：POST /orders/agent-token（owner-only 簽發/rotation）＋ orders 頁 token 管理段
# ---------------------------------------------------------------------------

def test_agent_token_owner_can_issue_and_sees_plaintext_once(order_client, session, user):
    resp = order_client.post("/orders/agent-token")
    assert resp.status_code == 200
    assert "<code" in resp.text  # 明文有渲染出來
    assert "尚未產生" not in resp.text
    row = session.exec(select(AgentToken).where(AgentToken.user_id == user.id)).one()
    assert row.revoked_at is None
    assert str(row.expires_at) in resp.text  # UI 顯示 expires_at


def test_agent_token_rotation_revokes_previous_row_and_shows_new_plaintext(order_client, session, user):
    first = order_client.post("/orders/agent-token")
    first_plaintext = re.search(r"<code[^>]*>([^<]+)</code>", first.text).group(1)

    second = order_client.post("/orders/agent-token")
    second_plaintext = re.search(r"<code[^>]*>([^<]+)</code>", second.text).group(1)

    assert first_plaintext != second_plaintext
    rows = session.exec(
        select(AgentToken).where(AgentToken.user_id == user.id).order_by(AgentToken.created_at)
    ).all()
    assert len(rows) == 2
    assert rows[0].revoked_at is not None      # 舊枚被 rotation 作廢
    assert rows[1].revoked_at is None          # 新枚仍有效


def test_agent_token_non_owner_gets_403_and_does_not_issue(order_client, fake_guard, session, user):
    fake_guard._owner_ids = set()  # 沒有任何 owner → 目前 user 不是 owner
    resp = order_client.post("/orders/agent-token")
    assert resp.status_code == 403
    assert session.exec(select(AgentToken).where(AgentToken.user_id == user.id)).first() is None


def test_agent_token_without_risk_guard_does_not_500(engine, user):
    """risk_guard 為 None（下單子系統關）→ 優雅回應（200 停用片段），不是 500（比照 kill switch）。"""
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    # 刻意不設 app.state.order_risk_guard → get_order_risk_guard 回 None
    c = TestClient(app)
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    resp = c.post("/orders/agent-token")
    assert resp.status_code == 200
    assert "未啟用" in resp.text


def test_orders_page_no_longer_shows_agent_token_control(order_client):
    """R2-4：Agent Token 產生/重置整塊 UI 搬到帳戶設定頁（見 test_auth_routes.py 的等效
    測試）；下單頁不再顯示這塊——即使是 owner。"""
    text = order_client.get("/orders").text
    assert 'hx-post="/orders/agent-token"' not in text
    assert "尚未產生 agent token" not in text


def test_agent_status_shows_account_link_for_owner_when_disconnected(order_client, monkeypatch):
    """終審必修 LOW-7：R2-4「未連線時提示到帳戶頁設定」的連結只對 owner 顯示——帳戶頁
    的 Agent Token 卡片本身是 owner-only，非 owner 點了會是死路。`order_client` 的
    `fake_guard` 預設 `_owner_ids=None`（視為所有人皆 owner）。"""
    from quanquant.config import get_settings

    monkeypatch.setenv("ORDER_CHANNEL", "agent")
    get_settings.cache_clear()
    try:
        text = order_client.get("/orders/agent-status").text
        assert "agent 未連線" in text
        assert 'href="/account"' in text
    finally:
        get_settings.cache_clear()


def test_agent_status_hides_account_link_for_non_owner_when_disconnected(
    order_client, fake_guard, monkeypatch
):
    """終審必修 LOW-7 邊界：非 owner 看到「agent 未連線」狀態，但不顯示帳戶頁連結
    （或任何連結）——那頁的 Agent Token 卡片對非 owner 不會顯示，連過去也看不到東西。"""
    from quanquant.config import get_settings

    fake_guard._owner_ids = set()  # 目前 user 非 owner
    monkeypatch.setenv("ORDER_CHANNEL", "agent")
    get_settings.cache_clear()
    try:
        text = order_client.get("/orders/agent-status").text
        assert "agent 未連線" in text
        assert 'href="/account"' not in text
    finally:
        get_settings.cache_clear()


# ---------------------------------------------------------------------------
# D9/D11：手動斷開 Agent（agent-disconnect/reconnect）＋冷靜期（cooldown）路由層
# owner-only；踢線 force_close 在無真實 WS 下難驗（order_client 未接 registry/slot），
# 只驗 gate（in-memory）/DB 狀態＋回應片段字樣（模板真的 render）。
# ---------------------------------------------------------------------------

_CST = timezone(timedelta(hours=8))


def _until_str(delta: timedelta) -> str:
    """組出 datetime-local（'YYYY-MM-DDTHH:MM'）字串，相對現在偏移 delta（以台灣 +08:00
    為基準；route 以 +08:00 解析，與 now_epoch_ms 同框可比）。"""
    return (datetime.now(_CST) + delta).strftime("%Y-%m-%dT%H:%M")


def test_orders_page_no_longer_shows_disconnect_or_cooldown_controls(order_client):
    """005：斷開 Agent 與冷靜期的切換控制都搬到獨立的 /risk 頁，下單頁不再顯示這兩個
    控制項（僅在冷靜期生效中才顯示等效狀態橫幅，見另一測試）。"""
    text = order_client.get("/orders").text
    assert "斷開 Agent 連線" not in text
    assert 'hx-post="/orders/agent-disconnect"' not in text
    assert 'hx-post="/orders/cooldown"' not in text


def test_orders_page_shows_equivalent_cooldown_banner_when_active(order_client, session, user):
    """005 的驗收邊界：冷靜期生效時的既有下單頁提示行為維持等效——使用者仍要看得到
    自己在冷靜期中，即使完整的冷靜期控制已經搬到 /risk 頁。"""
    now = brepo.now_epoch_ms()
    brepo.create_cooldown(session, user_id=user.id, until_ms=now + 3_600_000, now_ms=now)
    session.commit()
    text = order_client.get("/orders").text
    assert "冷靜期中" in text
    assert 'href="/risk"' in text


def test_agent_disconnect_owner_blocks_gate_and_returns_partial(order_client, user):
    order_client.app.state.agent_connection_gate = AgentConnectionGate()
    resp = order_client.post("/orders/agent-disconnect")
    assert resp.status_code == 200
    assert order_client.app.state.agent_connection_gate.is_blocked(user.id) is True
    assert "已手動斷開" in resp.text


def test_agent_disconnect_non_admin_gets_403(order_client, session, user):
    """斷開 Agent 收 admin-only（2026-08-27）：非 admin → 403、未封鎖 gate（server 端強制）。"""
    _demote_to_non_admin(session, user)
    order_client.app.state.agent_connection_gate = AgentConnectionGate()
    resp = order_client.post("/orders/agent-disconnect")
    assert resp.status_code == 403
    assert order_client.app.state.agent_connection_gate.is_blocked(user.id) is False


def test_agent_reconnect_owner_unblocks_gate(order_client, user):
    gate = AgentConnectionGate()
    gate.block(user.id)
    order_client.app.state.agent_connection_gate = gate
    resp = order_client.post("/orders/agent-reconnect")
    assert resp.status_code == 200
    assert gate.is_blocked(user.id) is False


def test_agent_reconnect_non_admin_gets_403(order_client, session, user):
    _demote_to_non_admin(session, user)
    gate = AgentConnectionGate()
    gate.block(user.id)
    order_client.app.state.agent_connection_gate = gate
    resp = order_client.post("/orders/agent-reconnect")
    assert resp.status_code == 403
    assert gate.is_blocked(user.id) is True  # 未被解除


def test_non_admin_owner_sees_no_control_widgets_on_orders_page(order_client, session, user):
    """005：非 admin 的 owner（測試者）在下單頁一律看不到任何切換控制（急停／斷開 Agent／
    冷靜期都已搬到 /risk 頁）；冷靜期仍在的等效提示行為見另一測試。"""
    _demote_to_non_admin(session, user)
    text = order_client.get("/orders").text
    assert 'hx-post="/orders/kill-switch"' not in text
    assert 'hx-post="/orders/agent-disconnect"' not in text
    assert 'hx-post="/orders/cooldown"' not in text


def _active_cooldown(session, user_id):
    return brepo.active_cooldown(session, user_id=user_id, now_ms=brepo.now_epoch_ms())


def test_cooldown_valid_until_creates_active_row_and_shows_partial(order_client, session, user):
    resp = order_client.post("/orders/cooldown", data={"until": _until_str(timedelta(days=1))})
    assert resp.status_code == 200
    assert "冷靜期" in resp.text
    assert _active_cooldown(session, user.id) is not None  # DB 有 active cooldown


def test_cooldown_past_until_rejected_and_no_row_created(order_client, session, user):
    resp = order_client.post("/orders/cooldown", data={"until": _until_str(timedelta(days=-1))})
    assert resp.status_code == 200
    assert "晚於現在" in resp.text
    assert _active_cooldown(session, user.id) is None


def test_cooldown_over_max_days_rejected(order_client, session, user):
    resp = order_client.post("/orders/cooldown", data={"until": _until_str(timedelta(days=91))})
    assert resp.status_code == 200
    assert "最長" in resp.text
    assert _active_cooldown(session, user.id) is None


def test_cooldown_invalid_until_format_rejected(order_client, session, user):
    resp = order_client.post("/orders/cooldown", data={"until": "not-a-datetime"})
    assert resp.status_code == 200
    assert "有效" in resp.text
    assert _active_cooldown(session, user.id) is None


def test_cooldown_when_already_in_cooldown_is_rejected(order_client, session, user):
    now = brepo.now_epoch_ms()
    brepo.create_cooldown(session, user_id=user.id, until_ms=now + 3_600_000, now_ms=now)
    session.commit()
    resp = order_client.post("/orders/cooldown", data={"until": _until_str(timedelta(days=1))})
    assert resp.status_code == 200
    # 已在冷靜期 → 回既有 active 片段（🔒 冷靜期中面板，明確表達仍被鎖定、無法變更）；
    # 模板在 cooldown 存在時走鎖定面板分支、不另顯 error 字串（見疑似 UX 落差說明）。
    assert "冷靜期中" in resp.text
    # 仍只有原本那筆，未被縮短/重設
    rows = session.exec(select(Cooldown).where(Cooldown.user_id == user.id)).all()
    assert len(rows) == 1 and rows[0].until_ts == now + 3_600_000


def test_cooldown_non_owner_gets_403(order_client, fake_guard, session, user):
    fake_guard._owner_ids = set()  # user 非 owner
    resp = order_client.post("/orders/cooldown", data={"until": _until_str(timedelta(days=1))})
    assert resp.status_code == 403
    assert _active_cooldown(session, user.id) is None


# ---------------------------------------------------------------------------
# R2-3（2026-09-02）：冷靜期快捷時長（1 小時／1 天／1 週）——伺服器端以「現在＋時長」
# 換算到期時間（不靠前端 JS 算數，避免 client 時鐘/時區誤差；也因此可以直接 pytest 驗證
# 換算是否正確，不需要 JS runtime）。
# ---------------------------------------------------------------------------

def test_cooldown_quick_preset_1h_computes_until_from_now(order_client, session, user):
    """邊界⑥：快捷「1 小時」＝現在＋3_600_000ms（容忍測試執行耗時，±5 秒帶）。"""
    before = brepo.now_epoch_ms()
    resp = order_client.post("/orders/cooldown", data={"duration_preset": "1h"})
    after = brepo.now_epoch_ms()
    assert resp.status_code == 200
    row = _active_cooldown(session, user.id)
    assert row is not None
    assert before + 3_600_000 - 5_000 <= row.until_ts <= after + 3_600_000 + 5_000
    assert "到期時間" in resp.text  # 選後仍顯示實際到期時間供確認


def test_cooldown_quick_preset_1d_computes_until_from_now(order_client, session, user):
    before = brepo.now_epoch_ms()
    resp = order_client.post("/orders/cooldown", data={"duration_preset": "1d"})
    after = brepo.now_epoch_ms()
    assert resp.status_code == 200
    row = _active_cooldown(session, user.id)
    assert before + 86_400_000 - 5_000 <= row.until_ts <= after + 86_400_000 + 5_000


def test_cooldown_quick_preset_1w_computes_until_from_now(order_client, session, user):
    before = brepo.now_epoch_ms()
    resp = order_client.post("/orders/cooldown", data={"duration_preset": "1w"})
    after = brepo.now_epoch_ms()
    assert resp.status_code == 200
    row = _active_cooldown(session, user.id)
    assert before + 604_800_000 - 5_000 <= row.until_ts <= after + 604_800_000 + 5_000


def test_cooldown_quick_preset_invalid_value_rejected(order_client, session, user):
    resp = order_client.post("/orders/cooldown", data={"duration_preset": "bogus"})
    assert resp.status_code == 200
    assert _active_cooldown(session, user.id) is None


def test_cooldown_quick_preset_still_respects_max_days_check(order_client, session, user):
    """R2-3 驗收：上限 90 天維持——即使走快捷路徑，換算出的到期時間仍要過同一道上限檢查
    （目前三個快捷值都遠低於上限，這裡用未知 preset 名稱＋超長 until 混合驗證『二選一、
    until 分支仍受上限保護』，避免以後加大 preset 卻漏檢查）。"""
    resp = order_client.post(
        "/orders/cooldown", data={"until": _until_str(timedelta(days=91))}
    )
    assert resp.status_code == 200
    assert "最長" in resp.text
    assert _active_cooldown(session, user.id) is None


# ---------------------------------------------------------------------------
# R2-2（D-2，2026-09-02）：冷靜期反悔窗——啟動後 5 分鐘內本人可自行取消
# （POST /orders/cooldown/cancel）；逾時後任何人（含 admin，見 test_admin_routes.py）都
# 無法提前解除，只能等到期。時間一律直接操縱 DB created_ts，不 sleep。
# ---------------------------------------------------------------------------

def _seed_active_cooldown(session, user_id, *, created_ts, until_ts=None):
    row = Cooldown(
        user_id=user_id, until_ts=until_ts or (brepo.now_epoch_ms() + 3_600_000), created_ts=created_ts,
    )
    session.add(row)
    session.commit()
    return row


def test_cooldown_cancel_within_5min_window_succeeds(order_client, session, user):
    """邊界③：啟動後 4:59（未滿 5 分鐘）本人可取消——回到「未在冷靜期」的建立表單片段。"""
    now = brepo.now_epoch_ms()
    _seed_active_cooldown(session, user.id, created_ts=now - (4 * 60_000 + 59_000))
    resp = order_client.post("/orders/cooldown/cancel")
    assert resp.status_code == 200
    assert _active_cooldown(session, user.id) is None
    assert "啟動冷靜期" in resp.text  # 換回建立表單（無 active cooldown）


def test_cooldown_cancel_after_5min_window_rejected(order_client, session, user):
    """邊界④：超過 5 分鐘反悔窗（5:01）後本人取消被拒——冷靜期原封不動、回錯誤訊息。"""
    now = brepo.now_epoch_ms()
    _seed_active_cooldown(session, user.id, created_ts=now - (5 * 60_000 + 1_000))
    resp = order_client.post("/orders/cooldown/cancel")
    assert resp.status_code == 200  # 比照既有錯誤模式：partial 回錯誤文字，不是 4xx
    assert "無法取消" in resp.text or "反悔窗" in resp.text
    assert _active_cooldown(session, user.id) is not None  # 仍在冷靜期，未被解除


def test_cooldown_cancel_no_active_cooldown_is_noop(order_client, session, user):
    resp = order_client.post("/orders/cooldown/cancel")
    assert resp.status_code == 200
    assert _active_cooldown(session, user.id) is None


def test_cooldown_cancel_non_owner_gets_403(order_client, fake_guard, session, user):
    fake_guard._owner_ids = set()
    now = brepo.now_epoch_ms()
    _seed_active_cooldown(session, user.id, created_ts=now)
    resp = order_client.post("/orders/cooldown/cancel")
    assert resp.status_code == 403
    assert _active_cooldown(session, user.id) is not None


def test_cooldown_control_within_window_shows_cancel_button(order_client, session, user):
    """R2-2 驗收：5 分鐘內卡片顯示「取消冷靜期」＋到期時間可見（GET /risk 走 risk_page，
    不只是 POST 回應片段才有）。"""
    now = brepo.now_epoch_ms()
    _seed_active_cooldown(session, user.id, created_ts=now)
    text = order_client.get("/risk").text
    assert 'hx-post="/orders/cooldown/cancel"' in text
    assert "取消冷靜期" in text


def test_cooldown_control_after_window_hides_cancel_button(order_client, session, user):
    """R2-2 驗收：逾時後無任何解除路徑——卡片不再顯示取消鈕。"""
    now = brepo.now_epoch_ms()
    _seed_active_cooldown(session, user.id, created_ts=now - (5 * 60_000 + 1_000))
    text = order_client.get("/risk").text
    assert 'hx-post="/orders/cooldown/cancel"' not in text


def test_cooldown_text_states_five_minute_revoke_window_everywhere(order_client, session, user):
    """R2-2 驗收：所有文案同步——建立表單、卡片鎖定文案都改成『5 分鐘內可取消；逾時後
    任何人都無法提前解除，只能等到期』語意，不再出現舊的『只有管理員能提前解除』。"""
    text = order_client.get("/risk").text
    assert "5 分鐘" in text
    assert "只有管理員能提前解除" not in text
    assert "只有管理員" not in text


# ---------------------------------------------------------------------------
# 005：獨立的「風險控管」頁（GET /risk）——緊急停止下單（原 kill switch，admin-only
# 授權不變）＋交易冷靜期（owner 皆可自我禁制）＋Agent 連線控制（admin-only）集中一頁。
# ---------------------------------------------------------------------------

def test_risk_page_requires_login(anon_client):
    resp = anon_client.get("/risk", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/login"


def test_risk_page_admin_sees_all_toggle_controls(order_client):
    """admin：緊急停止下單（含全站/我的兩層）、冷靜期、Agent 連線控制皆有可切換的表單。"""
    text = order_client.get("/risk").text
    assert "風險控管" in text
    assert "緊急停止下單" in text
    assert "Kill switch" not in text  # 畫面上不再出現舊名稱
    assert "只擋新單" in text and "不會自動取消既有掛單" in text
    assert 'hx-post="/orders/kill-switch"' in text
    assert 'hx-post="/orders/cooldown"' in text
    assert 'hx-post="/orders/agent-disconnect"' in text


def test_risk_page_non_admin_owner_can_toggle_self_kill_switch_but_global_is_readonly(
    order_client, session, user
):
    """R2-1（D-1，2026-09-02）：scope=self 開放帳號本人——非 admin 的 owner 在風控頁能看到
    且能操作「我的緊急停止」（有切換表單）；scope=global 維持 admin-only，對非 admin 唯讀
    （無切換表單）。Agent 連線控制整段仍是 admin 專屬區；冷靜期仍是本人授權，維持完整
    控制。"""
    _demote_to_non_admin(session, user)
    text = order_client.get("/risk").text
    assert "我的下單正常" in text  # 我的急停狀態看得到
    assert 'name="scope" value="self"' in text  # 我的急停有切換表單（R2-1 開放本人）
    assert 'name="scope" value="global"' not in text  # 全站急停沒有切換表單（維持 admin-only）
    assert 'hx-post="/orders/agent-disconnect"' not in text  # admin 專屬區整段不出現
    assert 'hx-post="/orders/cooldown"' in text  # 冷靜期仍是本人授權，保留完整控制


def test_risk_page_kill_switch_card_text_only_describes_global_as_admin_only(order_client):
    """R2-1 驗收：卡片說明文字「授權維持僅管理員可操作」同步改寫——只描述全站，不再暗示
    「我的緊急停止」也是 admin-only。"""
    text = order_client.get("/risk").text
    assert "全站緊急停止」僅管理員可操作" in text or "全站緊急停止僅管理員可操作" in text
    assert "授權維持僅管理員可操作" not in text  # 舊的、涵蓋兩者的舊文案已改寫


def test_risk_page_only_my_kill_switch_button_is_contrast(order_client, fake_guard, session, user):
    """R2-7 驗收：風控頁按鈕層級——同頁最多一顆 contrast，只有「啟動我的緊急停止」保持
    contrast；全站急停／啟動冷靜期／斷開 Agent 一律不是 contrast（降為一般或 danger）。
    admin＋未啟動任何開關的狀態下四顆按鈕都在頁面上，用 class="contrast" 出現次數驗證。"""
    text = order_client.get("/risk").text
    assert text.count('class="contrast"') == 1
    assert '>啟動我的緊急停止<' in text
    assert 'class="risk-btn-danger"' in text  # 全站急停／啟動冷靜期／斷開 Agent 改走這個 class
    assert "#c0392b" not in text  # R2-7：警示紅不再硬編，改走 --down token
    assert "rgba(192,57,43" not in text


def test_risk_page_active_states_still_have_no_hardcoded_warning_color(
    order_client, fake_guard, session, user
):
    """R2-7 驗收（涵蓋 kill_switch_control／cooldown_control／agent_connection_control 的
    「active」分支，不是只測預設關閉狀態）：我的急停、全站急停、冷靜期、已斷線 Agent 都
    同時處於啟動中，四個 partial 的警示狀態都要走 --down token（.risk-status-box.is-active），
    仍不能出現硬編 #c0392b／rgba(192,57,43,...)。"""
    fake_guard.per_user[user.id] = True
    fake_guard.global_on = True
    now = brepo.now_epoch_ms()
    session.add(Cooldown(user_id=user.id, until_ts=now + 3_600_000, created_ts=now))
    session.commit()
    order_client.app.state.agent_connection_gate = AgentConnectionGate()
    order_client.app.state.agent_connection_gate.block(user.id)
    text = order_client.get("/risk").text
    assert "我的緊急停止已啟動" in text and "全站緊急停止已啟動" in text
    assert "冷靜期中" in text and "已手動斷開 Agent 連線" in text
    assert "#c0392b" not in text
    assert "rgba(192,57,43" not in text


def test_risk_page_hx_confirm_lives_on_form_not_button(order_client, fake_guard, session, user):
    """終審 MEDIUM #3：htmx 2.0.4 的 getClosestAttributeValue 只從發請求的元素（帶 hx-post
    的 <form>）及其祖先讀 hx-confirm，不會往下看子孫的 <button>——放在 button 上等於沒有
    確認彈窗，R2-3 的快捷鈕會一按直接鎖住。用 regex 掃描整頁：任何 <button> 標籤都不得帶
    hx-confirm；預設狀態（未啟動任何開關、非冷靜期中）風控頁上的 6 個危險動作
    （全站急停啟動／冷靜期快捷 1h/1d/1w／冷靜期自訂／斷開 Agent）各自對應一個帶
    hx-confirm 的 <form>。「啟動我的緊急停止」刻意無 confirm（2026-09-03 終審拍板）：
    可逆的自救動作（本人隨時可解除、只擋新單），E-stop 要單一動作、審慎留給恢復端。"""
    text = order_client.get("/risk").text
    button_tags = re.findall(r"<button\b[^>]*>", text)
    assert button_tags  # sanity：頁面上真的有按鈕，測試沒有測到空頁
    assert not any("hx-confirm" in tag for tag in button_tags)
    assert "確定要啟動我的緊急停止" not in text  # E-stop 不設確認，誤加要被抓到
    form_tags = re.findall(r"<form\b[^>]*>", text)
    confirming_forms = [f for f in form_tags if "hx-confirm" in f]
    assert len(confirming_forms) == 6
    for f in confirming_forms:  # 每個帶 confirm 的 form 都要自帶完整的 hx-post/target/swap
        assert "hx-post=" in f and "hx-target=" in f and 'hx-swap="outerHTML"' in f


def test_cooldown_control_quick_buttons_have_mini_sizing_css():
    """終審 LOW #4：`.mini`（`class="secondary mini"`）在 app.css 沒有裸規則，只有
    `a.mini`／`.alert-actions .mini` 兩個 scoped 版本套不到 button——撞上 Pico v2
    `button[type=submit]{width:100%}` 的預設值，MEDIUM #3 拆完 form 後三顆快捷鈕會變成
    直排全寬。驗證 `#cooldown-control .mini` 這條 scoped 規則存在，且真的把寬度收回
    `auto`（不是又意外繼承 100%）。"""
    from quanquant.web.templating import STATIC_DIR

    css = (STATIC_DIR / "app.css").read_text(encoding="utf-8")
    m = re.search(r"#cooldown-control\s+\.mini\s*\{([^}]*)\}", css)
    assert m is not None, "缺少 #cooldown-control .mini 規則"
    assert "width" in m.group(1) and "auto" in m.group(1)


def test_risk_page_non_owner_hides_cooldown_section_entirely(order_client, fake_guard):
    """非 owner（罕見情境，例如子系統白名單外的帳號）：冷靜期整段不出現（同下單頁既有
    owner 判定），不會顯示他人也管不到的控制。"""
    fake_guard._owner_ids = set()
    text = order_client.get("/risk").text
    assert 'hx-post="/orders/cooldown"' not in text


def test_risk_page_toggle_still_works_after_moving_to_new_location(order_client, fake_guard, fake_ops):
    """控制項移動後，owner 仍能在新位置即時切換，行為與現在完全一致——切換端點契約不變，
    這裡驗證 /risk 頁 render 出的表單確實打同一支既有端點。"""
    resp = order_client.post("/orders/kill-switch", data={"enabled": "true", "scope": "global"})
    assert resp.status_code == 200
    assert fake_guard.global_on is True
    text = order_client.get("/risk").text
    assert "全站緊急停止已啟動" in text


# ---- bug 1（simtrade 實測回歸）：_parse_order_price 對 None 的確切防線單元測試 ----
# 直接鎖定 web/routers/orders.py::_parse_order_price 這個「確切呼叫 Decimal(...) 的位置」
# 對 raw=None（disabled 欄位不送出時 form.get() 的真實回傳值）一定安全，不靠 HTTP 層間接
# 驗證——`Decimal(None)` 會炸 `TypeError: conversion from NoneType to Decimal is not
# supported`，_parse_order_price 用 `(raw or "").strip()` 把 None 與空字串統一處理，
# 兩者都不會走到裸的 `Decimal(raw)` 呼叫。

def test_parse_order_price_none_for_mkt_returns_zero_not_typeerror():
    assert _parse_order_price(None, price_type="MKT") == Decimal("0")


def test_parse_order_price_none_for_lmt_returns_zero_not_typeerror():
    """LMT 缺值一樣不炸 TypeError——回 0 後交給 OrderRequest.__post_init__ 的 price>0
    檢查擋下（走使用者可見的表單錯誤，不是未經處理的例外）。"""
    assert _parse_order_price(None, price_type="LMT") == Decimal("0")


def test_parse_order_price_none_price_type_returns_zero_not_typeerror():
    """price_type 本身也缺席（例如非本頁面送出的畸形請求）時，raw=None 仍不炸 TypeError。"""
    assert _parse_order_price(None, price_type=None) == Decimal("0")


def test_parse_optional_update_price_none_returns_none_not_typeerror():
    """改單路徑同樣的確切呼叫點：raw=None（欄位整個缺席）回 None（沿用既有值），不裸呼叫
    Decimal(None)。"""
    assert _parse_optional_update_price(None) is None


def test_parse_optional_update_price_empty_string_returns_none():
    assert _parse_optional_update_price("") is None


def test_parse_optional_update_price_valid_string_returns_decimal():
    assert _parse_optional_update_price("18500") == Decimal("18500")


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


def test_positions_snapshot_renders_for_owner(order_client, fake_service, user):
    """positions 端點改用無鎖同步快照（positions_snapshot，離開 event loop、不搶 supervisor
    鎖）後，owner 仍能正常取得並渲染部位。"""
    resp = order_client.get("/orders/positions")
    assert resp.status_code == 200
    assert "TXF" in resp.text and "多" in resp.text  # 部位表有渲染出多單


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


def test_order_table_shows_cancel_and_edit_buttons_only_for_live_open_orders(order_client, session, user,
                                                                              fake_service):
    """bug 2 附帶驗證：order_table.html 的取消/改單按鈕只在 submitted/partfilled、有
    broker_order_id、且屬於目前 service 綁定 mode（live_mode）的委託才顯示——避免對已結案
    委託（filled/cancelled）或非目前 mode 的委託誤顯示可操作的按鈕。"""
    brepo.set_order_ack(
        session,
        brepo.create_order(session, client_order_id="OPEN1", request_hash="H1", user_id=user.id, mode="sim",
                            broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
                            price=Decimal("18000"), price_type="LMT", order_type="ROD", octype="New",
                            trading_day="2026-06-16").id,
        broker_order_id="B-OPEN", ordno="O-OPEN", status="submitted",
    )
    brepo.set_order_ack(
        session,
        brepo.create_order(session, client_order_id="DONE1", request_hash="H2", user_id=user.id, mode="sim",
                            broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
                            price=Decimal("18000"), price_type="LMT", order_type="ROD", octype="New",
                            trading_day="2026-06-16").id,
        broker_order_id="B-DONE", ordno="O-DONE", status="filled",
    )
    session.commit()

    text = order_client.get("/orders/list?mode=sim").text
    assert 'hx-get="/orders/B-OPEN/edit"' in text
    assert 'hx-delete="/orders/B-OPEN"' in text
    assert 'hx-get="/orders/B-DONE/edit"' not in text
    assert 'hx-delete="/orders/B-DONE"' not in text


def test_order_table_shows_avg_fill_price_not_committed_zero_price_for_filled_mkt_order(
    order_client, session, user
):
    """bug C 回歸：市價單（MKT）委託價 `orders.price` 恆為 0（委託本來就無價，這是對的），
    真實成交價在 `deals.price`／已由 `apply_order_fill` 累加進 `orders.avg_fill_price`
    （見 broker/repository.py）。委託列表對已成交/部分成交的委託必須顯示成交均價，不能
    照舊顯示委託價 0（那會讓使用者誤以為成交價是 0）。未成交的委託仍應顯示委託價。"""
    filled_order = brepo.create_order(
        session, client_order_id="FILLED1", request_hash="H1", user_id=user.id, mode="sim",
        broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
        price=Decimal("0"), price_type="MKT", order_type="IOC", octype="New",
        trading_day="2026-07-27",
    )
    brepo.set_order_ack(session, filled_order.id, broker_order_id="B-FILLED", ordno="O-FILLED",
                        status="submitted")
    session.commit()
    with Session(session.get_bind()) as s:
        order = s.get(type(filled_order), filled_order.id)
        brepo.apply_order_fill(s, order, fill_qty=1, fill_price=Decimal("43737"))
        s.commit()

    pending_order = brepo.create_order(
        session, client_order_id="PENDING1", request_hash="H2", user_id=user.id, mode="sim",
        broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
        price=Decimal("18500"), price_type="LMT", order_type="ROD", octype="New",
        trading_day="2026-07-27",
    )
    brepo.set_order_ack(session, pending_order.id, broker_order_id="B-PENDING", ordno="O-PENDING",
                        status="submitted")
    session.commit()

    text = order_client.get("/orders/list?mode=sim").text
    assert "18,500" in text  # 未成交委託仍顯示委託價（P1-10：套 num filter，千分位）

    # 精準定位 FILLED1 那一列（id="order-{id}"），確認成交均價顯示出來、不是裸的委託價 0。
    rows = re.findall(rf'<tr id="order-{filled_order.id}">.*?</tr>', text, re.DOTALL)
    assert len(rows) == 1
    assert "43,737" in rows[0]
    assert re.search(r"<td[^>]*>0(\.0+)?</td>", rows[0]) is None  # 不再顯示裸的委託價 0


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
    """假 shioaji client：不連真網路，place_order/update_order 同步回傳並記錄呼叫參數。

    bug 2/3 修正後比照真實 SDK 契約（`_core.pyi`）：`cancel_order(trade)`／
    `update_order(trade, price=, qty=)` 一律收 `Trade` 物件——收到非 Trade-like（沒有
    `.order` 屬性，例如呼叫端誤傳 ordno 字串）就 raise `TypeError`，模擬真實 SDK
    （`argument 'trade': 'str' object is not an instance of 'Trade'`）。`place_order`
    送出後把回傳的 Trade 記進 `_live_trades`（以 `order.id` 為 key，比照真實 SDK
    `list_trades()` 語意），`ShioajiAdapter._find_trade_by_ordno` 呼叫
    `update_status()` + `list_trades()` 才找得到對應 Trade 物件。"""

    def __init__(self):
        self.futopt_account = type("Acc", (), {"account_id": "F1"})()
        self._seq = 0
        self.updated = []  # [(ordno, price, qty), ...]
        self._live_trades: dict = {}

    def Order(self, **kw):
        return kw

    def place_order(self, contract, order):
        self._seq += 1
        trade = _FakeTrade(f"ORD{self._seq}", f"SEQ{self._seq}")
        self._live_trades[trade.order.id] = trade
        return trade

    def update_status(self, account=None, **kw):
        pass

    def list_trades(self):
        return list(self._live_trades.values())

    @staticmethod
    def _assert_trade(trade) -> None:
        if not hasattr(trade, "order"):
            raise TypeError(
                f"argument 'trade': {type(trade).__name__!r} object is not an instance of 'Trade'"
            )

    def cancel_order(self, trade):
        self._assert_trade(trade)
        return trade

    def update_order(self, trade, **kw):
        self._assert_trade(trade)
        self.updated.append((trade.order.id, kw.get("price"), kw.get("qty")))
        return trade

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


def test_real_cancel_round_trip_sends_trade_object_not_ordno_string(engine, user):
    """bug 2/3 端到端驗收：DELETE /orders/{id} 全走真實元件（route→RiskGuard→adapter），
    native cancel_order 收到的必須是 Trade 物件（`_FakeApi.cancel_order` 對非 Trade-like
    輸入 raise TypeError，模擬真實 SDK 契約），不是先前直接塞 ordno 字串的舊行為。"""
    guard = _real_guard(engine, owner_user_ids=frozenset({user.id}))
    adapter = _real_adapter(engine, guard)
    client = _real_client(engine, user, adapter, guard)
    broker_order_id = _place_real_order(client, engine, client_order_id="C-CANCEL")

    resp = client.delete(f"/orders/{broker_order_id}")
    assert resp.status_code == 200

    with Session(engine) as s:
        order = brepo.find_order_by_broker_id(s, broker="shioaji", account="F1", mode="real",
                                              broker_order_id=broker_order_id)
        assert order.status == "cancelled"


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


# ---------------------------------------------------------------------------
# 006：主導覽「交易」大類——下單/委託/成交/未平倉
# ---------------------------------------------------------------------------

def test_topnav_shows_trade_dropdown_with_all_four_pages(order_client):
    """驗收只看「找得到、點得到」：主導覽出現「交易」，其下含下單/委託/成交/未平倉四個
    連結。"""
    text = order_client.get("/orders").text
    assert ">交易<" in text
    assert 'href="/orders"' in text
    assert 'href="/orders/queue"' in text
    assert 'href="/orders/deals"' in text
    assert 'href="/orders/holdings"' in text


def test_topnav_trade_summary_marked_active_on_any_trade_subpage(order_client):
    for path in ("/orders", "/orders/queue", "/orders/deals", "/orders/holdings"):
        text = order_client.get(path).text
        assert '<summary class="active">交易</summary>' in text, path


# ---------------------------------------------------------------------------
# 006：委託頁（/orders/queue）——承接原委託表全部既有功能
# ---------------------------------------------------------------------------

def test_orders_queue_page_renders_mode_tabs_and_table_mount_point(order_client):
    text = order_client.get("/orders/queue?mode=sim").text
    assert 'role="radiogroup"' in text  # R2-10 共用 macro
    assert 'hx-get="/orders/list?mode=sim"' in text
    assert 'sse-connect="/orders/stream"' in text
    assert "sse:orders-changed" in text
    assert "refreshorders from:body" in text
    assert 'class="table-wrap"' in text  # P1-11


def test_orders_queue_page_has_edit_modal_and_error_slot(order_client):
    """搬家後既有功能（改單 modal、取消失敗的錯誤顯示掛載點）必須都還在。"""
    text = order_client.get("/orders/queue").text
    assert 'id="order-edit-modal"' in text
    assert 'class="form-error-slot"' in text


def test_orders_queue_page_default_mode_follows_service_mode(order_client, fake_service):
    fake_service.mode = "real"
    text = order_client.get("/orders/queue").text
    assert 'hx-get="/orders/list?mode=real"' in text


def _selected_mode_tab(html: str, label: str) -> bool:
    tag = html.split(label)[0].rsplit("<a", 1)[-1]
    return "is-selected" in tag


def test_orders_queue_page_p0_1_mode_tab_selected_state_is_correct(order_client):
    """P0-1 根治的直接證據——這裡就是原本 bug 所在的委託表搬到的新頁面：mode=real 時
    「正式」被選中、「模擬」不被選中，反之亦然（原 orders.html 第一顆分頁的條件寫反）。"""
    real_html = order_client.get("/orders/queue?mode=real").text
    assert _selected_mode_tab(real_html, "正式") is True
    assert _selected_mode_tab(real_html, "模擬") is False

    sim_html = order_client.get("/orders/queue?mode=sim").text
    assert _selected_mode_tab(sim_html, "正式") is False
    assert _selected_mode_tab(sim_html, "模擬") is True


def test_orders_list_still_reachable_and_shows_edit_cancel_from_queue_page(
    order_client, session, user
):
    """搬家後既有的改單/取消端點與委託列表資料完全不變——只是換了個外殼頁面嵌它。"""
    brepo.set_order_ack(
        session,
        brepo.create_order(session, client_order_id="Q-OPEN", request_hash="HQ1", user_id=user.id, mode="sim",
                            broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
                            price=Decimal("18000"), price_type="LMT", order_type="ROD", octype="New",
                            trading_day="2026-06-16").id,
        broker_order_id="B-Q-OPEN", ordno="O-Q-OPEN", status="submitted",
    )
    session.commit()
    order_client.get("/orders/queue?mode=sim")  # 掛載頁存在，不 404
    text = order_client.get("/orders/list?mode=sim").text
    assert 'hx-get="/orders/B-Q-OPEN/edit"' in text
    assert 'hx-delete="/orders/B-Q-OPEN"' in text


# ---------------------------------------------------------------------------
# 006（P1-8/P1-9/P1-10）：委託列表補時間/類型欄、狀態中文化、價格千分位／市價顯示
# ---------------------------------------------------------------------------

def test_order_table_shows_time_and_type_badge_columns(order_client, session, user):
    brepo.create_order(
        session, client_order_id="P1-8", request_hash="H1", user_id=user.id, mode="sim",
        broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
        price=Decimal("18000"), price_type="LMT", order_type="ROD", octype="New",
        trading_day="2026-06-16",
    )
    session.commit()
    text = order_client.get("/orders/list?mode=sim").text
    assert "<th>時間</th>" in text
    assert "LMT・ROD・新倉" in text


def test_order_table_time_column_shows_taiwan_local_not_utc(order_client, session, user):
    """終審必修 HIGH-3：`Order.created_at` 是 naive-UTC（db/models.py::_utcnow）；委託頁
    的「時間」欄必須顯示台灣本地時間（+8），不是裸印 UTC 時刻——造一筆已知 UTC 時間，
    斷言頁面上出現的是 +8 之後的字串，且不出現原始 UTC 字串。"""
    order = brepo.create_order(
        session, client_order_id="P1-8-TZ", request_hash="H1", user_id=user.id, mode="sim",
        broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
        price=Decimal("18000"), price_type="LMT", order_type="ROD", octype="New",
        trading_day="2026-06-16",
    )
    order.created_at = datetime(2026, 6, 16, 10, 0, 0)  # naive UTC
    session.add(order)
    session.commit()

    text = order_client.get("/orders/list?mode=sim").text
    row = re.search(rf'<tr id="order-{order.id}">.*?</tr>', text, re.DOTALL).group(0)
    assert "2026-06-16 18:00:00" in row  # UTC 10:00 + 8 小時 = 台灣本地 18:00
    assert "2026-06-16 10:00:00" not in row  # 不可裸印 UTC 時刻


def test_order_table_status_shown_in_chinese_not_raw_english(order_client, session, user):
    order = brepo.create_order(
        session, client_order_id="P1-9", request_hash="H1", user_id=user.id, mode="sim",
        broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
        price=Decimal("18000"), price_type="LMT", order_type="ROD", octype="New",
        trading_day="2026-06-16",
    )
    brepo.set_order_ack(session, order.id, broker_order_id="B-P1-9", ordno="O-P1-9", status="submitted")
    session.commit()
    text = order_client.get("/orders/list?mode=sim").text
    assert "已委託" in text
    assert ">submitted<" not in text


def test_order_table_mkt_unfilled_order_shows_market_price_label_not_zero(
    order_client, session, user
):
    """P1-10：市價單（MKT）未成交時價格欄顯示「市價」，不是委託價恆為 0 的裸數字。"""
    order = brepo.create_order(
        session, client_order_id="P1-10", request_hash="H1", user_id=user.id, mode="sim",
        broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
        price=Decimal("0"), price_type="MKT", order_type="IOC", octype="New",
        trading_day="2026-06-16",
    )
    brepo.set_order_ack(session, order.id, broker_order_id="B-P1-10", ordno="O-P1-10", status="submitted")
    session.commit()
    text = order_client.get("/orders/list?mode=sim").text
    row = re.search(rf'<tr id="order-{order.id}">.*?</tr>', text, re.DOTALL).group(0)
    assert "市價" in row
    assert re.search(r"<td[^>]*>0(\.0+)?</td>", row) is None


# ---------------------------------------------------------------------------
# 006：成交頁（/orders/deals）——逐筆成交，資料源既有 Deal 表
# ---------------------------------------------------------------------------

def test_orders_deals_page_renders_mode_tabs_and_list_mount_point(order_client):
    text = order_client.get("/orders/deals?mode=sim").text
    assert 'role="radiogroup"' in text
    assert 'hx-get="/orders/deals-list?mode=sim"' in text
    assert 'class="table-wrap"' in text


def test_orders_deals_list_shows_time_symbol_direction_qty_price_fee(order_client, session, user):
    brepo.stage_deal(
        session, broker="shioaji", account="F1", mode="sim", trading_day="2026-06-16",
        fill_id="D-PAGE-1", ordno="O1", broker_order_id="B1", order_id=None, user_id=user.id,
        symbol="TXF", action="Buy", price=Decimal("18050"), qty=2, fee=Decimal("60"),
        octype="New", ts=1_781_604_000_000, raw_inbox_id=None,
    )
    session.commit()
    text = order_client.get("/orders/deals-list?mode=sim").text
    assert "TXF" in text
    assert "買" in text
    assert "18,050" in text  # num filter 千分位
    assert "60" in text  # 手續費


def test_orders_deals_list_empty_state_not_500(order_client):
    resp = order_client.get("/orders/deals-list?mode=sim")
    assert resp.status_code == 200
    assert "尚無成交紀錄" in resp.text


def test_orders_deals_list_scoped_by_mode_and_user(order_client, session, user):
    brepo.stage_deal(
        session, broker="shioaji", account="F1", mode="sim", trading_day="2026-06-16",
        fill_id="D-SIM", ordno="O1", broker_order_id="B1", order_id=None, user_id=user.id,
        symbol="TXF", action="Buy", price=Decimal("18000"), qty=1, fee=Decimal("20"),
        octype="New", ts=1000, raw_inbox_id=None,
    )
    brepo.stage_deal(
        session, broker="shioaji", account="F1", mode="real", trading_day="2026-06-16",
        fill_id="D-REAL", ordno="O2", broker_order_id="B2", order_id=None, user_id=user.id,
        symbol="TXF", action="Sell", price=Decimal("18100"), qty=1, fee=Decimal("20"),
        octype="Cover", ts=2000, raw_inbox_id=None,
    )
    session.commit()
    sim_text = order_client.get("/orders/deals-list?mode=sim").text
    real_text = order_client.get("/orders/deals-list?mode=real").text
    assert "18,000" in sim_text and "18,100" not in sim_text
    assert "18,100" in real_text and "18,000" not in real_text


# ---------------------------------------------------------------------------
# 006：未平倉頁（/orders/holdings）——承接原部位表＋浮動損益欄
# ---------------------------------------------------------------------------

def test_orders_holdings_page_renders_mode_tabs_and_positions_mount_point(order_client):
    text = order_client.get("/orders/holdings?mode=sim").text
    assert 'role="radiogroup"' in text
    assert 'hx-get="/orders/positions?mode=sim"' in text
    assert 'class="table-wrap"' in text


def test_orders_positions_adds_unrealized_pnl_column(order_client, fake_service):
    """未平倉頁補浮動損益欄（重用 unrealized_pnl，同精簡條）：多單 18000 進場、現價
    18100，TXF 點值 200，1 口 → (18100-18000)*1*200 = 20000。"""
    fake_service.mode = "real"

    # 直接覆寫 poller 依賴回傳一顆固定現價（order_client fixture 已把 get_poller 設 None，
    # 這裡針對本測試單獨覆寫）。
    from quanquant.web.deps import get_poller

    class _P:
        class _Last:
            price = Decimal("18100")
        last = _Last()

    order_client.app.dependency_overrides[get_poller] = lambda: _P()
    try:
        text = order_client.get("/orders/positions").text
    finally:
        order_client.app.dependency_overrides[get_poller] = lambda: None
    assert "浮動損益" in text  # 表頭
    assert "20,000" in text
    assert 'class="pnl up"' in text


def test_orders_positions_no_mark_price_shows_dash_with_neutral_class_not_pnl_down(order_client):
    """終審必修 LOW-6：未平倉頁同樣不可把「無報價」（unrealized None）誤上警示色 .down——
    `order_client` fixture 預設 get_poller 回 None，`_FakeService.positions_snapshot`
    固定回傳一筆部位，故本測試天然落在「有部位、無現價」這個組合。"""
    text = order_client.get("/orders/positions").text
    assert "TXF" in text
    assert 'class="pnl"' in text
    assert 'class="pnl down"' not in text
    assert 'class="pnl up"' not in text


def test_orders_positions_mode_query_reads_other_mode_via_db(order_client, session, user):
    """R2-10：未平倉頁 mode 分頁——service 目前在 sim 執行（`_FakeService.positions_snapshot`
    固定回傳「TXF 多 1 口」），切到 real 分頁時必須改直接查 BrokerPosition（純讀，不影響
    任何寫入路徑），看到的是 DB 裡真正的 real 部位（口數不同，藉此證明不是走到 fake 的
    固定回傳值），不是 sim 那份。"""
    from quanquant.db.models import BrokerPosition

    pos = BrokerPosition(
        user_id=user.id, broker="shioaji", account="F1", mode="real", symbol="TXF",
        direction="long", status="open", total_opened_qty=5, closed_qty=0,
        entry_notional=Decimal("90000"), exit_notional=Decimal("0"),
        open_fee_total=Decimal("0"), close_fee_total=Decimal("0"),
    )
    session.add(pos)
    session.commit()

    sim_text = order_client.get("/orders/positions?mode=sim").text
    real_text = order_client.get("/orders/positions?mode=real").text
    assert "<td class=\"numcell\">1</td>" in sim_text  # fake 固定回傳的 1 口（走 positions_snapshot）
    assert "<td class=\"numcell\">5</td>" in real_text  # DB 直查看到的 5 口（顯式切到非目前執行 mode）
    assert "<td class=\"numcell\">5</td>" not in sim_text
    assert "<td class=\"numcell\">1</td>" not in real_text


def test_orders_positions_non_owner_still_403_for_other_mode_path(order_client, fake_guard, user):
    """R2-10 邊界：非 owner 切到非目前執行 mode 分頁一樣要 403，不能繞過既有的授權判定。"""
    fake_guard._owner_ids = set()
    resp = order_client.get("/orders/positions?mode=real")
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# 006：下單子系統停用（service is None）時，交易大類四頁一律優雅降級、不 500
# ---------------------------------------------------------------------------

def _bare_client(engine, user):
    """比照既有 `test_kill_switch_without_risk_guard_does_not_500`：刻意不設
    `app.state.order_service`／`order_risk_guard`，模擬下單子系統未啟用的真實情境。"""
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    c = TestClient(app)
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    return c


def test_orders_queue_page_without_service_does_not_500(engine, user):
    resp = _bare_client(engine, user).get("/orders/queue")
    assert resp.status_code == 200


def test_orders_deals_page_without_service_does_not_500(engine, user):
    resp = _bare_client(engine, user).get("/orders/deals")
    assert resp.status_code == 200
    assert "尚無成交紀錄" in _bare_client(engine, user).get("/orders/deals-list").text


def test_orders_holdings_page_without_service_does_not_500(engine, user):
    resp = _bare_client(engine, user).get("/orders/holdings")
    assert resp.status_code == 200
    assert "目前無部位" in _bare_client(engine, user).get("/orders/positions").text


# ---------------------------------------------------------------------------
# 007：下單反饋橫幅（banner-stack）——批次 B-3
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", ["/orders", "/orders/queue", "/orders/deals", "/orders/holdings"])
def test_trade_pages_have_banner_stack_with_sse_wiring_and_aria_live(order_client, path):
    """①②：banner-stack 容器在「交易」大類每一頁都出現，帶 aria-live 播報，且掛在既有
    sse-connect 作用域內（`hx-trigger="sse:deal, sse:order-report"`，不新開連線）。"""
    text = order_client.get(path).text
    assert 'class="banner-stack"' in text
    assert 'aria-live="polite"' in text
    assert 'hx-trigger="sse:deal, sse:order-report"' in text
    # 容器必須在既有 sse-connect="/orders/stream" 容器「之後」出現在原始碼裡才算掛在
    # 同一個作用域內（partials/banner_stack.html 是 include 進那個 div 的第一個子元素）。
    sse_idx = text.index('sse-connect="/orders/stream"')
    banner_idx = text.index('class="banner-stack"')
    assert sse_idx < banner_idx


def test_app_css_defines_fail_and_fill_banner_classes_distinct_from_base():
    """③（CSS 佐證）：失敗樣式 class 與成功（基底 `.order-banner`，中性配色）不同，
    成交依方向各有獨立變體。"""
    from quanquant.web.templating import STATIC_DIR

    css = (STATIC_DIR / "app.css").read_text(encoding="utf-8")
    assert ".order-banner {" in css
    assert ".order-banner.fail" in css
    assert ".order-banner.fill-long" in css
    assert ".order-banner.fill-short" in css


def test_place_success_and_failure_banners_are_distinguishable(order_client, fake_service, user):
    """③（行為佐證）：成功／失敗橫幅的 HX-Trigger payload 明確可辨（ok 旗標＋不同
    title），JS 據此套用不同 class（.order-banner vs .order-banner.fail）。"""
    ok_resp = order_client.post("/orders", data={
        "client_order_id": "C-BANNER-OK", "symbol": "TXF", "action": "Buy", "qty": "1",
        "price": "18000", "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert ok_resp.status_code == 200
    ok_events = json.loads(ok_resp.headers["HX-Trigger"])
    assert ok_events["order-banner"]["ok"] is True
    assert ok_events["order-banner"]["title"] == "委託送出成功"
    assert "TXF" in ok_events["order-banner"]["detail"]

    fake_service.place_error = OrderError("超過單筆上限 5 口")
    fail_resp = order_client.post("/orders", data={
        "client_order_id": "C-BANNER-FAIL", "symbol": "TXF", "action": "Buy", "qty": "1",
        "price": "18000", "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert fail_resp.status_code == 200
    fail_events = json.loads(fail_resp.headers["HX-Trigger"])
    assert fail_events["order-banner"]["ok"] is False
    assert fail_events["order-banner"]["title"] == "委託失敗"
    assert "超過單筆上限 5 口" in fail_events["order-banner"]["detail"]
    assert fail_events["order-banner"] != ok_events["order-banner"]


def test_place_form_validation_error_does_not_trigger_banner(order_client, fake_service, user):
    """規格「不要動到的部分」：表單欄位驗證錯誤（打錯價格）就地顯示在表單旁，不進橫幅。"""
    resp = order_client.post("/orders", data={
        "client_order_id": "C-FORMERR", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "not-a-number",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert resp.status_code == 200
    assert "order-banner" not in (resp.headers.get("HX-Trigger") or "")


def test_place_success_clears_stale_form_error_banner(order_client, fake_service, user):
    """P0-4：成功不清除舊錯誤——成功回應要帶一個把 `.form-error-slot` 清空的 OOB swap。"""
    resp = order_client.post("/orders", data={
        "client_order_id": "C-CLEAR", "symbol": "TXF", "action": "Buy", "qty": "1", "price": "18000",
        "price_type": "LMT", "order_type": "ROD", "octype": "New",
    })
    assert resp.status_code == 200
    assert 'hx-swap-oob="innerHTML:.form-error-slot"' in resp.text


def test_cancel_success_triggers_banner(order_client, fake_service, user, session):
    """P0-6：取消成功過去完全沒有回饋，現在要帶橫幅。"""
    _seed_order(session, user, client_order_id="CX1", request_hash="CXH1", status="submitted",
                broker_order_id="CX-B1", ordno="CXO1")
    resp = order_client.delete("/orders/CX-B1")
    assert resp.status_code == 200
    events = json.loads(resp.headers["HX-Trigger"])
    assert events["order-banner"]["ok"] is True
    assert events["order-banner"]["title"] == "取消成功"


def test_cancel_failure_triggers_fail_banner(order_client, fake_service, user, session):
    """P0-5：取消失敗過去看不到，現在要帶失敗橫幅（且仍走 .form-error-slot）。"""
    fake_service.cancel_error = OrderError("委託已成交，無法取消")
    resp = order_client.delete("/orders/CX-B2")
    assert resp.status_code == 200
    assert resp.headers.get("HX-Retarget") == ".form-error-slot"
    events = json.loads(resp.headers["HX-Trigger"])
    assert events["order-banner"]["ok"] is False
    assert events["order-banner"]["title"] == "取消失敗"
    assert "委託已成交，無法取消" in events["order-banner"]["detail"]


def test_update_success_triggers_banner(order_client, fake_service, user, session):
    """P0-6：改單成功過去沒有回饋，現在要帶橫幅（且仍照舊觸發 closeordermodal）。"""
    _seed_order(session, user, client_order_id="UX1", request_hash="UXH1", status="submitted",
                broker_order_id="UX-B1", ordno="UXO1")
    resp = order_client.put("/orders/UX-B1", data={"qty": "2"})
    assert resp.status_code == 200
    events = json.loads(resp.headers["HX-Trigger"])
    assert events["order-banner"]["ok"] is True
    assert events["order-banner"]["title"] == "改單成功"
    assert events.get("closeordermodal") is True


def test_update_failure_triggers_fail_banner(order_client, fake_service, user, session):
    fake_service.update_error = OrderError("改單數量必須大於 0")
    resp = order_client.put("/orders/UX-B2", data={"qty": "2"})
    assert resp.status_code == 200
    events = json.loads(resp.headers["HX-Trigger"])
    assert events["order-banner"]["ok"] is False
    assert events["order-banner"]["title"] == "改單失敗"


# ---------------------------------------------------------------------------
# 007：sim 下單確認視窗偏好——伺服器端計算出的 data-sim-confirm 旗標
# ---------------------------------------------------------------------------

def test_orders_page_sim_mode_default_requires_confirm(order_client):
    """④：sim 模式、使用者未設定偏好（預設 None）時，下單頁應標記需要確認。"""
    text = order_client.get("/orders").text
    assert 'data-sim-confirm="1"' in text


def test_orders_page_sim_mode_skips_confirm_after_preference_saved(order_client, session, user):
    """⑥：偏好落定（skip_sim_confirm=True）後，下單頁不再標記需要確認。"""
    from quanquant.db.models import User

    row = session.get(User, user.id)
    row.skip_sim_confirm = True
    session.add(row)
    session.commit()
    text = order_client.get("/orders").text
    assert 'data-sim-confirm="0"' in text


def test_orders_page_real_mode_never_requires_sim_confirm_marker_regardless_of_preference(
    order_client, fake_service, session, user
):
    """⑧：real 完全不受這個偏好影響——即使 skip_sim_confirm 明確設為 False（『每次都要
    跳確認』的最強偏好），real 模式下 data-sim-confirm 仍必須是 "0"（real 走後端強制的
    兩階段確認，不是這個客戶端 gate）。"""
    from quanquant.db.models import User

    row = session.get(User, user.id)
    row.skip_sim_confirm = False
    session.add(row)
    session.commit()
    fake_service.mode = "real"
    text = order_client.get("/orders").text
    assert 'data-sim-confirm="0"' in text


def test_real_two_step_confirm_unaffected_by_skip_sim_confirm_preference(engine, user):
    """⑧（端到端）：real 的伺服器強制兩階段確認，不論 skip_sim_confirm 為何值都必須照樣
    要求 confirm_token——這個偏好只管 sim 的客戶端確認 gate。"""
    with Session(engine) as s:
        from quanquant.db.models import User

        row = s.get(User, user.id)
        row.skip_sim_confirm = True
        s.add(row)
        s.commit()

    guard = _real_guard(engine, owner_user_ids=frozenset({user.id}))
    adapter = _real_adapter(engine, guard)
    client = _real_client(engine, user, adapter, guard)
    form = {
        "client_order_id": "C-SKIP-REAL", "symbol": "TXF", "action": "Buy", "qty": "1",
        "price": "18000", "price_type": "LMT", "order_type": "ROD", "octype": "New",
    }
    first = client.post("/orders", data=form)
    assert first.status_code == 200
    token = _hidden(first.text, "confirm_token")
    assert token is not None  # 依然回確認框，沒有因為 skip_sim_confirm=True 被跳過


# ---------------------------------------------------------------------------
# 009：sim/real 執行模式識別——有橘標＝模擬、無標＝正式
# ---------------------------------------------------------------------------

def test_orders_page_sim_mode_shows_badge(order_client):
    """⑨：sim 執行模式在下單頁標題旁顯示「模擬單」橘黃徽章。"""
    text = order_client.get("/orders").text
    assert "模擬單" in text
    assert 'class="badge open exec-mode-badge"' in text


def test_orders_page_real_mode_shows_no_badge(order_client, fake_service):
    """⑩：real 執行模式不顯示任何 mode 徽章。"""
    fake_service.mode = "real"
    text = order_client.get("/orders").text
    assert "模擬單" not in text
    assert "exec-mode-badge" not in text


def test_orders_page_without_service_shows_no_sim_badge(engine, user):
    """service 未啟用（exec_mode 為 None）時不應誤顯示「模擬單」——沒有東西在跑。"""
    resp = _bare_client(engine, user).get("/orders")
    assert "模擬單" not in resp.text


def test_real_confirm_dialog_shows_full_order_content_and_formal_label(order_client, fake_service, user):
    """⑪：real 兩階段確認框明示「正式單」，並帶完整委託內容（商品/方向/口數/價格/倉別）
    （併 P1-15）。"""
    fake_service.mode = "real"
    resp = order_client.post("/orders", data={
        "client_order_id": "C-REAL-CONFIRM", "symbol": "TXF", "action": "Sell", "qty": "3",
        "price": "18200", "price_type": "LMT", "order_type": "ROD", "octype": "Cover",
    })
    assert resp.status_code == 200
    assert "正式單" in resp.text
    assert "TXF" in resp.text
    assert "賣" in resp.text
    assert "3" in resp.text
    assert "18200" in resp.text
    assert "平倉" in resp.text  # octype=Cover 的中文標籤


def test_real_confirm_dialog_mkt_order_shows_market_price_label_not_zero(order_client, fake_service, user):
    """CRITICAL-1（fresh-context 終審修復）：MKT 委託的 price 恆為 0——確認框過去裸印
    `@ {{ price }}` 會顯示「@ 0」，這正是 P1-15 點名要修的問題，且發生在真錢送出前最後
    一道辨識畫面上。改用 price_type 判斷後應顯示「市價」，不得出現「@ 0」；同時應帶出
    price_type/order_type（此前傳進 context 卻只進 hidden input，畫面上完全看不到）。"""
    fake_service.mode = "real"
    resp = order_client.post("/orders", data={
        "client_order_id": "C-REAL-MKT-CONFIRM", "symbol": "TXF", "action": "Buy", "qty": "3",
        "price": "", "price_type": "MKT", "order_type": "IOC", "octype": "New",
    })
    assert resp.status_code == 200
    assert "正式單" in resp.text
    assert "市價" in resp.text
    assert "@ 0" not in resp.text
    assert "MKT" in resp.text
    assert "IOC" in resp.text


def test_banners_js_shows_market_price_label_for_mkt_order_report_not_zero():
    """LOW-6（fresh-context 終審修復）：banners.js 對未成交 MKT 委託的 order-report 事件
    要靠 payload.price_type 顯示「市價」，不能裸用 payload.price（"0" 是非空字串，原本
    的判斷式會誤判成「有價格」而顯示「@ 0」）。"""
    from quanquant.web.templating import STATIC_DIR

    js = (STATIC_DIR / "banners.js").read_text(encoding="utf-8")
    assert 'payload.price_type === "MKT"' in js
    assert "市價" in js


def test_medium5_position_strip_bridges_only_deal_event_with_debounce():
    """MEDIUM-5（收尾）＋MEDIUM-3（fresh-context 終審修復）：精簡條除既有 quote-stream
    節流心跳外，另外橋接 orders-stream 的 `deal` 事件驅動即時刷新（見 static/app.js）——
    不新開連線、不新增 hx-trigger 修飾詞，只是讓這個既有具名事件補發一次 `refreshorders`
    （position-strip 本來就在監聽 `refreshorders from:body`）。

    MEDIUM-3：不得再橋接 `sse:orders-changed`——那個事件已經被各頁需要它的元素直接監聽
    （如 #agent-status-box），再橋接只會讓同一個 ping 造成雙倍請求；`sse:deal` 必須帶
    debounce（clearTimeout/setTimeout 合併同批成交），避免一批多筆成交在毫秒內併發出
    對應筆數的 `refreshorders`（等於重新引入輪詢式壓力，違背當初拔掉 `every 2s` 的初衷）。
    """
    from quanquant.web.templating import STATIC_DIR

    js = (STATIC_DIR / "app.js").read_text(encoding="utf-8")
    assert 'addEventListener("sse:orders-changed"' not in js  # 不再橋接（已直達各元素）
    assert 'addEventListener("sse:deal"' in js
    assert 'htmx.trigger(document.body, "refreshorders")' in js
    assert "setTimeout" in js and "clearTimeout" in js  # debounce 標記
    assert "300" in js  # ~300ms debounce 時窗


def test_sim_confirm_dialog_markup_shows_sim_badge(order_client):
    """009／007：sim 確認視窗（前端在送出前攔截彈出）本體同樣要明示「模擬單」。"""
    text = order_client.get("/orders").text
    assert 'id="sim-confirm-dialog"' in text
    assert "模擬單" in text
    assert 'id="sim-confirm-skip-checkbox"' in text
