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
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, RiskError
from quanquant.broker.connection_gate import AgentConnectionGate
from quanquant.broker.risk import RiskGuard
from quanquant.broker.shioaji_adapter import ShioajiAdapter
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import OrderAck, Position
from quanquant.db.models import AgentToken, Cooldown
from quanquant.web.app import create_app
from quanquant.web.deps import get_poller, get_session
from quanquant.web.routers.orders import _parse_optional_update_price, _parse_order_price


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


def test_orders_page_uses_sse_push_not_polling_and_guards_double_submit(order_client):
    """委託/部位改用 SSE 推送（sse:orders-changed）取代每 2s 盲輪詢：頁面要有 sse-connect
    容器、三個 div 的 trigger 含 sse:orders-changed（agent 狀態 badge + 委託 + 部位，Task 9
    加了第一個）、且不再有 every 2s 盲輪詢（消除對 event loop / supervisor 鎖的壓力）。保留
    refreshorders（本分頁動作當下即時刷新）與防連點。"""
    text = order_client.get("/orders").text
    assert 'sse-connect="/orders/stream"' in text  # SSE 連線容器
    assert text.count("sse:orders-changed") == 3  # agent 狀態 + 委託 + 部位 三個 div 都靠 SSE 觸發
    assert "every 2s" not in text  # 不再盲輪詢
    assert "refreshorders from:body" in text  # 動作當下本分頁仍即時刷新
    assert 'hx-get="/orders/list' in text and 'hx-get="/orders/positions"' in text
    assert "hx-disabled-elt" in text  # 送出期間停用送出鈕（防 double-submit）


def test_orders_stream_without_hub_returns_empty_stream_not_500(order_client):
    """/orders/stream：app.state.order_events 未接線（此 fixture 未設 hub）時回空 stream、
    不 500——與 alerts_stream 同慣例，端點在無 lifespan 的測試/停用情境不壞。"""
    resp = order_client.get("/orders/stream")
    assert resp.status_code == 200


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


def test_orders_page_shows_kill_switch_control_for_owner(order_client):
    text = order_client.get("/orders").text
    assert 'hx-post="/orders/kill-switch"' in text


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
    """非 admin（即使是 owner）POST /orders/kill-switch → 403、未翻閘、不告警（server 端強制，
    非只靠 UI 隱藏——嚴重授權漏洞的核心修正）。"""
    _demote_to_non_admin(session, user)
    resp = order_client.post("/orders/kill-switch", data={"enabled": "true", "scope": "global"})
    assert resp.status_code == 403
    assert fake_guard.set_kill_switch_calls == []
    assert fake_ops.kill_switch_calls == []


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


def test_orders_page_shows_agent_token_control_for_owner(order_client):
    text = order_client.get("/orders").text
    assert 'hx-post="/orders/agent-token"' in text
    assert "尚未產生 agent token" in text  # 尚未簽發過


def test_orders_page_hides_agent_token_control_for_non_owner(order_client, fake_guard):
    fake_guard._owner_ids = set()  # user 非 owner
    text = order_client.get("/orders").text
    assert 'hx-post="/orders/agent-token"' not in text


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


def test_orders_page_shows_disconnect_and_cooldown_controls_for_owner(order_client):
    """owner GET /orders 頁面實際 render 出「斷開 Agent」與「冷靜期」控制（驗模板真的
    render，不只端點存在）。"""
    text = order_client.get("/orders").text
    assert "斷開 Agent 連線" in text
    assert 'hx-post="/orders/agent-disconnect"' in text
    assert "冷靜期" in text
    assert 'hx-post="/orders/cooldown"' in text


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


def test_non_admin_owner_sees_only_cooldown_not_killswitch_or_disconnect(order_client, session, user):
    """非 admin 的 owner（測試者）：看不到「急停」與「斷開 Agent」，但**保留冷靜期**
    （＝使用者要求「只要留冷靜期就好了」的驗收）。"""
    _demote_to_non_admin(session, user)
    text = order_client.get("/orders").text
    assert 'hx-post="/orders/kill-switch"' not in text
    assert 'hx-post="/orders/agent-disconnect"' not in text
    assert 'hx-post="/orders/cooldown"' in text  # 冷靜期仍在


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


def test_orders_page_reload_does_not_leak_plaintext_after_issue(order_client):
    """簽發當下的回應才看得到明文；重新整理 /orders 頁只看得到 expires_at/last_used_at，
    看不到明文（DB 本就只存 hash，route 只在簽發那次回應塞 raw_token）。"""
    issue_resp = order_client.post("/orders/agent-token")
    plaintext = re.search(r"<code[^>]*>([^<]+)</code>", issue_resp.text).group(1)

    reload_text = order_client.get("/orders").text
    assert plaintext not in reload_text
    assert "尚未使用" in reload_text  # 剛簽發、還沒被 WS 握手用過


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
    assert "18500" in text  # 未成交委託仍顯示委託價

    # 精準定位 FILLED1 那一列（id="order-{id}"），確認成交均價顯示出來、不是裸的委託價 0。
    rows = re.findall(rf'<tr id="order-{filled_order.id}">.*?</tr>', text, re.DOTALL)
    assert len(rows) == 1
    assert "43737" in rows[0]
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
