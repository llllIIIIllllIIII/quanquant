import asyncio
from types import SimpleNamespace
from fastapi.testclient import TestClient
from sqlmodel import Session

from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.broker.session_state import OrderSessionState
from quanquant.config import Settings
from quanquant.web.app import _start_agent_channel_subsystem, create_app
from quanquant.web.deps import get_poller, get_session


def _settings(**kw):
    base = dict(order_channel="agent", order_mode="sim",
                order_owner_user_ids="1", session_secret="s")
    base.update(kw)
    return Settings(**base)


def _app():
    return SimpleNamespace(state=SimpleNamespace(order_events=None))


def _make_logged_in_client(engine, user):
    """比照 test_orders_routes.py 的 order_client fixture：建 app（覆寫 get_session/
    get_poller，不觸發 lifespan）、簽登入 session cookie，回傳 (client, app) 供呼叫端自行
    塞 app.state（本檔測試需要塞 order_session_state，fixture 化不合適）。"""
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    client = TestClient(app)
    client.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    return client, app


async def _run(settings, engine, monkeypatch):
    # 防呆：wiring 測試絕不能碰真實 quanquant.db —— get_engine 換成 in-memory
    monkeypatch.setattr("quanquant.web.app.get_engine", lambda: engine)
    app, state, tasks = _app(), OrderSessionState(), []
    await _start_agent_channel_subsystem(app, settings, tasks, state, None)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    return app, state, tasks


async def test_agent_mode_requires_sim(engine, monkeypatch):
    _, state, tasks = await _run(_settings(order_mode="real"), engine, monkeypatch)
    assert not state.ready and not state.disabled          # 真故障 → /healthz 503
    assert "僅支援" in (state.last_error or "") and tasks == []


# D2：`agent_ws_token`（站台層級靜態密鑰）已整個移除，原
# `test_agent_mode_without_token_disabled`（測「未設定 AGENT_WS_TOKEN → 通道停用」）連同
# 這個機制一起消失——沒有站台層級的靜態密鑰可以「未設定」了，改成 per-user DB opaque
# token（見 `auth/agent_tokens.py`），token 存在與否是每個 user 自己的事，不再是
# channel 啟動與否的閘門。等價的「無效/缺席 token 一律拒絕連線」安全性保證改由
# `tests/test_agent_ws.py::test_ws_rejects_when_token_header_missing_or_empty` 與
# `test_bad_token_closed` 在 WS 握手層驗證（比啟動閘門更貼近真正的防線位置）。


async def test_agent_mode_without_owner_disabled(engine, monkeypatch):
    _, state, _ = await _run(_settings(order_owner_user_ids=""), engine, monkeypatch)
    assert state.disabled and "order_owner_user_ids" in (state.last_error or "")


async def test_agent_mode_backfill_conflict_fail_closed_disabled_not_wired(engine, monkeypatch):
    # Task 6（D10/R1-8/R2-7）：既有 Order 歷史 ownership 衝突（同帳號跨 user）→ backfill 讓
    # 這個子系統拒啟——order_state 標 disabled（不是 mark_unhealthy，比照既有 preflight 軟
    # 停用語意，/healthz 仍可回 200，不崩整站，屬於「需要人工裁決」而非「app 起不來」）；
    # 且不得繼續往下 wiring channel/adapter/inbox_worker（拒啟＝真的沒有 wiring，不是半套）。
    from decimal import Decimal

    from quanquant.db.models import Order

    with Session(engine) as s:
        s.add(Order(
            client_order_id="C1", request_hash="H1", user_id=1, mode="sim",
            broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
            price=Decimal("18000"), price_type="LMT", order_type="ROD", octype="New",
            trading_day="2026-06-16",
        ))
        s.add(Order(
            client_order_id="C2", request_hash="H1", user_id=2, mode="real",
            broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
            price=Decimal("18000"), price_type="LMT", order_type="ROD", octype="New",
            trading_day="2026-06-16",
        ))
        s.commit()

    app, state, tasks = await _run(_settings(), engine, monkeypatch)
    assert state.disabled is True and state.ready is False
    assert "backfill" in (state.last_error or "") and "衝突" in (state.last_error or "")
    assert getattr(app.state, "agent_channel", None) is None      # 沒有繼續 wiring
    assert getattr(app.state, "order_service", None) is None
    assert tasks == []


async def test_agent_mode_happy_path_wires_state(engine, monkeypatch):
    app, state, tasks = await _run(_settings(), engine, monkeypatch)
    from quanquant.broker.agent_channel import AgentChannel
    from quanquant.broker.shioaji_adapter import ShioajiAdapter
    assert isinstance(app.state.agent_channel, AgentChannel)
    assert isinstance(app.state.order_service, ShioajiAdapter)
    assert app.state.order_risk_guard is not None
    assert callable(app.state.order_session_factory)
    assert state.disabled and "agent 未連線" in (state.last_error or "")
    assert len(tasks) >= 2                                  # inbox worker + agent watchdog


# ---------------------------------------------------------------------------
# Task 9：UI agent 連線狀態 badge —— GET /orders/agent-status partial + orders.html 掛載點
# ---------------------------------------------------------------------------

def test_agent_status_partial_offline_and_online(engine, user, monkeypatch):
    from quanquant.broker.session_state import OrderSessionState
    from quanquant.config import get_settings
    monkeypatch.setenv("ORDER_CHANNEL", "agent")
    get_settings.cache_clear()
    client, app = _make_logged_in_client(engine, user)
    state = OrderSessionState()
    state.mark_disabled("agent 未連線")
    app.state.order_session_state = state
    body = client.get("/orders/agent-status").text
    assert "agent 未連線" in body
    state.mark_ready()
    assert "agent 已連線" in client.get("/orders/agent-status").text
    get_settings.cache_clear()


def test_agent_status_hidden_when_inprocess(engine, user, monkeypatch):
    monkeypatch.setenv("ORDER_CHANNEL", "inprocess")
    from quanquant.config import get_settings
    get_settings.cache_clear()
    client, app = _make_logged_in_client(engine, user)
    assert "agent" not in client.get("/orders/agent-status").text
    get_settings.cache_clear()


def test_orders_page_contains_agent_status_div(engine, user):
    # 比照 test_orders_routes 的頁面測試：orders.html 有 hx-get="/orders/agent-status"
    client, app = _make_logged_in_client(engine, user)
    app.state.order_service = None
    assert 'hx-get="/orders/agent-status"' in client.get("/orders").text
