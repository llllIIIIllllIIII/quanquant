import datetime as dt
from decimal import Decimal

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from quanquant.auth import service as auth_service
from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.candles import service as candle_service
from quanquant.journal import repository as repo
from quanquant.journal.schemas import TradeCreate
from quanquant.web.app import create_app
from quanquant.web.deps import get_poller, get_session


@pytest.fixture(autouse=True)
def _clear_candle_caches():
    candle_service.clear_caches()
    yield
    candle_service.clear_caches()


@pytest.fixture
def engine():
    eng = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(eng)
    return eng


@pytest.fixture
def session(engine):
    with Session(engine) as s:
        yield s


@pytest.fixture
def user(engine):
    """Default logged-in account for route tests. Admin so admin-only surfaces
    (pulse toggle, /admin) work without a second fixture in most tests."""
    with Session(engine) as s:
        return auth_service.create_user(
            s, "tester", "test-pw", display_name="Tester", role="admin"
        )


def _build_app(engine):
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    return app


@pytest.fixture
def client(engine, user):
    """TestClient already logged in as `user` (signed cookie, no /login round-trip)."""
    c = TestClient(_build_app(engine))
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    return c


@pytest.fixture
def anon_client(engine):
    return TestClient(_build_app(engine))


@pytest.fixture
async def gui_client():
    """agent 本機 GUI（`quanquant.agent.gui.coordinator.build_app`）已核發 session
    cookie 的 async httpx client，供 Task 9（`/setup`）與 Task 10（`/status`）的路由
    測試共用——手法比照 `tests/test_agent_gui_security.py`：`ASGITransport`＋手動種好
    `GuiSecurityState.session_token`／cookie／Host header，不走真的 `/bootstrap`
    exchange（該流程已由 Task 8 自己的測試覆蓋）。

    `app.state.agent_http_client` 預先塞入一個 `MockTransport` backed 的 client，
    預設對 device-code/device-token 兩個端點一律回「pending、interval 很大」的
    安全回應——這樣任何會觸發 `setup_routes._ensure_device_flow_started()` 的路由
    （例如 `GET /setup`）在測試裡都不會嘗試真的打網路；需要測特定 device flow 情境的
    測試可以直接改寫 `gui_client.app.state.agent_http_client`（或直接操弄
    `gui_client.app.state.device_flow_client`/`device_flow_result`）。"""
    from quanquant.agent.device_flow_client import AGENT_AUTHORIZE_PATH
    from quanquant.agent.gui.coordinator import build_app
    from quanquant.agent.gui.security import GUI_SESSION_COOKIE, GuiSecurityState

    port = 54321
    state = GuiSecurityState(port=port)
    state.session_token = "test-gui-session-token"
    app = build_app(state)
    app.state.site_origin = "https://quant.example"

    def _default_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/agent/device-code":
            return httpx.Response(200, json={
                "device_code": "test-device-code", "user_code": "TEST-CODE",
                "verification_path": AGENT_AUTHORIZE_PATH, "interval": 999999, "expires_in": 600,
            })
        return httpx.Response(200, json={"state": "pending", "interval": 999999})

    app.state.agent_http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_default_handler), base_url=app.state.site_origin,
    )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url=f"http://127.0.0.1:{port}",
        cookies={GUI_SESSION_COOKIE: state.session_token},
        headers={"Host": f"127.0.0.1:{port}"},
    ) as client:
        client.app = app
        yield client
    await app.state.agent_http_client.aclose()


def make_create(**overrides) -> TradeCreate:
    base = dict(
        symbol="TXF",
        direction="long",
        entry_time=dt.datetime(2026, 6, 1, 9, 0),
        entry_price=Decimal("18000"),
        size=1,
        point_value=Decimal("200"),
    )
    base.update(overrides)
    return TradeCreate(**base)


@pytest.fixture
def sample_trades(session, user):
    # +20000 win
    repo.create_trade(
        session,
        make_create(exit_time=dt.datetime(2026, 6, 1, 10, 0), exit_price=Decimal("18100"), tags=["突破"]),
        user_id=user.id,
    )
    # MTX short loss: (18000-18050)*1*50 = -2500
    repo.create_trade(
        session,
        make_create(
            symbol="MTX", direction="short", point_value=Decimal("50"),
            exit_time=dt.datetime(2026, 6, 2, 10, 0), exit_price=Decimal("18050"), tags=["消息面"],
        ),
        user_id=user.id,
    )
    # -20000 loss
    repo.create_trade(
        session,
        make_create(exit_time=dt.datetime(2026, 6, 3, 10, 0), exit_price=Decimal("17900"), tags=["突破", "均線"]),
        user_id=user.id,
    )
    # open position
    repo.create_trade(session, make_create(entry_time=dt.datetime(2026, 6, 4, 9, 0)), user_id=user.id)
    return repo.list_trades(session, user_id=user.id)
