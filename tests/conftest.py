import datetime as dt
from decimal import Decimal

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
