"""watchdog：探測到 session 掛掉後重連、重連成功後對帳補 RawInbox（不只 retry 本地
quarantine）、連續失敗 backoff 遞增、state 正確反映 ready/last_error、health probe 用真探測
（round3 #10：`_api` 物件還在不代表連線活著）、unknown 委託的配額 reconcile（round3
「quota unknown reconcile」）。用假 adapter，不連真網路。"""
import asyncio
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.broker import watchdog as watchdog_module
from quanquant.broker.session_state import OrderSessionState
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.watchdog import run_order_watchdog
from quanquant.db.models import QuotaReservation, RawInbox


class _FlakyAdapter:
    """round3 #10：`_api` 物件從一開始就存在（不是 None），但 health_probe() 回 False——
    模擬底層連線已死但 python object 還在的情境；只看 `_api is not None` 的舊版 watchdog
    永遠不會發現、永不重連。connect() 一次就成功並讓 health_probe 轉為 True；reconcile()
    帶回一筆待對帳的委託。"""

    def __init__(self, session_factory):
        self._session_factory = session_factory
        self.supervisor = BrokerSupervisor()
        self.broker = "shioaji"
        self._api = object()
        self.connect_calls = 0
        self.reconcile_calls = 0
        self._healthy = False

    async def health_probe(self) -> bool:
        return self._healthy

    async def connect(self) -> None:
        self.connect_calls += 1
        self._api = object()
        self._healthy = True

    async def reconcile(self) -> None:
        self.reconcile_calls += 1
        async with self.supervisor.lock:
            with self._session_factory() as session:
                brepo.stage_raw_inbox(session, kind="deal_report", broker=self.broker,
                                      payload=json.dumps({"deal_id": "RECONCILED"}))
                session.commit()


def test_watchdog_reconnects_and_reconciles_after_health_probe_failure(engine):
    adapter = _FlakyAdapter(lambda: Session(engine))
    state = OrderSessionState()

    async def scenario():
        task = asyncio.create_task(run_order_watchdog(
            adapter, state, interval=0.02, login_min_interval=0.0,
            unquarantine_after_seconds=9999, unknown_reconcile_grace_seconds=9999,
        ))
        await asyncio.sleep(0.1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(scenario())
    assert adapter.connect_calls >= 1
    assert adapter.reconcile_calls >= 1
    assert state.ready is True
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row is not None and json.loads(row.payload)["deal_id"] == "RECONCILED"


def test_watchdog_backoff_increases_on_repeated_connect_failure(engine):
    class _AlwaysFails:
        """沒有實作 health_probe（相容舊版極簡假 adapter）：_probe_healthy fallback 回
        `_api is not None`，起始 `_api=None` 視為不健康，觸發重連但每次都失敗。"""

        def __init__(self):
            self.supervisor = BrokerSupervisor()
            self._api = None
            self.connect_calls = 0

        async def connect(self):
            self.connect_calls += 1
            raise RuntimeError("login 失敗")

        async def reconcile(self):
            pass

    adapter = _AlwaysFails()
    state = OrderSessionState()

    async def scenario():
        task = asyncio.create_task(run_order_watchdog(
            adapter, state, interval=0.01, login_min_interval=0.0,
            unquarantine_after_seconds=9999, unknown_reconcile_grace_seconds=9999,
        ))
        await asyncio.sleep(0.15)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(scenario())
    assert adapter.connect_calls >= 1
    assert state.ready is False and state.last_error is not None
    assert state.reconnect_attempts >= 1


class _MinimalAdapter:
    """只給 watchdog 的 DB-only 背景工作（unquarantine/unknown reconcile）用，永遠健康、
    不觸發重連邏輯。"""

    def __init__(self, session_factory):
        self._session_factory = session_factory
        self.supervisor = BrokerSupervisor()
        self._api = object()

    async def health_probe(self) -> bool:
        return True

    async def reconcile(self) -> None:
        pass


def _make_unknown_order(session, *, client_order_id, ordno=None, broker_order_id=None, age_seconds=9999):
    order = brepo.create_order(
        session, client_order_id=client_order_id, request_hash="H1", user_id=1, mode="sim",
        broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", trading_day="2026-06-16",
    )
    order.status = "unknown"
    order.ordno = ordno
    order.broker_order_id = broker_order_id
    order.updated_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=age_seconds)
    session.add(order)
    return order


def test_unknown_quota_reconcile_resolves_orders_without_any_broker_id(engine):
    """round3「quota unknown reconcile」：從未拿到 ordno/broker_order_id 的 unknown 委託，
    native 呼叫幾乎確定沒送達券商——過了 grace period 判定失敗並釋放配額。"""
    with Session(engine) as s:
        order = _make_unknown_order(s, client_order_id="C-NEVER-SENT")
        assert brepo.reserve_quota(
            s, reservation_id=order.client_order_id, user_id=1, mode="sim",
            trading_day="2026-06-16", qty=1, daily_limit=20,
        )
        s.commit()

    adapter = _MinimalAdapter(lambda: Session(engine))
    asyncio.run(watchdog_module._reconcile_unknown_quota(adapter, grace_seconds=60))

    with Session(engine) as s:
        refreshed = brepo.find_order_by_client_order_id(s, "C-NEVER-SENT")
        assert refreshed.status == "failed"
        reservation = s.exec(
            select(QuotaReservation).where(QuotaReservation.reservation_id == "C-NEVER-SENT")
        ).first()
        assert reservation.state == "released"


def test_unknown_quota_reconcile_leaves_orders_with_known_broker_id_alone(engine):
    """有 ordno/broker_order_id 的 unknown 委託（多半來自 update 失敗，委託本身早已存在）
    不猜測——留給 reconcile() 的 order_report pipeline 自然解決，避免誤判。"""
    with Session(engine) as s:
        order = _make_unknown_order(s, client_order_id="C-HAS-ORDNO", ordno="O1", broker_order_id="B1")
        assert brepo.reserve_quota(
            s, reservation_id=order.client_order_id, user_id=1, mode="sim",
            trading_day="2026-06-16", qty=1, daily_limit=20,
        )
        s.commit()

    adapter = _MinimalAdapter(lambda: Session(engine))
    asyncio.run(watchdog_module._reconcile_unknown_quota(adapter, grace_seconds=60))

    with Session(engine) as s:
        refreshed = brepo.find_order_by_client_order_id(s, "C-HAS-ORDNO")
        assert refreshed.status == "unknown"  # 未被誤判
        reservation = s.exec(
            select(QuotaReservation).where(QuotaReservation.reservation_id == "C-HAS-ORDNO")
        ).first()
        assert reservation.state == "reserved"  # 未被誤釋放


def test_unknown_quota_reconcile_ignores_orders_still_within_grace_period(engine):
    with Session(engine) as s:
        order = _make_unknown_order(s, client_order_id="C-TOO-FRESH", age_seconds=1)
        assert brepo.reserve_quota(
            s, reservation_id=order.client_order_id, user_id=1, mode="sim",
            trading_day="2026-06-16", qty=1, daily_limit=20,
        )
        s.commit()

    adapter = _MinimalAdapter(lambda: Session(engine))
    asyncio.run(watchdog_module._reconcile_unknown_quota(adapter, grace_seconds=9999))

    with Session(engine) as s:
        refreshed = brepo.find_order_by_client_order_id(s, "C-TOO-FRESH")
        assert refreshed.status == "unknown"  # 還沒過 grace period，不動它
