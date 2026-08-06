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


def test_watchdog_reconnect_failure_alerts_connect_failed_with_redacted_message(engine):
    """T0.3：重連失敗時通知 OpsAlerter.connect_failed，訊息必須已 redact（login 例外可能
    夾帶 api_key/ca_passwd/person_id）。"""

    class _RecordingAlerter:
        def __init__(self):
            self.connect_failed_calls = []

        def connect_failed(self, message):
            self.connect_failed_calls.append(message)

    class _AlwaysFails:
        def __init__(self):
            self.supervisor = BrokerSupervisor()
            self._api = None
            self.secrets_to_redact = ["topsecretkey"]

        async def connect(self):
            raise RuntimeError("login failed api_key=topsecretkey")

        async def reconcile(self):
            pass

    adapter = _AlwaysFails()
    state = OrderSessionState()
    alerter = _RecordingAlerter()

    async def scenario():
        task = asyncio.create_task(run_order_watchdog(
            adapter, state, interval=0.01, login_min_interval=0.0,
            unquarantine_after_seconds=9999, unknown_reconcile_grace_seconds=9999,
            ops_alerter=alerter,
        ))
        await asyncio.sleep(0.1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(scenario())
    assert len(alerter.connect_failed_calls) >= 1
    assert all("topsecretkey" not in m for m in alerter.connect_failed_calls)  # 已 redact


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


# ---- round3 獨立驗收殘留1：update 路徑 quota unknown 未閉環 ----
# update 逾時（native 呼叫結果不明）標 order.status="unknown" 時，該筆委託早已有
# ordno/broker_order_id（委託本身在改單前就已存在），為這次改單 delta 建立的
# QuotaReservation（reservation_id 見 repository.reservation_id_for_update）過去永遠停在
# "reserved"——上面 `test_unknown_quota_reconcile_leaves_orders_with_known_broker_id_alone`
# 只驗證了「委託本身狀態不被誤判」，沒有涵蓋這個 delta 保留列的收尾。以下測試餵一個支援
# `_query_order_qty_blocking`（round3 殘留1 新增的 adapter 能力）的假 adapter，模擬「向券商
# 查詢這筆委託目前真實口數」兩種情境，驗證 watchdog 能依真實狀態 confirm/release，不再永遠
# 卡 reserved。


class _QueryableAdapter(_MinimalAdapter):
    """支援 `_query_order_qty_blocking` 的假 adapter：可設定回傳的「券商目前口數」，
    模擬改單生效/未生效兩種情境（round3 殘留1）。"""

    def __init__(self, session_factory, *, real_qty):
        super().__init__(session_factory)
        self._real_qty = real_qty
        self.query_calls = 0

    def _query_order_qty_blocking(self, ordno):
        self.query_calls += 1
        return self._real_qty


def _make_unknown_order_with_update_reservation(
    session, *, client_order_id, ordno, broker_order_id, original_qty, delta_qty, age_seconds=9999,
):
    """模擬「改單逾時」情境：委託本身以 original_qty 送出並早已有 ordno/broker_order_id，
    這次改單想加量 delta_qty（RiskGuard.check_update 只在增加時才建 delta 保留列），native
    呼叫逾時 → order.status 標 unknown，但 order.qty 維持 original_qty 不變（unknown 分支
    不覆寫），delta 保留列停在 reserved。"""
    order = brepo.create_order(
        session, client_order_id=client_order_id, request_hash="H1", user_id=1, mode="sim",
        broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=original_qty,
        price=Decimal("18000"), price_type="LMT", order_type="ROD", octype="New",
        trading_day="2026-06-16",
    )
    order.status = "unknown"
    order.ordno = ordno
    order.broker_order_id = broker_order_id
    order.updated_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=age_seconds)
    session.add(order)
    session.flush()
    reservation_id = brepo.reservation_id_for_update(client_order_id=client_order_id, request_hash="H2-UPDATE")
    assert brepo.reserve_quota(
        session, reservation_id=reservation_id, user_id=1, mode="sim",
        trading_day="2026-06-16", qty=delta_qty, daily_limit=20,
    )
    return order, reservation_id


def test_unknown_quota_reconcile_confirms_update_reservation_when_broker_shows_target_qty(engine):
    """殘留1：券商真實狀態顯示口數已是改單後目標值 → 改單其實生效，delta 保留 confirm。"""
    with Session(engine) as s:
        order, reservation_id = _make_unknown_order_with_update_reservation(
            s, client_order_id="C-UPD-OK", ordno="O1", broker_order_id="B1",
            original_qty=2, delta_qty=3,  # 目標 = 2+3 = 5
        )
        s.commit()

    adapter = _QueryableAdapter(lambda: Session(engine), real_qty=5)
    asyncio.run(watchdog_module._reconcile_unknown_quota(adapter, grace_seconds=60))

    with Session(engine) as s:
        reservation = s.exec(
            select(QuotaReservation).where(QuotaReservation.reservation_id == reservation_id)
        ).first()
        assert reservation.state == "confirmed"
        refreshed = brepo.find_order_by_client_order_id(s, "C-UPD-OK")
        assert refreshed.status == "unknown"  # 委託本身狀態留給 order_report pipeline，這裡不動


def test_unknown_quota_reconcile_releases_update_reservation_when_broker_shows_unchanged_qty(engine):
    """殘留1 對照情境：券商真實狀態顯示口數仍是改單前原值 → 改單其實沒生效，delta 保留 release。"""
    with Session(engine) as s:
        order, reservation_id = _make_unknown_order_with_update_reservation(
            s, client_order_id="C-UPD-FAIL", ordno="O2", broker_order_id="B2",
            original_qty=2, delta_qty=3,
        )
        s.commit()

    adapter = _QueryableAdapter(lambda: Session(engine), real_qty=2)  # 仍是改單前原值
    asyncio.run(watchdog_module._reconcile_unknown_quota(adapter, grace_seconds=60))

    with Session(engine) as s:
        reservation = s.exec(
            select(QuotaReservation).where(QuotaReservation.reservation_id == reservation_id)
        ).first()
        assert reservation.state == "released"


def test_unknown_quota_reconcile_update_reservation_leaves_ambiguous_qty_alone(engine):
    """既非改單前也非改單後的口數 → 無法判斷是哪次改單造成，不猜測，留待下一輪。"""
    with Session(engine) as s:
        order, reservation_id = _make_unknown_order_with_update_reservation(
            s, client_order_id="C-UPD-AMBIG", ordno="O3", broker_order_id="B3",
            original_qty=2, delta_qty=3,
        )
        s.commit()

    adapter = _QueryableAdapter(lambda: Session(engine), real_qty=99)
    asyncio.run(watchdog_module._reconcile_unknown_quota(adapter, grace_seconds=60))

    with Session(engine) as s:
        reservation = s.exec(
            select(QuotaReservation).where(QuotaReservation.reservation_id == reservation_id)
        ).first()
        assert reservation.state == "reserved"


def test_unknown_quota_reconcile_update_reservation_repeat_call_is_idempotent(engine):
    """一次性收尾：重複呼叫 reconcile 不會把已 confirmed 的列改成別的狀態。第二次呼叫時
    `list_reserved_update_reservations` 已經查不到這筆保留（狀態已離開 "reserved"），watchdog
    連券商都不必再查——比 confirm_quota/release_quota 本身的原子一次性保證更早一步省下
    白工，同樣達成「不重複釋放/確認」。"""
    with Session(engine) as s:
        order, reservation_id = _make_unknown_order_with_update_reservation(
            s, client_order_id="C-UPD-REPEAT", ordno="O4", broker_order_id="B4",
            original_qty=2, delta_qty=3,
        )
        s.commit()

    adapter = _QueryableAdapter(lambda: Session(engine), real_qty=5)
    asyncio.run(watchdog_module._reconcile_unknown_quota(adapter, grace_seconds=60))
    asyncio.run(watchdog_module._reconcile_unknown_quota(adapter, grace_seconds=60))  # 重複呼叫

    with Session(engine) as s:
        reservation = s.exec(
            select(QuotaReservation).where(QuotaReservation.reservation_id == reservation_id)
        ).first()
        assert reservation.state == "confirmed"  # 沒有因為重複呼叫變成別的狀態
    assert adapter.query_calls == 1  # 保留列已離開 reserved，第二次不必再查券商（省下白工）


def test_unknown_quota_reconcile_update_reservation_ignores_orders_still_within_grace_period(engine):
    """grace period 內完全不動——連券商都不該查。"""
    with Session(engine) as s:
        order, reservation_id = _make_unknown_order_with_update_reservation(
            s, client_order_id="C-UPD-FRESH", ordno="O5", broker_order_id="B5",
            original_qty=2, delta_qty=3, age_seconds=1,
        )
        s.commit()

    adapter = _QueryableAdapter(lambda: Session(engine), real_qty=5)
    asyncio.run(watchdog_module._reconcile_unknown_quota(adapter, grace_seconds=9999))

    with Session(engine) as s:
        reservation = s.exec(
            select(QuotaReservation).where(QuotaReservation.reservation_id == reservation_id)
        ).first()
        assert reservation.state == "reserved"
    assert adapter.query_calls == 0


def test_unknown_quota_reconcile_update_reservation_left_reserved_when_adapter_lacks_query_support(engine):
    """相容沒有實作 `_query_order_qty_blocking` 的假 adapter（如既有 `_MinimalAdapter`）：
    找不到查詢能力就不猜測，維持 reserved，不崩潰。"""
    with Session(engine) as s:
        order, reservation_id = _make_unknown_order_with_update_reservation(
            s, client_order_id="C-UPD-NOQUERY", ordno="O6", broker_order_id="B6",
            original_qty=2, delta_qty=3,
        )
        s.commit()

    adapter = _MinimalAdapter(lambda: Session(engine))  # 沒有 _query_order_qty_blocking
    asyncio.run(watchdog_module._reconcile_unknown_quota(adapter, grace_seconds=60))

    with Session(engine) as s:
        reservation = s.exec(
            select(QuotaReservation).where(QuotaReservation.reservation_id == reservation_id)
        ).first()
        assert reservation.state == "reserved"
