"""Task 13：跨 user 端到端整合測試（S#8 全項端到端層）。

沿用 `test_agent_ws.py` 的 TestClient websocket 手法（真 WS 訊息交換，「place」不重跑
route→gateway→DownPlace 這段送單管線本身——那段已由 Task 3/7/8 系列測試覆蓋；這裡直接以
`AgentCommand`/`Order`/`QuotaReservation` 落 DB 模擬「route 已決策＋DownPlace 已送」的狀態，
聚焦驗證兩個**真正獨立**的 owner（各自 token/slot/RiskGuard 白名單成員/RawInboxWorker）在
同一個 app 實例裡跑完整情境時彼此的資料與連線狀態完全隔離（I8）：

  1. A/B 各自 token 握手 → UpLogin 綁不同帳號 → health ok → 各自 place（cmd_ack）→
     UpReport 成交 → 各自 RawInboxWorker 收斂 → 各自 orders/positions 只看得到自己的資料。
  2. A offline 不影響 B 的連線狀態／badge。
  3. A 有 quarantine 積壓不影響 B 的登入 guard（per-user 化，D5 S2）。
  4. A 開自己的 kill switch（scope=self）不影響 B 下單（D3，兩層 kill switch）。

不重複已有的單元/協定層覆蓋（token 生命週期、協定驗證、CAS 競態等——見
tests/test_agent_ws.py、tests/test_agent_commands.py、tests/test_agent_failstop.py 等），
只補「兩個真實獨立 user 同時跑在同一個 app」這一層還沒有人證明過的縫。
"""
import datetime as dt
import json
import time
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlmodel import Session, select
from starlette.testclient import TestClient

from quanquant.auth import service as auth_service
from quanquant.auth.agent_tokens import issue_token
from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.broker import repository as brepo
from quanquant.broker.agent_channel import AgentChannel, AgentNativeGateway
from quanquant.broker.agent_registry import AgentRegistry, UserAgentSlot
from quanquant.broker.base import RiskError
from quanquant.broker.inbox_worker import OrderReport, RawInboxWorker
from quanquant.broker.risk import RiskGuard
from quanquant.broker.session_state import OrderSessionState
from quanquant.broker.shioaji_adapter import ShioajiAdapter
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import Fill, OrderRequest
from quanquant.config import get_settings
from quanquant.db.models import AgentCommand, BrokerPosition, Order, RawInbox
from quanquant.web.app import create_app
from quanquant.web.deps import get_session


# ---------------------------------------------------------------------------
# 測試替身：與 tests/test_inbox_worker.py 的 _ok_deal_mapper/_noop_order_report_mapper
# 同構——不拉真 ShioajiAdapter._map_deal_report（那條含 S3 mapper 蓋章驗證，已由
# test_inbox_worker.py 直接覆蓋），只驗證 worker→Order/BrokerPosition 這一段跨 user 是否
# 隔離；ShioajiAdapter 本身仍是真的（positions_snapshot/list 等路由依賴它）。
# ---------------------------------------------------------------------------
def _deal_payload(**over):
    base = dict(
        broker="shioaji", fill_id="F-DEFAULT", ordno="O-DEFAULT", broker_order_id="B-DEFAULT",
        symbol="TXF", action="Buy", price="21000", qty=1, fee="20", octype="New",
        ts=1_780_000_000_000, account="F1", mode="sim",
    )
    base.update(over)
    return base


def _deal_mapper(payload: dict, *, account: str | None = None) -> Fill:
    return Fill(
        broker=payload["broker"], fill_id=payload["fill_id"], ordno=payload["ordno"],
        broker_order_id=payload["broker_order_id"], symbol=payload["symbol"], action=payload["action"],
        price=Decimal(payload["price"]), qty=int(payload["qty"]), fee=Decimal(payload["fee"]),
        octype=payload["octype"], ts=payload["ts"], account=payload["account"], mode=payload["mode"],
        user_id=None,
    )


def _noop_order_report_mapper(payload: dict, *, account: str | None = None) -> OrderReport:
    return OrderReport(**payload)


def _wait(cond, timeout=2.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.02)
    return False


def _make_slot_and_worker(*, user_id, account, session_factory, guard):
    supervisor = BrokerSupervisor()
    channel = AgentChannel()
    gateway = AgentNativeGateway(channel, timeout_seconds=5)
    adapter = ShioajiAdapter(
        api_key="", secret_key="", ca_path=None, ca_passwd=None, person_id=None,
        symbol="TXF", mode="sim", session_factory=session_factory, supervisor=supervisor,
        risk_guard=guard, sim_fee_per_lot=Decimal("20"), remote_gateway=gateway,
    )
    state = OrderSessionState()
    state.mark_disabled("agent 未連線")
    slot = UserAgentSlot(user_id=user_id, channel=channel, gateway=gateway, adapter=adapter,
                         session_state=state, supervisor=supervisor, tasks=[])
    worker = RawInboxWorker(
        session_factory=session_factory, supervisor=supervisor, deal_mapper=_deal_mapper,
        order_report_mapper=_noop_order_report_mapper, idle_interval=0.02, user_id=user_id,
    )
    return slot, worker


@pytest.fixture
def two_user_env(engine, monkeypatch):
    """兩個各自獨立的 owner（各自 agent token/slot/worker），共用一個 app/RiskGuard/DB
    engine——比照 tests/test_agent_integration.py::live_server 的 slot 組裝手法，但不起真
    uvicorn（不需要，見檔頭說明：place 不走真的 route→WS DownPlace 送單管線）。"""
    # 比照 test_agent_integration.py::live_server：orders_agent_status 的 partial 只在
    # order_channel=="agent" 時才輸出任何文字（預設 "inprocess"），不補這行 badge 斷言必敗。
    monkeypatch.setenv("ORDER_CHANNEL", "agent")
    get_settings.cache_clear()
    app = create_app()

    def _session_override():
        with Session(engine) as s:
            yield s

    app.dependency_overrides[get_session] = _session_override

    def session_factory():
        return Session(engine)

    with Session(engine) as s:
        user_a = auth_service.create_user(s, "agent-owner-a", "pw12345", role="admin")
        user_b = auth_service.create_user(s, "agent-owner-b", "pw12345", role="admin")
        a_id, a_tv = user_a.id, user_a.token_version
        b_id, b_tv = user_b.id, user_b.token_version
        token_a = issue_token(s, user_id=a_id, ttl_days=30)
        token_b = issue_token(s, user_id=b_id, ttl_days=30)

    guard = RiskGuard(
        session_factory=session_factory, secret="s", owner_user_ids=frozenset({a_id, b_id}),
        symbol_whitelist=frozenset({"TXF"}), max_qty_per_order=5, max_qty_per_day=20,
        max_orders_per_day=20,
    )
    slot_a, worker_a = _make_slot_and_worker(
        user_id=a_id, account="FA", session_factory=session_factory, guard=guard
    )
    slot_b, worker_b = _make_slot_and_worker(
        user_id=b_id, account="FB", session_factory=session_factory, guard=guard
    )
    registry = AgentRegistry()
    registry.add(slot_a)
    registry.add(slot_b)
    app.state.agent_registry = registry
    app.state.order_session_factory = session_factory
    app.state.order_risk_guard = guard

    env = SimpleNamespace(
        app=app, engine=engine, guard=guard,
        a=SimpleNamespace(user_id=a_id, token_version=a_tv, token=token_a, slot=slot_a,
                          worker=worker_a, account="FA"),
        b=SimpleNamespace(user_id=b_id, token_version=b_tv, token=token_b, slot=slot_b,
                          worker=worker_b, account="FB"),
    )
    yield env
    get_settings.cache_clear()


def _login_and_ready(ws, who):
    ws.send_json({"type": "login", "account": who.account, "mode": "sim", "protocol": 2,
                 "health_epoch": 0})
    ws.send_json({"type": "health", "status": "ok", "health_epoch": 0})
    assert _wait(lambda: who.slot.session_state.ready)
    # UpLogin 分支用 asyncio.create_task 排了一個 login-reconcile（見 agent_ws.py
    # `_reconcile_after_login`）——會下行一則 DownReconcile 等回覆；不理它會讓
    # `AgentChannel.request` 卡到 timeout（拖慢測試、噴無關 log），這裡當一個乖 fake agent
    # 立刻回一個空快照（無 drift），讓它馬上收斂。
    down = ws.receive_json()
    assert down["type"] == "reconcile"
    ws.send_json({"type": "query_result", "cmd_id": down["cmd_id"],
                 "result": {"payloads": [], "newest": None}})


def _seed_place_command(engine, *, who, client_order_id, cmd_id):
    """模擬「route 已決策（DB 建單＋配額保留）＋DownPlace 已送」——與 test_agent_ws.py／
    test_agent_failstop.py 既有測試同一套手法（直接落 AgentCommand），不重跑送單路徑本身。"""
    with Session(engine) as s:
        order = brepo.create_order(
            s, client_order_id=client_order_id, request_hash="H", user_id=who.user_id,
            mode="sim", broker="shioaji", account=who.account, symbol="TXF", action="Buy",
            qty=1, price=Decimal("21000"), price_type="LMT", order_type="ROD", octype="Auto",
            trading_day="2026-08-07",
        )
        order.status = "unknown"
        s.add(order)
        brepo.reserve_quota(s, reservation_id=client_order_id, user_id=who.user_id, mode="sim",
                            trading_day="2026-08-07", qty=1, daily_limit=20)
        s.add(AgentCommand(
            cmd_id=cmd_id, user_id=who.user_id, kind="place", broker="shioaji",
            account=who.account, mode="sim", client_order_id=client_order_id,
            reservation_id=client_order_id,
            payload=json.dumps({"action": "Buy", "price": "21000", "qty": 1, "price_type": "LMT",
                                 "order_type": "ROD", "octype": "Auto"}),
            expires_at=dt.datetime(2099, 1, 1),
        ))
        s.commit()


# ---------------------------------------------------------------------------
# 1) 全鏈：握手 → 綁帳號 → health ok → place（cmd_ack）→ 成交（report）→ 各自資料隔離
# ---------------------------------------------------------------------------
def test_two_users_place_report_fill_and_data_isolated(two_user_env):
    env = two_user_env
    client = TestClient(env.app)

    with client.websocket_connect(
        "/ws/agent", headers={"x-agent-token": env.a.token}
    ) as ws_a, client.websocket_connect(
        "/ws/agent", headers={"x-agent-token": env.b.token}
    ) as ws_b:
        _login_and_ready(ws_a, env.a)
        _login_and_ready(ws_b, env.b)

        _seed_place_command(env.engine, who=env.a, client_order_id="c-a-1", cmd_id="cmd-a-1")
        _seed_place_command(env.engine, who=env.b, client_order_id="c-b-1", cmd_id="cmd-b-1")

        ws_a.send_json({"type": "cmd_ack", "cmd_id": "cmd-a-1", "event_id": 1, "ok": True,
                       "result": {"ordno": "OA1", "broker_order_id": "BA1"}})
        ws_b.send_json({"type": "cmd_ack", "cmd_id": "cmd-b-1", "event_id": 1, "ok": True,
                       "result": {"ordno": "OB1", "broker_order_id": "BB1"}})
        # C1（HIGH，codex 終審）：server 現在會在 applier commit 完成後回一則 DownReportAck
        # （event_id=cmd_ack 的 event_id）——不讀掉這則，下面針對 report（event_id=2）的
        # 單次 receive_json() 會先收到這則殘留在佇列裡的 event_id=1，斷言必敗。
        assert ws_a.receive_json() == {"type": "report_ack", "event_id": 1}
        assert ws_b.receive_json() == {"type": "report_ack", "event_id": 1}

        def _acked(client_order_id, ordno):
            with Session(env.engine) as s:
                o = s.exec(select(Order).where(Order.client_order_id == client_order_id)).one()
                return o.status == "submitted" and o.ordno == ordno
        assert _wait(lambda: _acked("c-a-1", "OA1"))
        assert _wait(lambda: _acked("c-b-1", "OB1"))

        ws_a.send_json({"type": "report", "event_id": 2, "kind": "deal_report",
                       "account": env.a.account, "mode": "sim",
                       "payload": _deal_payload(fill_id="FA1", ordno="OA1", broker_order_id="BA1",
                                                account=env.a.account)})
        assert ws_a.receive_json() == {"type": "report_ack", "event_id": 2}
        ws_b.send_json({"type": "report", "event_id": 2, "kind": "deal_report",
                       "account": env.b.account, "mode": "sim",
                       "payload": _deal_payload(fill_id="FB1", ordno="OB1", broker_order_id="BB1",
                                                account=env.b.account)})
        assert ws_b.receive_json() == {"type": "report_ack", "event_id": 2}

    # 各自 per-slot RawInboxWorker（D6）只處理自己 user 蓋章的列——A 的 worker 恰處理 1 筆
    # （自己的），B 的 worker 恰處理 1 筆（自己的）；交叉再跑一輪應皆回 0（無誤撿對方的列）。
    assert env.a.worker.process_batch_once() == 1
    assert env.b.worker.process_batch_once() == 1
    assert env.a.worker.process_batch_once() == 0
    assert env.b.worker.process_batch_once() == 0

    with Session(env.engine) as s:
        oa = s.exec(select(Order).where(Order.client_order_id == "c-a-1")).one()
        ob = s.exec(select(Order).where(Order.client_order_id == "c-b-1")).one()
        assert oa.status == "filled" and oa.filled_qty == 1 and oa.user_id == env.a.user_id
        assert ob.status == "filled" and ob.filled_qty == 1 and ob.user_id == env.b.user_id
        pos_a = s.exec(select(BrokerPosition).where(BrokerPosition.user_id == env.a.user_id)).all()
        pos_b = s.exec(select(BrokerPosition).where(BrokerPosition.user_id == env.b.user_id)).all()
        assert len(pos_a) == 1 and len(pos_b) == 1
        oa_id, ob_id = oa.id, ob.id

    # 各自資料隔離（HTTP route 層，真的各自 cookie 各自查）：A 的 orders/positions 查不到 B 的。
    client.cookies.set(SESSION_COOKIE, sign_session(env.a.user_id, env.a.token_version))
    page_a = client.get("/orders/list?mode=sim").text
    assert f'id="order-{oa_id}"' in page_a
    assert f'id="order-{ob_id}"' not in page_a
    assert "尚無部位" not in client.get("/orders/positions").text

    client.cookies.set(SESSION_COOKIE, sign_session(env.b.user_id, env.b.token_version))
    page_b = client.get("/orders/list?mode=sim").text
    assert f'id="order-{ob_id}"' in page_b
    assert f'id="order-{oa_id}"' not in page_b
    assert "尚無部位" not in client.get("/orders/positions").text


# ---------------------------------------------------------------------------
# 2) A offline 不影響 B 的連線狀態／per-user badge（D9/I8）
# ---------------------------------------------------------------------------
def test_a_offline_does_not_affect_b_connection_status(two_user_env):
    env = two_user_env
    client = TestClient(env.app)

    with client.websocket_connect("/ws/agent", headers={"x-agent-token": env.a.token}) as ws_a:
        _login_and_ready(ws_a, env.a)
    # A 連線關閉（with 區塊結束，非正常斷線）→ finally 分支標 offline。
    assert _wait(lambda: env.a.slot.session_state.ready is False)

    with client.websocket_connect("/ws/agent", headers={"x-agent-token": env.b.token}) as ws_b:
        _login_and_ready(ws_b, env.b)
        # B 上線的當下，A 仍然離線——彼此連線狀態互不牽動。
        assert env.a.slot.session_state.ready is False

        client.cookies.set(SESSION_COOKIE, sign_session(env.b.user_id, env.b.token_version))
        assert "agent 已連線" in client.get("/orders/agent-status").text
        client.cookies.set(SESSION_COOKIE, sign_session(env.a.user_id, env.a.token_version))
        assert "agent 未連線" in client.get("/orders/agent-status").text


# ---------------------------------------------------------------------------
# 3) A 有 quarantine 積壓不影響 B 的登入 guard（D5 S2 per-user 化）
# ---------------------------------------------------------------------------
def test_a_quarantine_backlog_does_not_block_b_login(two_user_env):
    env = two_user_env
    client = TestClient(env.app)

    with client.websocket_connect("/ws/agent", headers={"x-agent-token": env.a.token}) as ws_a:
        _login_and_ready(ws_a, env.a)

    with Session(env.engine) as s:
        s.add(RawInbox(kind="deal_report", broker="shioaji", payload="{}", processed=False,
                       quarantine=True, quarantine_reason="association_pending",
                       user_id=env.a.user_id, account=env.a.account, mode="sim"))
        s.commit()

    # B 第一次登入：guard 只查「B 自己 user_id」名下未處理/quarantine 列，A 的積壓（不同
    # user_id）不應誤擋——不會被 close(1008)，能正常轉 ready。
    with client.websocket_connect("/ws/agent", headers={"x-agent-token": env.b.token}) as ws_b:
        _login_and_ready(ws_b, env.b)


# ---------------------------------------------------------------------------
# 4) A 開自己的 kill switch（scope=self）不影響 B 下單（D3 兩層 kill switch）
# ---------------------------------------------------------------------------
def test_a_self_kill_switch_does_not_block_b_place(two_user_env):
    env = two_user_env
    client = TestClient(env.app)

    client.cookies.set(SESSION_COOKIE, sign_session(env.a.user_id, env.a.token_version))
    r = client.post("/orders/kill-switch", data={"enabled": "true", "scope": "self"})
    assert r.status_code == 200

    assert env.guard.blocked(env.a.user_id) is True
    assert env.guard.blocked(env.b.user_id) is False

    req_a = OrderRequest(client_order_id="ks-a-1", symbol="TXF", action="Buy", qty=1,
                         price=Decimal("21000"), price_type="LMT", order_type="ROD",
                         octype="Auto", user_id=env.a.user_id)
    req_b = OrderRequest(client_order_id="ks-b-1", symbol="TXF", action="Buy", qty=1,
                         price=Decimal("21000"), price_type="LMT", order_type="ROD",
                         octype="Auto", user_id=env.b.user_id)

    with Session(env.engine) as s:
        with pytest.raises(RiskError, match="kill switch"):
            env.guard.check_place(s, req_a, actor_user_id=env.a.user_id, mode="sim",
                                  broker="shioaji", account=env.a.account, request_hash="h-a")

    with Session(env.engine) as s:
        order_b = env.guard.check_place(s, req_b, actor_user_id=env.b.user_id, mode="sim",
                                        broker="shioaji", account=env.b.account,
                                        request_hash="h-b")
        assert order_b.client_order_id == "ks-b-1"  # B 未被擋，正常建單
