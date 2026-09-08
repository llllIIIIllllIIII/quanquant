"""Increment 0 骨幹自動化證明（真 WS socket；SDK 以 FakeNativeClient 替身）。

證明整條鏈：POST /orders（簽章 cookie、真 HTTP port）→ 下行 place（真 WS）→ agent child
（FakeNativeClient）→ 回報上行 → RawInbox → RawInboxWorker → Order=filled +
BrokerPosition + UI 局部 + kill switch 擋單。

本檔對任務簡報（.superpowers/sdd/task-15-brief.md）逐字版本做了兩處修正（跑出來才發現，
細節見 task-15-report.md）：

1. `live_server` fixture 補上 `monkeypatch.setenv("ORDER_CHANNEL", "agent")`：
   `orders_agent_status` 的 partial（`agent_status.html`）只在 `channel == "agent"` 時才
   輸出任何文字，`Settings.order_channel` 預設 `"inprocess"`——不補這行，
   `"agent 已連線" in ...` 的斷言必敗（partial 永遠回空字串）。比照既有
   `tests/test_agent_app_wiring.py::test_agent_status_partial_offline_and_online`。

2. 下單表單改用 `price_type="LMT"` + 非零 `price`（原簡報用 `MKT`+空 price）：
   `FakeNativeClient.place()` 把下單時的 `price` 原樣回填進 deal_report（`agent/testing.py`
   docstring 已言明其欄位形狀對齊真實 SDK），MKT 慣例送 price="0"；但 `Fill.__post_init__`
   （`broker/types.py`）對「真成交回報」的 price 一律要求 >0（不像 `OrderRequest` 對 MKT
   放行 0——委託單可以不帶限價，但成交回報一定有實際成交價）。MKT+price=0 這個組合送
   到 mapper 層會被 `_map_deal_report`→`Fill` 拒絕、quarantine，全鏈跑不完整。這正是既有
   `tests/test_agent_child.py`（Task 11）第 123-125 行已經記錄的既定慣例：「用非零價格
   （LMT）：deal_report 要能通過 Fill.__post_init__ 的 price>0 驗證，才能真的驗到
   `_map_deal_report` 全鏈路成功（MKT 的 price="0" 只適合測 ack/持久化路徑）」——非 src bug，
   是本檔跟隨既有慣例的必要修正。
"""
import asyncio
import threading
import time
from contextlib import asynccontextmanager
from decimal import Decimal

import httpx
import pytest
import uvicorn
from sqlmodel import Session, select

from quanquant.agent.buffer import DurableBuffer
from quanquant.agent.runner import AgentRunner
from quanquant.agent.testing import fake_native_factory
from quanquant.agent.ws_client import WebsocketsTransport
from quanquant.auth.agent_tokens import issue_token
from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.broker.agent_channel import AgentChannel, AgentNativeGateway
from quanquant.broker.agent_registry import AgentRegistry, UserAgentSlot
from quanquant.broker.inbox_worker import RawInboxWorker
from quanquant.broker.order_events import OrderEventHub
from quanquant.broker.risk import RiskGuard
from quanquant.broker.session_state import OrderSessionState
from quanquant.broker.shioaji_adapter import ShioajiAdapter
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.config import get_settings
from quanquant.db.models import BrokerPosition, Order
from quanquant.web.app import create_app
from quanquant.web.deps import get_session


class _ThreadChild:
    """ChildHandle 同介面、但用執行緒跑 child_main（重用 Task 11 測試手法）。"""

    def __init__(self, buffer_path):
        import multiprocessing as mp
        from quanquant.agent.native_runner import child_main

        self._parent, child_conn = mp.Pipe()
        self._lock = threading.Lock()
        self.ops = []
        self._thread = threading.Thread(
            target=child_main, args=(child_conn,),
            kwargs=dict(credentials={"api_key": "k", "secret_key": "s"}, symbol="TXF",
                        mode="sim", buffer_path=buffer_path,
                        native_factory=fake_native_factory),
            daemon=True)
        self._thread.start()
        self.alive = True

    def start(self):
        reply = self.request({"op": "connect"}, timeout=10)
        assert reply["ok"], reply
        return reply["account"]

    def request(self, op, *, timeout):
        with self._lock:
            self.ops.append(op)
            self._parent.send(op)
            if not self._parent.poll(timeout):
                raise TimeoutError
            return self._parent.recv()

    def ping(self, *, timeout):
        try:
            return self.request({"op": "ping"}, timeout=timeout).get("ok", False)
        except TimeoutError:
            return False

    def terminate(self):
        self.alive = False


@pytest.fixture
def live_server(engine, user, monkeypatch):
    # 修正 1（見檔頭說明）：orders_agent_status 的 partial 只在 order_channel=="agent" 時
    # 才輸出任何文字，不補這行最後 "agent 已連線" 斷言必敗。
    monkeypatch.setenv("ORDER_CHANNEL", "agent")
    get_settings.cache_clear()
    app = create_app()

    def _session_override():
        with Session(engine) as s:
            yield s

    app.dependency_overrides[get_session] = _session_override

    def session_factory():
        return Session(engine)

    # D2：全站靜態 AGENT_WS_TOKEN 已移除，改為每個 user 一枚 DB opaque token——這裡幫
    # `user`（conftest 的預設登入帳號，也是下面 RiskGuard 的唯一 owner）簽發一枚真正的
    # token，agent 端連線改帶這枚（見下方 WebsocketsTransport(token=agent_token)）。
    with session_factory() as s:
        agent_token = issue_token(s, user_id=user.id, ttl_days=30)

    # Task 7：per-user slot（單 owner，registry 只有一格）——channel/gateway/adapter/
    # session_state/supervisor 全部打包進 UserAgentSlot，`app.state.agent_registry` 取代
    # 舊的單一全域 app.state.agent_channel/order_service/order_session_state。
    supervisor = BrokerSupervisor()
    guard = RiskGuard(session_factory=session_factory, secret="s",
                      owner_user_ids=frozenset({user.id}),
                      symbol_whitelist=frozenset({"TXF"}), max_qty_per_order=5,
                      max_qty_per_day=20, max_orders_per_day=20)
    channel = AgentChannel()
    gateway = AgentNativeGateway(channel, timeout_seconds=5)
    adapter = ShioajiAdapter(api_key="", secret_key="", ca_path=None, ca_passwd=None,
                             person_id=None, symbol="TXF", mode="sim",
                             session_factory=session_factory, supervisor=supervisor,
                             risk_guard=guard, sim_fee_per_lot=Decimal("20"),
                             remote_gateway=gateway)
    hub = OrderEventHub()
    worker = RawInboxWorker(session_factory=session_factory, supervisor=supervisor,
                            deal_mapper=adapter._map_deal_report,
                            order_report_mapper=adapter._map_order_report,
                            order_events=hub, idle_interval=0.05, user_id=user.id)
    state = OrderSessionState()
    state.mark_disabled("agent 未連線")
    slot = UserAgentSlot(user_id=user.id, channel=channel, gateway=gateway, adapter=adapter,
                         session_state=state, supervisor=supervisor, tasks=[])
    registry = AgentRegistry()
    registry.add(slot)
    app.state.agent_registry = registry
    app.state.order_risk_guard = guard
    app.state.order_session_factory = session_factory
    app.state.order_events = hub

    @asynccontextmanager
    async def _lifespan(app):
        t = asyncio.create_task(worker.run())
        yield
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)

    app.router.lifespan_context = _lifespan
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0,
                                           log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(200):
        if server.started:
            break
        time.sleep(0.05)
    assert server.started, "uvicorn 未啟動"
    port = server.servers[0].sockets[0].getsockname()[1]
    yield f"127.0.0.1:{port}", app, agent_token
    server.should_exit = True
    thread.join(timeout=10)
    get_settings.cache_clear()


async def _until(cond, timeout=10.0):
    async def _poll():
        while not cond():
            await asyncio.sleep(0.05)
    await asyncio.wait_for(_poll(), timeout)


async def test_skeleton_roundtrip_and_kill_switch(live_server, engine, user, tmp_path):
    host, app, agent_token = live_server
    buf = DurableBuffer(tmp_path / "o.db")
    child = _ThreadChild(str(tmp_path / "o.db"))
    runner = AgentRunner(transport=WebsocketsTransport(f"ws://{host}/ws/agent", token=agent_token),
                         buffer=buf, child=child, pump_interval=0.05, resend_after=1.0,
                         child_command_timeout=5, child_ping_interval=30,
                         child_ping_timeout=5, heartbeat_interval=30)
    runner.ensure_child()
    run_task = asyncio.create_task(runner.run_once())
    try:
        slot = app.state.agent_registry.get(user.id)
        await _until(lambda: slot.channel.ready)
        assert slot.session_state.ready          # login → mark_ready

        async with httpx.AsyncClient(base_url=f"http://{host}") as client:
            client.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
            # 修正 2（見檔頭說明）：LMT + 非零 price，deal_report 才能通過 Fill 的
            # price>0 驗證、走完整條 mapper 鏈到 filled（比照 test_agent_child.py 慣例）。
            r = await client.post("/orders", data={
                "client_order_id": "e2e-1", "symbol": "TXF", "action": "Buy",
                "qty": "1", "price": "18500", "price_type": "LMT", "order_type": "ROD",
                "octype": "Auto"})
            assert r.status_code == 200

            def _filled():
                with Session(engine) as s:
                    o = s.exec(select(Order).where(
                        Order.client_order_id == "e2e-1")).first()
                    return o is not None and o.status == "filled"
            await _until(_filled)                            # 全鏈：place→report→worker

            with Session(engine) as s:
                assert s.exec(select(BrokerPosition)).first() is not None

            page = await client.get("/orders/list?mode=sim")
            assert "1/1" in page.text                        # UI 成交欄

            assert "agent 已連線" in (await client.get("/orders/agent-status")).text

            # kill switch：ON 後新單被擋、child 未收到新 place（D3：全站總閘 scope=global）
            await client.post("/orders/kill-switch", data={"enabled": "true", "scope": "global"})
            n_ops = len(child.ops)
            r = await client.post("/orders", data={
                "client_order_id": "e2e-2", "symbol": "TXF", "action": "Buy",
                "qty": "1", "price": "18500", "price_type": "LMT", "order_type": "ROD",
                "octype": "Auto"})
            assert "kill switch" in r.text
            assert len([op for op in child.ops[n_ops:] if op["op"] == "place"]) == 0
    finally:
        run_task.cancel()
        await asyncio.gather(run_task, return_exceptions=True)
        child.request({"op": "shutdown"}, timeout=5)
