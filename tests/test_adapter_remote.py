"""本機 broker agent（WS 通道）例外分類測試 + ShioajiAdapter `remote_gateway` 三段切行為矩陣。

`AgentUnavailableError`（指令送出前 agent 即不在線，保證未送達券商）
→ `_classify_place_failure` 判 `"failed"`（可安全退配額）。
`AgentCommandTimeoutError`（指令可能已送達但未收到 ack）
→ `_classify_place_failure` 判 `"unknown"`（保守保留配額）。
既有 `code: 4xx` 券商拒單規則不變。

下半段（Inc0 Task 6）：`ShioajiAdapter.__init__(remote_gateway=...)` 三段切——DB 決策/寫回留
server，native 呼叫改經 `_NativeGatewayLike` gateway 下行；Tier0 硬化語意（配額/失敗分類/kill
switch）必須跨網路後原樣保存，見任務簡報行為矩陣。
"""
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.broker.agent_channel import AgentChannel, AgentNativeGateway
from quanquant.broker.agent_commands import apply_command_ack
from quanquant.broker.agent_protocol import UpCmdAck
from quanquant.broker.base import (
    AgentCommandTimeoutError,
    AgentUnavailableError,
    OrderError,
    RiskError,
    TradeNotFoundError,
)
from quanquant.broker.risk import RiskGuard
from quanquant.broker.shioaji_adapter import ShioajiAdapter, _classify_place_failure
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import OrderRequest
from quanquant.db.models import AgentCommand, Order, QuotaReservation, RawInbox


def test_agent_unavailable_classified_failed():
    assert _classify_place_failure(AgentUnavailableError("agent 未連線")) == "failed"


def test_agent_timeout_classified_unknown():
    assert _classify_place_failure(AgentCommandTimeoutError("ack 逾時")) == "unknown"


def test_broker_reject_code_still_failed():
    assert _classify_place_failure(Exception("code: 406 not signed")) == "failed"


def test_agent_timeout_wrapping_broker_code_message_still_unknown():
    """結構性早退（非靠訊息不含 code: 4xx 的隱含保證）：即使底層例外訊息被
    `AgentChannel.request` 包成含 `code: 4xx` 字樣，型別判斷仍優先於字串內容，
    保持 `AgentCommandTimeoutError` 一律 unknown。"""
    exc = AgentCommandTimeoutError("底層錯誤 code: 404 xxx")
    assert _classify_place_failure(exc) == "unknown"


# ---- Task 6: remote_gateway 三段切行為矩陣 ----


class _FakeGateway:
    def __init__(self):
        self.ready = True
        self.place_calls, self.cancel_calls = [], []
        self.result = {"ordno": "101AA1", "broker_order_id": "101AA1"}
        self.raise_exc = None
        self.snapshot = ([], None)
        self.last_expires_at = None  # C9：記錄最後一次呼叫收到的 expires_at，供 wire 斷言

    @property
    def admission_ready(self) -> bool:
        # C2：測試替身沒有真正的健康狀態機——鏡射 `ready`，讓既有「gw.ready = False」
        # 寫法對 place/update 的 admission 檢查（現在改查 admission_ready）仍然生效。
        return self.ready

    async def place(self, req, *, cmd_id=None, expires_at=None):
        self.place_calls.append(req)
        self.last_expires_at = expires_at
        if self.raise_exc:
            raise self.raise_exc
        return self.result

    async def cancel(self, ordno, *, cmd_id=None, expires_at=None):
        self.cancel_calls.append(ordno)
        self.last_expires_at = expires_at
        if self.raise_exc:
            raise self.raise_exc

    async def update(self, ordno, *, price, qty, price_type=None, cmd_id=None, expires_at=None):
        self.last_expires_at = expires_at
        if self.raise_exc:
            raise self.raise_exc

    async def trades_snapshot(self, after):
        return self.snapshot


def _guard(engine):
    return RiskGuard(session_factory=lambda: Session(engine), secret="s",
                     owner_user_ids=frozenset({1}), symbol_whitelist=frozenset({"TXF"}),
                     max_qty_per_order=5, max_qty_per_day=20, max_orders_per_day=20)


def _adapter(engine, gw, guard=None):
    a = ShioajiAdapter(api_key="", secret_key="", ca_path=None, ca_passwd=None,
                       person_id=None, symbol="TXF", mode="sim",
                       session_factory=lambda: Session(engine),
                       supervisor=BrokerSupervisor(), risk_guard=guard,
                       sim_fee_per_lot=Decimal("20"), remote_gateway=gw)
    a.account = "F1"
    return a


def _req(cid="c-1"):
    return OrderRequest(client_order_id=cid, symbol="TXF", action="Buy", qty=1,
                        price=Decimal("21500"), price_type="LMT", order_type="ROD",
                        octype="Auto", user_id=1)


async def test_remote_place_success_submitted_and_quota_confirmed(engine):
    gw = _FakeGateway()
    a = _adapter(engine, gw, _guard(engine))
    ack = await a.place(_req(), actor_user_id=1)
    assert ack.status == "submitted" and ack.ordno == "101AA1"
    with Session(engine) as s:
        order = s.exec(select(Order)).one()
        assert order.status == "submitted" and order.ordno == "101AA1"
        assert s.exec(select(QuotaReservation)).one().state == "confirmed"


async def test_remote_place_timeout_unknown_and_quota_reserved(engine):
    gw = _FakeGateway()
    gw.raise_exc = AgentCommandTimeoutError("ack 逾時")
    a = _adapter(engine, gw, _guard(engine))
    with pytest.raises(AgentCommandTimeoutError):
        await a.place(_req(), actor_user_id=1)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "unknown"
        assert s.exec(select(QuotaReservation)).one().state == "reserved"


async def test_remote_place_unavailable_midflight_failed_and_quota_released(engine):
    gw = _FakeGateway()
    gw.raise_exc = AgentUnavailableError("斷線")
    a = _adapter(engine, gw, _guard(engine))
    with pytest.raises(AgentUnavailableError):
        await a.place(_req(), actor_user_id=1)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "failed"
        assert s.exec(select(QuotaReservation)).one().state == "released"


async def test_remote_place_offline_fails_fast_no_db_rows(engine):
    gw = _FakeGateway()
    gw.ready = False
    a = _adapter(engine, gw, _guard(engine))
    with pytest.raises(OrderError):
        await a.place(_req(), actor_user_id=1)
    with Session(engine) as s:
        assert s.exec(select(Order)).all() == []
        assert s.exec(select(QuotaReservation)).all() == []


# ---------------------------------------------------------------------------
# C2（HIGH，codex 終審）：gateway.admission_ready 必須反映真健康——pending_health/
# failstop/lease 過期時 admission 不得建任何 DB 決策列（Order/QuotaReservation/
# AgentCommand）。用真的 AgentChannel/AgentNativeGateway（不是 _FakeGateway——
# `_FakeGateway.ready` 只是可自由設定的 bool，測不到 AgentChannel 本身的健康收斂邏輯，
# 也就測不到這個修復）。`ready`（寬鬆，只看連線存活＋已登入）刻意與 `admission_ready`
# 分離——reconcile/query_qty 等唯讀背景動作不受健康狀態影響，只有真的會建新 DB 決策列
# 的 place/update 才查 `admission_ready`；下面測試會同時斷言兩者，證明這個刻意的分離。
# ---------------------------------------------------------------------------


def _no_decision_rows(engine) -> None:
    with Session(engine) as s:
        assert s.exec(select(Order)).all() == []
        assert s.exec(select(QuotaReservation)).all() == []
        assert s.exec(select(AgentCommand)).all() == []


def _real_channel_adapter(engine, channel, guard=None):
    gw = AgentNativeGateway(channel, timeout_seconds=1)
    a = ShioajiAdapter(api_key="", secret_key="", ca_path=None, ca_passwd=None,
                       person_id=None, symbol="TXF", mode="sim",
                       session_factory=lambda: Session(engine),
                       supervisor=BrokerSupervisor(), risk_guard=guard,
                       sim_fee_per_lot=Decimal("20"), remote_gateway=gw)
    a.account = "F1"
    channel.account = "F1"
    return a


async def test_place_blocked_no_db_rows_when_pending_health(engine):
    """C2 三態之一：剛登入、本 session 尚未收過任何被接受的 `UpHealth(ok)`
    （pending_health）——`admission_ready` 必須是 False（`ready` 寬鬆定義仍是 True，
    連線本身還活著），place admission 拒絕、不建任何 DB 決策列。"""
    channel = AgentChannel()
    channel.attach(lambda msg: None)
    channel.mark_logged_in("F1")  # 未 note_health
    assert channel.ready is True  # 寬鬆定義：連線+已登入即真，供 reconcile/query_qty 使用
    assert channel.admission_ready is False
    a = _real_channel_adapter(engine, channel, _guard(engine))
    with pytest.raises(OrderError):
        await a.place(_req(), actor_user_id=1)
    _no_decision_rows(engine)


async def test_place_blocked_no_db_rows_when_failstop(engine):
    """C2 三態之二：agent 已回報 `status="failstop"`。"""
    channel = AgentChannel()
    channel.attach(lambda msg: None)
    channel.mark_logged_in("F1")
    channel.note_health(status="ok", health_epoch=0)
    channel.note_health(status="failstop", health_epoch=1)
    assert channel.ready is True
    assert channel.admission_ready is False
    a = _real_channel_adapter(engine, channel, _guard(engine))
    with pytest.raises(OrderError):
        await a.place(_req(), actor_user_id=1)
    _no_decision_rows(engine)


async def test_place_blocked_no_db_rows_when_lease_expired(engine):
    """C2 三態之三：曾經 admission_ready，但 server 端 heartbeat lease 過期
    （`AgentChannel.mark_lease_expired`，由 `agent_registry.run_health_lease_watchdog`
    呼叫）——`admission_ready` 必須立即失效，不必等下一則 UpHealth。"""
    channel = AgentChannel()
    channel.attach(lambda msg: None)
    channel.mark_logged_in("F1")
    channel.note_health(status="ok", health_epoch=0)
    assert channel.admission_ready is True
    channel.mark_lease_expired()
    assert channel.ready is True  # 連線仍活著（lease 過期不等於斷線）
    assert channel.admission_ready is False
    a = _real_channel_adapter(engine, channel, _guard(engine))
    with pytest.raises(OrderError):
        await a.place(_req(), actor_user_id=1)
    _no_decision_rows(engine)


async def test_update_blocked_no_new_reservation_when_channel_pending_health(engine):
    """C2：`update()` 的 offline fail-fast 同樣要吃到新語意——先用 `_FakeGateway`
    （`ready=True`）正常送出一張委託，再把 adapter 換上一個真 `AgentChannel`（pending_
    health，未 note_health），驗證 update 被擋、沒有新 QuotaReservation。"""
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    with Session(engine) as s:
        reservations_before = len(list(s.exec(select(QuotaReservation))))

    channel = AgentChannel()
    channel.attach(lambda msg: None)
    channel.mark_logged_in("F1")  # pending_health：未 note_health
    a._remote_gateway = AgentNativeGateway(channel, timeout_seconds=1)

    with pytest.raises(OrderError):
        await a.update(ack.broker_order_id, actor_user_id=1, qty=3)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "submitted"  # 改單失敗不影響委託本身狀態
        assert len(list(s.exec(select(QuotaReservation)))) == reservations_before  # 沒建新保留列


async def test_remote_place_idempotent_replay_served_while_offline(engine):
    """Fix Round 1（審查 Important #1）：offline fail-fast 必須排在冪等查找之後——已成功
    送出的委託，client 用同一 client_order_id 重送時（例如網路逾時重試），即使 agent 此刻
    恰好離線，也要能命中冪等 shortcut 回快取 ack，不得被 fail-fast 攔截、也不能真的再送
    一次單。"""
    gw = _FakeGateway()
    a = _adapter(engine, gw, _guard(engine))
    req = _req(cid="c-replay")
    first = await a.place(req, actor_user_id=1)
    assert first.status == "submitted" and first.ordno == "101AA1"

    gw.ready = False
    replay = await a.place(req, actor_user_id=1)
    assert replay.status == "submitted"
    assert replay.ordno == "101AA1"
    with Session(engine) as s:
        assert len(s.exec(select(Order)).all()) == 1
    assert len(gw.place_calls) == 1


async def test_kill_switch_blocks_before_gateway_called(engine):
    gw = _FakeGateway()
    guard = _guard(engine)
    guard.set_kill_switch(True, scope="global", actor_user_id=1)
    a = _adapter(engine, gw, guard)
    with pytest.raises(RiskError):
        await a.place(_req(), actor_user_id=1)
    assert gw.place_calls == []


async def test_remote_kill_switch_self_scope_blocks_only_actor_not_other_owner(engine):
    """D3 S#40：remote 路徑（多 agent slot 情境）下兩層 kill switch 一樣成立——A 開自己的
    急停只擋 A，B 的下單經 gateway 正常送出。"""
    gw = _FakeGateway()
    guard = RiskGuard(session_factory=lambda: Session(engine), secret="s",
                      owner_user_ids=frozenset({1, 2}), symbol_whitelist=frozenset({"TXF"}),
                      max_qty_per_order=5, max_qty_per_day=20, max_orders_per_day=20)
    guard.set_kill_switch(True, scope="self", actor_user_id=1)
    a = _adapter(engine, gw, guard)

    with pytest.raises(RiskError):
        await a.place(_req(cid="c-a"), actor_user_id=1)
    assert gw.place_calls == []

    ack = await a.place(_req(cid="c-b"), actor_user_id=2)
    assert ack.status == "submitted"
    assert len(gw.place_calls) == 1


class _KillSwitchBlindGuard:
    """驗收者發現：`test_kill_switch_blocks_before_gateway_called` 用的真 `RiskGuard.
    check_place` 本身就會查 kill switch 並提早擋下——那支測試其實只驗到 check_place 這層，
    `_send_gate()`（鎖內、native/remote gateway 呼叫前的最後線性化點，見 shioaji_adapter.py
    `_send_gate` docstring）在 remote 路徑上從未被單獨驗證過（套套邏輯）。這個 fake guard
    比照 tests/test_shioaji_adapter.py 的 `test_send_gate_blocks_when_kill_switch_on`
    手法：`check_place` 完全不看 kill switch、直接放行建單，把「擋下」的責任完全留給
    `_send_gate()` 自己的 `self._risk_guard.blocked(user_id)` 檢查——這樣才是 `_send_gate`
    這道深度防禦真正被獨立驗證，而不是被上層 check_place 順便擋掉。"""

    kill_switch = True

    def assert_owner(self, actor_user_id):
        pass

    def blocked(self, user_id):
        return True

    def check_place(self, session, req, **kw):
        order = brepo.create_order(
            session, client_order_id=req.client_order_id, request_hash="H",
            user_id=kw["actor_user_id"], mode=kw["mode"], broker=kw["broker"], account=kw["account"],
            symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
            price_type=req.price_type, order_type=req.order_type, octype=req.octype,
            trading_day="2026-06-16",
        )
        session.commit()  # 比照真正 RiskGuard.check_place：quota reserve + create_order 同一交易提交
        return order


async def test_send_gate_blocks_kill_switch_in_remote_path_when_check_place_blind(engine):
    """Fix 6（順修）：remote gateway 路徑上，`_send_gate()` 自己的 kill switch 檢查要在
    鎖內、gateway.place() 呼叫之前獨立擋下——不依賴 check_place 是否也查了 kill switch。"""
    gw = _FakeGateway()
    a = _adapter(engine, gw, _KillSwitchBlindGuard())
    with pytest.raises(RiskError):
        await a.place(_req(), actor_user_id=1)
    assert gw.place_calls == []  # _send_gate 在 gateway.place() 之前就擋下，gateway 從未被呼叫
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "failed"  # 不留在 pending 卡死（同 V3-2 收尾原則）


# ---- Fix Round 1（審查 Important #2）：remote cancel/update 專屬測試補齊 ----
# cancel 的 `_do_cancel`、update 的 `_do_update` 實作在 Task 6 原始交付就已存在（gateway.ready
# 檢查、TradeNotFoundError/AgentUnavailableError/AgentCommandTimeoutError 傳遞），當時只缺
# 專屬測試覆蓋，這裡補上。


async def _placed_order(engine, gw, guard):
    """建立一筆已成功送出（status=submitted）的委託，供以下 cancel/update 測試操作。"""
    a = _adapter(engine, gw, guard)
    ack = await a.place(_req(), actor_user_id=1)
    return a, ack


async def test_remote_cancel_success_marks_cancelled(engine):
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    result = await a.cancel(ack.broker_order_id, actor_user_id=1)
    assert result.status == "cancelled"
    assert gw.cancel_calls == ["101AA1"]
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "cancelled"


async def test_remote_cancel_offline_rejected_state_unchanged(engine):
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    gw.ready = False
    with pytest.raises(OrderError):
        await a.cancel(ack.broker_order_id, actor_user_id=1)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "submitted"


async def test_remote_cancel_trade_not_found_propagates(engine):
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    gw.raise_exc = TradeNotFoundError("101AA1")
    with pytest.raises(OrderError):
        await a.cancel(ack.broker_order_id, actor_user_id=1)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "submitted"


async def test_remote_update_offline_fails_fast_no_new_reservation(engine):
    """Task 7（D9）：`gateway.ready=False` 時改單必須在 `check_update` 之前就被拒絕——
    `check_update` 若判定口數增加會建立 delta QuotaReservation（DB 決策段的一部分），offline
    fail-fast 必須搬到它之前，這筆保留列才不會被建立又要靠例外分支釋放。比照既有
    `test_remote_place_offline_fails_fast_no_db_rows`/`test_remote_cancel_offline_rejected_
    state_unchanged` 的驗收手法：委託狀態不變、且完全沒有新的 QuotaReservation 列。"""
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    with Session(engine) as s:
        reservations_before = len(list(s.exec(select(QuotaReservation))))
    gw.ready = False
    with pytest.raises(OrderError):
        await a.update(ack.broker_order_id, actor_user_id=1, qty=3)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "submitted"  # 改單失敗不影響委託本身狀態
        assert len(list(s.exec(select(QuotaReservation)))) == reservations_before  # 沒建新保留列


async def test_remote_update_timeout_marks_unknown_keeps_quota(engine):
    """Task 8（D4 kind×outcome 轉移表）行為變更，明確flag：spec v8 D4 表格「update | timeout
    | acked_unknown | **不改** | delta 保留」——Inc0 舊版對「結果不明」一律 `mark_order_status
    (unknown)`，但 update 是對一張**已經有效**委託的修改，把整張 Order 標成 unknown 會誤導
    （委託本身其實還健在，只是這次改單的結果不確定）；Inc1 導入 ledger 後，這個不確定性改由
    `agent_commands` 列的 outcome='unknown'（未 resolved）承載，不再需要污染 Order.status。
    place 沒有這個問題（timeout 前 Order 本來就沒有『上一個有效狀態』可以保留），維持
    「timeout → unknown」不變（見 test_remote_place_timeout_unknown_and_quota_reserved）。"""
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    gw.raise_exc = AgentCommandTimeoutError("逾時")
    with pytest.raises(AgentCommandTimeoutError):
        await a.update(ack.broker_order_id, actor_user_id=1, qty=3)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "submitted"  # 不改（新語意，見上方說明）
        rows = list(s.exec(select(QuotaReservation)))
        update_row = next(r for r in rows if r.reservation_id != ack.client_order_id)
        assert update_row.state == "reserved"  # 結果不明，watchdog reconcile 前不擅自 release


async def test_remote_update_unavailable_releases_delta_quota(engine):
    """讀碼結論（`update()` 的 `except Exception` 分支，見 shioaji_adapter.py ~707-747 行）：
    update 失敗分支語意與 place 不同——`classification=="failed"`（`AgentUnavailableError`
    正是一例）只會 release「若有」保留的 delta 配額，**不**把委託本身標成 failed；委託
    status 維持呼叫前的既有值（這裡是 place 留下的 "submitted"），因為「改單失敗不代表
    委託本身壞了」（同 in-process 版 test_update_releases_delta_quota_reservation_on_broker_
    explicit_rejection / ...when_send_gate_raises_riskerror 的既有原則）。只有
    classification=="unknown" 才會把委託標成 "unknown"（見上一個測試）。兩支分支最後都
    re-raise 原始例外型別（不包成 OrderError），故這裡 pytest.raises 抓的是
    AgentUnavailableError 本身。"""
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    gw.raise_exc = AgentUnavailableError("斷線")
    with pytest.raises(AgentUnavailableError):
        await a.update(ack.broker_order_id, actor_user_id=1, qty=3)
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "submitted"  # 改單失敗不影響委託本身狀態
        rows = list(s.exec(select(QuotaReservation)))
        update_row = next(r for r in rows if r.reservation_id != ack.client_order_id)
        assert update_row.state == "released"  # 明確判定失敗，立即釋放 delta 配額


# ---- Task 8：G1 command ledger 全鏈整合（真正經過 ShioajiAdapter 決策段建立的 ledger 列，
# 不是 test_agent_commands.py 那樣手工建構——驗證 insert_command/mark_timeout_observed 真的
# 接線到 place()/update()，而不只是 agent_commands.py 模組本身正確） ----


def _find_cmd(engine, *, client_order_id: str, kind: str) -> AgentCommand:
    with Session(engine) as s:
        stmt = select(AgentCommand).where(
            AgentCommand.client_order_id == client_order_id, AgentCommand.kind == kind
        )
        return s.exec(stmt).one()


async def test_place_timeout_then_late_ack_converges_full_chain(engine):
    """S#1 全鏈收斂，經真正的 ShioajiAdapter：route 逾時（AgentCommandTimeoutError）先把
    Order 標 unknown、`mark_timeout_observed` 贏得 CAS；隨後模擬 late ack（透過
    `apply_command_ack`，即 agent_ws.py 的 UpCmdAck handler 實際呼叫的同一函式）補上 ordno，
    Order 收斂為 submitted、配額 confirmed、ledger 列 resolved。"""
    gw = _FakeGateway()
    gw.raise_exc = AgentCommandTimeoutError("ack 逾時")
    guard = _guard(engine)
    a = _adapter(engine, gw, guard)
    a._agent_user_id = 1

    with pytest.raises(AgentCommandTimeoutError):
        await a.place(_req(), actor_user_id=1)

    cmd = _find_cmd(engine, client_order_id="c-1", kind="place")
    assert cmd.transport_acked_at is None and cmd.timeout_observed_at is not None
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "unknown"

    ack = UpCmdAck(cmd_id=cmd.cmd_id, event_id=1, ok=True,
                   result={"ordno": "101AA1", "broker_order_id": "101AA1"})
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1, ack=ack)
    assert outcome.applied and outcome.outcome == "ok"

    with Session(engine) as s:
        order = s.exec(select(Order)).one()
        assert order.status == "submitted" and order.ordno == "101AA1"
        assert s.exec(select(QuotaReservation)).one().state == "confirmed"


async def test_place_never_dispatched_locally_resolves_ledger(engine):
    """AgentUnavailableError（`_send_gate`/`channel.request` 的 ready 檢查落空，指令從未
    送達 agent）——route 本地終結 ledger（`resolve_never_dispatched`），Order failed＋
    quota released，且 ledger 不再是永遠 unresolved 的孤兒列。"""
    gw = _FakeGateway()
    gw.raise_exc = AgentUnavailableError("斷線")
    a = _adapter(engine, gw, _guard(engine))
    a._agent_user_id = 1
    with pytest.raises(AgentUnavailableError):
        await a.place(_req(), actor_user_id=1)

    cmd = _find_cmd(engine, client_order_id="c-1", kind="place")
    assert cmd.resolved_at is not None and cmd.resolved_via == "local" and cmd.outcome == "error"
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "failed"
        assert s.exec(select(QuotaReservation)).one().state == "released"


async def test_update_singleflight_rejects_second_unresolved_update(engine):
    """R5-2/R6-1：同一張 Order 已有一筆未 resolved 的 update ledger 列時，第二筆改單在
    decision 段插 ledger 就撞 `uq_agent_cmd_update_singleflight`，轉友善訊息「前一筆改單
    結果未定」，且第二筆的 ledger 列**不會**落地（`agent_commands` 只有第一筆）。

    C8（MEDIUM，codex 終審）修復：`RiskGuard.check_update` 內部會自行 commit 它建立的
    delta `QuotaReservation`，這個 commit 早於 ledger insert 撞鍵，撞鍵後的
    `session.rollback()` 本身救不回它——但這不再是「已知落差、放著不管」：撞鍵代表這筆
    ledger 從未真正落地送出，adapter 現在會在撞鍵當下主動查詢贏家（U1）目前的
    reservation_id，若這次（U2）自己 reserve 的 reservation_id 與贏家不同（本測試的
    U1/U2 qty 不同 → request_hash 不同 → reservation_id 不同），就原子釋放這筆孤兒
    reservation——不再永久占用配額（codex 終審 C8 原話：「正常請求可確定觸發，非 crash
    窗口」，兩個併發改單就會踩到）。"""
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    a._agent_user_id = 1
    gw.raise_exc = AgentCommandTimeoutError("逾時")  # 第一筆改單卡在 unresolved
    with pytest.raises(AgentCommandTimeoutError):
        await a.update(ack.broker_order_id, actor_user_id=1, qty=3)

    with Session(engine) as s:
        cmds_before = len(list(s.exec(select(AgentCommand))))

    gw.raise_exc = None
    with pytest.raises(OrderError, match="前一筆改單結果未定"):
        await a.update(ack.broker_order_id, actor_user_id=1, qty=5)

    with Session(engine) as s:
        # ledger 沒有第二筆（singleflight 真正擋下的是這個——未來重連補送/watchdog 只看
        # agent_commands，這筆孤兒 reservation 不會被誤判成「有一筆待收斂的指令」）。
        assert len(list(s.exec(select(AgentCommand)))) == cmds_before
        rows = list(s.exec(select(QuotaReservation)))
        orphan = next(r for r in rows if r.qty == 4)
        assert orphan.state == "released"  # C8：撞鍵孤兒 reservation 已被主動釋放
        winner = next(r for r in rows if r.qty == 2)  # U1（qty=3，delta=2）仍是贏家的保留列
        assert winner.state == "reserved"  # U1 尚未 ack，仍在 unresolved，配額不受影響


async def test_update_singleflight_loser_with_same_content_does_not_release_winner_reservation(engine):
    """C8（MEDIUM，codex 終審）：撞鍵嘗試若與贏家內容完全相同（同一 `qty`/`price` →
    `reservation_id_for_update` 是 client_order_id+request_hash 的確定性推導，會得到**同一個
    reservation_id**）——這種情況下絕不能釋放，那正是贏家（U1）目前仍在使用中的保留列，
    釋放會讓贏家後續 ack 時 `confirm_quota` 命中一個已被 release 的列（`state='reserved'`
    的 CAS 會落空），造成配額帳務錯誤。"""
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    a._agent_user_id = 1
    gw.raise_exc = AgentCommandTimeoutError("逾時")  # U1（qty=5，delta=4）卡在 unresolved
    with pytest.raises(AgentCommandTimeoutError):
        await a.update(ack.broker_order_id, actor_user_id=1, qty=5)

    with Session(engine) as s:
        reservations = list(s.exec(select(QuotaReservation)))
        winner = next(r for r in reservations if r.qty == 4)  # U1 的 delta 保留列
        winner_reservation_id = winner.reservation_id
        assert winner.state == "reserved"
        reservations_count_before = len(reservations)

    gw.raise_exc = None
    # U2：完全相同的內容（同 qty=5）——`reservation_id_for_update` 是 client_order_id+
    # request_hash 的確定性推導，內容相同會得到與 U1 相同的 reservation_id，撞鍵後不得
    # 誤釋放贏家的保留列。
    with pytest.raises(OrderError, match="前一筆改單結果未定"):
        await a.update(ack.broker_order_id, actor_user_id=1, qty=5)

    with Session(engine) as s:
        reservation = s.exec(
            select(QuotaReservation).where(QuotaReservation.reservation_id == winner_reservation_id)
        ).one()
        assert reservation.state == "reserved"  # 贏家的保留列完全沒被動到
        assert len(list(s.exec(select(QuotaReservation)))) == reservations_count_before  # 沒新增列


async def test_cancel_not_blocked_by_unresolved_update_singleflight(engine):
    """Task 13 補（S#33/R4-3 後半，盤點發現原本只測了「U1 timeout → U2 被拒」，沒有測「取消
    優先，不受單飛限制」半句）：同一張 Order 有一筆未 resolved 的 update ledger 列時，`cancel`
    仍應正常送出——單飛規則（`uq_agent_cmd_update_singleflight`）只約束 `kind='update'`，
    `cancel` 的 ledger insert 走不同 kind，結構上不會撞鍵；這裡直接以行為驗證，不只信任
    schema 推論。"""
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    a._agent_user_id = 1
    gw.raise_exc = AgentCommandTimeoutError("逾時")  # 改單卡在 unresolved
    with pytest.raises(AgentCommandTimeoutError):
        await a.update(ack.broker_order_id, actor_user_id=1, qty=3)

    gw.raise_exc = None
    result = await a.cancel(ack.broker_order_id, actor_user_id=1)  # 取消不受單飛擋
    assert result.status == "cancelled"
    with Session(engine) as s:
        assert s.exec(select(Order)).one().status == "cancelled"


async def test_update_late_ack_converges_price_qty_and_confirms_delta(engine):
    """update 版 late-ack 全鏈：route 逾時→unknown-ledger（不改 Order，delta 保留）→ late ack
    透過 apply_command_ack 補寫 price/qty、confirm delta、Order 回到 submitted。"""
    gw = _FakeGateway()
    a, ack = await _placed_order(engine, gw, _guard(engine))
    a._agent_user_id = 1
    gw.raise_exc = AgentCommandTimeoutError("逾時")
    with pytest.raises(AgentCommandTimeoutError):
        await a.update(ack.broker_order_id, actor_user_id=1, qty=3)

    cmd = _find_cmd(engine, client_order_id=ack.client_order_id, kind="update")
    assert cmd.resolved_at is None and cmd.reservation_id is not None

    late_ack = UpCmdAck(cmd_id=cmd.cmd_id, event_id=1, ok=True)
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1, ack=late_ack)
    assert outcome.applied and outcome.outcome == "ok"

    with Session(engine) as s:
        order = s.exec(select(Order)).one()
        assert order.qty == 3 and order.status == "submitted"
        rows = list(s.exec(select(QuotaReservation)))
        update_row = next(r for r in rows if r.reservation_id != ack.client_order_id)
        assert update_row.state == "confirmed"


async def test_remote_reconcile_stages_payloads_and_returns_count(engine):
    gw = _FakeGateway()
    gw.snapshot = ([{"order_id": "101AA1", "seqno": "101AA1", "status": "Filled"}],
                   datetime(2026, 8, 4, 9, 0))
    a = _adapter(engine, gw, _guard(engine))
    await a.reconcile()
    with Session(engine) as s:
        rows = s.exec(select(RawInbox)).all()
        assert len(rows) == 1 and rows[0].kind == "order_report"


# ---- Task 10：D4/R6-1 update admission（ordno IS NULL 拒絕）＋ agent_command_expiry_seconds
# 接線 ----


async def test_update_rejects_order_without_ordno_no_reservation_no_ledger(engine):
    """R6-1（S#38）：`DownUpdate` 協定的 `ordno` 是非空必填欄位——這張委託尚未取得券商流水號
    （例如 place 還在 unknown/等 ack 的中間態）時，admission 在建立 reservation/ledger 之前
    就直接拒絕，不進 DB 決策段：不建 QuotaReservation、不寫 agent_commands，也不呼叫
    gateway.update（否則會撞 `gw.raise_exc`）。"""
    gw = _FakeGateway()
    gw.raise_exc = RuntimeError("update 不該被呼叫——admission 應該在更早就拒絕")
    a = _adapter(engine, gw, _guard(engine))
    a._agent_user_id = 1
    with Session(engine) as s:
        order = brepo.create_order(
            s, client_order_id="c-noord", request_hash="H", user_id=1, mode="sim",
            broker="shioaji", account="F1", symbol="TXF", action="Buy", qty=1,
            price=Decimal("21500"), price_type="LMT", order_type="ROD", octype="Auto",
            trading_day="2026-08-07",
        )
        order.broker_order_id = "B-NOORD"  # 已知 broker_order_id，但 ordno 仍是 NULL
        s.add(order)
        s.commit()

    with pytest.raises(OrderError, match="ordno"):
        await a.update("B-NOORD", actor_user_id=1, qty=5)

    with Session(engine) as s:
        assert s.exec(select(AgentCommand)).all() == []
        assert s.exec(select(QuotaReservation)).all() == []
    assert gw.place_calls == [] and gw.cancel_calls == []


async def test_place_uses_configured_agent_command_expiry_seconds(engine):
    """config.py `agent_command_expiry_seconds` 真的接線到 `new_command(...)`——不是只讀
    `agent_commands.DEFAULT_COMMAND_EXPIRY_SECONDS` 這個模組層常數字面值。"""
    gw = _FakeGateway()
    a = ShioajiAdapter(
        api_key="", secret_key="", ca_path=None, ca_passwd=None, person_id=None,
        symbol="TXF", mode="sim", session_factory=lambda: Session(engine),
        supervisor=BrokerSupervisor(), risk_guard=_guard(engine),
        sim_fee_per_lot=Decimal("20"), remote_gateway=gw,
        agent_command_expiry_seconds=45,
    )
    a.account = "F1"
    await a.place(_req(), actor_user_id=1)
    cmd = _find_cmd(engine, client_order_id="c-1", kind="place")
    assert cmd.expires_at - cmd.created_at == timedelta(seconds=45)


async def test_place_wire_expires_at_matches_ledger_frozen_value_not_gateway_default(engine):
    """C9（LOW，codex 終審）：首次下行（非重連補送）也必須用 ledger 建立當下凍結的
    `expires_at`——不能像舊版一樣讓 `AgentNativeGateway.place()` 自己獨立呼叫
    `_default_expires_at()` 重算一次（該函式固定用 `agent_channel.py` 模組層硬編碼的
    `_DEFAULT_COMMAND_EXPIRY_SECONDS=120`，與這裡設定的 45 秒完全不同）。用非預設值
    （45s）配置：若舊 bug 還在，wire frame 的 `expires_at` 會是「建立時間+約 120 秒」，
    與 ledger 記的「建立時間+45 秒」相差近 75 秒，明顯不相等——這裡直接比對 wire frame
    （`gw.last_expires_at`）與 ledger 值字串相等，不只驗 DB。"""
    gw = _FakeGateway()
    a = ShioajiAdapter(
        api_key="", secret_key="", ca_path=None, ca_passwd=None, person_id=None,
        symbol="TXF", mode="sim", session_factory=lambda: Session(engine),
        supervisor=BrokerSupervisor(), risk_guard=_guard(engine),
        sim_fee_per_lot=Decimal("20"), remote_gateway=gw,
        agent_command_expiry_seconds=45,
    )
    a.account = "F1"
    await a.place(_req(), actor_user_id=1)
    cmd = _find_cmd(engine, client_order_id="c-1", kind="place")
    assert gw.last_expires_at == cmd.expires_at.isoformat()

    # cancel 走同一套修法，一併驗證 wire frame（place 已用掉 gw.result 的 "101AA1"）。
    await a.cancel("101AA1", actor_user_id=1)
    cancel_cmd = _find_cmd(engine, client_order_id="c-1", kind="cancel")
    assert gw.last_expires_at == cancel_cmd.expires_at.isoformat()
