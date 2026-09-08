"""BrokerSupervisor 序列化鎖 + RawInboxWorker：raw-inbox（durable spool，不經 volatile
asyncio.Queue——QueueFull 這個攻擊面已被架構消除）逐列一交易處理、業務失敗 quarantine
不留部分寫入、非預期例外不 quarantine（留待下一輪重試，零丟單）、複合鍵解析委託關聯、
Deal 重播冪等只補 processed 不重跑帳務、序列化鎖防兩協程同時改同一 BrokerPosition。"""
import asyncio
import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.broker.agent_commands import apply_command_ack, insert_command
from quanquant.broker.agent_protocol import UpCmdAck
from quanquant.broker.inbox_worker import (
    OrderReport,
    RawInboxDeadLetterError,
    RawInboxWorker,
    commit_raw_callback,
)
from quanquant.broker.order_events import OrderEventHub
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import Fill
from quanquant.db.models import (
    AgentAccountBinding,
    AgentCommand,
    BrokerPosition,
    Deal,
    Order,
    QuotaReservation,
    RawInbox,
)


def _order_kwargs(**over):
    base = dict(
        client_order_id="C1", request_hash="H1", user_id=1, mode="sim", broker="shioaji", account="F1",
        symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", trading_day="2026-06-16",
    )
    base.update(over)
    return base


def _deal_payload(**over):
    base = dict(
        broker="shioaji", fill_id="F1", ordno="O1", broker_order_id="B1", symbol="TXF",
        action="Buy", price="18000", qty=1, fee="20", octype="New",
        ts=1_780_000_000_000, account="F1", mode="sim",
    )
    base.update(over)
    return base


def _ok_deal_mapper(payload: dict, *, account: str | None = None) -> Fill:
    # Inc1 D5：`_process_deal` 現在一律呼叫 `mapper(payload, account=row.account)`——這些既有
    # 測試都直接用 `stage_raw_inbox` 落列、不帶 account（row.account 恆為 None），`account`
    # 收下但不使用，維持既有位元級行為（同 R2-2 的 NULL 放行原則）。
    return Fill(
        broker=payload["broker"], fill_id=payload["fill_id"], ordno=payload["ordno"],
        broker_order_id=payload["broker_order_id"], symbol=payload["symbol"], action=payload["action"],
        price=Decimal(payload["price"]), qty=int(payload["qty"]), fee=Decimal(payload["fee"]),
        octype=payload["octype"], ts=payload["ts"], account=payload["account"], mode=payload["mode"],
        user_id=None,
    )


def _noop_order_report_mapper(payload: dict, *, account: str | None = None) -> OrderReport:
    return OrderReport(**payload)


def _worker(engine, *, deal_mapper=_ok_deal_mapper, order_report_mapper=_noop_order_report_mapper,
            supervisor=None, ops_alerter=None, user_id=None, idle_interval=0.01, order_events=None):
    return RawInboxWorker(
        session_factory=lambda: Session(engine),
        supervisor=supervisor or BrokerSupervisor(),
        deal_mapper=deal_mapper,
        order_report_mapper=order_report_mapper,
        idle_interval=idle_interval,
        ops_alerter=ops_alerter,
        user_id=user_id,
        order_events=order_events,
    )


class _RecordingAlerter:
    """假 OpsAlerter：記錄 quarantine 呼叫 kwargs（T0.3 告警串接）；本身絕不 raise。"""

    def __init__(self) -> None:
        self.quarantine_calls: list[dict] = []

    def quarantine(self, **kw) -> None:
        self.quarantine_calls.append(kw)


def _seed_order(session, **over):
    order = brepo.create_order(session, **_order_kwargs(**over))
    brepo.set_order_ack(session, order.id, broker_order_id="B1", ordno="O1", status="submitted")
    session.commit()
    return order


def test_deal_report_resolves_order_by_composite_scope_and_applies_fill(session, engine):
    _seed_order(session)
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()

    worker = _worker(engine)
    handled = worker.process_batch_once()
    assert handled == 1

    with Session(engine) as s:
        assert s.exec(select(Deal)).first() is not None
        pos = s.exec(select(BrokerPosition)).first()
        assert pos is not None and pos.total_opened_qty == 1
        order = s.exec(select(Order)).first()
        assert order.filled_qty == 1
        row = s.exec(select(RawInbox)).first()
        assert row.processed is True and row.quarantine is False


def test_worker_scoped_to_user_id_ignores_other_users_rows(session, engine):
    """Task 7（D6/D9）：per-slot RawInboxWorker（傳 `user_id`）批次查詢只認領這個 user
    蓋章的列——另一個 user（或未蓋章 NULL）的殘留完全不被撿走、不影響 batch 的 handled 計數，
    跨 user 隔離（I8）在 worker 這一層直接驗證。"""
    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload()), user_id=2,  # 另一個 user 的列
        )
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload()),  # 未蓋章 NULL（in-process 舊列）
        )
        s.commit()

    worker = _worker(engine, user_id=1)  # 這個 slot 只認領 user_id=1
    assert worker.process_batch_once() == 0

    with Session(engine) as s:
        rows = s.exec(select(RawInbox)).all()
        assert len(rows) == 2
        assert all(r.processed is False and r.quarantine is False for r in rows)  # 完全沒被動到


# ---------------------------------------------------------------------------
# 007（批次 B-1）：Deal 落地／委託回報事件——user-scoped、只送給 owner；一筆 Deal 一事件
# （滑價一價一橫幅天然成立）；不影響既有無資料 orders-changed 廣播 ping。
#
# CRITICAL-1（fresh-context opus 終審修復，2026-09）：`_process_deal`/`_process_order_
# report` 現在只把事件 append 進 `worker._pending_events`（可能跑在 worker thread 上，
# 見 inbox_worker.py `run()`/`process_batch_once` docstring）——真正呼叫 hub、把事件送進
# `asyncio.Queue` 的動作，搬到 `worker._flush_pending_events()`（只能在 event loop 執行
# 緒上呼叫）。下面這 6 個測試直接同步呼叫 `process_batch_once()`（單一測試執行緒，呼叫
# `_flush_pending_events()` 本身安全），呼叫完後補一行 `worker._flush_pending_events()`
# 才能在 queue 上看到事件——這 6 個測試驗證的是「payload 內容/事件種類/user 隔離」等業務
# 邏輯，不驗證跨執行緒安全本身；跨執行緒安全（真正的 CRITICAL-1 重現）另見
# `test_deal_landing_publish_runs_on_loop_thread_safe_under_asyncio_debug_mode`（走真正
# 的 `run()` + `asyncio.to_thread` + `loop.set_debug(True)`）。
# ---------------------------------------------------------------------------

def test_deal_landing_publishes_scoped_deal_event_to_owner(session, engine):
    """①：成交落地觸發一個 user-scoped 'deal' 事件，payload 含商品/方向/口數/價格。"""
    _seed_order(session)  # user_id=1
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()

    hub = OrderEventHub()
    worker = _worker(engine, order_events=hub)
    queue = hub.subscribe(user_id=1)
    assert worker.process_batch_once() == 1
    worker._flush_pending_events()  # CRITICAL-1：直呼 process_batch_once 不再自動送出事件

    events = []
    while not queue.empty():
        events.append(queue.get_nowait())
    deal_events = [e for e in events if e["event"] == "deal"]
    assert len(deal_events) == 1
    payload = deal_events[0]["payload"]
    assert payload["symbol"] == "TXF"
    assert payload["action"] == "Buy"
    assert payload["qty"] == 1
    assert payload["price"] == "18000"


def test_deal_landing_also_publishes_order_report_reflecting_new_status(session, engine):
    """委託狀態變化（部分成交/全部成交）是 Deal 落地觸發 apply_order_fill 的直接結果——
    同一次落地也要發一個 'order-report' 事件，反映委託目前的最新狀態，不只有成交事件。"""
    _seed_order(session)  # qty=1，成交 1 口後應轉 filled
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()

    hub = OrderEventHub()
    worker = _worker(engine, order_events=hub)
    queue = hub.subscribe(user_id=1)
    assert worker.process_batch_once() == 1
    worker._flush_pending_events()

    events = []
    while not queue.empty():
        events.append(queue.get_nowait())
    kinds = {e["event"] for e in events}
    assert kinds == {"deal", "order-report"}
    order_evt = next(e for e in events if e["event"] == "order-report")
    assert order_evt["payload"]["status"] == "filled"
    assert order_evt["payload"]["symbol"] == "TXF"
    assert order_evt["payload"]["price_type"] == "LMT"  # LOW-6：payload schema 帶 price_type


def test_multiple_deals_in_one_batch_each_publish_a_separate_scoped_event(session, engine):
    """②：市價單滑價分兩口不同價成交——一次批次內兩筆 Deal，各自觸發一個獨立的
    'deal' 事件（不聚合成一條），前端才能一價一橫幅。"""
    _seed_order(session, qty=2)
    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload(fill_id="F1", price="18000")),
        )
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload(fill_id="F2", price="18010")),
        )
        s.commit()

    hub = OrderEventHub()
    worker = _worker(engine, order_events=hub)
    queue = hub.subscribe(user_id=1)
    assert worker.process_batch_once() == 2
    worker._flush_pending_events()

    deal_prices = []
    while not queue.empty():
        item = queue.get_nowait()
        if item["event"] == "deal":
            deal_prices.append(item["payload"]["price"])
    assert sorted(deal_prices) == ["18000", "18010"]  # 兩筆各自獨立的事件，兩個價格都在


def test_deal_landing_event_not_delivered_to_a_different_users_subscriber(session, engine):
    """③：非 owner 收不到——另一個 user 的訂閱者完全收不到這筆 Deal 的任何事件。"""
    _seed_order(session)  # user_id=1
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()

    hub = OrderEventHub()
    worker = _worker(engine, order_events=hub)
    owner_queue = hub.subscribe(user_id=1)  # 真正的 owner——用來證明事件確實有送出
    other_queue = hub.subscribe(user_id=2)  # 不是這筆委託的 owner
    assert worker.process_batch_once() == 1
    worker._flush_pending_events()
    assert not owner_queue.empty()  # 事件確實送出了（不是因為根本沒送出才收不到）
    assert other_queue.empty()


def test_order_report_status_change_publishes_scoped_order_report_event(session, engine):
    """委託回報（券商 callback，如 cancelled）落地時發一個 user-scoped 'order-report' 事件。"""
    _seed_order(session)
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="order_report", broker="shioaji", payload=json.dumps(dict(
            broker="shioaji", account="F1", mode="sim", ordno="O1", broker_order_id="B1", status="cancelled",
        )))
        s.commit()

    hub = OrderEventHub()
    worker = _worker(engine, order_events=hub)
    queue = hub.subscribe(user_id=1)
    assert worker.process_batch_once() == 1
    worker._flush_pending_events()

    item = queue.get_nowait()
    assert item["event"] == "order-report"
    assert item["payload"]["status"] == "cancelled"
    assert item["payload"]["symbol"] == "TXF"
    assert item["payload"]["price_type"] == "LMT"  # LOW-6：payload schema 帶 price_type


def test_order_report_event_payload_includes_price_type_for_unfilled_mkt_order(session, engine):
    """LOW-6（fresh-context 終審修復）：未成交的 MKT 委託，`order.price`／
    `avg_fill_price` 皆是 0/None，過去的 payload 沒有 price_type，前端只能看到裸的 "0"
    字串顯示成「@ 0」——與 CRITICAL-1（confirm_dialog.html）同病。payload 現在必須帶
    `price_type: "MKT"`，前端才能據此改顯示「市價」。"""
    _seed_order(session, price=Decimal("0"), price_type="MKT", order_type="IOC")
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="order_report", broker="shioaji", payload=json.dumps(dict(
            broker="shioaji", account="F1", mode="sim", ordno="O1", broker_order_id="B1", status="submitted",
        )))
        s.commit()

    hub = OrderEventHub()
    worker = _worker(engine, order_events=hub)
    queue = hub.subscribe(user_id=1)
    assert worker.process_batch_once() == 1
    worker._flush_pending_events()

    item = queue.get_nowait()
    assert item["event"] == "order-report"
    assert item["payload"]["price_type"] == "MKT"
    assert item["payload"]["price"] == "0"  # 欄位本身仍是 "0"——前端要靠 price_type 判斷顯示文字，不是靠這裡改值


def test_replayed_deal_report_does_not_republish_events(session, engine):
    """冪等重播（watchdog 對帳補進同一筆 fill_id）只補 processed，不重跑帳務——同理也不得
    重複發送成交/委託回報事件，否則同一筆成交會被展示兩次，誤導使用者。"""
    _seed_order(session)
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()

    hub = OrderEventHub()
    worker = _worker(engine, order_events=hub)
    queue = hub.subscribe(user_id=1)
    assert worker.process_batch_once() == 1
    worker._flush_pending_events()
    assert not queue.empty()  # 第一次落地：有事件
    while not queue.empty():
        queue.get_nowait()

    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()
    assert worker.process_batch_once() == 1  # 仍算「處理完」（mark_raw_inbox_processed）
    worker._flush_pending_events()
    assert queue.empty()  # 但重播冪等路徑不再重發任何事件


def test_unresolvable_order_correlation_quarantines_not_dropped(session, engine):
    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload(ordno="GHOST", broker_order_id="GHOST-B")),
        )
        s.commit()

    worker = _worker(engine)
    handled = worker.process_batch_once()
    assert handled == 1  # quarantine 也算「確定處理完」

    with Session(engine) as s:
        assert s.exec(select(Deal)).first() is None  # 沒有部分寫入
        row = s.exec(select(RawInbox)).first()
        assert row.quarantine is True and row.processed is False


def test_quarantine_emits_ops_alert_with_row_id_kind_and_error(session, engine):
    """T0.3：走進 quarantine 分支時通知 OpsAlerter（row_id/kind/error）。"""
    alerter = _RecordingAlerter()
    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload(ordno="GHOST", broker_order_id="GHOST-B")),
        )
        s.commit()
        row_id = s.exec(select(RawInbox)).first().id

    worker = _worker(engine, ops_alerter=alerter)
    worker.process_batch_once()
    assert len(alerter.quarantine_calls) == 1
    call = alerter.quarantine_calls[0]
    assert call["row_id"] == row_id and call["kind"] == "deal_report"
    assert "無法解析委託關聯" in call["error"]


def test_normal_processing_does_not_emit_quarantine_alert(session, engine):
    alerter = _RecordingAlerter()
    _seed_order(session)
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()

    worker = _worker(engine, ops_alerter=alerter)
    worker.process_batch_once()
    assert alerter.quarantine_calls == []  # 正常落地不發告警


def test_deal_report_octype_comes_from_resolved_order_not_mapper_payload(session, engine):
    """成交回報（真實 FuturesDealEvent）本身沒有 octype 欄位；_process_deal 必須用解析到的
    對應 Order 的 octype 覆蓋 mapper 回傳的任何占位值，不是照單全收 mapper 給的值
    （見 broker/shioaji_adapter.py::_map_deal_report 的 "Auto" 占位說明）。"""
    _seed_order(session, octype="Cover", action="Sell")  # 對應委託是平倉

    def _mapper_with_wrong_octype_placeholder(payload: dict, *, account: str | None = None) -> Fill:
        return Fill(
            broker=payload["broker"], fill_id=payload["fill_id"], ordno=payload["ordno"],
            broker_order_id=payload["broker_order_id"], symbol=payload["symbol"],
            action=payload["action"], price=Decimal(payload["price"]), qty=int(payload["qty"]),
            fee=Decimal(payload["fee"]), octype="Auto", ts=payload["ts"],
            account=payload["account"], mode=payload["mode"], user_id=None,
        )

    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji",
                              payload=json.dumps(_deal_payload(action="Sell")))
        s.commit()

    worker = _worker(engine, deal_mapper=_mapper_with_wrong_octype_placeholder)
    handled = worker.process_batch_once()
    assert handled == 1

    with Session(engine) as s:
        # 沒有預先開倉的部位：若 octype 真的被覆蓋成 Cover（對應 Order 的值），Cover 分支
        # 會因為找不到開倉部位而 quarantine；若沒被覆蓋、停留在 mapper 給的占位 "Auto"，
        # Auto 分支會直接成功開一個新的 short 部位（不 quarantine）——用這個反差證明覆蓋真的
        # 發生。
        row = s.exec(select(RawInbox)).first()
        assert row.quarantine is True and row.processed is False
        assert s.exec(select(BrokerPosition)).first() is None


def test_deal_report_symbol_comes_from_resolved_order_not_specific_contract_code(session, engine):
    """部位顯示 bug 回歸：真實 FuturesDealEvent 的 `code` 欄位是「具體月合約代碼」
    （如 "TXFH6"），不是我方全域慣用的「通用商品代碼」（如 "TXF"，見 config.Settings.symbol/
    ShioajiAdapter.symbol，下單表單、`positions()` 查詢一律用這個通用代碼）。若成交回報直接
    照抄 mapper 給的 `payload["code"]` 當 BrokerPosition.symbol，會跟 `list_open_positions`
    用 `symbol=self.symbol="TXF"` 查詢的過濾條件對不起來——部位明明已入帳，`positions()`
    卻永遠查不到（使用者看到的現象是「部位沒顯示」）。同 octype 的既有覆蓋慣例：symbol 一律
    以解析到的 Order.symbol（通用代碼）為準，不信任 mapper 給的具體合約代碼。"""
    _seed_order(session, symbol="TXF")  # 下單當下存的是通用代碼
    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload(symbol="TXFH6")),  # 成交回報帶的是具體月合約代碼
        )
        s.commit()

    worker = _worker(engine)
    handled = worker.process_batch_once()
    assert handled == 1

    with Session(engine) as s:
        pos = s.exec(select(BrokerPosition)).first()
        assert pos is not None
        assert pos.symbol == "TXF"  # 不是 mapper 給的 "TXFH6"

        # 與 ShioajiAdapter.positions() 實際查詢條件一致：symbol=self.symbol（通用代碼）。
        found = brepo.list_open_positions(
            s, user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF",
        )
        assert len(found) == 1


def test_position_mismatch_quarantines_without_partial_deal_write(session, engine):
    """Cover 缺對應開倉部位 → PositionMismatchError → quarantine，且已 stage 的 Deal 也要 rollback 掉。"""
    _seed_order(session, octype="Cover", action="Sell")
    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload(octype="Cover", action="Sell")),
        )
        s.commit()

    worker = _worker(engine)
    handled = worker.process_batch_once()
    assert handled == 1

    with Session(engine) as s:
        assert s.exec(select(Deal)).first() is None  # stage_deal 的 flush 被 rollback 掉，沒有殘留
        row = s.exec(select(RawInbox)).first()
        assert row.quarantine is True


# ---- Inc1 D5/R2-6：quarantine 分級（scope_violation/payload_mismatch/user_mismatch 永久
#      dead-letter；association_pending 才可重試）＋唯一 scoped staging API ----

def test_dead_letter_error_from_mapper_marks_permanent_quarantine_with_reason(session, engine):
    """R2-6：mapper 拋 RawInboxDeadLetterError（如 payload_mismatch）必須標
    processed=True＋quarantine=True＋對應 reason——不是既有 ValueError 的
    association_pending（可重試）語意。"""
    def _mismatch_mapper(payload: dict, *, account=None) -> Fill:
        raise RawInboxDeadLetterError("payload_mismatch", "payload.account_id 與列蓋章不符")

    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()

    worker = _worker(engine, deal_mapper=_mismatch_mapper)
    handled = worker.process_batch_once()
    assert handled == 1  # dead-letter 也算「確定處理完」

    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.quarantine is True
        assert row.processed is True  # R2-6：永久 dead-letter，退出換帳號 guard 的 unprocessed 計數
        assert row.quarantine_reason == "payload_mismatch"
        assert s.exec(select(Deal)).first() is None  # 沒有部分寫入


def test_dead_letter_rows_are_never_retried_by_unquarantine_stale(session, engine):
    """R2-6：association_pending 才進 unquarantine 重試迴圈；dead-letter 永不被解除。"""
    def _mismatch_mapper(payload: dict, *, account=None) -> Fill:
        raise RawInboxDeadLetterError("payload_mismatch", "payload.account_id 與列蓋章不符")

    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()
    worker = _worker(engine, deal_mapper=_mismatch_mapper)
    worker.process_batch_once()

    import datetime as dt
    with Session(engine) as s:
        released = brepo.unquarantine_stale_raw_inbox(
            s, older_than=dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) + dt.timedelta(seconds=1)
        )
        assert released == 0
        s.commit()
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.quarantine is True and row.processed is True


def test_deal_report_user_mismatch_dead_letters_no_partial_write(session, engine):
    """R2-2：row.user_id 非 None（agent 模式蓋章列）且與解析到的 Order.user_id 不符 →
    user_mismatch 永久 dead-letter，不留部分寫入（Deal/BrokerPosition 都不得被寫入）。"""
    _seed_order(session)  # user_id=1（_order_kwargs 預設）
    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()), user_id=999,
        )
        s.commit()

    worker = _worker(engine)
    handled = worker.process_batch_once()
    assert handled == 1

    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.quarantine is True and row.processed is True
        assert row.quarantine_reason == "user_mismatch"
        assert s.exec(select(Deal)).first() is None
        assert s.exec(select(BrokerPosition)).first() is None


def test_order_report_user_mismatch_dead_letters_status_untouched(session, engine):
    _seed_order(session)  # user_id=1
    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="order_report", broker="shioaji", payload=json.dumps(dict(
                broker="shioaji", account="F1", mode="sim", ordno="O1", broker_order_id="B1", status="cancelled",
            )),
            user_id=999,
        )
        s.commit()
    worker = _worker(engine)
    handled = worker.process_batch_once()
    assert handled == 1
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.quarantine is True and row.processed is True
        assert row.quarantine_reason == "user_mismatch"
        order = s.exec(select(Order)).first()
        assert order.status == "submitted"  # 沒被亂改


def test_commit_raw_callback_no_binding_yet_permits_staging(engine):
    """R1-5：查無 `agent_account_bindings` 列 → 先放行，不誤擋（fail-open 預設）。Task 6
    落地後，正常 WS 流程下 UpLogin 必先 `bind_account` 才會放行登入——已登入連線送出的
    UpReport 理論上不會再命中這個分支（見 `inbox_worker._validate_report_scope` 的收口
    說明）。這裡直接呼叫 `commit_raw_callback` 繞過 WS/login，單獨驗證這個 fail-open 預設
    本身仍然正確（防禦性退路，涵蓋舊資料庫/未跑 backfill 等情境）。"""
    commit_raw_callback(
        lambda: Session(engine), kind="deal_report", broker="shioaji", payload={"trade_id": "D1"},
        user_id=2, account="F1", mode="sim",
    )
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row is not None
        assert row.quarantine is False and row.processed is False
        assert row.user_id == 2 and row.account == "F1" and row.mode == "sim"


def test_commit_raw_callback_binding_match_permits_staging(engine):
    with Session(engine) as s:
        s.add(AgentAccountBinding(broker="shioaji", account="F1", user_id=2))
        s.commit()
    commit_raw_callback(
        lambda: Session(engine), kind="deal_report", broker="shioaji", payload={"trade_id": "D1"},
        user_id=2, account="F1", mode="sim",
    )
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.quarantine is False


def test_commit_raw_callback_binding_mismatch_dead_letters_scope_violation(engine):
    """R1-5：偽造 scope——UpReport 帶的 account 已綁定給別的 user → scope_violation 永久
    dead-letter，仍然落地（保留稽核證據，不是丟棄）。"""
    with Session(engine) as s:
        s.add(AgentAccountBinding(broker="shioaji", account="F1", user_id=1))
        s.commit()

    commit_raw_callback(
        lambda: Session(engine), kind="deal_report", broker="shioaji", payload={"trade_id": "D1"},
        user_id=2, account="F1", mode="sim",  # user 2 冒用綁定給 user 1 的帳號
    )

    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row is not None
        assert row.quarantine is True and row.processed is True
        assert row.quarantine_reason == "scope_violation"
        assert row.user_id == 2 and row.account == "F1"  # 蓋章仍是連線認證的 user，供稽核


def test_commit_raw_callback_in_process_user_id_none_never_checks_binding(engine):
    """in-process 呼叫端一律 user_id=None——即使綁定表存在矛盾列，也不觸發 scope 檢查
    （in-process 零變更，這個檢查只在 user_id 非 None 時才跑）。"""
    with Session(engine) as s:
        s.add(AgentAccountBinding(broker="shioaji", account="F1", user_id=999))
        s.commit()
    commit_raw_callback(
        lambda: Session(engine), kind="deal_report", broker="shioaji", payload={"trade_id": "D1"},
        user_id=None, account="F1", mode="sim",
    )
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.quarantine is False and row.user_id is None


def test_commit_raw_callback_scope_violation_alerts_ops_once(engine):
    class _Recording:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        def quarantine(self, **kw) -> None:
            self.calls.append(kw)

    with Session(engine) as s:
        s.add(AgentAccountBinding(broker="shioaji", account="F1", user_id=1))
        s.commit()

    alerter = _Recording()
    commit_raw_callback(
        lambda: Session(engine), kind="deal_report", broker="shioaji", payload={"trade_id": "D1"},
        user_id=2, account="F1", mode="sim", ops_alerter=alerter,
    )
    assert len(alerter.calls) == 1
    assert alerter.calls[0]["kind"] == "deal_report"


# ---------------------------------------------------------------------------
# 事件喚醒（RawInboxWorker request_wake）：commit_raw_callback 的 on_committed hook
# ---------------------------------------------------------------------------

def test_commit_raw_callback_invokes_on_committed_after_commit_succeeds(engine):
    """喚醒只能發生在落地 commit 成功之後——`on_committed` 必須在 `session.commit()`
    真正跑完、資料已可查得之後才被呼叫（best-effort，用來取代 idle_interval 純逾時輪詢）。"""
    calls: list[int] = []

    def _on_committed() -> None:
        with Session(engine) as s:
            # 呼叫當下必須已經看得到剛落地的列——證明 hook 在 commit 之後才觸發。
            calls.append(len(s.exec(select(RawInbox)).all()))

    commit_raw_callback(
        lambda: Session(engine), kind="deal_report", broker="shioaji", payload={"trade_id": "D1"},
        user_id=None, account=None, mode=None, on_committed=_on_committed,
    )
    assert calls == [1]


def test_commit_raw_callback_on_committed_default_none_is_noop(engine):
    """未接線（預設 None）維持現行行為完全不變——不傳 on_committed 不得出錯。"""
    commit_raw_callback(
        lambda: Session(engine), kind="deal_report", broker="shioaji", payload={"trade_id": "D1"},
        user_id=None, account=None, mode=None,
    )
    with Session(engine) as s:
        assert s.exec(select(RawInbox)).first() is not None


def test_commit_raw_callback_on_committed_exception_swallowed_does_not_lose_commit(engine):
    """喚醒是 best-effort：hook 炸掉絕不能反噬已經成功的落地（durable 保證不受影響）。"""
    def _boom() -> None:
        raise RuntimeError("wake hook 炸了")

    commit_raw_callback(
        lambda: Session(engine), kind="deal_report", broker="shioaji", payload={"trade_id": "D1"},
        user_id=None, account=None, mode=None, on_committed=_boom,
    )  # 不得往外 raise
    with Session(engine) as s:
        assert s.exec(select(RawInbox)).first() is not None  # 落地不受影響


def test_unexpected_exception_leaves_row_pending_not_quarantined_zero_loss(session, engine):
    """V3-2 核心回歸：非預期例外（非 ValueError/PositionMismatchError）不得 quarantine，
    必須留 processed=False 讓下一輪重試——這就是『重啟後 raw-inbox 仍在，最終恰一次 effect』。"""
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()

    calls = {"n": 0}

    def _flaky_mapper(payload: dict, *, account: str | None = None) -> Fill:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("模擬暫時性故障（非業務邏輯錯誤）")
        return _ok_deal_mapper(payload, account=account)

    worker = _worker(engine, deal_mapper=_flaky_mapper)
    _seed_order(session)

    first = worker.process_batch_once()
    assert first == 0  # 未預期例外不算「確定處理完」
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.processed is False and row.quarantine is False  # 沒丟、也沒被誤判為壞資料

    second = worker.process_batch_once()
    assert second == 1  # 下一輪（mapper 這次成功）恰好處理一次
    with Session(engine) as s:
        assert s.exec(select(Deal)).first() is not None


def test_replayed_deal_report_is_idempotent_marks_processed_without_reapplying(session, engine):
    """watchdog 對帳把已經處理過的 fill 又補進 raw-inbox（新 RawInbox 列、同 fill_id）→
    stage_deal 撞唯一鍵回 None → 只補 processed，不重跑部位帳務。"""
    _seed_order(session)
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()
    worker = _worker(engine)
    assert worker.process_batch_once() == 1

    with Session(engine) as s:  # 模擬 watchdog 對帳又補了一筆同 fill_id 的 raw-inbox
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()
    assert worker.process_batch_once() == 1

    with Session(engine) as s:
        pos = s.exec(select(BrokerPosition)).first()
        assert pos.total_opened_qty == 1  # 沒有被重複套用成 2
        rows = list(s.exec(select(RawInbox)))
        assert all(r.processed for r in rows)


def test_order_report_updates_status_via_composite_scope(session, engine):
    _seed_order(session)
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="order_report", broker="shioaji", payload=json.dumps(dict(
            broker="shioaji", account="F1", mode="sim", ordno="O1", broker_order_id="B1", status="cancelled",
        )))
        s.commit()
    worker = _worker(engine)
    assert worker.process_batch_once() == 1
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "cancelled" and order.filled_qty == 0


def _agent_command_row(*, cmd_id, kind, ordno, client_order_id=None, reservation_id=None,
                        price="18500", qty=5):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    payload = {"price": price, "qty": qty, "price_type": "LMT"} if kind == "update" else {}
    return AgentCommand(
        cmd_id=cmd_id, user_id=1, kind=kind, broker="shioaji", account="F1", mode="sim",
        ordno=ordno, client_order_id=client_order_id, reservation_id=reservation_id,
        payload=json.dumps(payload), created_at=now, expires_at=now + timedelta(minutes=2),
    )


def test_order_report_terminal_status_resolves_agent_commands_same_transaction(session, engine):
    """S#37 worker 路徑變體（Task 11 修復回合 1，發現 2）：agent 模式下，U1（update，
    outcome='unknown'）與一筆尚未 ack 的 cancel 都掛在同一張委託上——這筆 order_report 把
    Order 推進 cancelled 終態的**同一次 worker 處理**內，U1／cancel 立即收斂（終態 resolver
    保守 confirm／report 分支），不必等 watchdog 下一個週期（預設 300s）。"""
    _seed_order(session)  # C1: ordno=O1, broker_order_id=B1, user_id=1, status=submitted
    with Session(engine) as s:
        assert brepo.reserve_quota(s, reservation_id="delta-1", user_id=1, mode="sim",
                                   trading_day="2026-06-16", qty=3, daily_limit=20)
        insert_command(s, cmd=_agent_command_row(
            cmd_id="cmd-u1", kind="update", ordno="O1", client_order_id="C1",
            reservation_id="delta-1",
        ))
        insert_command(s, cmd=_agent_command_row(cmd_id="cmd-cancel", kind="cancel", ordno="O1"))
        s.commit()

    # U1 timeout ack 先落地（outcome='unknown', resolved_at IS NULL）——進入適用集合；
    # cancel cmd 保持 created/sent（從未 ack），同 watchdog 版既有測試手法。
    outcome = apply_command_ack(
        lambda: Session(engine), cmd_id="cmd-u1", user_id=1,
        ack=UpCmdAck(cmd_id="cmd-u1", event_id=1, ok=False, error_kind="timeout", message="t/o"),
    )
    assert outcome.resolved is False and outcome.outcome == "unknown"

    with Session(engine) as s:
        # agent 模式蓋章列：user_id 非 None（同 agent_ws UpReport handler 的既有落地慣例）。
        brepo.stage_raw_inbox(s, kind="order_report", broker="shioaji", payload=json.dumps(dict(
            broker="shioaji", account="F1", mode="sim", ordno="O1", broker_order_id="B1", status="cancelled",
        )), user_id=1, account="F1", mode="sim")
        s.commit()

    worker = _worker(engine, user_id=1)
    assert worker.process_batch_once() == 1  # 單一交易內完成：mark_order_status + 兩筆 resolver 收斂

    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "cancelled"

        u1 = s.get(AgentCommand, "cmd-u1")
        assert u1.resolved_at is not None
        assert u1.outcome == "unknown" and u1.resolved_via == "report"  # 終態保守 confirm 分支

        cancel = s.get(AgentCommand, "cmd-cancel")
        assert cancel.resolved_at is not None
        assert cancel.outcome == "unknown" and cancel.resolved_via == "report"

        assert s.exec(select(QuotaReservation).where(
            QuotaReservation.reservation_id == "delta-1")).one().state == "confirmed"  # 保守 confirm，不 release
        # guard 解除：U1/cancel 都已 resolved，換帳號不再被它們擋。
        assert not brepo.has_unresolved_risky_commands_other_account(s, user_id=1, account="OTHER")


def test_in_process_order_report_does_not_touch_agent_command_ledger(session, engine):
    """Task 11 修復回合 1（發現 2）in-process 迴歸：純 in-process 回報（`row.user_id is
    None`）把 Order 推進終態時，即使剛好有一筆未 resolved 的 AgentCommand 掛在同一個 ordno
    上（理論上不該同時發生，純屬防禦性驗證），worker 也絕不觸碰它——新掛載點只在
    `row.user_id is not None` 才啟動，in-process 原路徑逐位元不變。"""
    _seed_order(session)  # C1: ordno=O1, broker_order_id=B1, user_id=1（_order_kwargs 預設）
    with Session(engine) as s:
        insert_command(s, cmd=_agent_command_row(cmd_id="cmd-cancel", kind="cancel", ordno="O1"))
        s.commit()

    with Session(engine) as s:
        # in-process 呼叫端一律 user_id=None（同 `_process_deal`/既有測試慣例，
        # 見 `stage_raw_inbox` 預設值）。
        brepo.stage_raw_inbox(s, kind="order_report", broker="shioaji", payload=json.dumps(dict(
            broker="shioaji", account="F1", mode="sim", ordno="O1", broker_order_id="B1", status="cancelled",
        )))
        s.commit()

    worker = _worker(engine)  # user_id=None（in-process 單一 worker，既有慣例）
    assert worker.process_batch_once() == 1

    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "cancelled"  # Order 本身照常推進終態，不受影響
        cmd = s.get(AgentCommand, "cmd-cancel")
        assert cmd.resolved_at is None  # 完全沒被碰——新掛載點沒有啟動


def test_order_report_conflicting_dual_keys_quarantines_fail_closed(session, engine):
    """殘留2（round3 獨立驗收）：deal_report 路徑經 PositionTracker._resolve_order 早已對
    ordno/broker_order_id 雙鍵各自 scoped 查詢、矛盾即 quarantine（fail-closed）；
    order_report 路徑先前只有 ordno 命中即用、broker_order_id 淪為 fallback，完全沒驗第二鍵
    ——payload 若 ordno 指向某張 Order、broker_order_id 卻指向另一張，舊行為會直接任選
    ordno 命中的那張改狀態。比照 _resolve_order 補上交叉驗證：矛盾必須 quarantine，
    兩張 Order 的 status 都不得被亂改。"""
    _seed_order(session)  # C1: ordno=O1, broker_order_id=B1
    with Session(engine) as s:
        other = brepo.create_order(s, **_order_kwargs(client_order_id="C2", request_hash="H2"))
        brepo.set_order_ack(s, other.id, broker_order_id="B2", ordno="O2", status="submitted")
        s.commit()

    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="order_report", broker="shioaji", payload=json.dumps(dict(
            broker="shioaji", account="F1", mode="sim", ordno="O1", broker_order_id="B2", status="cancelled",
        )))  # ordno 指向 C1，broker_order_id 指向 C2 —— 矛盾
        s.commit()

    worker = _worker(engine)
    handled = worker.process_batch_once()
    assert handled == 1  # quarantine 也算「確定處理完」

    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.quarantine is True and row.processed is False
        order_c1 = brepo.find_order_by_client_order_id(s, "C1")
        order_c2 = brepo.find_order_by_client_order_id(s, "C2")
        assert order_c1.status == "submitted"  # 沒被亂改
        assert order_c2.status == "submitted"  # 沒被亂改


def test_order_report_resolves_via_ordno_only_when_broker_order_id_missing(session, engine):
    """殘留2 對照組：只帶單鍵（broker_order_id 缺）仍正常處理，不因加了交叉驗證而退化。"""
    _seed_order(session)
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="order_report", broker="shioaji", payload=json.dumps(dict(
            broker="shioaji", account="F1", mode="sim", ordno="O1", broker_order_id=None, status="cancelled",
        )))
        s.commit()
    worker = _worker(engine)
    assert worker.process_batch_once() == 1
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "cancelled"


def test_order_report_resolves_via_broker_order_id_only_when_ordno_missing(session, engine):
    """殘留2 對照組：只帶單鍵（ordno 缺）仍正常處理，不因加了交叉驗證而退化。"""
    _seed_order(session)
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="order_report", broker="shioaji", payload=json.dumps(dict(
            broker="shioaji", account="F1", mode="sim", ordno=None, broker_order_id="B1", status="cancelled",
        )))
        s.commit()
    worker = _worker(engine)
    assert worker.process_batch_once() == 1
    with Session(engine) as s:
        order = s.exec(select(Order)).first()
        assert order.status == "cancelled"


def test_callback_before_order_context_then_retry_resolves_ownership(session, engine):
    """round3 BLOCKER#2 情境：fill 先於 order context 建立就到（callback-before-ack）。
    先 stage raw-inbox（此時還沒有任何 Order），worker drain 一次 → 無法解析委託關聯 → quarantine。
    之後 order context 建好，若靠 unquarantine_stale_raw_inbox 解除隔離重試，就能歸屬成功——
    證明「先 quarantine 不等於永久丟單」。"""
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()

    worker = _worker(engine)
    assert worker.process_batch_once() == 1
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.quarantine is True and row.processed is False
        assert s.exec(select(Deal)).first() is None  # 沒有部分寫入

    # order context 現在才建好（模擬 ack 較晚到）
    _seed_order(session)

    import datetime as dt
    with Session(engine) as s:
        released = brepo.unquarantine_stale_raw_inbox(
            s, older_than=dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) + dt.timedelta(seconds=1)
        )
        assert released == 1
        s.commit()

    assert worker.process_batch_once() == 1
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.processed is True and row.quarantine is False
        assert s.exec(select(Deal)).first() is not None
        pos = s.exec(select(BrokerPosition)).first()
        assert pos is not None and pos.total_opened_qty == 1


def test_commit_raw_callback_lands_durably_before_returning(engine):
    """round3 BLOCKER#2 核心：callback thread 呼叫的同步落地函式，函式返回後 payload 已經是
    committed DB 狀態（獨立 Session、真正 commit，不是排程一個協程晚點才寫）——即使呼叫端
    之後 crash、從未啟動任何 worker，raw payload 也已經安全落地，不會遺失。"""
    payload = _deal_payload()
    commit_raw_callback(
        lambda: Session(engine), kind="deal_report", broker="shioaji", payload=payload,
        user_id=None, account=None, mode=None,
    )

    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row is not None
        assert row.processed is False and row.quarantine is False
        assert json.loads(row.payload) == payload


def test_supervisor_lock_serializes_two_concurrent_batches_no_interleaving(engine):
    """V3-2：live fill 與 watchdog retry 同時發生不並發改同一 BrokerPosition——
    用一個共享計數器證明持鎖期間永遠只有一個呼叫者在跑。"""
    supervisor = BrokerSupervisor()
    concurrent = {"n": 0, "max": 0}

    async def _guarded_section():
        async with supervisor.lock:
            concurrent["n"] += 1
            concurrent["max"] = max(concurrent["max"], concurrent["n"])
            await asyncio.sleep(0.02)
            concurrent["n"] -= 1

    async def scenario():
        await asyncio.gather(*[_guarded_section() for _ in range(5)])

    asyncio.run(scenario())
    assert concurrent["max"] == 1  # 任何時刻最多一個持鎖者，證明真正序列化


def test_stop_and_drain_waits_for_in_flight_batch(engine):
    supervisor = BrokerSupervisor()
    worker = _worker(engine, supervisor=supervisor)

    async def scenario():
        async with supervisor.lock:
            drain_task = asyncio.create_task(worker.stop_and_drain(timeout=1.0))
            await asyncio.sleep(0.05)
            assert not drain_task.done()  # 鎖被佔用時 drain 應該還在等
        await drain_task
        assert drain_task.done()
        return drain_task.result()

    assert asyncio.run(scenario()) is True  # 鎖釋放後真正 drain 完成 → True


def test_stop_and_drain_returns_false_on_timeout_not_claiming_success(engine):
    """round3 #17：逾時必須回 False，不得讓呼叫端誤以為背景工作已經真的停下來。"""
    supervisor = BrokerSupervisor()
    worker = _worker(engine, supervisor=supervisor)

    async def scenario():
        async with supervisor.lock:
            return await worker.stop_and_drain(timeout=0.05)  # 鎖全程被佔用 → 必逾時

    assert asyncio.run(scenario()) is False


def test_worker_run_processes_pending_row_then_idles(engine):
    """`run()` 走真正的 async 迴圈：拿鎖 → to_thread 跑 process_batch_once → 處理完停止旗標生效即結束。
    驗證迴圈確實會呼叫 process_batch_once 並套用 supervisor 序列化，而非只是裝飾用的殼。

    D（flaky 修復，fix/flaky-timing-tests）：原本用固定 `for _ in range(200): sleep(0.01)`
    輪詢——每輪都另開一個 `Session(engine)`，而 `engine` fixture 是 `StaticPool`＋
    `check_same_thread=False`（單一實體連線共用），這個輪詢會跟 worker 背景執行緒
    （`run()` 內 `asyncio.to_thread(process_batch_once)`）真正併發碰同一條 SQLite
    連線——真正根因見 `test_request_wake_from_separate_thread_without_event_loop_is_safe_
    and_effective` 的 docstring（CPU 壓力下 30 次 14 敗，`StaleDataError`）。改訂閱
    `OrderEventHub` 的廣播 ping（`run()` 迴圈 handled>0 時只在 event loop 執行緒發布，見
    `inbox_worker.py::run`）取代輪詢——等待期間完全不對這個共享連線開新 Session，條件
    本身不變（仍是「真的處理完」才算數，最後仍用一次性 Session 讀回 `row.processed`
    覆核）。

    第二輪修復（用 `debug_inbox_hang.py` 加 trace log 逐行定位才抓到）：第一版在
    `_wait_until_loop_captured(worker)` 之後才 `hub.subscribe()`——但這筆列在
    `worker.run()` 啟動前就已經落地，本機 8 核心跑滿 8 個 `yes` 的極端排程延遲下，
    `run()` 第一輪 `to_thread(process_batch_once)` 有時會在我們的輪詢迴圈拿回 CPU
    之前就整批做完並呼叫過 `hub.publish()`（trace 證實：`hub.publish: CALLED` 早於
    `loop captured: True` 印出）——等我們才 `subscribe()`，那唯一一次事件已經錯過，
    之後沒有更多列可處理，`queue.get()` 永遠不會有第二次 publish，只會在逾時上限
    原地掛滿（不管上限拉多高都一樣：10 秒 15 次 4 敗、30 秒 20 次 1 敗、60 秒 20 次
    2 敗，全部卡在同一行——不是「太慢」，是「已經錯過」，加大逾時無法修正）。真正
    修法：`subscribe()` 移到 `create_task(worker.run())` 之前——訂閱先於任何可能的
    處理，結構上不可能再錯過這唯一一次 publish；比照
    `test_request_wake_from_separate_thread_without_event_loop_is_safe_and_effective`
    的安全寫法（該測試把落地動作擺在 `subscribe()` 之後，天然沒有這個窗口）。上限
    改回本檔其餘測試沿用的 10 秒——race 消除後不再需要用加大逾時去換餘裕。"""
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()
    with Session(engine) as s:
        _seed_order(s)

    hub = OrderEventHub()
    worker = _worker(engine, order_events=hub)
    queue = hub.subscribe()  # 訂閱必須先於 create_task——見上方 docstring 的競態說明

    async def scenario():
        run_task = asyncio.create_task(worker.run())
        await asyncio.wait_for(queue.get(), timeout=10.0)
        await worker.stop_and_drain(timeout=1.0)
        await run_task

    asyncio.run(scenario())
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.processed is True


# ---------------------------------------------------------------------------
# 事件喚醒（RawInboxWorker.request_wake）：把 run() 從 idle_interval 純逾時輪詢喚醒
# ---------------------------------------------------------------------------

async def _wait_until_loop_captured(worker: RawInboxWorker, timeout: float = 2.0) -> None:
    """`run()` 進迴圈第一件事就是 `self._loop = asyncio.get_running_loop()`——等這個
    發生，確保接下來呼叫 `request_wake()` 時 worker 真的已經在跑（不是巧合式
    `asyncio.sleep(0)` 賭排程時機）。"""
    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        if worker._loop is not None:
            return
        await asyncio.sleep(0.005)
    raise AssertionError("worker.run() 逾時仍未捕捉到 event loop")


def test_request_wake_processes_new_row_faster_than_idle_interval(engine):
    """驗收條件1：事件喚醒快於逾時。worker 以刻意拉長的 idle_interval（5s）啟動；commit
    一筆新列後呼叫 request_wake()，必須在遠小於 idle_interval 的時間內（<1s 的 asyncio
    等待，不使用真 sleep 硬等 5s）被處理，且 hub 收到一次 publish。"""
    with Session(engine) as s:
        _seed_order(s)
    hub = OrderEventHub()
    worker = _worker(engine, idle_interval=5.0, order_events=hub)

    async def scenario():
        run_task = asyncio.create_task(worker.run())
        await _wait_until_loop_captured(worker)
        queue = hub.subscribe()

        with Session(engine) as s:
            brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji",
                                   payload=json.dumps(_deal_payload()))
            s.commit()
        worker.request_wake()

        # 遠小於 idle_interval=5s 的上限——事件喚醒若沒生效，這裡會逾時失敗，
        # 證明喚醒確實比純逾時輪詢快。
        await asyncio.wait_for(queue.get(), timeout=1.0)

        await worker.stop_and_drain(timeout=1.0)
        await run_task

    asyncio.run(scenario())
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.processed is True


def test_deal_landing_scoped_event_coexists_with_unaffected_broadcast_ping(engine):
    """④：既有 `orders-changed` 廣播 ping（`subscribe()` 無參數、`run()` 迴圈 handled>0
    才 `publish()`）在新增 007 的 scoped 事件之後行為完全不變——同一批落地同時觸發兩種
    訊號，各自送到各自的訂閱者，互不干擾、互不取代。"""
    with Session(engine) as s:
        _seed_order(s)  # user_id=1
    hub = OrderEventHub()
    worker = _worker(engine, order_events=hub)

    async def scenario():
        run_task = asyncio.create_task(worker.run())
        await _wait_until_loop_captured(worker)
        broadcast_queue = hub.subscribe()       # 既有無資料廣播訂閱者（如另一個分頁）
        owner_queue = hub.subscribe(user_id=1)  # 這筆委託的當事人

        with Session(engine) as s:
            brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji",
                                   payload=json.dumps(_deal_payload()))
            s.commit()
        worker.request_wake()

        broadcast_item = await asyncio.wait_for(broadcast_queue.get(), timeout=1.0)
        owner_item = await asyncio.wait_for(owner_queue.get(), timeout=1.0)

        await worker.stop_and_drain(timeout=1.0)
        await run_task
        return broadcast_item, owner_item

    # debug=True：與 CRITICAL-1 的重現/回歸測試同規格（見下一個測試），這裡也順便驗證
    # ping 與 scoped 事件並存不會觸發 asyncio 的跨執行緒不變式檢查。
    broadcast_item, owner_item = asyncio.run(scenario(), debug=True)
    assert broadcast_item == "1"  # 既有 orders-changed ping 語意逐位元不變
    assert owner_item["event"] in ("deal", "order-report")  # scoped 事件也照常送達，互不排擠


def test_deal_landing_publish_runs_on_loop_thread_safe_under_asyncio_debug_mode(engine, caplog):
    """CRITICAL-1（fresh-context opus 終審修復）：`process_batch_once()` 整批在
    `asyncio.to_thread()` 裡執行（worker thread，見 `run()`）；`_process_deal`/
    `_process_order_report` 若直接呼叫 hub 的 `publish_deal`/`publish_order_report`
    （進而 `asyncio.Queue.put_nowait`），就是從非本執行緒操作 `asyncio.Queue`——違反
    thread-affinity 不變式。`loop.set_debug(True)` 下，若這個 queue 當下有人正在
    `await queue.get()`（本測試就是這樣：先掛上 `asyncio.wait_for(queue.get(), ...)`
    再觸發批次），非本執行緒的 `put_nowait` 會促使 asyncio 排程一個 callback 回原執行緒
    （`Future.set_result` → `loop.call_soon`），debug 模式下的執行緒檢查會直接
    RuntimeError；且這個例外原本會被 `process_batch_once` 逐列的 `except Exception`
    吞掉（該列不計 handled，SSE 靜默降級，不會讓 pytest 顯式失敗，只會逾時）。

    這裡真正走 `run()` 的 async 路徑（`asyncio.to_thread` 真實跨執行緒），在
    `asyncio.run(..., debug=True)` 下斷言：①不逾時、事件確實送達（不是被吞掉例外後
    卡住）；②這一列確實被算進「處理完」（`processed=True`，不是卡在 quarantine=False/
    processed=False 的半吊子狀態）；③沒有任何「處理時發生未預期例外」的 log（證明沒有
    例外被靜默吞掉——CRITICAL-1 修復前的確切失敗模式）。"""
    caplog.set_level(logging.ERROR, logger="quanquant.broker.inbox_worker")
    with Session(engine) as s:
        _seed_order(s)  # user_id=1
    hub = OrderEventHub()
    worker = _worker(engine, order_events=hub)

    async def scenario():
        run_task = asyncio.create_task(worker.run())
        await _wait_until_loop_captured(worker)
        queue = hub.subscribe(user_id=1)  # 掛上 waiter——重現條件（見 docstring）

        with Session(engine) as s:
            brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji",
                                   payload=json.dumps(_deal_payload()))
            s.commit()
        worker.request_wake()

        # 若 CRITICAL-1 重現（例外被吞、該列不計 handled），這裡會逾時而非拿到事件。
        item = await asyncio.wait_for(queue.get(), timeout=1.0)

        await worker.stop_and_drain(timeout=1.0)
        await run_task
        return item

    item = asyncio.run(scenario(), debug=True)
    assert item["event"] in ("deal", "order-report")

    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.processed is True  # 這一批真的算「處理完」，不是被吞掉例外後卡住

    assert "處理時發生未預期例外" not in caplog.text  # 沒有例外被靜默吞掉


def test_request_wake_from_separate_thread_without_event_loop_is_safe_and_effective(engine):
    """驗收條件2（有效性那一半）：從另一個沒有 event loop 的執行緒（比照 Shioaji SDK
    callback thread）呼叫 request_wake 必須安全、且真的把 worker 喚醒——用
    `threading.Thread`（非 asyncio 任何東西）呼叫，不能假設呼叫端在 loop 執行緒上。

    flaky 修復備忘（fix/flaky-timing-tests）：原本用另開 `Session(engine)` 輪詢
    `row.processed` 確認處理完——但 `engine` fixture 是 `StaticPool`＋
    `check_same_thread=False`（單一實體連線共用），這個輪詢會在主執行緒跟 worker
    背景執行緒（`run()` 內 `asyncio.to_thread(process_batch_once)`）真正併發碰同一條
    SQLite 連線；CPU 壓力下（本機 8 個 `yes` 壓力源重現：30 次 14 敗）幾乎每次失敗都是
    `mark_raw_inbox_processed`／`session.flush()` 炸出 `StaleDataError`（0 rows
    matched），把「還沒處理完」誤判成「處理失敗」——診斷版（拿掉併發輪詢改純
    `sleep`）失敗率從 47% 掉到 10%、且不再出現 StaleDataError，證實輪詢本身的併發
    存取才是主因，不是 `request_wake`/worker 的正式碼有 race。改訂閱
    `OrderEventHub` 的廣播 ping（`run()` 迴圈 handled>0 時只在 event loop 執行緒發布，
    見 `inbox_worker.py::run`）取代輪詢——等待期間完全不對這個共享連線開新
    Session；等待上限放寬到 10 秒只是讓 CPU 壓力下的合理處理延遲不被錯殺，條件本身
    不變（仍是「真的處理完」才算數，最後仍用一次性 Session 讀回 `row.processed`
    覆核）。"""
    with Session(engine) as s:
        _seed_order(s)
    hub = OrderEventHub()
    worker = _worker(engine, idle_interval=5.0, order_events=hub)

    async def scenario():
        run_task = asyncio.create_task(worker.run())
        await _wait_until_loop_captured(worker)
        queue = hub.subscribe()

        with Session(engine) as s:
            brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji",
                                   payload=json.dumps(_deal_payload()))
            s.commit()

        errors: list[BaseException] = []

        def _call_from_thread() -> None:
            try:
                worker.request_wake()
            except BaseException as exc:  # noqa: BLE001 — 就是要證明「絕不 raise」
                errors.append(exc)

        t = threading.Thread(target=_call_from_thread)
        t.start()
        t.join(timeout=10.0)

        try:
            assert not t.is_alive()
            assert errors == []
            await asyncio.wait_for(queue.get(), timeout=10.0)
        except asyncio.TimeoutError:
            raise AssertionError("跨執行緒 request_wake 逾時仍未喚醒 worker 處理新列") from None
        finally:
            # try/finally：即使上面逾時失敗也要把 worker 收乾淨，避免背景執行緒
            # 的 process_batch_once 帶著同一個 engine 連線繼續孤兒式跑，殘留輸出
            # 干擾到後面的測試（原本失敗路徑不會走到這兩行，是本次修復一併補上的）。
            await worker.stop_and_drain(timeout=1.0)
            await run_task

    asyncio.run(scenario())
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.processed is True


def test_request_wake_before_worker_started_is_quiet_noop(engine):
    """驗收條件2（安全性那一半）：worker 尚未啟動（`run()` 從未被排程過，`_loop` 恆為
    None）時呼叫 request_wake 必須安靜 no-op——不 raise（callback 執行緒炸掉會丟券商回報）。"""
    worker = _worker(engine)
    worker.request_wake()  # 不 raise 就是通過


def test_request_wake_after_worker_stopped_is_quiet_noop(engine):
    """worker 正常停止（`stop_and_drain` 完成、`run()` 已返回）後呼叫 request_wake 一樣要
    安靜 no-op，不得 raise——`run()` 的 finally 必須把 `_loop` 重新歸零。"""
    worker = _worker(engine)

    async def scenario():
        run_task = asyncio.create_task(worker.run())
        await _wait_until_loop_captured(worker)
        await worker.stop_and_drain(timeout=1.0)
        await run_task
        assert worker._loop is None
        worker.request_wake()  # 不 raise 就是通過

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# 批次內快速重試（association_pending 隔離列）：本批有列真正成功落地（非單純 quarantine）
# 時，對本 worker scope 內 reason=association_pending 的隔離列做一次 unquarantine→重新
# 處理，把原本得等 watchdog `_retry_quarantined`（預設 300s）的尾端延遲壓到與觸發批次
# 同一次或緊接下一次 process_batch_once。
# ---------------------------------------------------------------------------

def test_association_pending_deal_resolves_within_batch_when_order_ack_lands_later(session, engine):
    """核心情境：deal 比自己委託的 ack 早進 RawInbox → quarantine（association_pending）；
    委託的 ack 之後才落地（模擬 `ShioajiAdapter.place` 同步寫回 ordno 較晚完成的競態），這筆
    委託自己的 order_report 落地時觸發批次內快速重試——原本孤立的 deal 在緊接的下一次
    process_batch_once 呼叫內解隔離並成功落地（Fill/BrokerPosition 更新）。全程只呼叫
    process_batch_once，不呼叫 watchdog 任何函式、不 sleep、不等 300s/15s。"""
    # 1) deal 先到，此時對應委託尚未存在 → quarantine（association_pending）
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()

    worker = _worker(engine)
    first = worker.process_batch_once()
    assert first == 1  # quarantine 也算「確定處理完」；這批沒有任何東西真正成功落地

    with Session(engine) as s:
        deal_row = s.exec(select(RawInbox)).first()
        assert deal_row.quarantine is True and deal_row.quarantine_reason == "association_pending"
        assert s.exec(select(Deal)).first() is None  # 沒有部分寫入

    # 2) 委託的 ack 現在才落地（模擬同步下單路徑較晚完成 set_order_ack）
    _seed_order(session)  # ordno=O1, broker_order_id=B1，與 _deal_payload() 預設一致

    # 3) 這筆委託自己的 order_report（狀態回報）落地——本批唯一的新列，成功處理，
    #    觸發批次內快速重試。
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="order_report", broker="shioaji", payload=json.dumps(dict(
            broker="shioaji", account="F1", mode="sim", ordno="O1", broker_order_id="B1", status="cancelled",
        )))
        s.commit()

    second = worker.process_batch_once()
    # 1（order_report 落地）+ 1（deal 批次內快速重試,這次成功解隔離）
    assert second == 2

    with Session(engine) as s:
        deal_row = s.exec(select(RawInbox).where(RawInbox.kind == "deal_report")).first()
        assert deal_row.processed is True and deal_row.quarantine is False
        assert s.exec(select(Deal)).first() is not None
        pos = s.exec(select(BrokerPosition)).first()
        assert pos is not None and pos.total_opened_qty == 1


def test_batch_retry_never_touches_permanent_dead_letter_quarantine(session, engine):
    """永久 dead-letter reason（scope_violation/payload_mismatch/user_mismatch）的隔離列必須
    完全不被批次內快速重試碰到——即使本批有其他新列成功落地觸發重試檢查，這種列也要維持
    quarantine=True／reason 不變、mapper 不再被呼叫（沿用 watchdog
    `unquarantine_stale_raw_inbox` 既有的 reason 篩選，不是本分支另造規則）。"""
    calls = {"n": 0}

    def _selective_mismatch_mapper(payload: dict, *, account: str | None = None) -> Fill:
        calls["n"] += 1
        if payload["fill_id"] == "DEAD1":
            raise RawInboxDeadLetterError("payload_mismatch", "payload.account_id 與列蓋章不符")
        return _ok_deal_mapper(payload, account=account)

    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload(fill_id="DEAD1")),
        )
        s.commit()

    worker = _worker(engine, deal_mapper=_selective_mismatch_mapper)
    first = worker.process_batch_once()
    assert first == 1
    assert calls["n"] == 1

    with Session(engine) as s:
        dead_row = s.exec(select(RawInbox)).first()
        dead_row_id = dead_row.id
        assert dead_row.quarantine is True and dead_row.processed is True
        assert dead_row.quarantine_reason == "payload_mismatch"

    # 另一筆全新委託＋成交，與上面無關，會成功落地（landed>0，觸發批次內快速重試檢查）
    with Session(engine) as s:
        other = brepo.create_order(s, **_order_kwargs(client_order_id="C2", request_hash="H2"))
        brepo.set_order_ack(s, other.id, broker_order_id="B2", ordno="O2", status="submitted")
        s.commit()
    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload(fill_id="F2", ordno="O2", broker_order_id="B2")),
        )
        s.commit()

    second = worker.process_batch_once()
    assert second == 1  # 只有這筆全新委託落地；dead-letter 列完全沒被重試撿走
    assert calls["n"] == 2  # mapper 沒有為 DEAD1 再被呼叫第二次

    with Session(engine) as s:
        dead_row = s.get(RawInbox, dead_row_id)
        assert dead_row.quarantine is True and dead_row.processed is True
        assert dead_row.quarantine_reason == "payload_mismatch"  # reason 不變、attempts 不增


def test_batch_retry_retries_orphan_deal_once_per_landing_batch_no_hot_loop(session, engine):
    """孤兒 deal（對應委託永遠不存在）在「含有新落地列」的批次被批次內快速重試檢查撿到、
    重試一次後仍失敗、回到 association_pending 隔離；同一次 process_batch_once 呼叫內只重試
    一次，不會反覆重試造成熱迴圈——用 mapper 呼叫次數證明恰好只多一次，不是無限次。"""
    calls = {"n": 0}

    def _counting_ok_mapper(payload: dict, *, account: str | None = None) -> Fill:
        calls["n"] += 1
        return _ok_deal_mapper(payload, account=account)

    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload(ordno="GHOST", broker_order_id="GHOST-B")),
        )
        s.commit()

    worker = _worker(engine, deal_mapper=_counting_ok_mapper)
    first = worker.process_batch_once()
    assert first == 1
    assert calls["n"] == 1

    with Session(engine) as s:
        orphan = s.exec(select(RawInbox)).first()
        orphan_id = orphan.id
        assert orphan.quarantine is True and orphan.quarantine_reason == "association_pending"

    # 另一筆全新委託＋成交，與孤兒無關，會成功落地（landed>0）
    with Session(engine) as s:
        other = brepo.create_order(s, **_order_kwargs(client_order_id="C2", request_hash="H2"))
        brepo.set_order_ack(s, other.id, broker_order_id="B2", ordno="O2", status="submitted")
        s.commit()
    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload(fill_id="F2", ordno="O2", broker_order_id="B2")),
        )
        s.commit()

    second = worker.process_batch_once()
    # 1（F2 成功落地）+ 1（孤兒重試一次，仍失敗→重新 quarantine，quarantine 也算「確定處理完」）
    assert second == 2
    # mapper 恰好只多被呼叫兩次（F2 一次＋孤兒重試一次），不是無限次熱迴圈
    assert calls["n"] == 3

    with Session(engine) as s:
        orphan = s.get(RawInbox, orphan_id)
        assert orphan.quarantine is True and orphan.processed is False
        assert orphan.quarantine_reason == "association_pending"  # reason 不變，沒有升級/降級


def test_batch_retry_scoped_to_worker_user_id_does_not_touch_other_scope(engine):
    """Inc1 多人：per-slot worker（各自 user_id）批次內快速重試必須沿用批次查詢同一個 scope
    過濾——worker A（user_id=1）觸發的重試不得撿走 worker B（user_id=2）scope 的隔離列，
    跨 slot 完全互不影響（I8），比照既有
    `test_worker_scoped_to_user_id_ignores_other_users_rows` 的 scope 驗證 pattern。"""
    # worker B（user_id=2）的孤兒 deal：早就隔離，association_pending
    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload(ordno="GHOST-B-SLOT", broker_order_id="GHOST-B-SLOT-B")),
            user_id=2,
        )
        s.commit()
    worker_b = _worker(engine, user_id=2)
    assert worker_b.process_batch_once() == 1  # B 自己的批次先把它隔離掉

    with Session(engine) as s:
        b_row = s.exec(select(RawInbox).where(RawInbox.user_id == 2)).first()
        b_row_id = b_row.id
        assert b_row.quarantine is True and b_row.quarantine_reason == "association_pending"

    # worker A（user_id=1）現在有一筆全新、可正常落地的委託成交（landed>0，
    # 觸發 A 自己批次內快速重試——scope 必須只認領 user_id=1）
    with Session(engine) as s:
        order_a = brepo.create_order(s, **_order_kwargs(user_id=1, client_order_id="CA", request_hash="HA"))
        brepo.set_order_ack(s, order_a.id, broker_order_id="B1", ordno="O1", status="submitted")
        s.commit()
    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()), user_id=1,
        )
        s.commit()

    worker_a = _worker(engine, user_id=1)
    assert worker_a.process_batch_once() == 1  # 只有 A 自己這筆落地；沒有撿到 B 的隔離列

    with Session(engine) as s:
        b_row = s.get(RawInbox, b_row_id)
        assert b_row.quarantine is True and b_row.processed is False  # B 的隔離列完全沒被動到
        assert b_row.quarantine_reason == "association_pending"


# ---------------------------------------------------------------------------
# F1（opus 終審發現）：健康常態（零隔離列）下，批次內快速重試每次落地批次都白付
# unquarantine_stale_raw_inbox 的 O(n) 全表掃——加 in-memory guard（`_maybe_assoc_pending`），
# released=0 後關閉，直到又有新的 association_pending 隔離列出現才重新開啟。
# ---------------------------------------------------------------------------

def _land_new_deal(engine, *, client_order_id: str, request_hash: str, ordno: str,
                    broker_order_id: str, fill_id: str) -> None:
    """建一筆全新委託＋對應成交，跑過 process_batch_once 必然成功落地（landed=1）。"""
    with Session(engine) as s:
        order = brepo.create_order(
            s, **_order_kwargs(client_order_id=client_order_id, request_hash=request_hash)
        )
        brepo.set_order_ack(s, order.id, broker_order_id=broker_order_id, ordno=ordno, status="submitted")
        s.commit()
    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload(fill_id=fill_id, ordno=ordno, broker_order_id=broker_order_id)),
        )
        s.commit()


def test_healthy_steady_state_stops_calling_unquarantine_after_first_zero_release(engine, monkeypatch):
    """F1 驗收(a)：建構後第一個落地批次跑過一次重試查詢；零隔離列常態下 released=0，
    flag 轉 False——之後的落地批次不再呼叫 unquarantine_stale_raw_inbox（呼叫計數驗證，
    不是黑箱猜測行為）。"""
    calls = {"n": 0}
    original = brepo.unquarantine_stale_raw_inbox

    def _counting(*a, **kw):
        calls["n"] += 1
        return original(*a, **kw)

    monkeypatch.setattr(brepo, "unquarantine_stale_raw_inbox", _counting)

    worker = _worker(engine)
    assert worker._maybe_assoc_pending is True  # 建構時保守初始化 True

    _land_new_deal(engine, client_order_id="C1", request_hash="H1", ordno="O1",
                   broker_order_id="B1", fill_id="F1")
    first = worker.process_batch_once()
    assert first == 1
    assert calls["n"] == 1  # 第一個落地批次付了一次重試查詢
    assert worker._maybe_assoc_pending is False  # 零隔離列 → released=0 → flag 關閉

    _land_new_deal(engine, client_order_id="C2", request_hash="H2", ordno="O2",
                   broker_order_id="B2", fill_id="F2")
    second = worker.process_batch_once()
    assert second == 1
    assert calls["n"] == 1  # guard 生效：沒有再呼叫 unquarantine_stale_raw_inbox

    _land_new_deal(engine, client_order_id="C3", request_hash="H3", ordno="O3",
                   broker_order_id="B3", fill_id="F3")
    third = worker.process_batch_once()
    assert third == 1
    assert calls["n"] == 1  # 連續第三個落地批次仍然不再白付 O(n) 查詢


def test_flag_reactivates_after_new_association_pending_quarantine_appears(engine, monkeypatch):
    """F1 驗收(b)：guard 進入 False 之後，一旦出現新的 association_pending 隔離列，
    flag 必須回到 True，讓下一個落地批次重新跑重試查詢——不是永久關掉。"""
    calls = {"n": 0}
    original = brepo.unquarantine_stale_raw_inbox

    def _counting(*a, **kw):
        calls["n"] += 1
        return original(*a, **kw)

    monkeypatch.setattr(brepo, "unquarantine_stale_raw_inbox", _counting)

    worker = _worker(engine)
    _land_new_deal(engine, client_order_id="C1", request_hash="H1", ordno="O1",
                   broker_order_id="B1", fill_id="F1")
    worker.process_batch_once()
    assert worker._maybe_assoc_pending is False
    assert calls["n"] == 1

    # 孤兒 deal（對應委託不存在）→ quarantine(association_pending)；quarantine 不算「落地」，
    # 這批不會觸發重試查詢，但必須把 flag 撥回 True。
    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload(fill_id="ORPHAN", ordno="GHOST", broker_order_id="GHOST-B")),
        )
        s.commit()
    worker.process_batch_once()
    assert worker._maybe_assoc_pending is True
    assert calls["n"] == 1  # 純 quarantine 批次沒有觸發重試查詢

    # 下一個全新落地批次：guard 現在是 True，重試查詢應該再跑一次。
    _land_new_deal(engine, client_order_id="C2", request_hash="H2", ordno="O2",
                   broker_order_id="B2", fill_id="F2")
    worker.process_batch_once()
    assert calls["n"] == 2  # 重試查詢真的又跑了一次，flag 不是永久關掉


def test_existing_fast_retry_regressions_unaffected_by_guard(engine):
    """F1 驗收(c)：guard 不得弱化既有四支批次內快速重試回歸測試——這裡只是把它們的核心
    斷言原樣重跑一次（fresh worker，guard 預設 True），確認新增的 guard 邏輯沒有改變任何
    既有可觀察行為。完整覆蓋見上面同名情境的既有測試本身，這裡是收斂性補證。"""
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()
    worker = _worker(engine)
    assert worker.process_batch_once() == 1  # quarantine 也算「確定處理完」

    _seed_order_for_engine(engine)
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="order_report", broker="shioaji", payload=json.dumps(dict(
            broker="shioaji", account="F1", mode="sim", ordno="O1", broker_order_id="B1", status="cancelled",
        )))
        s.commit()
    assert worker.process_batch_once() == 2  # order_report 落地 + 批次內快速重試解隔離


def _seed_order_for_engine(engine, **over):
    with Session(engine) as s:
        order = brepo.create_order(s, **_order_kwargs(**over))
        brepo.set_order_ack(s, order.id, broker_order_id="B1", ordno="O1", status="submitted")
        s.commit()
        return order


def test_retry_exception_does_not_kill_worker_and_flag_stays_true(engine, monkeypatch):
    """F2 驗收：`unquarantine_stale_raw_inbox` 拋暫時性例外 → `process_batch_once` 不得往外
    拋、本批自己的落地照計入 handled、worker 不死；例外時不動 `_maybe_assoc_pending`
    （保守維持 True——下一個落地批次還會再試，不是被這次失敗永久關掉）。"""
    calls = {"n": 0}

    def _boom(*a, **kw):
        calls["n"] += 1
        raise RuntimeError("暫時性 DB 故障（模擬）")

    monkeypatch.setattr(brepo, "unquarantine_stale_raw_inbox", _boom)

    worker = _worker(engine)
    _land_new_deal(engine, client_order_id="C1", request_hash="H1", ordno="O1",
                   broker_order_id="B1", fill_id="F1")
    handled = worker.process_batch_once()  # 不應該往外拋例外
    assert handled == 1  # 本批自己的落地照計，快速重試失敗不影響這個數字
    assert calls["n"] == 1
    assert worker._maybe_assoc_pending is True  # 例外時不動 flag，保守維持 True

    _land_new_deal(engine, client_order_id="C2", request_hash="H2", ordno="O2",
                   broker_order_id="B2", fill_id="F2")
    second = worker.process_batch_once()  # worker 沒有因為上一批例外而死掉
    assert second == 1
    assert calls["n"] == 2  # flag 仍是 True，下一個落地批次還會再試一次（不是被永久關掉）


# ---------------------------------------------------------------------------
# F7（opus 終審發現，LOW）：批次內快速重試解除隔離筆數時補 log（比照 watchdog.py
# `_retry_quarantined_blocking` 的既有訊息風格，標明來源是「批次內快速重試」而非 watchdog）。
# ---------------------------------------------------------------------------

def test_batch_retry_logs_release_count_on_success(engine, caplog):
    caplog.set_level("INFO", logger="quanquant.broker.inbox_worker")
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()
    worker = _worker(engine)
    worker.process_batch_once()  # 孤兒 deal → quarantine(association_pending)

    _seed_order_for_engine(engine)
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="order_report", broker="shioaji", payload=json.dumps(dict(
            broker="shioaji", account="F1", mode="sim", ordno="O1", broker_order_id="B1", status="cancelled",
        )))
        s.commit()
    worker.process_batch_once()  # 觸發批次內快速重試，解除剛剛那筆隔離

    assert any("批次內快速重試" in r.message and "1" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# N1（opus 二輪複審發現，MEDIUM）：永不可解的孤兒 association_pending 列（如券商官方 App
# 下的單產生的成交回報，對應委託永遠不存在）過去會讓 F1 的 guard 形同虛設——每個落地批次都
# 釋放→重掃失敗→_process_one 重新隔離→撥回 True→下個落地批次再來一輪，O(n) 查詢成本全額
# 回歸。零進展偵測：本輪重試若零列成功落地、且重掃到的 id 集合與上一輪相同（或上一輪是
# 「尚無記錄」），視為這批解不開，關閉 guard，交還 watchdog 300s 慢速路徑照顧。
# ---------------------------------------------------------------------------

def test_orphan_disables_guard_after_second_consecutive_zero_progress_retry(engine, monkeypatch):
    """N1 驗收(1)（opus 三輪複審修正後的嚴格語意）：孤兒（對應委託永遠不存在）第 1 個落地
    批次觸發第一次重試查詢——這是本函式第一次遇到這個 id 集合，即使零進展也不關閉 guard
    （給它一次機會，避免誤殺 deal-before-ack 競態，見
    `test_first_failed_retry_does_not_disable_guard_before_own_ack_gets_a_chance`）；第 2 個
    落地批次重試到的仍是同一個 id 集合、依然零進展 → 連續兩輪零進展才關閉 guard。之後連續
    多個落地批次都不再呼叫 unquarantine_stale_raw_inbox（呼叫計數維持在 2），不是每個落地
    批次都白付一次 O(n) 全表掃——代價是純孤兒情境多付一輪觀察，可忽略。"""
    calls = {"n": 0}
    original = brepo.unquarantine_stale_raw_inbox

    def _counting(*a, **kw):
        calls["n"] += 1
        return original(*a, **kw)

    monkeypatch.setattr(brepo, "unquarantine_stale_raw_inbox", _counting)

    worker = _worker(engine)
    with Session(engine) as s:
        brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload(fill_id="ORPHAN", ordno="GHOST", broker_order_id="GHOST-B")),
        )
        s.commit()
    worker.process_batch_once()  # 孤兒 quarantine(association_pending)；quarantine 不算落地
    assert calls["n"] == 0  # 純 quarantine 批次沒有觸發重試查詢

    _land_new_deal(engine, client_order_id="C1", request_hash="H1", ordno="O1",
                   broker_order_id="B1", fill_id="F1")
    worker.process_batch_once()  # 第 1 個落地批次 → 第一次遇到孤兒集合，零進展但先給一次機會
    assert calls["n"] == 1
    assert worker._maybe_assoc_pending is True  # 還沒關閉——只是第一次遇到

    _land_new_deal(engine, client_order_id="C2", request_hash="H2", ordno="O2",
                   broker_order_id="B2", fill_id="F2")
    worker.process_batch_once()  # 第 2 個落地批次 → 同一個孤兒集合、依然零進展 → 連續兩輪關閉
    assert calls["n"] == 2
    assert worker._maybe_assoc_pending is False

    _land_new_deal(engine, client_order_id="C3", request_hash="H3", ordno="O3",
                   broker_order_id="B3", fill_id="F3")
    worker.process_batch_once()
    assert calls["n"] == 2  # guard 已關閉，沒有再呼叫 unquarantine_stale_raw_inbox

    _land_new_deal(engine, client_order_id="C4", request_hash="H4", ordno="O4",
                   broker_order_id="B4", fill_id="F4")
    worker.process_batch_once()
    assert calls["n"] == 2  # 連續第四個落地批次仍然沒有再白付 O(n) 查詢


def test_new_pending_row_reactivates_guard_and_resolves_while_orphan_stays_quarantined(engine, monkeypatch):
    """N1 驗收(2)（嚴格語意）：孤兒連續兩輪零進展、guard 關閉之後，出現一筆全新的
    association_pending 隔離列（既有 `_process_one` 邏輯無條件把 flag 撥回 True）→ 下一個
    落地批次的批次內快速重試應該再跑一次；這筆新列若委託 ack 已經補上，會在這次重試中成功
    落地（有進展），孤兒本身仍然解不開、留在隔離——不會因為孤兒又失敗一次就被誤判成「沒有
    進展」而繼續關閉（這次的集合裡有新列，`retry_landed>0`，兩個條件都不成立）。"""
    calls = {"n": 0}
    original = brepo.unquarantine_stale_raw_inbox

    def _counting(*a, **kw):
        calls["n"] += 1
        return original(*a, **kw)

    monkeypatch.setattr(brepo, "unquarantine_stale_raw_inbox", _counting)

    worker = _worker(engine)
    with Session(engine) as s:
        orphan_row = brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload(fill_id="ORPHAN", ordno="GHOST", broker_order_id="GHOST-B")),
        )
        orphan_id = orphan_row.id
        s.commit()
    worker.process_batch_once()  # 孤兒 quarantine，非落地批次

    _land_new_deal(engine, client_order_id="C1", request_hash="H1", ordno="O1",
                   broker_order_id="B1", fill_id="F1")
    worker.process_batch_once()  # 第 1 個落地批次 → 第一次遇到孤兒集合，零進展但先給一次機會
    assert calls["n"] == 1
    assert worker._maybe_assoc_pending is True  # 還沒關閉

    _land_new_deal(engine, client_order_id="C2", request_hash="H2", ordno="O2",
                   broker_order_id="B2", fill_id="F2")
    worker.process_batch_once()  # 第 2 個落地批次 → 同一個孤兒集合、依然零進展 → 連續兩輪關閉
    assert calls["n"] == 2
    assert worker._maybe_assoc_pending is False

    # 全新一筆 deal（N，委託 ack 還沒補上）→ quarantine(association_pending)；純 quarantine
    # 批次沒有東西落地，不會觸發重試查詢，但既有 `_process_one` 邏輯會把 flag 撥回 True。
    with Session(engine) as s:
        n_row = brepo.stage_raw_inbox(
            s, kind="deal_report", broker="shioaji",
            payload=json.dumps(_deal_payload(fill_id="N", ordno="O3", broker_order_id="B3")),
        )
        n_id = n_row.id
        s.commit()
    worker.process_batch_once()
    assert worker._maybe_assoc_pending is True
    assert calls["n"] == 2  # 純 quarantine 批次不會觸發重試查詢

    # N 的委託 ack 現在補上；下一個全新落地批次（C4）觸發批次內快速重試——這次會撿到孤兒與
    # N 兩筆隔離列：N 因為委託已存在而成功落地（retry_landed>0），孤兒依然解不開、重新隔離。
    with Session(engine) as s:
        order_n = brepo.create_order(s, **_order_kwargs(client_order_id="CN", request_hash="HN"))
        brepo.set_order_ack(s, order_n.id, broker_order_id="B3", ordno="O3", status="submitted")
        s.commit()
    _land_new_deal(engine, client_order_id="C4", request_hash="H4", ordno="O4",
                   broker_order_id="B4", fill_id="F4")
    worker.process_batch_once()
    assert calls["n"] == 3  # 重試查詢真的又跑了一次，flag 不是永久關掉
    assert worker._maybe_assoc_pending is True  # N 成功落地（有進展）→ 維持 True

    with Session(engine) as s:
        n_after = s.get(RawInbox, n_id)
        assert n_after.processed is True and n_after.quarantine is False  # N 成功落地
        orphan_after = s.get(RawInbox, orphan_id)
        assert orphan_after.quarantine is True and orphan_after.processed is False  # 孤兒仍解不開
        assert orphan_after.quarantine_reason == "association_pending"


def test_first_failed_retry_does_not_disable_guard_before_own_ack_gets_a_chance(session, engine):
    """N1 語意修正（opus 三輪複審發現，MEDIUM——先前「上一輪是 None 也算沒有新列」的寬鬆
    判斷會提前判死）：deal D 先到、無對應委託 → quarantine(association_pending)；**不相關**
    的另一筆回報接著落地（landed=1）觸發批次內快速重試——這是本函式第一次被呼叫
    （`_last_retry_released_ids` 還是 None），D 的委託 ack 還沒到，重試失敗、原地重新隔離
    （retry_landed=0）。這第一次零進展**不該**關閉 guard——D 才剛拿到第一次機會，只是運氣
    不好在忙碌時段被不相關的批次抽到而已；緊接著 D 自己委託的 ack 落地時，批次內快速重試
    必須還在，讓 D 當場解隔離成功，不必倒退回 watchdog 300 秒 fallback——這正是批次內快速
    重試存在的核心情境（deal-before-ack 競態），在「多筆委託併發、有不相關回報插隊」的忙碌
    時段不能失效。"""
    with Session(engine) as s:
        d_row = brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        d_id = d_row.id
        s.commit()
    worker = _worker(engine)
    worker.process_batch_once()  # D quarantine(association_pending)，非落地批次

    with Session(engine) as s:
        d = s.get(RawInbox, d_id)
        assert d.quarantine is True and d.quarantine_reason == "association_pending"

    # 不相關的另一筆委託成交落地（與 D 完全無關）→ 觸發第一次批次內快速重試；D 的委託還
    # 不存在，重試失敗、原地重新隔離——這是本函式第一次被呼叫，「上一輪」尚無記錄。
    _land_new_deal(engine, client_order_id="U1", request_hash="HU1", ordno="U-O1",
                   broker_order_id="U-B1", fill_id="U-F1")
    worker.process_batch_once()
    assert worker._maybe_assoc_pending is True  # 關鍵斷言：第一次零進展不該關閉 guard

    with Session(engine) as s:
        d = s.get(RawInbox, d_id)
        assert d.quarantine is True and d.quarantine_reason == "association_pending"  # D 仍隔離

    # D 自己委託的 ack 現在落地（同既有回歸情境
    # test_association_pending_deal_resolves_within_batch_when_order_ack_lands_later）——這個
    # 批次本身就有東西落地（order_report），觸發快速重試，這次應該讓 D 也一併解隔離成功，
    # 不必等 watchdog。
    _seed_order(session)  # ordno=O1, broker_order_id=B1，對應 _deal_payload() 預設值
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="order_report", broker="shioaji", payload=json.dumps(dict(
            broker="shioaji", account="F1", mode="sim", ordno="O1", broker_order_id="B1", status="cancelled",
        )))
        s.commit()
    worker.process_batch_once()

    with Session(engine) as s:
        d = s.get(RawInbox, d_id)
        assert d.processed is True and d.quarantine is False  # D 當場解隔離成功，不必等 watchdog
        assert s.exec(select(Deal)).first() is not None
