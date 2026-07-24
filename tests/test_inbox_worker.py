"""BrokerSupervisor 序列化鎖 + RawInboxWorker：raw-inbox（durable spool，不經 volatile
asyncio.Queue——QueueFull 這個攻擊面已被架構消除）逐列一交易處理、業務失敗 quarantine
不留部分寫入、非預期例外不 quarantine（留待下一輪重試，零丟單）、複合鍵解析委託關聯、
Deal 重播冪等只補 processed 不重跑帳務、序列化鎖防兩協程同時改同一 BrokerPosition。"""
import asyncio
import json
from decimal import Decimal

from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.broker.inbox_worker import OrderReport, RawInboxWorker, commit_raw_callback
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import Fill
from quanquant.db.models import BrokerPosition, Deal, Order, RawInbox


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


def _ok_deal_mapper(payload: dict) -> Fill:
    return Fill(
        broker=payload["broker"], fill_id=payload["fill_id"], ordno=payload["ordno"],
        broker_order_id=payload["broker_order_id"], symbol=payload["symbol"], action=payload["action"],
        price=Decimal(payload["price"]), qty=int(payload["qty"]), fee=Decimal(payload["fee"]),
        octype=payload["octype"], ts=payload["ts"], account=payload["account"], mode=payload["mode"],
        user_id=None,
    )


def _noop_order_report_mapper(payload: dict) -> OrderReport:
    return OrderReport(**payload)


def _worker(engine, *, deal_mapper=_ok_deal_mapper, order_report_mapper=_noop_order_report_mapper, supervisor=None):
    return RawInboxWorker(
        session_factory=lambda: Session(engine),
        supervisor=supervisor or BrokerSupervisor(),
        deal_mapper=deal_mapper,
        order_report_mapper=order_report_mapper,
        idle_interval=0.01,
    )


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


def test_unexpected_exception_leaves_row_pending_not_quarantined_zero_loss(session, engine):
    """V3-2 核心回歸：非預期例外（非 ValueError/PositionMismatchError）不得 quarantine，
    必須留 processed=False 讓下一輪重試——這就是『重啟後 raw-inbox 仍在，最終恰一次 effect』。"""
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()

    calls = {"n": 0}

    def _flaky_mapper(payload: dict) -> Fill:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("模擬暫時性故障（非業務邏輯錯誤）")
        return _ok_deal_mapper(payload)

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
    commit_raw_callback(lambda: Session(engine), kind="deal_report", broker="shioaji", payload=payload)

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

    asyncio.run(scenario())


def test_worker_run_processes_pending_row_then_idles(engine):
    """`run()` 走真正的 async 迴圈：拿鎖 → to_thread 跑 process_batch_once → 處理完停止旗標生效即結束。
    驗證迴圈確實會呼叫 process_batch_once 並套用 supervisor 序列化，而非只是裝飾用的殼。"""
    with Session(engine) as s:
        brepo.stage_raw_inbox(s, kind="deal_report", broker="shioaji", payload=json.dumps(_deal_payload()))
        s.commit()
    with Session(engine) as s:
        _seed_order(s)

    worker = _worker(engine)

    async def scenario():
        run_task = asyncio.create_task(worker.run())
        for _ in range(200):
            with Session(engine) as s:
                row = s.exec(select(RawInbox)).first()
                if row is not None and row.processed:
                    break
            await asyncio.sleep(0.01)
        await worker.stop_and_drain(timeout=1.0)
        await run_task

    asyncio.run(scenario())
    with Session(engine) as s:
        row = s.exec(select(RawInbox)).first()
        assert row.processed is True
