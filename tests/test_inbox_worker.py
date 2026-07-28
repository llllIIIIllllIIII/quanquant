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


def test_deal_report_octype_comes_from_resolved_order_not_mapper_payload(session, engine):
    """成交回報（真實 FuturesDealEvent）本身沒有 octype 欄位；_process_deal 必須用解析到的
    對應 Order 的 octype 覆蓋 mapper 回傳的任何占位值，不是照單全收 mapper 給的值
    （見 broker/shioaji_adapter.py::_map_deal_report 的 "Auto" 占位說明）。"""
    _seed_order(session, octype="Cover", action="Sell")  # 對應委託是平倉

    def _mapper_with_wrong_octype_placeholder(payload: dict) -> Fill:
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
