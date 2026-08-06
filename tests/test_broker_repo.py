"""broker 倉儲：client_order_id 冪等建委託(+request_hash防竄改)、
複合 scope 委託查詢(取代裸鍵 .first())、Deal (broker,mode,account,trading_day,fill_id) 去重、
raw-inbox spool CRUD、BrokerPosition 累計欄位+invariant CHECK、confirm token 原子 claim、
quota per-reservation 狀態機 CAS（round3 #4）。"""
import datetime as dt
import threading
from decimal import Decimal

import pytest
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import CreateTable
from sqlmodel import Session, SQLModel, create_engine, select

from quanquant.broker import repository as brepo
from quanquant.db.models import (
    AgentAccountBinding,
    BrokerPosition,
    ConfirmToken,
    Deal,
    Order,
    OrderAudit,
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


def _deal_kwargs(**over):
    base = dict(
        broker="shioaji", account="F1", mode="sim", trading_day="2026-07-24",
        fill_id="D1", ordno="O1", broker_order_id="B1", order_id=None, user_id=1, symbol="TXF",
        action="Buy", price=Decimal("18000"), qty=1, fee=Decimal("50"),
        octype="New", ts=1_780_000_000_000, raw_inbox_id=None,
    )
    base.update(over)
    return base


# ---- Order ----

def test_create_order_defaults_pending(session):
    o = brepo.create_order(session, **_order_kwargs())
    session.commit()
    assert o.id is not None and o.status == "pending" and o.mode == "sim"


def test_create_order_idempotent_on_replay_same_hash(session):
    first = brepo.create_order(session, **_order_kwargs())
    session.commit()
    replay = brepo.create_order(session, **_order_kwargs())  # 同 client_order_id + 同 request_hash
    assert replay.id == first.id


def test_create_order_same_client_id_different_hash_rejected(session):
    """V3-3：同 client_order_id、不同 payload（request_hash 不符）一律拒絕，不得靜默覆寫或誤回舊單。"""
    brepo.create_order(session, **_order_kwargs())
    session.commit()
    with pytest.raises(ValueError):
        brepo.create_order(session, **_order_kwargs(request_hash="H2", qty=99))


def test_create_order_non_replay_integrity_error_raised(session):
    """round3 B5：NOT NULL 撞的不是 client_order_id 這個冪等鍵本身，重新查詢確認查無
    該鍵後必須把原始 IntegrityError 往上拋，不可誤判成「重播」。"""
    with pytest.raises(IntegrityError):
        brepo.create_order(session, **_order_kwargs(client_order_id="C-BAD", price=None))


def test_order_mode_check_constraint_rejects_illegal_value(session):
    bad = Order(
        client_order_id="C-ILLEGAL", request_hash="H", user_id=1, mode="paper", broker="shioaji", account="F1",
        symbol="TXF", action="Buy", qty=1, price=Decimal("1"),
        price_type="LMT", order_type="ROD", octype="New", trading_day="2026-06-16",
    )
    session.add(bad)
    with pytest.raises(IntegrityError):
        session.commit()


def test_find_order_by_ordno_is_scoped_not_bare_lookup(session):
    """round3 #3：解析 fill→order 一律複合 scope，同 ordno 不同 (account,mode) 不可誤配（BLOCKER#3）。"""
    a = brepo.create_order(session, **_order_kwargs(client_order_id="CA", account="F1", mode="sim"))
    brepo.set_order_ack(session, a.id, broker_order_id="BA", ordno="SHARED", status="submitted")
    b = brepo.create_order(session, **_order_kwargs(client_order_id="CB", account="F2", mode="sim", request_hash="H2"))
    brepo.set_order_ack(session, b.id, broker_order_id="BB", ordno="SHARED", status="submitted")
    session.commit()

    found_a = brepo.find_order_by_ordno(session, broker="shioaji", account="F1", mode="sim", ordno="SHARED")
    found_b = brepo.find_order_by_ordno(session, broker="shioaji", account="F2", mode="sim", ordno="SHARED")
    assert found_a.id == a.id and found_b.id == b.id  # 同 ordno 不同 account 分別命中，不互相污染


def test_find_order_by_broker_id_is_scoped(session):
    a = brepo.create_order(session, **_order_kwargs(client_order_id="CA2", account="F1", mode="sim"))
    brepo.set_order_ack(session, a.id, broker_order_id="SHARED-BID", ordno="OA", status="submitted")
    b = brepo.create_order(session, **_order_kwargs(client_order_id="CB2", account="F2", mode="sim", request_hash="H2"))
    brepo.set_order_ack(session, b.id, broker_order_id="SHARED-BID", ordno="OB", status="submitted")
    session.commit()

    found_a = brepo.find_order_by_broker_id(session, broker="shioaji", account="F1", mode="sim", broker_order_id="SHARED-BID")
    found_b = brepo.find_order_by_broker_id(session, broker="shioaji", account="F2", mode="sim", broker_order_id="SHARED-BID")
    assert found_a.id == a.id and found_b.id == b.id


def test_apply_order_fill_weighted_average_and_terminal_status(session):
    o = brepo.create_order(session, **_order_kwargs(qty=3))
    session.commit()
    brepo.apply_order_fill(session, o, fill_qty=1, fill_price=Decimal("18000"))
    brepo.apply_order_fill(session, o, fill_qty=2, fill_price=Decimal("18030"))
    session.commit()
    assert o.filled_qty == 3
    assert o.avg_fill_price == (Decimal("18000") * 1 + Decimal("18030") * 2) / 3
    assert o.status == "filled"  # filled_qty(3) >= qty(3)


def test_mark_order_status_does_not_touch_filled_qty(session):
    o = brepo.create_order(session, **_order_kwargs())
    session.commit()
    brepo.mark_order_status(session, o, status="cancelled")
    session.commit()
    assert o.status == "cancelled" and o.filled_qty == 0


def test_mark_order_status_unknown_transition_and_reconcile_can_override(session):
    """Task 6：native 呼叫失敗但不確定是否已送達券商時標 unknown；待 reconcile 之後任何
    進度/終態回報都可以覆蓋 unknown（暫態，不是進度序列/終態的一部分）。"""
    o = brepo.create_order(session, **_order_kwargs())
    session.commit()
    brepo.mark_order_status(session, o, status="unknown")
    session.commit()
    assert o.status == "unknown"
    brepo.mark_order_status(session, o, status="submitted")
    session.commit()
    assert o.status == "submitted"


def test_mark_order_status_unknown_does_not_override_filled(session):
    o = brepo.create_order(session, **_order_kwargs(qty=1))
    session.commit()
    brepo.apply_order_fill(session, o, fill_qty=1, fill_price=Decimal("18000"))
    session.commit()
    assert o.status == "filled"
    brepo.mark_order_status(session, o, status="unknown")
    session.commit()
    assert o.status == "filled"  # 已成交不可回退成 unknown


def test_count_and_sum_qty_today_scoped_by_mode_and_trading_day(session):
    brepo.create_order(session, **_order_kwargs(client_order_id="S1", mode="sim", qty=2))
    brepo.create_order(session, **_order_kwargs(client_order_id="R1", mode="real", qty=5, request_hash="H-R1"))
    session.commit()
    assert brepo.count_orders_today(session, user_id=1, mode="sim", trading_day="2026-06-16") == 1
    assert brepo.sum_qty_today(session, user_id=1, mode="sim", trading_day="2026-06-16") == 2
    assert brepo.sum_qty_today(session, user_id=1, mode="real", trading_day="2026-06-16") == 5


# ---- RawInbox ----

def test_raw_inbox_stage_list_process_quarantine_round_trip(session):
    row = brepo.stage_raw_inbox(session, kind="deal_report", broker="shioaji", payload='{"a":1}')
    session.commit()
    pending = brepo.list_unprocessed_raw_inbox(session)
    assert [r.id for r in pending] == [row.id]

    brepo.mark_raw_inbox_processed(session, row)
    session.commit()
    assert brepo.list_unprocessed_raw_inbox(session) == []

    row2 = brepo.stage_raw_inbox(session, kind="deal_report", broker="shioaji", payload='{"bad":true}')
    session.commit()
    brepo.quarantine_raw_inbox(session, row2, error="缺 fill_id")
    session.commit()
    assert brepo.list_unprocessed_raw_inbox(session) == []  # quarantine 不算未處理佇列
    refreshed = session.get(RawInbox, row2.id)
    assert refreshed.quarantine is True and refreshed.error == "缺 fill_id"


def test_list_unprocessed_raw_inbox_scoped_to_user_id(session):
    """Task 7（D6/D9）：agent per-slot RawInboxWorker 傳 `user_id` 精確篩選
    `RawInbox.user_id == user_id`——不是 `IS NULL OR =`，未蓋章的 NULL 列不會被任何 agent
    slot 誤認領（只由不帶 user_id 的 in-process worker 處理，D5）；不帶 `user_id`（in-process
    既有呼叫）維持全量、位元級不變。"""
    brepo.stage_raw_inbox(session, kind="deal_report", broker="shioaji", payload='{"a":1}', user_id=1)
    brepo.stage_raw_inbox(session, kind="deal_report", broker="shioaji", payload='{"b":1}', user_id=2)
    brepo.stage_raw_inbox(session, kind="deal_report", broker="shioaji", payload='{"c":1}')  # NULL（in-process 舊列）
    session.commit()

    scoped_to_1 = brepo.list_unprocessed_raw_inbox(session, user_id=1)
    assert [r.payload for r in scoped_to_1] == ['{"a":1}']

    scoped_to_2 = brepo.list_unprocessed_raw_inbox(session, user_id=2)
    assert [r.payload for r in scoped_to_2] == ['{"b":1}']

    assert len(brepo.list_unprocessed_raw_inbox(session)) == 3  # 不篩：既有 in-process 行為


def test_unquarantine_stale_raw_inbox_scoped_to_user_id(session):
    """Task 7（D6）：per-slot watchdog 的 quarantine 解除 scope 到自己的 user，不誤解除/誤
    重試其他 user 的殘留（I8：跨 user 完全互不影響）。"""
    row_a = brepo.stage_raw_inbox(session, kind="deal_report", broker="shioaji", payload='{"a":1}', user_id=1)
    row_b = brepo.stage_raw_inbox(session, kind="deal_report", broker="shioaji", payload='{"b":1}', user_id=2)
    session.commit()
    for row in (row_a, row_b):
        row.received_at = dt.datetime(2020, 1, 1)
        session.add(row)
        brepo.quarantine_raw_inbox(session, row, error="待重試")
    session.commit()

    reopened = brepo.unquarantine_stale_raw_inbox(session, older_than=dt.datetime(2025, 1, 1), user_id=1)
    session.commit()
    assert reopened == 1
    assert session.get(RawInbox, row_a.id).quarantine is False
    assert session.get(RawInbox, row_b.id).quarantine is True  # user 2 的列不受影響


def test_unquarantine_stale_raw_inbox_reopens_old_rows(session):
    row = brepo.stage_raw_inbox(session, kind="deal_report", broker="shioaji", payload='{"a":1}')
    session.commit()
    row.received_at = dt.datetime(2020, 1, 1)
    session.add(row)
    brepo.quarantine_raw_inbox(session, row, error="暫時解不到委託")
    session.commit()

    reopened = brepo.unquarantine_stale_raw_inbox(session, older_than=dt.datetime(2025, 1, 1))
    session.commit()
    assert reopened == 1
    refreshed = session.get(RawInbox, row.id)
    assert refreshed.quarantine is False and refreshed.error is None
    assert [r.id for r in brepo.list_unprocessed_raw_inbox(session)] == [row.id]


# ---- RawInbox quarantine 分級（Inc1 D5/R2-6） ----

def test_quarantine_raw_inbox_default_reason_association_pending_stays_unprocessed(session):
    """既有呼叫端（不帶 reason）維持 reason="association_pending"、processed 仍是 False——
    可重試，不退出換帳號 guard 的 unprocessed 計數。"""
    row = brepo.stage_raw_inbox(session, kind="deal_report", broker="shioaji", payload='{"a":1}')
    session.commit()
    brepo.quarantine_raw_inbox(session, row, error="暫時解不到委託")
    session.commit()
    refreshed = session.get(RawInbox, row.id)
    assert refreshed.quarantine_reason == "association_pending"
    assert refreshed.processed is False
    assert refreshed.processed_at is None


@pytest.mark.parametrize("reason", ["scope_violation", "payload_mismatch", "user_mismatch"])
def test_quarantine_raw_inbox_dead_letter_reason_marks_processed_true(session, reason):
    """R2-6：三個永久 dead-letter reason 必須連同 processed=True（＋processed_at）一起落地——
    這是唯一的方式讓這列退出換帳號 guard 的 unprocessed 計數（那個計數只看 processed，不管
    quarantine，見 `repository.count_unprocessed_for_login`，Task 6 取代舊版
    `agent_ws._count_unprocessed_raw_inbox`）。"""
    row = brepo.stage_raw_inbox(session, kind="deal_report", broker="shioaji", payload='{"a":1}')
    session.commit()
    brepo.quarantine_raw_inbox(session, row, error="fail closed", reason=reason)
    session.commit()
    refreshed = session.get(RawInbox, row.id)
    assert refreshed.quarantine is True
    assert refreshed.processed is True
    assert refreshed.processed_at is not None
    assert refreshed.quarantine_reason == reason


@pytest.mark.parametrize("reason", ["scope_violation", "payload_mismatch", "user_mismatch"])
def test_unquarantine_stale_raw_inbox_never_releases_dead_letter_rows(session, reason):
    """R2-6：dead-letter 列永不進 unquarantine 重試迴圈——否則會把已經 fail-closed 判定的列
    重新丟回處理管線，製造無限重試/告警洪水。"""
    row = brepo.stage_raw_inbox(session, kind="deal_report", broker="shioaji", payload='{"a":1}')
    session.commit()
    row.received_at = dt.datetime(2020, 1, 1)
    session.add(row)
    brepo.quarantine_raw_inbox(session, row, error="fail closed", reason=reason)
    session.commit()

    released = brepo.unquarantine_stale_raw_inbox(session, older_than=dt.datetime(2025, 1, 1))
    session.commit()
    assert released == 0
    refreshed = session.get(RawInbox, row.id)
    assert refreshed.quarantine is True and refreshed.quarantine_reason == reason


def test_unquarantine_stale_raw_inbox_still_releases_null_reason_legacy_rows(session):
    """既有列（部署本功能前就已 quarantine、`quarantine_reason` 恆 NULL）保守視為可重試，
    不因新增分級而退化（向後相容——不確定就不要判死刑，同模組一貫的零丟單哲學）。"""
    row = brepo.stage_raw_inbox(session, kind="deal_report", broker="shioaji", payload='{"a":1}')
    session.commit()
    row.received_at = dt.datetime(2020, 1, 1)
    row.quarantine = True
    row.error = "legacy quarantine（reason 未蓋章）"
    session.add(row)
    session.commit()

    released = brepo.unquarantine_stale_raw_inbox(session, older_than=dt.datetime(2025, 1, 1))
    session.commit()
    assert released == 1


# ---- agent_account_bindings 查詢（Inc1 D5/D10，Task 6 才建立寫入邏輯） ----

def test_find_account_binding_returns_none_when_unbound(session):
    assert brepo.find_account_binding(session, broker="shioaji", account="F1") is None


def test_find_account_binding_returns_bound_row(session):
    session.add(AgentAccountBinding(broker="shioaji", account="F1", user_id=7))
    session.commit()
    binding = brepo.find_account_binding(session, broker="shioaji", account="F1")
    assert binding is not None and binding.user_id == 7


# ---- Deal ----

def test_stage_deal_dedup_on_replay(session):
    first = brepo.stage_deal(session, **_deal_kwargs())
    session.commit()
    assert first is not None and first.broker_order_id == "B1"
    replay = brepo.stage_deal(session, **_deal_kwargs())  # 同 (broker,mode,account,trading_day,fill_id)
    assert replay is None  # 撞唯一鍵 → 跳過，不重寫


def test_stage_deal_distinct_fill_id_ok(session):
    assert brepo.stage_deal(session, **_deal_kwargs(fill_id="A")) is not None
    session.commit()
    assert brepo.stage_deal(session, **_deal_kwargs(fill_id="B")) is not None


def test_stage_deal_non_replay_integrity_error_is_raised(session):
    """round3 B5：NOT NULL 撞的不是 (broker,mode,account,trading_day,fill_id) 這個唯一鍵，
    重新以該鍵查詢確認查無後必須拋出，不可誤判成重播。"""
    with pytest.raises(IntegrityError):
        brepo.stage_deal(session, **_deal_kwargs(fill_id="F-BAD", price=None))


def test_deal_mode_check_constraint_rejects_illegal_value(session):
    bad = Deal(
        broker="shioaji", account="F1", mode="paper", trading_day="2026-07-24", fill_id="D-BAD",
        symbol="TXF", action="Buy", price=Decimal("18000"), qty=1, octype="New", ts=1,
    )
    session.add(bad)
    with pytest.raises(IntegrityError):
        session.commit()


# ---- BrokerPosition ----

def test_find_and_list_open_positions_scoped(session):
    p1 = BrokerPosition(user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                        direction="long", total_opened_qty=1, entry_notional=Decimal("18000"))
    p2 = BrokerPosition(user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                        direction="short", total_opened_qty=1, entry_notional=Decimal("18000"))
    p3 = BrokerPosition(user_id=2, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                        direction="long", total_opened_qty=1, entry_notional=Decimal("18000"))
    session.add_all([p1, p2, p3])
    session.commit()
    found = brepo.find_open_position(session, user_id=1, broker="shioaji", account="F1",
                                     mode="sim", symbol="TXF", direction="long")
    assert found is not None and found.id == p1.id
    opens = brepo.list_open_positions(session, user_id=1, broker="shioaji", account="F1",
                                      mode="sim", symbol="TXF")
    assert {p.id for p in opens} == {p1.id, p2.id}  # user=2 的不混進來


def test_remaining_and_avg_price_helpers():
    pos = BrokerPosition(
        user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF", direction="long",
        total_opened_qty=3, entry_notional=Decimal("400"), closed_qty=1, exit_notional=Decimal("105"),
    )
    assert brepo.remaining_qty(pos) == 2
    assert brepo.avg_entry_price(pos) == Decimal("400") / 3
    assert brepo.avg_exit_price(pos) == Decimal("105")


def test_broker_position_second_open_same_scope_blocked_by_unique_index(session):
    """round3 #15：同 (user,broker,account,mode,symbol,direction) scope 只能有一個 status='open'
    的部位——partial unique index 擋第二個 open，不是應用層才發現（避免競態下帳本分裂）。"""
    p1 = BrokerPosition(user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                        direction="long", total_opened_qty=1, entry_notional=Decimal("18000"))
    session.add(p1)
    session.commit()

    p2 = BrokerPosition(user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                        direction="long", total_opened_qty=1, entry_notional=Decimal("18100"))
    session.add(p2)
    with pytest.raises(IntegrityError):
        session.commit()


def test_broker_position_new_open_allowed_after_previous_closed(session):
    """historical 的 closed 部位不受 active-scope 唯一鍵限制——平倉後可以再開新倉。"""
    p1 = BrokerPosition(user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                        direction="long", total_opened_qty=1, entry_notional=Decimal("18000"),
                        status="closed", closed_qty=1, exit_notional=Decimal("18100"))
    session.add(p1)
    session.commit()

    p2 = BrokerPosition(user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                        direction="long", total_opened_qty=2, entry_notional=Decimal("36400"))
    session.add(p2)
    session.commit()  # 不應拋錯
    assert p2.id is not None


def test_broker_position_total_opened_qty_must_be_positive(session):
    bad = BrokerPosition(user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                         direction="long", total_opened_qty=0, entry_notional=Decimal("0"))
    session.add(bad)
    with pytest.raises(IntegrityError):
        session.commit()


def test_broker_position_closed_qty_cannot_exceed_total_opened_qty(session):
    bad = BrokerPosition(user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                         direction="long", total_opened_qty=1, closed_qty=5, entry_notional=Decimal("18000"))
    session.add(bad)
    with pytest.raises(IntegrityError):
        session.commit()


def test_broker_position_mode_check_constraint_rejects_illegal_value(session):
    bad = BrokerPosition(user_id=1, broker="shioaji", account="F1", mode="paper", symbol="TXF",
                         direction="long", total_opened_qty=1, entry_notional=Decimal("18000"))
    session.add(bad)
    with pytest.raises(IntegrityError):
        session.commit()


def test_broker_position_direction_check_constraint_rejects_illegal_value(session):
    bad = BrokerPosition(user_id=1, broker="shioaji", account="F1", mode="sim", symbol="TXF",
                         direction="sideways", total_opened_qty=1, entry_notional=Decimal("18000"))
    session.add(bad)
    with pytest.raises(IntegrityError):
        session.commit()


# ---- OrderAudit ----

def test_append_audit_visible_after_commit(session):
    brepo.append_audit(session, actor_user_id=1, mode="sim", action="place",
                       payload_hash="abc123", result="ok")
    session.commit()
    rows = list(session.exec(select(OrderAudit)))
    assert len(rows) == 1 and rows[0].result == "ok" and rows[0].action == "place"


def test_order_audit_mode_check_constraint_rejects_illegal_value(session):
    bad = OrderAudit(ts=1, mode="paper", action="place", payload_hash="h", result="ok")
    session.add(bad)
    with pytest.raises(IntegrityError):
        session.commit()


# ---- ConfirmToken ----

def test_confirm_token_claim_is_atomic_and_one_time(session):
    expires = dt.datetime(2026, 6, 16, 12, 5)
    brepo.create_confirm_token_row(session, jti="J1", actor_user_id=1, payload_hash="H1", expires_at=expires)
    session.commit()

    now = dt.datetime(2026, 6, 16, 12, 0)
    ok = brepo.claim_confirm_token(session, jti="J1", actor_user_id=1, payload_hash="H1", now=now)
    session.commit()
    assert ok is True

    replay = brepo.claim_confirm_token(session, jti="J1", actor_user_id=1, payload_hash="H1", now=now)
    session.commit()
    assert replay is False  # 已消費，不得重放


def test_confirm_token_claim_rejects_wrong_user_or_hash(session):
    expires = dt.datetime(2026, 6, 16, 12, 5)
    brepo.create_confirm_token_row(session, jti="J2", actor_user_id=1, payload_hash="H1", expires_at=expires)
    session.commit()
    now = dt.datetime(2026, 6, 16, 12, 0)
    assert brepo.claim_confirm_token(session, jti="J2", actor_user_id=99, payload_hash="H1", now=now) is False
    assert brepo.claim_confirm_token(session, jti="J2", actor_user_id=1, payload_hash="WRONG", now=now) is False


def test_confirm_token_claim_rejects_expired(session):
    expires = dt.datetime(2026, 6, 16, 12, 5)
    brepo.create_confirm_token_row(session, jti="J3", actor_user_id=1, payload_hash="H1", expires_at=expires)
    session.commit()
    late = dt.datetime(2026, 6, 16, 12, 6)
    assert brepo.claim_confirm_token(session, jti="J3", actor_user_id=1, payload_hash="H1", now=late) is False


# ---- QuotaReservation（round3 #4：per-reservation 狀態機） ----

def test_reserve_quota_cas_blocks_once_limit_hit(session):
    assert brepo.reserve_quota(session, reservation_id="r1", user_id=1, mode="sim",
                               trading_day="2026-06-16", qty=6, daily_limit=10) is True
    session.commit()
    assert brepo.reserve_quota(session, reservation_id="r2", user_id=1, mode="sim",
                               trading_day="2026-06-16", qty=6, daily_limit=10) is False  # 6+6>10
    session.commit()
    assert brepo.quota_used_today(session, user_id=1, mode="sim", trading_day="2026-06-16") == 6  # 被擋的那次沒有偷偷加上去
    rows = list(session.exec(select(QuotaReservation)))
    assert [r.reservation_id for r in rows] == ["r1"]  # r2 完全沒有留下列


def test_reserve_quota_scoped_by_mode_and_trading_day(session):
    assert brepo.reserve_quota(session, reservation_id="r1", user_id=1, mode="sim",
                               trading_day="2026-06-16", qty=10, daily_limit=10) is True
    session.commit()
    # 不同 mode/不同交易日互不影響額度
    assert brepo.reserve_quota(session, reservation_id="r2", user_id=1, mode="real",
                               trading_day="2026-06-16", qty=10, daily_limit=10) is True
    assert brepo.reserve_quota(session, reservation_id="r3", user_id=1, mode="sim",
                               trading_day="2026-06-17", qty=10, daily_limit=10) is True
    session.commit()


def test_reserve_quota_replay_same_reservation_id_is_idempotent(session):
    first = brepo.reserve_quota(session, reservation_id="r1", user_id=1, mode="sim",
                                trading_day="2026-06-16", qty=6, daily_limit=10)
    session.commit()
    replay = brepo.reserve_quota(session, reservation_id="r1", user_id=1, mode="sim",
                                 trading_day="2026-06-16", qty=6, daily_limit=10)
    assert first is True and replay is True
    # 重放不應該讓已用配額被重複計算成 12（若真的重複插入會超過 daily_limit=10 但這裡刻意
    # 用等於 limit 的第二筆驗證「用掉的還是 6」）
    assert brepo.quota_used_today(session, user_id=1, mode="sim", trading_day="2026-06-16") == 6


def test_reserve_confirm_then_repeat_confirm_is_noop(session):
    assert brepo.reserve_quota(session, reservation_id="r1", user_id=1, mode="sim",
                               trading_day="2026-06-16", qty=1, daily_limit=10) is True
    session.commit()
    assert brepo.confirm_quota(session, reservation_id="r1") is True
    session.commit()
    assert brepo.confirm_quota(session, reservation_id="r1") is False  # 已是終態，不重複生效
    session.commit()
    row = session.exec(select(QuotaReservation).where(QuotaReservation.reservation_id == "r1")).first()
    assert row.state == "confirmed"


def test_reserve_release_then_repeat_release_is_noop(session):
    assert brepo.reserve_quota(session, reservation_id="r1", user_id=1, mode="sim",
                               trading_day="2026-06-16", qty=5, daily_limit=10) is True
    session.commit()
    assert brepo.release_quota(session, reservation_id="r1") is True
    session.commit()
    assert brepo.release_quota(session, reservation_id="r1") is False  # 已是終態，不重複生效
    session.commit()
    row = session.exec(select(QuotaReservation).where(QuotaReservation.reservation_id == "r1")).first()
    assert row.state == "released"
    assert brepo.quota_used_today(session, user_id=1, mode="sim", trading_day="2026-06-16") == 0

    # 釋放後配額真的還回去了：可以再保留滿額
    assert brepo.reserve_quota(session, reservation_id="r2", user_id=1, mode="sim",
                               trading_day="2026-06-16", qty=10, daily_limit=10) is True


def test_confirmed_reservation_cannot_be_released(session):
    """confirmed 是終態：委託已確認送出後，即使後續要處理失敗，也不可以透過 release
    偷偷把已經真實發生的委託配額退還（round3 #4：confirm/release 必須各自只能從
    reserved 轉移一次，且互斥終態不可逆）。"""
    assert brepo.reserve_quota(session, reservation_id="r1", user_id=1, mode="sim",
                               trading_day="2026-06-16", qty=1, daily_limit=10) is True
    session.commit()
    assert brepo.confirm_quota(session, reservation_id="r1") is True
    session.commit()
    assert brepo.release_quota(session, reservation_id="r1") is False
    session.commit()
    assert brepo.quota_used_today(session, user_id=1, mode="sim", trading_day="2026-06-16") == 1


def test_confirm_or_release_unknown_reservation_id_returns_false(session):
    assert brepo.confirm_quota(session, reservation_id="does-not-exist") is False
    assert brepo.release_quota(session, reservation_id="does-not-exist") is False


def test_quota_reservation_mode_and_state_check_constraints(session):
    bad_mode = QuotaReservation(reservation_id="x1", user_id=1, mode="paper",
                                trading_day="2026-06-16", qty=1, state="reserved")
    session.add(bad_mode)
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()

    bad_state = QuotaReservation(reservation_id="x2", user_id=1, mode="sim",
                                 trading_day="2026-06-16", qty=1, state="bogus")
    session.add(bad_state)
    with pytest.raises(IntegrityError):
        session.commit()


def test_reserve_quota_concurrent_two_writers_never_oversell(tmp_path):
    """round3 #4「並行兩 reserve 對日額 CAS 正確（不超賣）」——用真正的多執行緒 + 各自
    獨立 Session/連線打同一個檔案 SQLite（而非 in-memory StaticPool 共用單一連線的
    conftest session fixture，那樣測不出跨連線的寫入序列化行為），驗證
    INSERT...SELECT...WHERE 的 CAS 在真實併發下不超賣。"""
    db_path = tmp_path / "quota_concurrency.db"
    engine = create_engine(f"sqlite:///{db_path}", connect_args={"timeout": 30})
    SQLModel.metadata.create_all(engine)

    results: list[tuple[int, bool]] = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        with Session(engine) as s:
            ok = brepo.reserve_quota(
                s, reservation_id=f"r{i}", user_id=1, mode="sim",
                trading_day="2026-06-16", qty=6, daily_limit=10,
            )
            s.commit()  # repo 只 flush 不 commit（呼叫端負責交易邊界）——測試扮演呼叫端
            with lock:
                results.append((i, ok))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    successes = [i for i, ok in results if ok]
    assert len(successes) == 1  # qty=6 每筆，limit=10 → 同時最多只有一筆能成功

    with Session(engine) as s:
        total = brepo.quota_used_today(s, user_id=1, mode="sim", trading_day="2026-06-16")
    assert total == 6  # 絕不超過 daily_limit=10（更不可能是 5*6=30）


# ---- trading_day_for ----

def test_trading_day_for_cst_calendar_date():
    # 2026-06-16 04:00 UTC = 2026-06-16 12:00 CST（同一天）
    ts_ms = int(dt.datetime(2026, 6, 16, 4, 0, tzinfo=dt.timezone.utc).timestamp() * 1000)
    assert brepo.trading_day_for(ts_ms) == "2026-06-16"


# ---- 雙方言可攜 smoke（round3 #20：不能只 assert CREATE TABLE 不噴例外，
# 要逐項確認 CHECK/UniqueConstraint/BigInteger 真的出現在編譯出的 DDL 裡） ----

@pytest.mark.parametrize(
    "model,expected_fragments",
    [
        (RawInbox, ["raw_inbox"]),
        (Order, ["ck_orders_mode", "uq_orders_ordno_scope", "uq_orders_broker_order_id_scope"]),
        (Deal, ["uq_deal_fill", "ck_deals_mode"]),
        (BrokerPosition, [
            "ck_broker_positions_mode", "ck_broker_positions_direction",
            "ck_broker_positions_total_opened_qty_positive", "ck_broker_positions_closed_qty_range",
        ]),
        (OrderAudit, ["ck_order_audits_mode"]),
        (ConfirmToken, ["uq_confirm_tokens_jti"]),
        (QuotaReservation, [
            "uq_quota_reservations_reservation_id", "ck_quota_reservations_mode", "ck_quota_reservations_state",
        ]),
    ],
)
def test_new_tables_ddl_compiles_on_both_dialects_with_expected_constraints(model, expected_fragments):
    for dialect in (sqlite.dialect(), postgresql.dialect()):
        ddl = str(CreateTable(model.__table__).compile(dialect=dialect))
        for fragment in expected_fragments:
            assert fragment in ddl, f"{model.__name__} DDL missing {fragment!r} on {dialect.name}: {ddl}"


def test_broker_position_active_scope_partial_unique_index_compiles_on_both_dialects():
    from sqlalchemy.schema import CreateIndex

    idx = next(
        i for i in BrokerPosition.__table__.indexes if i.name == "uq_broker_positions_active_scope"
    )
    for dialect in (sqlite.dialect(), postgresql.dialect()):
        ddl = str(CreateIndex(idx).compile(dialect=dialect))
        assert "UNIQUE" in ddl.upper()
        assert "status = 'open'" in ddl


def test_epoch_ms_columns_use_biginteger_not_integer():
    """雙方言可攜鐵律：epoch-ms 欄位必用 BigInteger，否則 Postgres 4-byte INTEGER 會溢位。"""
    import sqlalchemy as sa

    assert isinstance(Deal.__table__.c.ts.type, sa.BigInteger)
    assert isinstance(OrderAudit.__table__.c.ts.type, sa.BigInteger)


# ---- Task 8：unknown 委託 reconcile 候選 / confirm token 清理 / 對帳 cursor ----

def test_list_unknown_orders_older_than_scopes_by_status_and_age(session):
    old = brepo.create_order(session, **_order_kwargs(client_order_id="U-OLD", request_hash="H1"))
    old.status = "unknown"
    old.updated_at = dt.datetime(2026, 6, 1, 0, 0)
    session.add(old)
    fresh = brepo.create_order(session, **_order_kwargs(client_order_id="U-FRESH", request_hash="H2"))
    fresh.status = "unknown"
    fresh.updated_at = dt.datetime(2026, 6, 16, 12, 30)  # 還沒過 grace period（晚於 cutoff）
    session.add(fresh)
    not_unknown = brepo.create_order(session, **_order_kwargs(client_order_id="U-OK", request_hash="H3"))
    session.commit()
    assert not_unknown.status != "unknown"

    stuck = brepo.list_unknown_orders_older_than(session, older_than=dt.datetime(2026, 6, 16, 12, 0))
    ids = {o.client_order_id for o in stuck}
    assert ids == {"U-OLD"}  # 太新的 unknown 還沒過 grace period，非 unknown 的不列入


def test_cleanup_expired_confirm_tokens_removes_expired_and_consumed_keeps_valid(session):
    brepo.create_confirm_token_row(
        session, jti="EXPIRED", actor_user_id=1, payload_hash="H1",
        expires_at=dt.datetime(2026, 6, 16, 11, 0),
    )
    brepo.create_confirm_token_row(
        session, jti="CONSUMED", actor_user_id=1, payload_hash="H1",
        expires_at=dt.datetime(2026, 6, 16, 13, 0),
    )
    brepo.create_confirm_token_row(
        session, jti="VALID", actor_user_id=1, payload_hash="H1",
        expires_at=dt.datetime(2026, 6, 16, 13, 0),
    )
    session.commit()
    now = dt.datetime(2026, 6, 16, 12, 0)
    assert brepo.claim_confirm_token(session, jti="CONSUMED", actor_user_id=1, payload_hash="H1", now=now) is True
    session.commit()

    removed = brepo.cleanup_expired_confirm_tokens(session, now=now)
    session.commit()
    assert removed == 2  # EXPIRED（過期）+ CONSUMED（已消費）

    remaining = {row.jti for row in session.exec(select(ConfirmToken))}
    assert remaining == {"VALID"}


def test_reconcile_cursor_get_defaults_none_then_upsert_roundtrips(session):
    assert brepo.get_reconcile_cursor(session, broker="shioaji", account="F1", mode="sim") is None

    at1 = dt.datetime(2026, 6, 16, 9, 0)
    brepo.upsert_reconcile_cursor(session, broker="shioaji", account="F1", mode="sim", at=at1)
    session.commit()
    assert brepo.get_reconcile_cursor(session, broker="shioaji", account="F1", mode="sim") == at1

    at2 = dt.datetime(2026, 6, 16, 9, 30)
    brepo.upsert_reconcile_cursor(session, broker="shioaji", account="F1", mode="sim", at=at2)
    session.commit()
    assert brepo.get_reconcile_cursor(session, broker="shioaji", account="F1", mode="sim") == at2
    # 另一個 scope（不同 account）互不影響
    assert brepo.get_reconcile_cursor(session, broker="shioaji", account="F2", mode="sim") is None
