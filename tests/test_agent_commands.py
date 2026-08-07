"""Task 8：AgentCommand repository＋兩維 CAS applier（G1 server 核心）。

涵蓋 brief 指定的測試清單（S#1/2/15/25/32）：
  - late ack 全鏈收斂（ordno/quota/quarantine/狀態）。
  - 重複 ack no-op（transport CAS 驗證）。
  - ack 先 vs timeout 先兩交錯（R1-1：Order 不得被逾時路徑改回 unknown；timeout 先標、
    late ack 後到仍可把 unknown 收斂為 submitted/failed）。
  - cancel/update kind×outcome 轉移表逐格。
  - 雙模式契約：四個純函式的效果與既有 in-process 寫回語意逐位元相同。
"""
import json
import threading
from datetime import datetime, timedelta

import pytest
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.broker.agent_commands import (
    apply_cancel_ack,
    apply_command_ack,
    apply_place_ack,
    apply_place_failure,
    apply_update_ack,
    insert_command,
    list_unresolved_cancels,
    list_unresolved_for_replay,
    list_unresolved_unknown_updates,
    mark_timeout_observed,
    new_command,
    prepare_replay,
    resolve_never_dispatched,
    resolve_unresolved_cancel_via_report,
    resolve_update_via_query_qty,
    to_downlink_dict,
)
from quanquant.broker.agent_protocol import UpCmdAck
from quanquant.db.models import AgentCommand, Order, QuotaReservation, RawInbox

BROKER, ACCOUNT, MODE = "shioaji", "F1", "sim"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _make_order(session: Session, *, client_order_id="c-1", user_id=1, ordno=None,
                 status="pending", qty=1, price="21500") -> Order:
    order = brepo.create_order(
        session, client_order_id=client_order_id, request_hash="H", user_id=user_id,
        mode=MODE, broker=BROKER, account=ACCOUNT, symbol="TXF", action="Buy", qty=qty,
        price=price, price_type="LMT", order_type="ROD", octype="Auto",
        trading_day="2026-08-07",
    )
    if ordno is not None:
        order.ordno = ordno
        order.broker_order_id = ordno
    if status != "pending":
        order.status = status
    session.add(order)
    session.commit()
    session.refresh(order)
    return order


def _reserve(session: Session, *, reservation_id, user_id=1, qty=1) -> None:
    ok = brepo.reserve_quota(
        session, reservation_id=reservation_id, user_id=user_id, mode=MODE,
        trading_day="2026-08-07", qty=qty, daily_limit=100,
    )
    assert ok
    session.commit()


def _quota_state(session: Session, reservation_id: str) -> str:
    row = session.exec(
        select(QuotaReservation).where(QuotaReservation.reservation_id == reservation_id)
    ).one()
    return row.state


def _place_cmd(session: Session, *, cmd_id="cmd-1", client_order_id="c-1", user_id=1,
                reservation_id="c-1") -> AgentCommand:
    cmd = AgentCommand(
        cmd_id=cmd_id, user_id=user_id, kind="place", broker=BROKER, account=ACCOUNT, mode=MODE,
        client_order_id=client_order_id, reservation_id=reservation_id,
        payload=json.dumps({"action": "Buy", "price": "21500", "qty": 1, "price_type": "LMT",
                             "order_type": "ROD", "octype": "Auto"}),
        created_at=datetime(2026, 8, 7, 9, 0), expires_at=datetime(2026, 8, 7, 9, 2),
    )
    insert_command(session, cmd=cmd)
    session.commit()
    return cmd


def _cancel_cmd(session: Session, *, cmd_id="cmd-2", ordno="O1", user_id=1) -> AgentCommand:
    cmd = AgentCommand(
        cmd_id=cmd_id, user_id=user_id, kind="cancel", broker=BROKER, account=ACCOUNT, mode=MODE,
        ordno=ordno, payload=json.dumps({}),
        created_at=datetime(2026, 8, 7, 9, 0), expires_at=datetime(2026, 8, 7, 9, 2),
    )
    insert_command(session, cmd=cmd)
    session.commit()
    return cmd


def _update_cmd(session: Session, *, cmd_id="cmd-3", ordno="O1", user_id=1,
                 reservation_id=None, price="21600", qty=2) -> AgentCommand:
    cmd = AgentCommand(
        cmd_id=cmd_id, user_id=user_id, kind="update", broker=BROKER, account=ACCOUNT, mode=MODE,
        ordno=ordno, client_order_id="c-1", reservation_id=reservation_id,
        payload=json.dumps({"price": price, "qty": qty, "price_type": "LMT"}),
        created_at=datetime(2026, 8, 7, 9, 0), expires_at=datetime(2026, 8, 7, 9, 2),
    )
    insert_command(session, cmd=cmd)
    session.commit()
    return cmd


def _ok_ack(cmd_id, **result) -> UpCmdAck:
    return UpCmdAck(cmd_id=cmd_id, event_id=1, ok=True, result=result or None)


def _err_ack(cmd_id, *, error_kind, message="拒絕") -> UpCmdAck:
    return UpCmdAck(cmd_id=cmd_id, event_id=1, ok=False, error_kind=error_kind, message=message)


# ---------------------------------------------------------------------------
# insert_command / new_command
# ---------------------------------------------------------------------------


def test_new_command_sets_cmd_id_and_expiry():
    cmd = new_command(kind="place", user_id=1, broker=BROKER, account=ACCOUNT, mode=MODE,
                       payload={"a": 1}, client_order_id="c-1", reservation_id="c-1")
    assert cmd.cmd_id and cmd.kind == "place"
    assert cmd.expires_at - cmd.created_at == timedelta(seconds=120)
    assert json.loads(cmd.payload) == {"a": 1}


def test_insert_command_persists_row(session, engine):
    cmd = new_command(kind="place", user_id=1, broker=BROKER, account=ACCOUNT, mode=MODE,
                       payload={}, client_order_id="c-1", reservation_id="c-1")
    insert_command(session, cmd=cmd)
    session.commit()
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        assert row is not None and row.outcome is None and row.resolved_at is None


def test_has_unresolved_update_command_false_for_unrelated_integrity_error(session):
    """Task 8 修復（round 1）負向覆蓋（codex R6-3）：`repository.has_unresolved_update_command`
    必須精確辨認撞到的是不是 `uq_agent_cmd_update_singleflight` 這個 partial unique index——
    這裡故意撞 `AgentCommand` 主鍵 `cmd_id`（與 update-singleflight 完全無關的另一種
    `IntegrityError`），驗證：① 對這個撞鍵無關的 `client_order_id` 回 False；② 鏡射
    `shioaji_adapter.update()` 決策段的實際 try/except 寫法，確認這種情況下原例外會被原樣
    上拋，不會被誤轉成「前一筆改單結果未定」的友善訊息（那句訊息只該在真的撞到
    update-singleflight 時出現）。"""
    first = new_command(kind="place", user_id=1, broker=BROKER, account=ACCOUNT, mode=MODE,
                         payload={}, client_order_id="c-pk-1")
    insert_command(session, cmd=first)
    session.commit()

    colliding = new_command(kind="place", user_id=1, broker=BROKER, account=ACCOUNT, mode=MODE,
                             payload={}, client_order_id="c-pk-2")
    colliding.cmd_id = first.cmd_id  # 故意撞主鍵，不是 update-singleflight 那個 partial index

    class _MisclassifiedAsSingleflight(Exception):
        """僅供本測試辨識「誤判成單飛撞鍵」用，不對應任何生產例外型別。"""

    with pytest.raises(IntegrityError):
        try:
            insert_command(session, cmd=colliding)
            session.commit()
        except IntegrityError:
            session.rollback()
            if brepo.has_unresolved_update_command(session, client_order_id="c-pk-2"):
                raise _MisclassifiedAsSingleflight(
                    "PK 撞鍵不該被誤判成 update-singleflight 撞鍵"
                ) from None
            raise


# ---------------------------------------------------------------------------
# S#1: late ack 全鏈收斂（place：ordno/quota/quarantine/狀態）
# ---------------------------------------------------------------------------


def test_late_ack_converges_ordno_quota_quarantine_and_status(session, engine):
    _make_order(session, status="unknown")  # 已被 route timeout fail-safe 標成 unknown
    _reserve(session, reservation_id="c-1")
    cmd = _place_cmd(session)

    # 一筆與這個 user/account 相關的 association_pending quarantine 殘留（ordno 尚未補寫時
    # worker 解不到關聯而卡住）——late ack 補上 ordno 後應該被解除。
    row = brepo.stage_raw_inbox(session, kind="order_report", broker=BROKER, payload="{}",
                                 user_id=1, account=ACCOUNT, mode=MODE)
    brepo.quarantine_raw_inbox(session, row, error="ordno 未知", reason="association_pending")
    session.commit()

    ack = _ok_ack(cmd.cmd_id, ordno="101AA1", broker_order_id="101AA1")
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1, ack=ack)

    assert outcome.applied and outcome.outcome == "ok" and outcome.resolved
    with Session(engine) as s2:
        o = s2.exec(select(Order)).one()
        assert o.status == "submitted" and o.ordno == "101AA1" and o.broker_order_id == "101AA1"
        assert _quota_state(s2, "c-1") == "confirmed"
        inbox = s2.exec(select(RawInbox)).one()
        assert inbox.quarantine is False and inbox.quarantine_reason is None
        cmd_row = s2.get(AgentCommand, cmd.cmd_id)
        assert cmd_row.transport_acked_at is not None
        assert cmd_row.resolved_via == "ack"


# ---------------------------------------------------------------------------
# S#2: 重複 ack no-op
# ---------------------------------------------------------------------------


def test_duplicate_ack_is_noop(session, engine):
    _make_order(session)
    _reserve(session, reservation_id="c-1")
    cmd = _place_cmd(session)

    ack1 = _ok_ack(cmd.cmd_id, ordno="101AA1", broker_order_id="101AA1")
    first = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1, ack=ack1)
    assert first.transport_won and first.applied

    ack2 = _ok_ack(cmd.cmd_id, ordno="999ZZ", broker_order_id="999ZZ")  # 若重套會腐化資料
    second = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1, ack=ack2)
    assert second.transport_won is False and second.applied is False

    with Session(engine) as s2:
        o = s2.exec(select(Order)).one()
        assert o.ordno == "101AA1"  # 第二次 ack 沒有腐化掉第一次的結果


def test_user_mismatch_rejected_without_writes(session, engine):
    _make_order(session)
    _reserve(session, reservation_id="c-1")
    cmd = _place_cmd(session, user_id=1)

    ack = _ok_ack(cmd.cmd_id, ordno="101AA1", broker_order_id="101AA1")
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=2, ack=ack)
    assert outcome.found and outcome.user_mismatch and not outcome.applied

    with Session(engine) as s2:
        cmd_row = s2.get(AgentCommand, cmd.cmd_id)
        assert cmd_row.transport_acked_at is None and cmd_row.resolved_at is None


def test_not_found_cmd_id_returns_found_false(engine):
    outcome = apply_command_ack(lambda: Session(engine), cmd_id="nope", user_id=1,
                                 ack=_ok_ack("nope"))
    assert outcome.found is False


# ---------------------------------------------------------------------------
# S#15/R1-1: ack 先 vs timeout 先兩交錯
# ---------------------------------------------------------------------------


def test_ack_first_then_route_timeout_loses_cas_order_not_reverted(session, engine):
    """ack 先落庫、route timeout 後到 → mark_timeout_observed 必須落空，Order 不得被
    route 的逾時分支改回 unknown。"""
    _make_order(session)
    _reserve(session, reservation_id="c-1")
    cmd = _place_cmd(session)

    ack = _ok_ack(cmd.cmd_id, ordno="101AA1", broker_order_id="101AA1")
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1, ack=ack)
    assert outcome.applied and outcome.outcome == "ok"

    with Session(engine) as s2:
        won = mark_timeout_observed(s2, cmd_id=cmd.cmd_id)
        s2.commit()
    assert won is False  # ack 已勝出——route 不得再視為逾時、不得寫 unknown

    with Session(engine) as s2:
        assert s2.exec(select(Order)).one().status == "submitted"  # 未被腐化回 unknown


def test_route_timeout_first_then_late_ack_converges_unknown_to_submitted(session, engine):
    """timeout 先標（route 贏得 CAS，Order 標 unknown）→ late ack 後到 → applier 仍可把
    unknown 收斂為 submitted（狀態機明文允許 unknown→非 filled 的任何狀態）。"""
    order = _make_order(session)
    _reserve(session, reservation_id="c-1")
    cmd = _place_cmd(session)

    with Session(engine) as s2:
        won = mark_timeout_observed(s2, cmd_id=cmd.cmd_id)
        assert won is True
        o = s2.get(Order, order.id)
        apply_place_failure(s2, o, reservation_id=None, status="unknown")
        s2.commit()
    with Session(engine) as s2:
        assert s2.exec(select(Order)).one().status == "unknown"

    ack = _ok_ack(cmd.cmd_id, ordno="101AA1", broker_order_id="101AA1")
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1, ack=ack)
    assert outcome.applied and outcome.outcome == "ok"
    with Session(engine) as s2:
        o = s2.exec(select(Order)).one()
        assert o.status == "submitted" and o.ordno == "101AA1"
        assert _quota_state(s2, "c-1") == "confirmed"


def test_second_mark_timeout_observed_is_noop(session, engine):
    _make_order(session)
    cmd = _place_cmd(session, reservation_id=None)
    with Session(engine) as s2:
        assert mark_timeout_observed(s2, cmd_id=cmd.cmd_id) is True
        s2.commit()
    with Session(engine) as s2:
        assert mark_timeout_observed(s2, cmd_id=cmd.cmd_id) is False  # 第二次不得再贏


# ---------------------------------------------------------------------------
# S#32: 四交錯——ack-first/resolver-first(本 task 以 resolve_never_dispatched 代表本地終結)
#        /並發(等價重複 ack)/斷線間（timeout 先標，之後仍可被別的路徑終結）——效果恰一次
# ---------------------------------------------------------------------------


def test_local_resolution_then_late_ack_is_noop_on_business_dimension(session, engine):
    """route 判定『從未送達 agent』先本地終結（resolve_never_dispatched）——之後若真的有
    一筆遲到 ack 抵達（理論上不該發生，但防禦性驗證 CAS 邊界），只補 transport，不改 outcome。"""
    _make_order(session, status="failed")
    cmd = _place_cmd(session, reservation_id=None)
    with Session(engine) as s2:
        assert resolve_never_dispatched(s2, cmd_id=cmd.cmd_id, message="agent 未連線") is True
        s2.commit()

    ack = _ok_ack(cmd.cmd_id, ordno="101AA1", broker_order_id="101AA1")
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1, ack=ack)
    assert outcome.transport_won is True  # 這是第一次 transport ack
    assert outcome.already_resolved is True and outcome.applied is False
    assert outcome.outcome == "error"  # 維持本地終結時寫入的 outcome，不被 late ack 改寫

    with Session(engine) as s2:
        assert s2.exec(select(Order)).one().status == "failed"  # 未被 late ack 重套成功效果


def test_second_resolve_never_dispatched_is_noop(session, engine):
    cmd = _place_cmd(session, reservation_id=None)
    with Session(engine) as s2:
        assert resolve_never_dispatched(s2, cmd_id=cmd.cmd_id, message="m1") is True
        s2.commit()
    with Session(engine) as s2:
        assert resolve_never_dispatched(s2, cmd_id=cmd.cmd_id, message="m2") is False
        s2.commit()
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        assert json.loads(row.result)["message"] == "m1"


# ---------------------------------------------------------------------------
# place 轉移表逐格
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("error_kind", ["trade_not_found", "exception", "mode_mismatch",
                                        "expired", "scope_mismatch"])
def test_place_explicit_reject_marks_failed_and_releases(session, engine, error_kind):
    _make_order(session)
    _reserve(session, reservation_id="c-1")
    cmd = _place_cmd(session)
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1,
                                 ack=_err_ack(cmd.cmd_id, error_kind=error_kind))
    assert outcome.applied and outcome.outcome == "error" and outcome.resolved
    with Session(engine) as s2:
        assert s2.exec(select(Order)).one().status == "failed"
        assert _quota_state(s2, "c-1") == "released"


def test_place_timeout_marks_unknown_keeps_quota_reserved_and_not_resolved(session, engine):
    _make_order(session)
    _reserve(session, reservation_id="c-1")
    cmd = _place_cmd(session)
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1,
                                 ack=_err_ack(cmd.cmd_id, error_kind="timeout"))
    assert outcome.applied and outcome.outcome == "unknown" and outcome.resolved is False
    with Session(engine) as s2:
        assert s2.exec(select(Order)).one().status == "unknown"
        assert _quota_state(s2, "c-1") == "reserved"  # 永不自動 release


def test_place_ack_with_unrecognized_error_kind_falls_back_to_unknown_fail_safe(session, engine):
    """R2-4/brief 附註：applier 對 error_kind 未涵蓋值要 fail-safe 處理——寧可 unknown 也不
    誤判 failed 而提前 release 配額。"""
    _make_order(session)
    _reserve(session, reservation_id="c-1")
    cmd = _place_cmd(session)
    ack = UpCmdAck(cmd_id=cmd.cmd_id, event_id=1, ok=False, error_kind=None, message="??")
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1, ack=ack)
    assert outcome.outcome == "unknown" and outcome.resolved is False
    with Session(engine) as s2:
        assert _quota_state(s2, "c-1") == "reserved"


# ---------------------------------------------------------------------------
# cancel 轉移表逐格
# ---------------------------------------------------------------------------


def test_cancel_ok_marks_cancelled_no_quota_effect(session, engine):
    _make_order(session, ordno="O1", status="submitted")
    cmd = _cancel_cmd(session, ordno="O1")
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1,
                                 ack=_ok_ack(cmd.cmd_id))
    assert outcome.applied and outcome.outcome == "ok"
    with Session(engine) as s2:
        assert s2.exec(select(Order)).one().status == "cancelled"


def test_cancel_ok_on_already_terminal_order_is_noop_via_transition_guard(session, engine):
    _make_order(session, ordno="O1", status="filled")
    cmd = _cancel_cmd(session, ordno="O1")
    apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1, ack=_ok_ack(cmd.cmd_id))
    with Session(engine) as s2:
        assert s2.exec(select(Order)).one().status == "filled"  # 已終態，不被 cancel ack 腐化


@pytest.mark.parametrize("error_kind", ["trade_not_found", "expired", "scope_mismatch", "timeout"])
def test_cancel_non_ok_never_touches_order_status(session, engine, error_kind):
    _make_order(session, ordno="O1", status="submitted")
    cmd = _cancel_cmd(session, ordno="O1")
    apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1,
                       ack=_err_ack(cmd.cmd_id, error_kind=error_kind))
    with Session(engine) as s2:
        assert s2.exec(select(Order)).one().status == "submitted"  # 一律不改


def test_cancel_explicit_reject_resolves_and_audits(session, engine):
    _make_order(session, ordno="O1", status="submitted")
    cmd = _cancel_cmd(session, ordno="O1")
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1,
                                 ack=_err_ack(cmd.cmd_id, error_kind="trade_not_found"))
    assert outcome.resolved and outcome.outcome == "error"
    with Session(engine) as s2:
        from quanquant.db.models import OrderAudit
        audits = list(s2.exec(select(OrderAudit)))
        assert any(a.action == "cancel" for a in audits)


def test_cancel_timeout_leaves_unresolved(session, engine):
    _make_order(session, ordno="O1", status="submitted")
    cmd = _cancel_cmd(session, ordno="O1")
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1,
                                 ack=_err_ack(cmd.cmd_id, error_kind="timeout"))
    assert outcome.resolved is False and outcome.outcome == "unknown"


# ---------------------------------------------------------------------------
# update 轉移表逐格
# ---------------------------------------------------------------------------


def test_update_ok_writes_price_qty_and_confirms_delta(session, engine):
    _make_order(session, ordno="O1", status="submitted", qty=1, price="21500")
    _reserve(session, reservation_id="delta-1", qty=1)
    cmd = _update_cmd(session, ordno="O1", reservation_id="delta-1", price="21600", qty=2)
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1,
                                 ack=_ok_ack(cmd.cmd_id))
    assert outcome.applied and outcome.outcome == "ok"
    with Session(engine) as s2:
        o = s2.exec(select(Order)).one()
        assert o.qty == 2 and str(o.price) == "21600" and o.status == "submitted"
        assert _quota_state(s2, "delta-1") == "confirmed"


@pytest.mark.parametrize("error_kind", ["trade_not_found", "exception", "mode_mismatch",
                                        "expired", "scope_mismatch"])
def test_update_explicit_reject_only_releases_delta_does_not_mark_failed(session, engine, error_kind):
    _make_order(session, ordno="O1", status="submitted", qty=1, price="21500")
    _reserve(session, reservation_id="delta-1", qty=1)
    cmd = _update_cmd(session, ordno="O1", reservation_id="delta-1")
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1,
                                 ack=_err_ack(cmd.cmd_id, error_kind=error_kind))
    assert outcome.resolved and outcome.outcome == "error"
    with Session(engine) as s2:
        o = s2.exec(select(Order)).one()
        assert o.status == "submitted" and o.qty == 1  # 整張單仍有效、不標 failed、不改內容
        assert _quota_state(s2, "delta-1") == "released"


def test_update_timeout_does_not_touch_order_delta_reserved(session, engine):
    _make_order(session, ordno="O1", status="submitted", qty=1, price="21500")
    _reserve(session, reservation_id="delta-1", qty=1)
    cmd = _update_cmd(session, ordno="O1", reservation_id="delta-1")
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1,
                                 ack=_err_ack(cmd.cmd_id, error_kind="timeout"))
    assert outcome.resolved is False and outcome.outcome == "unknown"
    with Session(engine) as s2:
        o = s2.exec(select(Order)).one()
        assert o.status == "submitted" and o.qty == 1  # 不改
        assert _quota_state(s2, "delta-1") == "reserved"  # delta 保留（D8 保護）


def test_update_ok_without_delta_reservation_no_quota_call(session, engine):
    """減量改單（無 delta 保留列）——ack ok 仍要正確寫 price/qty，只是不 confirm 任何配額。"""
    _make_order(session, ordno="O1", status="submitted", qty=3, price="21500")
    cmd = _update_cmd(session, ordno="O1", reservation_id=None, price="21500", qty=1)
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1,
                                 ack=_ok_ack(cmd.cmd_id))
    assert outcome.applied
    with Session(engine) as s2:
        assert s2.exec(select(Order)).one().qty == 1


# ---------------------------------------------------------------------------
# 雙模式契約：四個純函式與既有 in-process 寫回語意逐位元相同
# ---------------------------------------------------------------------------


def test_apply_place_ack_matches_inprocess_writeback_semantics(session, engine):
    order = _make_order(session)
    _reserve(session, reservation_id="c-1")
    with Session(engine) as s2:
        o = s2.get(Order, order.id)
        apply_place_ack(s2, o, ordno="101AA1", broker_order_id="101AA1", reservation_id="c-1")
        s2.commit()
    with Session(engine) as s2:
        o = s2.exec(select(Order)).one()
        assert o.status == "submitted" and o.ordno == "101AA1" and o.broker_order_id == "101AA1"
        assert _quota_state(s2, "c-1") == "confirmed"


def test_apply_place_failure_failed_status_releases_quota(session, engine):
    order = _make_order(session)
    _reserve(session, reservation_id="c-1")
    with Session(engine) as s2:
        o = s2.get(Order, order.id)
        apply_place_failure(s2, o, reservation_id="c-1", status="failed")
        s2.commit()
    with Session(engine) as s2:
        assert s2.exec(select(Order)).one().status == "failed"
        assert _quota_state(s2, "c-1") == "released"


def test_apply_place_failure_unknown_status_keeps_quota(session, engine):
    order = _make_order(session)
    _reserve(session, reservation_id="c-1")
    with Session(engine) as s2:
        o = s2.get(Order, order.id)
        apply_place_failure(s2, o, reservation_id="c-1", status="unknown")
        s2.commit()
    with Session(engine) as s2:
        assert s2.exec(select(Order)).one().status == "unknown"
        assert _quota_state(s2, "c-1") == "reserved"


def test_apply_cancel_ack_matches_inprocess_semantics(session, engine):
    order = _make_order(session, ordno="O1", status="submitted")
    with Session(engine) as s2:
        o = s2.get(Order, order.id)
        apply_cancel_ack(s2, o)
        s2.commit()
    with Session(engine) as s2:
        assert s2.exec(select(Order)).one().status == "cancelled"


def test_apply_update_ack_matches_inprocess_semantics(session, engine):
    order = _make_order(session, ordno="O1", status="submitted", qty=1, price="21500")
    _reserve(session, reservation_id="delta-1", qty=1)
    with Session(engine) as s2:
        o = s2.get(Order, order.id)
        from decimal import Decimal
        apply_update_ack(s2, o, new_price=Decimal("21700"), new_qty=4, reservation_id="delta-1")
        s2.commit()
    with Session(engine) as s2:
        o = s2.exec(select(Order)).one()
        assert o.qty == 4 and str(o.price) == "21700" and o.status == "submitted"
        assert _quota_state(s2, "delta-1") == "confirmed"


# ---------------------------------------------------------------------------
# Task 10：重連補送（限同 scope；D4）—— list_unresolved_for_replay / to_downlink_dict /
# prepare_replay（S#3/5/16）
# ---------------------------------------------------------------------------


def _cmd_row(*, cmd_id, kind="cancel", user_id=1, account=ACCOUNT, ordno="O1",
             client_order_id=None, reservation_id=None, payload="{}",
             created_at=datetime(2026, 8, 7, 9, 0), expires_at=datetime(2099, 1, 1),
             transport_acked_at=None, outcome=None, resolved_via=None, resolved_at=None) -> AgentCommand:
    return AgentCommand(
        cmd_id=cmd_id, user_id=user_id, kind=kind, broker=BROKER, account=account, mode=MODE,
        ordno=ordno, client_order_id=client_order_id, reservation_id=reservation_id,
        payload=payload, created_at=created_at, expires_at=expires_at,
        transport_acked_at=transport_acked_at, outcome=outcome, resolved_via=resolved_via,
        resolved_at=resolved_at,
    )


def test_list_unresolved_for_replay_filters_by_user_and_account(session):
    """R1-2：只掃這個 user、這次登入綁定帳號的未 transport-ack/未 resolved 指令——別的 user、
    或同一個 user 名下別的帳號（換帳號後的舊帳號殘留），一律不下行（S#16）。"""
    matching = _cmd_row(cmd_id="cmd-match", ordno="O1")
    other_account = _cmd_row(cmd_id="cmd-other-acct", account="OTHER", ordno="O2")
    other_user = _cmd_row(cmd_id="cmd-other-user", user_id=2, ordno="O3")
    for c in (matching, other_account, other_user):
        insert_command(session, cmd=c)
    session.commit()

    rows = list_unresolved_for_replay(session, user_id=1, account=ACCOUNT)
    assert [r.cmd_id for r in rows] == ["cmd-match"]


def test_list_unresolved_for_replay_excludes_transport_acked_or_resolved(session):
    """`outcome='unknown'` 天然被 `transport_acked_at IS NULL` 排除（timeout ack 已寫過
    transport_acked_at）；已 resolved（如 report 先收斂的 cancel）同樣不重播——只剩『從未
    收到任何 ack』的列。"""
    acked = _cmd_row(cmd_id="cmd-acked", ordno="O1", transport_acked_at=datetime(2026, 8, 7, 9, 5))
    resolved = _cmd_row(cmd_id="cmd-resolved", ordno="O2", outcome="unknown",
                        resolved_via="report", resolved_at=datetime(2026, 8, 7, 9, 6))
    pending = _cmd_row(cmd_id="cmd-pending", ordno="O3")
    for c in (acked, resolved, pending):
        insert_command(session, cmd=c)
    session.commit()

    rows = list_unresolved_for_replay(session, user_id=1, account=ACCOUNT)
    assert [r.cmd_id for r in rows] == ["cmd-pending"]


def test_list_unresolved_for_replay_includes_already_expired_command_server_does_not_auto_resolve(session, engine):
    """S#5：server 不自主過期——`expires_at` 早已過去、但仍 `resolved_at IS NULL` 的指令，
    重連補送查詢仍照常回傳（是否過期只由 agent 收到重播後自行判斷），且這個查詢本身完全不
    改寫 outcome/resolved_at（server 沒有任何背景工作單方面判死這筆指令；配額跨 trading_day
    歸零由既有 `quota_used_today` 的 trading_day 過濾天然成立，不需要另外測）。"""
    cmd = _cmd_row(cmd_id="cmd-expired", kind="place", ordno=None, client_order_id="c-expired",
                   payload=json.dumps({"action": "Buy", "price": "21500", "qty": 1,
                                        "price_type": "LMT", "order_type": "ROD", "octype": "Auto"}),
                   created_at=datetime(2020, 1, 1, 9, 0), expires_at=datetime(2020, 1, 1, 9, 2))
    insert_command(session, cmd=cmd)
    session.commit()

    rows = list_unresolved_for_replay(session, user_id=1, account=ACCOUNT)
    assert [r.cmd_id for r in rows] == ["cmd-expired"]

    with Session(engine) as s2:
        row = s2.get(AgentCommand, "cmd-expired")
        assert row.resolved_at is None and row.outcome is None  # 沒有背景工作判死它


def test_to_downlink_dict_reconstructs_place_cancel_update():
    place = _cmd_row(cmd_id="cmd-p", kind="place", ordno=None, client_order_id="c-p",
                     payload=json.dumps({"action": "Buy", "price": "21500", "qty": 1,
                                          "price_type": "LMT", "order_type": "ROD", "octype": "Auto"}),
                     expires_at=datetime(2026, 8, 7, 9, 2))
    assert to_downlink_dict(place) == {
        "type": "place", "cmd_id": "cmd-p", "account": ACCOUNT, "mode": MODE,
        "expires_at": "2026-08-07T09:02:00",
        "native": {"action": "Buy", "price": "21500", "qty": 1, "price_type": "LMT",
                   "order_type": "ROD", "octype": "Auto"},
    }

    cancel = _cmd_row(cmd_id="cmd-c", kind="cancel", ordno="O1", expires_at=datetime(2026, 8, 7, 9, 3))
    assert to_downlink_dict(cancel) == {
        "type": "cancel", "cmd_id": "cmd-c", "account": ACCOUNT, "mode": MODE,
        "expires_at": "2026-08-07T09:03:00", "ordno": "O1",
    }

    update = _cmd_row(cmd_id="cmd-u", kind="update", ordno="O2", client_order_id="c-u",
                      payload=json.dumps({"price": "21600", "qty": 2, "price_type": "LMT"}),
                      expires_at=datetime(2026, 8, 7, 9, 4))
    assert to_downlink_dict(update) == {
        "type": "update", "cmd_id": "cmd-u", "account": ACCOUNT, "mode": MODE,
        "expires_at": "2026-08-07T09:04:00", "ordno": "O2", "price": "21600", "qty": 2,
        "price_type": "LMT",
    }


def test_prepare_replay_orders_by_created_at_and_marks_sent_at(session, engine):
    place = _cmd_row(cmd_id="cmd-p", kind="place", ordno=None, client_order_id="c-p",
                     payload=json.dumps({"action": "Buy", "price": "21500", "qty": 1,
                                          "price_type": "LMT", "order_type": "ROD", "octype": "Auto"}),
                     created_at=datetime(2026, 8, 7, 9, 0))
    cancel = _cmd_row(cmd_id="cmd-c", kind="cancel", ordno="O1", created_at=datetime(2026, 8, 7, 9, 1))
    update = _cmd_row(cmd_id="cmd-u", kind="update", ordno="O2", client_order_id="c-u",
                      payload=json.dumps({"price": "21600", "qty": 2, "price_type": "LMT"}),
                      created_at=datetime(2026, 8, 7, 9, 2))
    for c in (update, place, cancel):  # 故意亂序 insert，驗證回傳仍按 created_at 排序
        insert_command(session, cmd=c)
    session.commit()

    dicts = prepare_replay(lambda: Session(engine), user_id=1, account=ACCOUNT)
    assert [d["cmd_id"] for d in dicts] == ["cmd-p", "cmd-c", "cmd-u"]
    assert dicts[0]["type"] == "place" and dicts[1]["type"] == "cancel" and dicts[2]["type"] == "update"

    with Session(engine) as s2:
        for cmd_id in ("cmd-p", "cmd-c", "cmd-u"):
            assert s2.get(AgentCommand, cmd_id).sent_at is not None


def test_prepare_replay_empty_when_nothing_unresolved(engine):
    assert prepare_replay(lambda: Session(engine), user_id=1, account=ACCOUNT) == []


# ---------------------------------------------------------------------------
# Task 10：R5-2/R6-1（S#36/38）—— update 單飛在真並發下恰一成功
# ---------------------------------------------------------------------------


def test_update_singleflight_concurrent_two_writers_exactly_one_wins(tmp_path):
    """兩個獨立執行緒＋獨立連線，對同一個 `client_order_id` 同時嘗試插入
    `kind='update' AND resolved_at IS NULL` 的 ledger 列——`uq_agent_cmd_update_singleflight`
    partial unique index 必須恰讓一個成功，另一個撞 `IntegrityError`；鏡射
    `shioaji_adapter.update()` 決策段的實際 try/except 寫法（同
    `test_has_unresolved_update_command_false_for_unrelated_integrity_error` 既有手法：直接在
    測試裡重演 route 的 insert→except IntegrityError→`has_unresolved_update_command` 精確
    辨認流程，而非經過完整 adapter/RiskGuard）。

    用 `threading.Barrier` 讓兩執行緒盡量同時打 insert，驗證的是這個 partial unique index
    在**真正的執行緒級競爭**下（非序列呼叫）仍然成立——比 Task 8 既有的
    `test_update_singleflight_rejects_second_unresolved_update`（序列：第一筆先完全結束才打
    第二筆）多驗一層時序保證。改用檔案 SQLite（獨立連線）而非 in-memory：StaticPool 共用
    單一底層連線會讓兩執行緒同時 flush 產生與本測試無關的 identity-map 交錯錯誤（比照
    `tests/test_risk_guard.py::test_cas_quota_blocks_one_of_two_concurrent_places` 既有理由）。

    Postgres 方言：`uq_agent_cmd_update_singleflight` 這個 partial unique index 的 DDL 已在
    `tests/test_agent_models.py::test_agent_command_update_singleflight_index_compiles_on_
    both_dialects` 驗證兩方言編譯等價；本機無 Postgres 服務可跑執行期測試，而唯一鍵語意在
    兩方言下皆是 DB 強制（非 best-effort、非僅 sqlite 特有行為），故 SQLite 執行期真並發＋
    兩方言 DDL 編譯等價已足以涵蓋 R5-2 的並發保證，不偽造一個本機不存在的 Postgres 服務。
    """
    from sqlmodel import SQLModel, create_engine

    db_path = tmp_path / "update_singleflight_concurrency.db"
    eng = create_engine(f"sqlite:///{db_path}", connect_args={"timeout": 30})
    SQLModel.metadata.create_all(eng)

    with Session(eng) as s:
        _make_order(s, client_order_id="c-race", ordno="O-RACE")

    results: list[tuple[str, str]] = []
    barrier = threading.Barrier(2)

    def _attempt(cmd_id: str) -> None:
        with Session(eng) as s:
            cmd = AgentCommand(
                cmd_id=cmd_id, user_id=1, kind="update", broker=BROKER, account=ACCOUNT, mode=MODE,
                client_order_id="c-race", ordno="O-RACE",
                payload=json.dumps({"price": "21600", "qty": 2, "price_type": "LMT"}),
                created_at=datetime(2026, 8, 7, 9, 0), expires_at=datetime(2099, 1, 1),
            )
            barrier.wait(timeout=5)
            try:
                insert_command(s, cmd=cmd)
                s.commit()
                results.append(("ok", cmd_id))
            except IntegrityError:
                s.rollback()
                hit = brepo.has_unresolved_update_command(s, client_order_id="c-race")
                results.append(("rejected" if hit else "unexpected_integrity_error", cmd_id))

    t1 = threading.Thread(target=_attempt, args=("cmd-race-1",))
    t2 = threading.Thread(target=_attempt, args=("cmd-race-2",))
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert sorted(r[0] for r in results) == ["ok", "rejected"]
    with Session(eng) as s:
        rows = list(s.exec(select(AgentCommand).where(AgentCommand.kind == "update")))
        assert len(rows) == 1  # 只有贏家真的落地


# ---------------------------------------------------------------------------
# Task 11（G3）：unknown-resolver 純函式（S#7/29/34/39）
# ---------------------------------------------------------------------------


def _mark_unknown_via_timeout(session: Session, engine, cmd: AgentCommand) -> None:
    """走既有 `apply_command_ack` 的 timeout 分支，讓這筆指令進入 unknown-resolver 的
    適用集合（`outcome='unknown'`, `resolved_at IS NULL`）——不手動改欄位，確保跟正式收斂
    路徑走的是同一套 transport CAS。"""
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1,
                                 ack=_err_ack(cmd.cmd_id, error_kind="timeout"))
    assert outcome.resolved is False and outcome.outcome == "unknown"


def test_list_unresolved_unknown_updates_excludes_created_sent(session, engine):
    """S#7：created/sent（尚未收到任何 ack，`outcome IS NULL`）不在適用集合內——resolver
    的收尾前置：watchdog 對這種列一律跳過本輪，不 confirm/不 release。"""
    _make_order(session, ordno="O1", status="submitted", qty=2)
    _reserve(session, reservation_id="delta-1", qty=3)
    _update_cmd(session, ordno="O1", reservation_id="delta-1", price="21600", qty=5)
    with Session(engine) as s2:
        assert list_unresolved_unknown_updates(s2, user_id=1) == []


def test_list_unresolved_unknown_updates_includes_after_timeout(session, engine):
    """S#7 後半：timeout ack 落地（`outcome='unknown'`）後才進入適用集合。"""
    _make_order(session, ordno="O1", status="submitted", qty=2)
    _reserve(session, reservation_id="delta-1", qty=3)
    cmd = _update_cmd(session, ordno="O1", reservation_id="delta-1", price="21600", qty=5)
    _mark_unknown_via_timeout(session, engine, cmd)
    with Session(engine) as s2:
        rows = list_unresolved_unknown_updates(s2, user_id=1)
        assert [r.cmd_id for r in rows] == [cmd.cmd_id]


def test_resolve_update_via_query_qty_confirms_on_target_match(session, engine):
    """S#7/S#29：query_qty 兩分支之一——改後值 → confirm＋寫 Order 新 price/qty。"""
    _make_order(session, ordno="O1", status="submitted", qty=2)
    _reserve(session, reservation_id="delta-1", qty=3)
    cmd = _update_cmd(session, ordno="O1", reservation_id="delta-1", price="21600", qty=5)
    _mark_unknown_via_timeout(session, engine, cmd)
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        order = s2.exec(select(Order)).one()
        action = resolve_update_via_query_qty(s2, row=row, order=order, real_qty=5)
        s2.commit()
    assert action == "confirmed"
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        assert row.resolved_at is not None
        assert row.outcome == "ok" and row.resolved_via == "query_qty"
        order = s2.exec(select(Order)).one()
        assert order.qty == 5 and str(order.price) == "21600"
        assert _quota_state(s2, "delta-1") == "confirmed"


def test_resolve_update_via_query_qty_releases_on_original_match(session, engine):
    """S#7/S#29：query_qty 兩分支之二——改單前原值 → release，Order 不被改寫。"""
    _make_order(session, ordno="O1", status="submitted", qty=2)
    _reserve(session, reservation_id="delta-1", qty=3)
    cmd = _update_cmd(session, ordno="O1", reservation_id="delta-1", price="21600", qty=5)
    _mark_unknown_via_timeout(session, engine, cmd)
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        order = s2.exec(select(Order)).one()
        action = resolve_update_via_query_qty(s2, row=row, order=order, real_qty=2)
        s2.commit()
    assert action == "released"
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        assert row.resolved_at is not None
        assert row.outcome == "error" and row.resolved_via == "query_qty"
        assert s2.exec(select(Order)).one().qty == 2  # 未被改寫
        assert _quota_state(s2, "delta-1") == "released"


def test_resolve_update_via_query_qty_ambiguous_non_terminal_left_pending(session, engine):
    """S#29：既非改單前也非改單後的口數，Order 也還沒進終態 → 留待下一輪，不猜測。"""
    _make_order(session, ordno="O1", status="submitted", qty=2)
    _reserve(session, reservation_id="delta-1", qty=3)
    cmd = _update_cmd(session, ordno="O1", reservation_id="delta-1", price="21600", qty=5)
    _mark_unknown_via_timeout(session, engine, cmd)
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        order = s2.exec(select(Order)).one()
        action = resolve_update_via_query_qty(s2, row=row, order=order, real_qty=99)
        s2.commit()
    assert action == "left_pending"
    with Session(engine) as s2:
        assert s2.get(AgentCommand, cmd.cmd_id).resolved_at is None
        assert _quota_state(s2, "delta-1") == "reserved"


def test_resolve_update_via_query_qty_terminal_ambiguous_conservative_confirms(session, engine):
    """S#37/R5-3/R6-2：Order 已終態、real_qty 查無（None，委託已結案）→ 無法判斷 → 終態
    resolver 保守 confirm、不 release，且不改寫 Order price/qty（不知道真正執行了什麼）。"""
    _make_order(session, ordno="O1", status="cancelled", qty=2)
    _reserve(session, reservation_id="delta-1", qty=3)
    cmd = _update_cmd(session, ordno="O1", reservation_id="delta-1", price="21600", qty=5)
    _mark_unknown_via_timeout(session, engine, cmd)
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        order = s2.exec(select(Order)).one()
        action = resolve_update_via_query_qty(s2, row=row, order=order, real_qty=None)
        s2.commit()
    assert action == "conservative_confirmed"
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        assert row.resolved_at is not None
        assert row.outcome == "unknown" and row.resolved_via == "report"
        order = s2.exec(select(Order)).one()
        assert order.status == "cancelled" and order.qty == 2  # Order 內容不被亂寫
        assert _quota_state(s2, "delta-1") == "confirmed"  # 保守 confirm，不 release


# ---------------------------------------------------------------------------
# C6（MEDIUM，codex 終審）：減量 update（無 delta reservation）target 修法——舊版誤用
# `order.qty + reservation.qty` 當 target，二分判定分支被 `has_active_delta`（減量恆
# False，`RiskGuard.check_update` 只在增量時才 reserve）綁死，永遠落到 left_pending，
# 永久卡住 update 單飛鎖。修復後：target 直接取 `row.payload["qty"]`。
# ---------------------------------------------------------------------------


def test_resolve_update_via_query_qty_decrement_without_reservation_confirms_on_target_match(
    session, engine
):
    """減量 update（`reservation_id=None`）改單其實生效：`real_qty` 命中減量後目標值
    （4）→ confirmed，寫回 Order 新 qty。"""
    _make_order(session, ordno="O1", status="submitted", qty=6)
    cmd = _update_cmd(session, ordno="O1", reservation_id=None, price="21600", qty=4)  # 減量
    _mark_unknown_via_timeout(session, engine, cmd)
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        order = s2.exec(select(Order)).one()
        action = resolve_update_via_query_qty(s2, row=row, order=order, real_qty=4)
        s2.commit()
    assert action == "confirmed"
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        assert row.resolved_at is not None
        assert row.outcome == "ok" and row.resolved_via == "query_qty"
        order = s2.exec(select(Order)).one()
        assert order.qty == 4 and str(order.price) == "21600"


def test_resolve_update_via_query_qty_decrement_without_reservation_releases_on_no_effect(
    session, engine
):
    """減量 update 未生效（`real_qty` 仍是改單前原值 6）→ error/released 分支；沒有
    reservation 可 release（`row.reservation_id is None`），不誤放 None 進
    `release_quota`（呼叫端 guard，不是 release_quota 內部判斷）。"""
    _make_order(session, ordno="O1", status="submitted", qty=6)
    cmd = _update_cmd(session, ordno="O1", reservation_id=None, price="21600", qty=4)  # 減量
    _mark_unknown_via_timeout(session, engine, cmd)
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        order = s2.exec(select(Order)).one()
        action = resolve_update_via_query_qty(s2, row=row, order=order, real_qty=6)  # 仍是原值
        s2.commit()
    assert action == "released"
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        assert row.resolved_at is not None
        assert row.outcome == "error" and row.resolved_via == "query_qty"
        assert s2.exec(select(Order)).one().qty == 6  # 未被改寫


def test_resolve_update_via_query_qty_price_only_no_qty_change_stays_undeterminable(session, engine):
    """邊界：純改價（target==original，qty 不變）——`real_qty` 在「生效」與「未生效」兩種
    情境下會是同一個值，無法用口數判斷，仍要落到終態/`left_pending`（不因為 C6 修復而
    誤判為可判斷）。"""
    _make_order(session, ordno="O1", status="submitted", qty=6)
    cmd = _update_cmd(session, ordno="O1", reservation_id=None, price="21600", qty=6)  # 純改價
    _mark_unknown_via_timeout(session, engine, cmd)
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        order = s2.exec(select(Order)).one()
        action = resolve_update_via_query_qty(s2, row=row, order=order, real_qty=6)
        s2.commit()
    assert action == "left_pending"  # 非終態、口數不變 → 無法判斷，維持既有保守行為
    with Session(engine) as s2:
        assert s2.get(AgentCommand, cmd.cmd_id).resolved_at is None


def test_resolve_unresolved_cancel_via_report_on_terminal_order(session, engine):
    """S#34/R4-4：cancel unknown × Order filled → resolve(outcome=unknown, via=report)，
    無 quota 效果、不改寫 Order。"""
    _make_order(session, ordno="O1", status="filled", qty=2)
    cmd = _cancel_cmd(session, ordno="O1")
    _mark_unknown_via_timeout(session, engine, cmd)
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        order = s2.exec(select(Order)).one()
        applied = resolve_unresolved_cancel_via_report(s2, row=row, order=order)
        s2.commit()
    assert applied is True
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        assert row.resolved_at is not None
        assert row.outcome == "unknown" and row.resolved_via == "report"
        assert s2.exec(select(Order)).one().status == "filled"  # 不改寫


def test_resolve_unresolved_cancel_via_report_noop_when_order_not_terminal(session, engine):
    """state-based 掃描：Order 還沒到終態 → no-op，之後重跑再檢查一次（不依賴單一事件）。"""
    _make_order(session, ordno="O1", status="submitted", qty=2)
    cmd = _cancel_cmd(session, ordno="O1")
    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        order = s2.exec(select(Order)).one()
        applied = resolve_unresolved_cancel_via_report(s2, row=row, order=order)
    assert applied is False
    with Session(engine) as s2:
        assert s2.get(AgentCommand, cmd.cmd_id).resolved_at is None


# ---------------------------------------------------------------------------
# Task 11 修復回合 1（發現 1）：resolver 的 resolve 寫入改成原子 CAS（`_cas_resolve`，
# `WHERE resolved_at IS NULL`）——與 ack path（`apply_command_ack`）互不覆寫，誰先誰贏，
# 輸家 no-op，不套用任何 Order/quota 效果，也不改 outcome/resolved_via。
# ---------------------------------------------------------------------------


def test_resolve_unresolved_cancel_via_report_cas_wins_race_then_late_ack_is_noop(session, engine):
    """resolver 先贏 CAS（cancel state-based resolver，Order 已被別的回報推進終態）→ 之後
    才抵達的第一次 ack 進 `apply_command_ack`——只能補 transport，不得覆寫 resolver 已經落地
    的 outcome/resolved_via（比照既有 `test_local_resolution_then_late_ack_is_noop_on_
    business_dimension` 的驗證手法，但這次驗證的是新加的 CAS 路徑，不是 `resolve_never_
    dispatched`）。"""
    _make_order(session, ordno="O1", status="submitted", qty=2)
    cmd = _cancel_cmd(session, ordno="O1")  # 尚未收到任何 ack（created/sent）

    with Session(engine) as s2:
        order = s2.exec(select(Order)).one()
        brepo.mark_order_status(s2, order, status="cancelled")  # 別的回報先把 Order 推進終態
        row = s2.get(AgentCommand, cmd.cmd_id)
        applied = resolve_unresolved_cancel_via_report(s2, row=row, order=order)
        s2.commit()
    assert applied is True

    ack = _ok_ack(cmd.cmd_id)
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1, ack=ack)
    assert outcome.transport_won is True  # 這是這筆 cmd 第一次 transport ack
    assert outcome.already_resolved is True and outcome.applied is False
    assert outcome.outcome == "unknown"  # 維持 resolver 落地的值，不被遲到 ack 改寫

    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        assert row.outcome == "unknown" and row.resolved_via == "report"
        assert row.transport_acked_at is not None  # transport 欄位仍照補


def test_resolve_unresolved_cancel_via_report_loses_race_to_earlier_ack_with_stale_row(session, engine):
    """反向（真實跨交易 TOCTOU）：resolver 側先讀到 row（此刻 resolved_at 仍是 None，模擬
    watchdog pre-check 通過瞬間）→ 期間第一次 ack 搶先在別的交易完整落地（transport_won +
    resolved via ack）→ resolver 才真正執行、但手上仍是查完當下的舊 row 物件——`_cas_resolve`
    的 `WHERE resolved_at IS NULL` 直接查 DB 當下狀態（不信任呼叫端可能過期的 Python 物件），
    CAS 必須輸，不得覆寫 ack 已經落地的 outcome/resolved_via。"""
    _make_order(session, ordno="O1", status="submitted", qty=2)
    cmd = _cancel_cmd(session, ordno="O1")

    with Session(engine) as s_read:
        stale_row = s_read.get(AgentCommand, cmd.cmd_id)
        assert stale_row.resolved_at is None
        s_read.expunge(stale_row)  # 模擬 resolver 側在讀到之後、真正呼叫前被搶走 CPU

    ack = _ok_ack(cmd.cmd_id)
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1, ack=ack)
    assert outcome.applied is True and outcome.outcome == "ok"
    with Session(engine) as s_check:
        assert s_check.exec(select(Order)).one().status == "cancelled"

    with Session(engine) as s_resolver:
        order = s_resolver.exec(select(Order)).one()
        applied = resolve_unresolved_cancel_via_report(s_resolver, row=stale_row, order=order)
        s_resolver.commit()
    assert applied is False  # CAS 輸，no-op

    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        assert row.outcome == "ok" and row.resolved_via == "ack"  # 未被 resolver 覆寫
        assert s2.exec(select(Order)).one().status == "cancelled"


def test_resolve_update_via_query_qty_second_call_after_already_resolved_returns_race_lost(session, engine):
    """update 版對應覆蓋：同一 update cmd 已經被 query_qty 分支正確 resolve(ok) 過（例如
    watchdog 某一輪贏了）——resolver 之後被重複呼叫第二次（例如 worker 終態掛載點與 watchdog
    對同一筆列重疊觸發，見 Task 11 修復回合 1 發現 2 新增的第二個掛載點），CAS 必須輸
    （`"race_lost"`），不得把已經正確的 outcome='ok'/resolved_via='query_qty' 覆寫成
    'unknown'/'report'，也不得再動 Order price/qty 或重複 confirm。"""
    _make_order(session, ordno="O1", status="submitted", qty=2)
    _reserve(session, reservation_id="delta-1", qty=3)
    cmd = _update_cmd(session, ordno="O1", reservation_id="delta-1", price="21600", qty=5)
    _mark_unknown_via_timeout(session, engine, cmd)

    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        order = s2.exec(select(Order)).one()
        action = resolve_update_via_query_qty(s2, row=row, order=order, real_qty=5)
        s2.commit()
    assert action == "confirmed"

    with Session(engine) as s2:
        order = s2.exec(select(Order)).one()
        brepo.mark_order_status(s2, order, status="cancelled")  # Order 之後進終態（另一筆回報）
        s2.commit()

    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        order = s2.exec(select(Order)).one()
        action2 = resolve_update_via_query_qty(s2, row=row, order=order, real_qty=None)
        s2.commit()
    assert action2 == "race_lost"

    with Session(engine) as s2:
        row = s2.get(AgentCommand, cmd.cmd_id)
        assert row.outcome == "ok" and row.resolved_via == "query_qty"  # 未被覆寫
        order = s2.exec(select(Order)).one()
        assert order.qty == 5 and str(order.price) == "21600"  # 未被亂改回改單前
        assert _quota_state(s2, "delta-1") == "confirmed"  # 未被重複轉移


# ---------------------------------------------------------------------------
# Task 11 修復回合 2（re-reviewer 用真實函式重現的反向競態）：`apply_command_ack` 業務維
# 舊實作是函式開頭 `session.get` 讀一次 `row.resolved_at`（Python 物件判斷），不是 DB 原子
# CAS——resolver 若剛好在 ack 的「讀」與「（任何）寫」之間，於另一個連線把同一筆列 resolve
# 掉並 commit，ack 仍會走「未 resolved」分支，用 `_resolve`（無條件 ORM 賦值）覆寫掉
# resolver 已經落地的 outcome/resolved_via/resolved_at，也會對 Order/quota 重套一次效果
# （update 情境更嚴重：可能把 resolver 保守不動的 Order price/qty 覆寫成改單後的值）。
#
# 下面兩個測試用真正的檔案型 SQLite（獨立連線，同 `test_update_singleflight_concurrent_
# two_writers_exactly_one_wins` 既有手法）＋monkeypatch `_mark_transport_acked`（`apply_
# command_ack` 讀完 row 之後的第一個寫入點）注入「resolver 搶先在另一個連線完整 commit」，
# 精確重現這個 TOCTOU 窗口——不用真執行緒也能決定式重現，因為交錯順序由注入點而非排程
# 決定。修復後（業務維終結寫入改走 `_cas_resolve`）這兩個測試必須綠；對修復前的程式碼跑
# 這兩個測試必須紅（見 task-11-report.md Fix round 2 附的紅燈證據）。
# ---------------------------------------------------------------------------


def test_apply_command_ack_loses_business_cas_race_to_concurrent_cancel_resolver(tmp_path, monkeypatch):
    """cancel 版：resolver（`resolve_unresolved_cancel_via_report`）在 ack 讀完 row 之後、
    任何寫入之前，於另一個連線先把這筆 cancel 指令 resolve 掉並 commit——ack 的業務維 CAS
    必須輸，只補 transport，不得把 outcome/resolved_via 從 resolver 的 'unknown'/'report'
    改寫成 ack 的 'ok'/'ack'。"""
    from sqlmodel import SQLModel, create_engine

    import quanquant.broker.agent_commands as ac

    db_path = tmp_path / "ack_vs_resolver_cancel_race.db"
    eng = create_engine(f"sqlite:///{db_path}", connect_args={"timeout": 30})
    SQLModel.metadata.create_all(eng)

    with Session(eng) as s:
        _make_order(s, ordno="O1", status="submitted", qty=2)
        cmd = _cancel_cmd(s, ordno="O1")  # 尚未收到任何 ack（created/sent）
        cmd_id = cmd.cmd_id  # session 關閉後 ORM 物件會 expire，先取出純字串供之後使用

    real_mark_transport_acked = ac._mark_transport_acked

    def _racing_mark_transport_acked(sess, *, cmd_id, user_id):
        # 模擬：ack 已經讀完 row（此刻看到 resolved_at 仍是 None）、正要做任何寫入之前，
        # resolver 在另一個連線先把 Order 推進終態、resolve 掉這筆 cancel 指令並 commit。
        with Session(eng) as s_resolver:
            order = s_resolver.exec(select(Order)).one()
            brepo.mark_order_status(s_resolver, order, status="cancelled")
            row = s_resolver.get(AgentCommand, cmd_id)
            won = resolve_unresolved_cancel_via_report(s_resolver, row=row, order=order)
            s_resolver.commit()
        assert won is True
        return real_mark_transport_acked(sess, cmd_id=cmd_id, user_id=user_id)

    monkeypatch.setattr(ac, "_mark_transport_acked", _racing_mark_transport_acked)

    ack = _ok_ack(cmd_id)
    outcome = apply_command_ack(lambda: Session(eng), cmd_id=cmd_id, user_id=1, ack=ack)

    assert outcome.transport_won is True  # 這是這筆 cmd 第一次 transport ack
    assert outcome.already_resolved is True
    assert outcome.applied is False
    assert outcome.outcome == "unknown"  # resolver 落地的值，不是遲到 ack 的 "ok"

    with Session(eng) as s2:
        row = s2.get(AgentCommand, cmd_id)
        assert row.outcome == "unknown" and row.resolved_via == "report"  # 未被覆寫
        assert row.transport_acked_at is not None  # transport 欄位仍照補
        assert s2.exec(select(Order)).one().status == "cancelled"


def test_apply_command_ack_loses_business_cas_race_to_concurrent_update_resolver_preserves_order(
    tmp_path, monkeypatch
):
    """update 版（reviewer 指出的更嚴重情境）：這筆 update 指令從未收過任何 ack
    （created/sent，`outcome IS NULL`）——`transport_acked_at` 一次 CAS 定終身（D4/R4-2），
    resolver 若要跟一筆已經被 ack 過的 update cmd 競爭，第二次呼叫只會在 transport 維就
    短路（既有 duplicate-ack no-op），根本進不到本次要修的業務維 CAS，不構成真實反向競態；
    真正可能交錯的窗口是**這筆 cmd 唯一一次、即將贏得 transport CAS 的 ack**，對上一個直接
    呼叫 `resolve_update_via_query_qty`（同 Task 11 回合 1 既有測試手法，不經過
    `list_unresolved_unknown_updates` 的 `outcome='unknown'` 前置過濾——CAS 本身的正確性不能
    只靠上層呼叫慣例保護，函式簽章也沒有這個前置要求）的 resolver：Order 先被另一筆回報
    推進終態，resolver 因此走終態保守分支（不碰 Order price/qty，只 conservative confirm
    保留額度）並 commit；緊接著這筆 update 唯一的『ok』ack 抵達——若業務維終結寫入不是原子
    CAS，會把已終態 Order 的 price/qty 覆寫成改單後的值、還會重複 confirm 配額。CAS 化之後
    ack 必須輸。"""
    from sqlmodel import SQLModel, create_engine

    import quanquant.broker.agent_commands as ac

    db_path = tmp_path / "ack_vs_resolver_update_race.db"
    eng = create_engine(f"sqlite:///{db_path}", connect_args={"timeout": 30})
    SQLModel.metadata.create_all(eng)

    with Session(eng) as s:
        _make_order(s, ordno="O1", status="submitted", qty=2, price="21500")
        _reserve(s, reservation_id="delta-1", qty=3)
        cmd = _update_cmd(s, ordno="O1", reservation_id="delta-1", price="21600", qty=5)
        cmd_id = cmd.cmd_id  # session 關閉後 ORM 物件會 expire，先取出純字串供之後使用

    with Session(eng) as s:
        order = s.exec(select(Order)).one()
        brepo.mark_order_status(s, order, status="cancelled")  # 別的回報先推進終態
        s.commit()

    real_mark_transport_acked = ac._mark_transport_acked

    def _racing_mark_transport_acked(sess, *, cmd_id, user_id):
        with Session(eng) as s_resolver:
            row = s_resolver.get(AgentCommand, cmd_id)
            order = s_resolver.exec(select(Order)).one()
            action = resolve_update_via_query_qty(s_resolver, row=row, order=order, real_qty=None)
            s_resolver.commit()
        assert action == "conservative_confirmed"
        return real_mark_transport_acked(sess, cmd_id=cmd_id, user_id=user_id)

    monkeypatch.setattr(ac, "_mark_transport_acked", _racing_mark_transport_acked)

    late_ack = _ok_ack(cmd_id)  # 這筆指令唯一的一次 ack（真正的『其實成功』）
    outcome = apply_command_ack(lambda: Session(eng), cmd_id=cmd_id, user_id=1, ack=late_ack)

    assert outcome.transport_won is True  # 這筆 cmd 第一次也是唯一一次 transport ack
    assert outcome.already_resolved is True
    assert outcome.applied is False
    assert outcome.outcome == "unknown"  # 維持 resolver 的保守結果，不被這次 ack 改成 "ok"

    with Session(eng) as s2:
        row = s2.get(AgentCommand, cmd_id)
        assert row.outcome == "unknown" and row.resolved_via == "report"
        assert row.transport_acked_at is not None
        order = s2.exec(select(Order)).one()
        assert order.status == "cancelled"
        assert order.qty == 2 and str(order.price) == "21500"  # 未被遲到 ack 覆寫成改單後值
        assert _quota_state(s2, "delta-1") == "confirmed"  # resolver 保守 confirm，未被重複轉移


def test_list_unresolved_cancels_includes_created_sent_and_timeout(session, engine):
    """cancel 適用集合不像 update 那樣排除 created/sent（cancel 無 quota 效果，state-based
    掃描只看 Order 是否已終態，不需要先等 ack）。"""
    _make_order(session, ordno="O1", status="submitted", qty=1)
    cmd = _cancel_cmd(session, ordno="O1")
    with Session(engine) as s2:
        assert [r.cmd_id for r in list_unresolved_cancels(s2, user_id=1)] == [cmd.cmd_id]


def test_terminal_order_before_ack_does_not_block_subsequent_explicit_reject_release(session, engine):
    """S#39/R6-2：終態 report 先到、update 仍 created/sent → resolver 不碰（不在適用集合）；
    隨後收到明確拒絕 ack 仍正常 release delta，Order 終態不被腐化。"""
    _make_order(session, ordno="O1", status="submitted", qty=2)
    _reserve(session, reservation_id="delta-1", qty=3)
    cmd = _update_cmd(session, ordno="O1", reservation_id="delta-1", price="21600", qty=5)
    # Order 先進終態（例如同時有一筆 cancel 被回報接受）——這筆 update 指令本身仍是
    # created/sent（outcome IS NULL），resolver 完全不該碰它。
    with Session(engine) as s2:
        order = s2.exec(select(Order)).one()
        order.status = "cancelled"
        s2.add(order)
        s2.commit()
    with Session(engine) as s2:
        assert list_unresolved_unknown_updates(s2, user_id=1) == []  # 不在適用集合
    outcome = apply_command_ack(lambda: Session(engine), cmd_id=cmd.cmd_id, user_id=1,
                                 ack=_err_ack(cmd.cmd_id, error_kind="expired"))
    assert outcome.resolved and outcome.outcome == "error"
    with Session(engine) as s2:
        assert _quota_state(s2, "delta-1") == "released"
        assert s2.exec(select(Order)).one().status == "cancelled"  # 終態不被腐化，只 release
