"""D10 帳號↔使用者唯一綁定（Task 6）：`bind_account` 先綁先贏、`backfill_account_bindings`
用既有 Order 歷史灌初始資料（合併全部 mode／跨 user 衝突 fail closed）、UpLogin guard v2 的
兩個查詢函式（`count_unprocessed_for_login`／`has_unresolved_risky_commands_other_account`）
與 `account_owned_by_other_user_in_orders` 雙查。WS 層的整合行為（四步接受順序、rollback 不
留殘影）見 tests/test_agent_ws.py 的「Task 6：UpLogin guard v2」段。"""
import datetime as dt
from decimal import Decimal

import pytest
from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.db.models import AgentAccountBinding, AgentCommand, Order, RawInbox


def _order_kwargs(**over):
    base = dict(
        client_order_id="C1", request_hash="H1", user_id=1, mode="sim", broker="shioaji", account="F1",
        symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", trading_day="2026-06-16",
    )
    base.update(over)
    return base


def _agent_command_kwargs(**over):
    base = dict(
        cmd_id="cmd-1", user_id=1, kind="place", broker="shioaji", account="F1", mode="sim",
        payload="{}", expires_at=dt.datetime(2099, 1, 1),
    )
    base.update(over)
    return base


# ---- bind_account ----

def test_bind_account_first_bind_succeeds(session):
    assert brepo.bind_account(session, broker="shioaji", account="F1", user_id=1) is True
    session.commit()
    row = session.exec(select(AgentAccountBinding)).one()
    assert row.broker == "shioaji" and row.account == "F1" and row.user_id == 1


def test_bind_account_same_user_reconnect_is_noop(session):
    assert brepo.bind_account(session, broker="shioaji", account="F1", user_id=1) is True
    session.commit()
    assert brepo.bind_account(session, broker="shioaji", account="F1", user_id=1) is True
    session.commit()
    rows = session.exec(select(AgentAccountBinding)).all()
    assert len(rows) == 1  # 沒有重複插入


def test_bind_account_other_user_already_bound_returns_false(session):
    # S#10：B 綁 A 已綁的帳號 → False（呼叫端據此拒登）。
    assert brepo.bind_account(session, broker="shioaji", account="F1", user_id=1) is True
    session.commit()
    assert brepo.bind_account(session, broker="shioaji", account="F1", user_id=2) is False
    session.commit()
    row = session.exec(select(AgentAccountBinding)).one()
    assert row.user_id == 1  # 綁定沒被 B 搶走


# ---- backfill_account_bindings ----

def test_backfill_merges_multi_mode_same_user_into_one_binding(engine):
    # S#28（R2-7）：同 account 同 user 的多 mode Order backfill 合併為一列。
    with Session(engine) as s:
        s.add(Order(**_order_kwargs(client_order_id="C1", mode="sim", user_id=1, account="F1")))
        s.add(Order(**_order_kwargs(client_order_id="C2", mode="real", user_id=1, account="F1")))
        s.commit()

    brepo.backfill_account_bindings(lambda: Session(engine))

    with Session(engine) as s:
        rows = s.exec(select(AgentAccountBinding)).all()
        assert len(rows) == 1
        assert rows[0].broker == "shioaji" and rows[0].account == "F1" and rows[0].user_id == 1


def test_backfill_cross_user_conflict_fail_closed_writes_nothing(engine):
    # S#20/28（R1-8/R2-7）：同 account 歷史多 user → agent 模式 fail closed，不留任何部分寫入。
    with Session(engine) as s:
        s.add(Order(**_order_kwargs(client_order_id="C1", mode="sim", user_id=1, account="F1")))
        s.add(Order(**_order_kwargs(client_order_id="C2", mode="real", user_id=2, account="F1")))
        s.commit()

    with pytest.raises(brepo.BackfillConflictError):
        brepo.backfill_account_bindings(lambda: Session(engine))

    with Session(engine) as s:
        assert s.exec(select(AgentAccountBinding)).all() == []


def test_backfill_partial_conflict_does_not_leak_clean_accounts(engine):
    # 一個帳號衝突時整批不寫，即使另一個帳號本身乾淨也不會先寫進去——app.py 呼叫端會讓整個
    # 子系統拒啟，半途寫入沒有意義、只會製造不一致。
    with Session(engine) as s:
        s.add(Order(**_order_kwargs(client_order_id="C1", mode="sim", user_id=1, account="CLEAN")))
        s.add(Order(**_order_kwargs(client_order_id="C2", mode="sim", user_id=1, account="F1")))
        s.add(Order(**_order_kwargs(client_order_id="C3", mode="real", user_id=2, account="F1")))
        s.commit()

    with pytest.raises(brepo.BackfillConflictError):
        brepo.backfill_account_bindings(lambda: Session(engine))

    with Session(engine) as s:
        assert s.exec(select(AgentAccountBinding)).all() == []


def test_backfill_idempotent_when_binding_already_correct(engine):
    with Session(engine) as s:
        s.add(Order(**_order_kwargs(client_order_id="C1", mode="sim", user_id=1, account="F1")))
        s.add(AgentAccountBinding(broker="shioaji", account="F1", user_id=1))
        s.commit()

    brepo.backfill_account_bindings(lambda: Session(engine))  # 不應該炸、不應該重複插入

    with Session(engine) as s:
        rows = s.exec(select(AgentAccountBinding)).all()
        assert len(rows) == 1 and rows[0].user_id == 1


def test_backfill_existing_binding_mismatch_fail_closed(engine):
    # 既有綁定列與 Order 歷史 owner 不符（如人工誤改 DB）→ 一樣 fail closed，不是靜默覆寫。
    with Session(engine) as s:
        s.add(Order(**_order_kwargs(client_order_id="C1", mode="sim", user_id=1, account="F1")))
        s.add(AgentAccountBinding(broker="shioaji", account="F1", user_id=999))
        s.commit()

    with pytest.raises(brepo.BackfillConflictError):
        brepo.backfill_account_bindings(lambda: Session(engine))


def test_backfill_no_orders_is_noop(engine):
    brepo.backfill_account_bindings(lambda: Session(engine))
    with Session(engine) as s:
        assert s.exec(select(AgentAccountBinding)).all() == []


# ---- account_owned_by_other_user_in_orders（R1-8 雙查） ----

def test_account_owned_by_other_user_in_orders_true(session):
    session.add(Order(**_order_kwargs(user_id=2, account="F1")))
    session.commit()
    assert brepo.account_owned_by_other_user_in_orders(
        session, broker="shioaji", account="F1", user_id=1
    ) is True


def test_account_owned_by_other_user_in_orders_false_when_same_user(session):
    session.add(Order(**_order_kwargs(user_id=1, account="F1")))
    session.commit()
    assert brepo.account_owned_by_other_user_in_orders(
        session, broker="shioaji", account="F1", user_id=1
    ) is False


def test_account_owned_by_other_user_in_orders_false_when_no_orders(session):
    assert brepo.account_owned_by_other_user_in_orders(
        session, broker="shioaji", account="F1", user_id=1
    ) is False


# ---- count_unprocessed_for_login（S2 per-user 化，S#9） ----

def test_count_unprocessed_for_login_zero_when_no_rows(session):
    assert brepo.count_unprocessed_for_login(session, user_id=1, account="F1") == 0


def test_count_unprocessed_for_login_excludes_same_account_rows(session):
    # 同帳號重連不擋：這個 user 名下未處理列的 account 若和這次登入帳號相同，不計入。
    session.add(RawInbox(kind="deal_report", broker="shioaji", payload="{}", user_id=1, account="F1"))
    session.commit()
    assert brepo.count_unprocessed_for_login(session, user_id=1, account="F1") == 0


def test_count_unprocessed_for_login_counts_different_account_rows(session):
    # 同 user 換帳號且有未處理列 → 擋。
    session.add(RawInbox(kind="deal_report", broker="shioaji", payload="{}", user_id=1, account="F1"))
    session.commit()
    assert brepo.count_unprocessed_for_login(session, user_id=1, account="F2") == 1


def test_count_unprocessed_for_login_counts_null_account_rows(session):
    # NULL 蓋章歷史列 → 擋。
    session.add(RawInbox(kind="deal_report", broker="shioaji", payload="{}", user_id=1, account=None))
    session.commit()
    assert brepo.count_unprocessed_for_login(session, user_id=1, account="F2") == 1


def test_count_unprocessed_for_login_scoped_to_this_user_only(session):
    # I8 跨 user 隔離：別的 user 的殘留不該擋這個 user 換帳號。
    session.add(RawInbox(kind="deal_report", broker="shioaji", payload="{}", user_id=2, account="F1"))
    session.commit()
    assert brepo.count_unprocessed_for_login(session, user_id=1, account="F2") == 0


def test_count_unprocessed_for_login_ignores_processed_rows(session):
    session.add(RawInbox(kind="deal_report", broker="shioaji", payload="{}", user_id=1, account="F1",
                          processed=True))
    session.commit()
    assert brepo.count_unprocessed_for_login(session, user_id=1, account="F2") == 0


def test_count_unprocessed_for_login_counts_quarantined_not_dead_lettered_rows(session):
    # codex round4：quarantined 但仍 processed=False 的列一樣要擋（之後會被自動解除隔離重試）。
    session.add(RawInbox(kind="deal_report", broker="shioaji", payload="{}", user_id=1, account="F1",
                          quarantine=True))
    session.commit()
    assert brepo.count_unprocessed_for_login(session, user_id=1, account="F2") == 1


# ---- has_unresolved_risky_commands_other_account（R1-2） ----

def test_has_unresolved_risky_place_other_account_blocks(session):
    session.add(AgentCommand(**_agent_command_kwargs(kind="place", account="F1", resolved_at=None)))
    session.commit()
    assert brepo.has_unresolved_risky_commands_other_account(session, user_id=1, account="F2") is True


def test_has_unresolved_risky_update_other_account_blocks(session):
    session.add(AgentCommand(**_agent_command_kwargs(
        cmd_id="cmd-2", kind="update", account="F1", resolved_at=None,
    )))
    session.commit()
    assert brepo.has_unresolved_risky_commands_other_account(session, user_id=1, account="F2") is True


def test_has_unresolved_risky_cancel_other_account_does_not_block(session):
    # cancel 不算曝險——只有 place/update 才擋。
    session.add(AgentCommand(**_agent_command_kwargs(kind="cancel", account="F1", resolved_at=None)))
    session.commit()
    assert brepo.has_unresolved_risky_commands_other_account(session, user_id=1, account="F2") is False


def test_has_unresolved_risky_same_account_does_not_block(session):
    session.add(AgentCommand(**_agent_command_kwargs(kind="place", account="F2", resolved_at=None)))
    session.commit()
    assert brepo.has_unresolved_risky_commands_other_account(session, user_id=1, account="F2") is False


def test_has_unresolved_risky_resolved_does_not_block(session):
    session.add(AgentCommand(**_agent_command_kwargs(
        kind="place", account="F1", resolved_at=dt.datetime(2026, 1, 1), resolved_via="ack",
        transport_acked_at=dt.datetime(2026, 1, 1),
    )))
    session.commit()
    assert brepo.has_unresolved_risky_commands_other_account(session, user_id=1, account="F2") is False


def test_has_unresolved_risky_scoped_to_this_user_only(session):
    session.add(AgentCommand(**_agent_command_kwargs(
        user_id=2, kind="place", account="F1", resolved_at=None,
    )))
    session.commit()
    assert brepo.has_unresolved_risky_commands_other_account(session, user_id=1, account="F2") is False
