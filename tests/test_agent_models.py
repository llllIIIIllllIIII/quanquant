"""Inc1（多人 simtrade）DB 基礎：`agent_tokens`／`agent_commands`／`agent_account_bindings`
三新表 ＋ `raw_inbox` scope 四欄（D2/D4/D5/D10，spec v8）。

`agent_commands` 是 G1 command ledger 的核心：兩維終結模型——`transport_acked_at`（transport
維，agent 是否已回覆）與 `outcome`/`resolved_via`/`resolved_at`（業務維，resolved 唯一由
`resolved_at IS NOT NULL` 定義，codex R4-4）。update 單飛靠 partial unique index
`uq_agent_cmd_update_singleflight`（R5-2/R6-1）；狀態組合合法性靠兩條 CHECK（R4-5）。
"""
import datetime as dt

import pytest
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import CreateIndex, CreateTable
from sqlmodel import select

from quanquant.db.models import (
    AgentAccountBinding,
    AgentCommand,
    AgentToken,
    RawInbox,
)


def _cmd_kwargs(**over):
    base = dict(
        cmd_id="c1", user_id=1, kind="update", broker="shioaji", account="F1", mode="sim",
        client_order_id="ORD-1", payload="{}", expires_at=dt.datetime(2026, 8, 7, 12, 0),
    )
    base.update(over)
    return base


# ---- AgentToken（D2）----

def test_agent_token_round_trip(session):
    tok = AgentToken(user_id=1, token_hash="hash1", expires_at=dt.datetime(2026, 9, 6, 12, 0))
    session.add(tok)
    session.commit()
    row = session.exec(select(AgentToken)).one()
    assert row.user_id == 1
    assert row.id is not None
    assert row.revoked_at is None and row.last_used_at is None


def test_agent_token_hash_unique(session):
    session.add(AgentToken(user_id=1, token_hash="dup", expires_at=dt.datetime(2026, 9, 6, 12, 0)))
    session.commit()
    session.add(AgentToken(user_id=2, token_hash="dup", expires_at=dt.datetime(2026, 9, 6, 12, 0)))
    with pytest.raises(IntegrityError):
        session.commit()


# ---- C10（LOW，codex 終審）：uq_agent_tokens_active_per_user——每 user 同時只能有一枚
# revoked_at IS NULL 的有效 token（partial unique index，DB 層強制，不再只靠應用層先
# revoke 再 insert）。----


def test_agent_tokens_active_per_user_index_blocks_second_active_token(session):
    """兩枚 `revoked_at IS NULL` 的 token 給同一個 user_id → 撞
    `uq_agent_tokens_active_per_user`。"""
    session.add(AgentToken(user_id=1, token_hash="t1", expires_at=dt.datetime(2026, 9, 6, 12, 0)))
    session.commit()
    session.add(AgentToken(user_id=1, token_hash="t2", expires_at=dt.datetime(2026, 9, 6, 12, 0)))
    with pytest.raises(IntegrityError):
        session.commit()


def test_agent_tokens_active_per_user_index_allows_second_after_first_revoked(session):
    """第一枚被 revoke 之後（`revoked_at` 非 NULL），同一 user 再簽發第二枚不再撞鍵——
    partial index 的 `WHERE revoked_at IS NULL` 只約束「目前有效」的集合。"""
    tok1 = AgentToken(user_id=1, token_hash="t1", expires_at=dt.datetime(2026, 9, 6, 12, 0))
    session.add(tok1)
    session.commit()

    tok1.revoked_at = dt.datetime(2026, 8, 7, 12, 0)
    session.add(tok1)
    session.commit()

    session.add(AgentToken(user_id=1, token_hash="t2", expires_at=dt.datetime(2026, 9, 6, 12, 0)))
    session.commit()  # 不應拋錯
    rows = list(session.exec(select(AgentToken).where(AgentToken.user_id == 1)))
    assert len(rows) == 2


def test_agent_tokens_active_per_user_index_allows_different_users(session):
    """不同 user_id 各自一枚有效 token 不衝突（partial index 是 per-user 而非全域唯一）。"""
    session.add(AgentToken(user_id=1, token_hash="t1", expires_at=dt.datetime(2026, 9, 6, 12, 0)))
    session.commit()
    session.add(AgentToken(user_id=2, token_hash="t2", expires_at=dt.datetime(2026, 9, 6, 12, 0)))
    session.commit()  # 不應拋錯


def test_agent_tokens_active_per_user_index_compiles_on_both_dialects():
    idx = next(
        i for i in AgentToken.__table__.indexes if i.name == "uq_agent_tokens_active_per_user"
    )
    for dialect in (sqlite.dialect(), postgresql.dialect()):
        ddl = str(CreateIndex(idx).compile(dialect=dialect))
        assert "UNIQUE" in ddl.upper()
        assert "revoked_at IS NULL" in ddl


# ---- AgentCommand（D4：G1 command ledger）----

def test_agent_command_round_trip_defaults(session):
    cmd = AgentCommand(**_cmd_kwargs())
    session.add(cmd)
    session.commit()
    row = session.exec(select(AgentCommand)).one()
    assert row.cmd_id == "c1"
    assert row.outcome is None and row.resolved_via is None and row.resolved_at is None
    assert row.transport_acked_at is None and row.timeout_observed_at is None


def test_update_singleflight_blocks_second_unresolved_same_client_order_id(session):
    """R5-2/R6-1：同一 client_order_id、kind='update'、resolved_at IS NULL 的第二筆撞唯一鍵。"""
    session.add(AgentCommand(**_cmd_kwargs(cmd_id="c1")))
    session.commit()

    session.add(AgentCommand(**_cmd_kwargs(cmd_id="c2")))
    with pytest.raises(IntegrityError):
        session.commit()


def test_update_singleflight_allows_insert_after_previous_resolved(session):
    cmd1 = AgentCommand(**_cmd_kwargs(cmd_id="c1"))
    session.add(cmd1)
    session.commit()

    cmd1.transport_acked_at = dt.datetime(2026, 8, 7, 12, 0)
    cmd1.outcome = "ok"
    cmd1.resolved_via = "ack"
    cmd1.resolved_at = dt.datetime(2026, 8, 7, 12, 0)
    session.add(cmd1)
    session.commit()

    session.add(AgentCommand(**_cmd_kwargs(cmd_id="c2")))
    session.commit()  # 不應拋錯——前一筆已 resolved
    rows = list(session.exec(select(AgentCommand)))
    assert {r.cmd_id for r in rows} == {"c1", "c2"}


def test_update_singleflight_does_not_apply_to_place_kind(session):
    """單飛只限 kind='update'——place/cancel 不受此限（brief：只 update 適用）。"""
    session.add(AgentCommand(**_cmd_kwargs(cmd_id="c1", kind="place")))
    session.commit()
    session.add(AgentCommand(**_cmd_kwargs(cmd_id="c2", kind="place")))
    session.commit()  # 不應拋錯


def test_check_resolved_at_requires_resolved_via(session):
    """R4-5：resolved_at 非空但 resolved_via 為空 → CHECK 擋下。"""
    cmd = AgentCommand(**_cmd_kwargs())
    cmd.resolved_at = dt.datetime(2026, 8, 7, 12, 0)
    session.add(cmd)
    with pytest.raises(IntegrityError):
        session.commit()


def test_check_ack_resolution_requires_transport_acked_at(session):
    """R4-5：resolved_via='ack' 但 transport_acked_at 為空 → CHECK 擋下。"""
    cmd = AgentCommand(**_cmd_kwargs())
    cmd.outcome = "ok"
    cmd.resolved_via = "ack"
    cmd.resolved_at = dt.datetime(2026, 8, 7, 12, 0)
    session.add(cmd)
    with pytest.raises(IntegrityError):
        session.commit()


def test_check_allows_valid_ack_resolution(session):
    cmd = AgentCommand(**_cmd_kwargs())
    cmd.transport_acked_at = dt.datetime(2026, 8, 7, 12, 0)
    cmd.outcome = "ok"
    cmd.resolved_via = "ack"
    cmd.resolved_at = dt.datetime(2026, 8, 7, 12, 0)
    session.add(cmd)
    session.commit()  # 不應拋錯
    assert cmd.resolved_via == "ack"


def test_check_allows_query_qty_resolution_without_transport_ack(session):
    """D4 unknown-resolver：resolved_via='query_qty' 收斂 timeout 指令，本就沒有 transport ack。"""
    cmd = AgentCommand(**_cmd_kwargs())
    cmd.outcome = "ok"
    cmd.resolved_via = "query_qty"
    cmd.resolved_at = dt.datetime(2026, 8, 7, 12, 0)
    session.add(cmd)
    session.commit()  # 不應拋錯
    assert cmd.resolved_via == "query_qty"


def test_check_allows_report_resolution_with_unknown_outcome(session):
    """D4：cancel 允許 outcome=unknown 且 resolved(via=report)——誠實紀錄「回報已終結曝險、
    指令本身效果不可知」（R4-4）。"""
    cmd = AgentCommand(**_cmd_kwargs(cmd_id="c1", kind="cancel", client_order_id=None))
    cmd.outcome = "unknown"
    cmd.resolved_via = "report"
    cmd.resolved_at = dt.datetime(2026, 8, 7, 12, 0)
    session.add(cmd)
    session.commit()  # 不應拋錯


# ---- AgentAccountBinding（D10）----

def test_agent_account_binding_round_trip(session):
    row = AgentAccountBinding(broker="shioaji", account="F1", user_id=1)
    session.add(row)
    session.commit()
    got = session.exec(select(AgentAccountBinding)).one()
    assert got.broker == "shioaji" and got.account == "F1" and got.user_id == 1
    assert got.bound_at is not None


def test_agent_account_binding_unique_broker_account_blocks_second_user(session):
    """先綁先贏——同一 (broker,account) 被第二個 user 綁定要撞唯一鍵。"""
    session.add(AgentAccountBinding(broker="shioaji", account="F1", user_id=1))
    session.commit()
    session.add(AgentAccountBinding(broker="shioaji", account="F1", user_id=2))
    with pytest.raises(IntegrityError):
        session.commit()


def test_agent_account_binding_allows_different_account(session):
    session.add(AgentAccountBinding(broker="shioaji", account="F1", user_id=1))
    session.commit()
    session.add(AgentAccountBinding(broker="shioaji", account="F2", user_id=2))
    session.commit()  # 不同帳號不衝突


# ---- RawInbox scope 四欄（D5）----

def test_raw_inbox_scope_columns_present_on_fresh_create(session):
    row = RawInbox(
        kind="order_report", broker="shioaji", payload="{}",
        user_id=1, account="F1", mode="sim", quarantine_reason="association_pending",
    )
    session.add(row)
    session.commit()
    got = session.exec(select(RawInbox)).one()
    assert got.user_id == 1
    assert got.account == "F1"
    assert got.mode == "sim"
    assert got.quarantine_reason == "association_pending"


def test_raw_inbox_scope_columns_default_to_null(session):
    row = RawInbox(kind="order_report", broker="shioaji", payload="{}")
    session.add(row)
    session.commit()
    got = session.exec(select(RawInbox)).one()
    assert got.user_id is None and got.account is None
    assert got.mode is None and got.quarantine_reason is None


# ---- 雙方言可攜 smoke（DDL 逐項 fragment 比對，範式 test_broker_repo.py） ----

@pytest.mark.parametrize(
    "model,expected_fragments",
    [
        (AgentToken, ["agent_tokens"]),
        (AgentCommand, [
            "agent_commands",
            "ck_agent_commands_resolved_requires_via",
            "ck_agent_commands_ack_requires_transport_ack",
        ]),
        (AgentAccountBinding, ["agent_account_bindings", "uq_agent_account_bindings_broker_account"]),
    ],
)
def test_new_tables_ddl_compiles_on_both_dialects_with_expected_constraints(model, expected_fragments):
    for dialect in (sqlite.dialect(), postgresql.dialect()):
        ddl = str(CreateTable(model.__table__).compile(dialect=dialect))
        for fragment in expected_fragments:
            assert fragment in ddl, f"{model.__name__} DDL missing {fragment!r} on {dialect.name}: {ddl}"


def test_agent_command_update_singleflight_index_compiles_on_both_dialects():
    idx = next(
        i for i in AgentCommand.__table__.indexes if i.name == "uq_agent_cmd_update_singleflight"
    )
    for dialect in (sqlite.dialect(), postgresql.dialect()):
        ddl = str(CreateIndex(idx).compile(dialect=dialect))
        assert "UNIQUE" in ddl.upper()
        assert "kind = 'update'" in ddl
        assert "resolved_at IS NULL" in ddl
