"""G1 command ledger（server 端）：`agent_commands` 的送前持久化＋兩維 CAS 收斂（D4）。

**兩維終結模型**（詳見 `db/models.py::AgentCommand` docstring）：
  - transport 維：`transport_acked_at`——agent 是否已回覆過這筆指令（不代表業務結果）。
  - 業務維：`outcome`（'ok'|'error'|'unknown'）＋`resolved_via`＋`resolved_at`——
    `resolved_at IS NOT NULL` 唯一定義「已終結」（codex R4-4）。

**本檔提供的兩類函式**：
  1. Ledger 生命週期原語：`new_command`/`insert_command`/`mark_timeout_observed`/
     `resolve_never_dispatched`/`apply_command_ack`——這些操作 `agent_commands` 表本身。
  2. 四個純函式 `apply_place_ack`/`apply_place_failure`/`apply_cancel_ack`/
     `apply_update_ack`——只操作 `Order`/`QuotaReservation`（經 `repository.py`），
     **完全不碰 `agent_commands` 表**，讓 in-process 寫回段與 agent 模式 applier 共用同一份
     實作（R1-10：in-process 的 RiskGuard 自行 commit 的行為零變更；in-process 從不建立
     ledger 列，呼叫這些函式時只是單純的 Order/quota 寫回，語意與 Task 8 之前逐位元相同）。

**呼叫責任邊界（重要，避免誤用）**：
  - `insert_command` 必須與該指令的 DB 決策段（create_order/reserve_quota 等）同一交易
    （見 `shioaji_adapter.py` place/cancel/update 決策段的呼叫方式）。已知落差：`RiskGuard.
    check_place`/`check_update`（Task 7 既有程式，不在本 task 修改範圍）在內部自行
    `session.commit()`，故 risk_guard 存在時，ledger insert 只能是緊接著、不含任何
    await/IO 的**獨立小交易**（而非同一次 SQL COMMIT）——這是本 task 已知且明確揭露的
    落差，殘留風險與 spec 8.1 所列的「executed-but-unrecorded 窗口」同量級（純同步 Python
    賦值間的行程崩潰機率），詳見 task-8-report.md。
  - `apply_command_ack` 是 agent 模式**唯一**的 ack 效果套用入口（`agent_ws.py` 的
    `UpCmdAck` handler 呼叫）；route（`shioaji_adapter.py`）收到 ack 衍生的例外
    （`TradeNotFoundError`/一般 `OrderError`，非 `AgentUnavailableError`/
    `AgentCommandTimeoutError`）時**必須信任 applier 已完成寫回**，不得重新分類寫入
    ——本檔的 CAS 保證這件事在時序上是安全的（`AgentChannel` 的 future 只在 applier
    commit 後才 resolve，route 拿到的例外/結果必然是 commit 後的狀態）。
  - `mark_timeout_observed`／`resolve_never_dispatched` 只給 route 自己在「這筆指令確定
    不會有 ack（從未送達 agent）」或「route 等 ack 逾時、需要判定這次逾時是否仍然有效」
    時呼叫，皆以 `resolved_at IS NULL`／`transport_acked_at IS NULL` 為 CAS 條件，與
    `apply_command_ack` 互斥、不會互相覆寫。
"""
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Literal

from sqlalchemy import update as sa_update
from sqlmodel import Session, select

from quanquant.broker import repository as brepo
from quanquant.broker.agent_protocol import DownCancel, DownPlace, DownUpdate, PlaceNative, UpCmdAck
from quanquant.db.models import AgentCommand, Order

# spec D4/D7 §7：設定鍵 `agent_command_expiry_seconds`（預設 120）留 Task 10 才接線到
# Settings——本 task 先用常數（`agent_channel.py` 既有的 `_DEFAULT_COMMAND_EXPIRY_SECONDS`
# 也是同一份預設值的獨立常數，兩處都明確附註解、非重複定義同一個名字，避免誤以為共用）。
DEFAULT_COMMAND_EXPIRY_SECONDS = 120

# D4：agent 對每個 error_kind 的分類——timeout 是「非終結」的 acked_unknown；其餘五個是
# 「確定未執行」或「明確拒絕」的 acked_error。任何不在這個集合內的值（含 error_kind=None
# 但 ok=False 的畸形 ack——理論上 Pydantic Literal 已擋掉，這裡是防禦層）一律 fail-safe
# 落入 unknown 分支（寧可漏判 unknown，不可誤判 error 而提前 release/confirm——同
# `_classify_place_failure` 的既有保守哲學）。
_EXPLICIT_REJECT_KINDS = frozenset(
    {"trade_not_found", "exception", "mode_mismatch", "expired", "scope_mismatch"}
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# 送前持久化
# ---------------------------------------------------------------------------


def new_command(
    *,
    kind: Literal["place", "cancel", "update"],
    user_id: int,
    broker: str,
    account: str,
    mode: str,
    payload: dict,
    client_order_id: str | None = None,
    ordno: str | None = None,
    reservation_id: str | None = None,
    expiry_seconds: int = DEFAULT_COMMAND_EXPIRY_SECONDS,
) -> AgentCommand:
    """建構一筆尚未 insert 的 `AgentCommand`（cmd_id 用 uuid4 hex；`expires_at` = 建立時間 +
    `expiry_seconds`）。呼叫端（決策段）自行決定何時 `insert_command`＋commit；`sent_at`
    以建立時間近似「commit 後即將下行」（見模組頂部落差說明，精確下行時間追蹤留待接線
    重連補送的後續 task）。"""
    now = _utcnow()
    return AgentCommand(
        cmd_id=uuid.uuid4().hex,
        user_id=user_id,
        kind=kind,
        broker=broker,
        account=account,
        mode=mode,
        client_order_id=client_order_id,
        ordno=ordno,
        reservation_id=reservation_id,
        payload=json.dumps(payload, ensure_ascii=False),
        created_at=now,
        sent_at=now,
        expires_at=now + timedelta(seconds=expiry_seconds),
    )


def insert_command(session: Session, *, cmd: AgentCommand) -> None:
    """送前持久化（D4）：只 `add`+`flush`，不 commit——commit 時機交呼叫端決定（同
    `repository.py` 頂部既有交易邊界政策），確保呼叫端能把這筆 insert 併入決策段的同一次
    commit。"""
    session.add(cmd)
    session.flush()


# ---------------------------------------------------------------------------
# route 逾時 / 本地終結（未曾送達 agent）
# ---------------------------------------------------------------------------


def mark_timeout_observed(session: Session, *, cmd_id: str) -> bool:
    """route 逾時 CAS：`WHERE transport_acked_at IS NULL AND timeout_observed_at IS NULL`。

    True＝這次呼叫贏得標記權（尚無任何 ack 抵達）——呼叫端可安全套用既有 unknown
    fail-safe 語意。False＝ack 已搶先（`transport_acked_at` 已非 NULL，代表
    `apply_command_ack` 在別的交易已完整 commit 過——寫入該欄位與該次呼叫的其餘寫入／
    commit 在同一交易內，故看到非 NULL 保證整筆已落地）——呼叫端**不得**再視為逾時、
    不得寫入 Order/quota，只能信任已落庫的結果。"""
    t = AgentCommand.__table__
    stmt = (
        sa_update(t)
        .where(
            t.c.cmd_id == cmd_id,
            t.c.transport_acked_at.is_(None),
            t.c.timeout_observed_at.is_(None),
        )
        .values(timeout_observed_at=_utcnow())
    )
    result = session.exec(stmt)  # type: ignore[call-overload]
    won = result.rowcount == 1
    if won:
        session.flush()
    return won


def resolve_never_dispatched(session: Session, *, cmd_id: str, message: str) -> bool:
    """route 本地判定「這筆指令從未送達 agent、往後也不會有任何 ack」時的本地終結（第二層
    `_send_gate` 攔下的 kill switch、或 `channel.request`/`_send_gate` 判定 agent 未連線的
    `AgentUnavailableError`）。CAS：`WHERE resolved_at IS NULL`。

    `resolved_via='local'` 是本 task 新增值（D4 文件原列 'ack'|'query_qty'|'report'|
    'manual' 四種；schema 的 CHECK 只驗證 `resolved_via='ack'` 時是否有 `transport_acked_at`，
    不限制列舉值，故新增合法）——語意是「route 未經任何與 agent 的 round-trip 就本地判定」，
    與 `'manual'`（人工 ops 介入既有 unknown 委託）不同，刻意不共用同一個值以維持稽核可讀性。

    不解決這個列會讓 ledger 卡在『未 resolved』——Order/quota 已經正確終結（呼叫端另外
    呼叫 `apply_place_failure`），但這筆 ledger 列會被未來的『重連補送未 resolved 指令』
    誤判成『還沒送過、可以送』而重播一次已經被判定失敗/釋放配額的指令（本 task 尚未實作
    重連補送，此處先確保未來加上後天生正確）。"""
    t = AgentCommand.__table__
    stmt = (
        sa_update(t)
        .where(t.c.cmd_id == cmd_id, t.c.resolved_at.is_(None))
        .values(
            outcome="error",
            resolved_via="local",
            resolved_at=_utcnow(),
            result=json.dumps({"message": message}, ensure_ascii=False),
        )
    )
    result = session.exec(stmt)  # type: ignore[call-overload]
    resolved = result.rowcount == 1
    if resolved:
        session.flush()
    return resolved


# ---------------------------------------------------------------------------
# 重連補送（限同 scope；D4／Task 10）
# ---------------------------------------------------------------------------


def list_unresolved_for_replay(session: Session, *, user_id: int, account: str) -> list[AgentCommand]:
    """UpLogin 接受、`mark_logged_in` 後查詢這個 user 在**這次登入綁定帳號**下，尚未
    transport ack 也尚未 resolved 的指令，按 `created_at` 序回傳供重新下行（agent 端 ledger
    去重負責收斂，見 Task 9）。

    - `outcome='unknown'` 天然被 `transport_acked_at IS NULL` 排除——timeout ack 已經在
      `_mark_transport_acked` 寫過 transport_acked_at，這裡只會看到「從未收到任何 ack」的
      列（route 本地 `mark_timeout_observed` 只寫 `timeout_observed_at`，不影響這個過濾）。
    - `account=<本次綁定帳號>` 是唯一的 scope 過濾（codex R1-2）：**他帳號的未 transport-ack
      指令永不下行到不同帳號的 session**，即使同一個 user 換帳號重連也一樣。
    - 刻意不濾 `expires_at`——**server 不自主過期**（見模組底部 `to_downlink_dict`
      docstring），是否過期只由 agent 收到重播後自行判斷。
    """
    stmt = (
        select(AgentCommand)
        .where(
            AgentCommand.user_id == user_id,
            AgentCommand.account == account,
            AgentCommand.transport_acked_at.is_(None),
            AgentCommand.resolved_at.is_(None),
        )
        .order_by(AgentCommand.created_at)
    )
    return list(session.exec(stmt))


def to_downlink_dict(cmd: AgentCommand) -> dict:
    """把一筆 ledger 列還原成可重送的下行指令 dict（`websocket.send_json` 可直接吃，比照
    `AgentNativeGateway` 平常組 `DownPlace/DownCancel/DownUpdate` 後 `.model_dump()` 的既有
    慣例）。`expires_at` 沿用**原始**建立時凍結的值，不因重送而延長——**server 不自主過期**
    未 ack 的指令（agent 才知道有沒有執行過；server 單方面判死會把『已在券商成交的單』記成
    failed＋錯誤退配額，見 spec D4「過期語意」）：過期只由 agent 收到重播後自行檢查
    `expires_at`、拒絕執行時回報 `error_kind='expired'`。"""
    expires_at = cmd.expires_at.isoformat()
    if cmd.kind == "place":
        payload = json.loads(cmd.payload)
        return DownPlace(
            cmd_id=cmd.cmd_id, account=cmd.account, mode=cmd.mode, expires_at=expires_at,
            native=PlaceNative(**payload),
        ).model_dump()
    if cmd.kind == "cancel":
        return DownCancel(
            cmd_id=cmd.cmd_id, account=cmd.account, mode=cmd.mode, expires_at=expires_at,
            ordno=cmd.ordno,
        ).model_dump()
    if cmd.kind == "update":
        payload = json.loads(cmd.payload)
        return DownUpdate(
            cmd_id=cmd.cmd_id, account=cmd.account, mode=cmd.mode, expires_at=expires_at,
            ordno=cmd.ordno, price=payload.get("price"), qty=payload["qty"],
            price_type=payload.get("price_type"),
        ).model_dump()
    raise ValueError(f"unknown AgentCommand.kind={cmd.kind!r}")  # pragma: no cover - kind 由決策段 Literal 保證


def mark_resent(session: Session, *, cmd_id: str) -> None:
    """重連補送時記錄『這筆指令剛剛又送出去一次』。只 flush，不 commit（呼叫端決定交易邊界，
    同本檔其餘寫入函式的既有慣例）——與 `list_unresolved_for_replay` 的查詢在同一個呼叫端
    交易內完成，之間不夾雜其他 DB 寫入，`sent_at` 因此不會被中途的其他操作腐化。"""
    t = AgentCommand.__table__
    stmt = sa_update(t).where(t.c.cmd_id == cmd_id).values(sent_at=_utcnow())
    session.exec(stmt)  # type: ignore[call-overload]
    session.flush()


def prepare_replay(session_factory, *, user_id: int, account: str) -> list[dict]:
    """`agent_ws.py` 的 UpLogin 分支用 `asyncio.to_thread` 呼叫一次：查詢＋標記 sent_at＋
    還原下行 dict，全部包在同一個交易內完成，回傳按 `created_at` 序排列、可直接
    `await websocket.send_json(...)` 的 dict 清單。呼叫端在 `channel.mark_logged_in` 之後、
    `_reconcile_after_login` 觸發之前呼叫（D4：補送完成後才觸發 login reconcile）；補送本身
    用該連線的 WS 直接 send（不經 `AgentChannel.request` 等待——這些是 fire-and-resend，
    ack 由 agent 端 ledger 去重＋server 端兩維 CAS applier 冪等吸收）。"""
    with session_factory() as session:
        rows = list_unresolved_for_replay(session, user_id=user_id, account=account)
        dicts = [to_downlink_dict(row) for row in rows]
        for row in rows:
            mark_resent(session, cmd_id=row.cmd_id)
        session.commit()
        return dicts


# ---------------------------------------------------------------------------
# 四個純函式：Order/quota 效果套用（in-process 與 applier 共用，R1-10）
# ---------------------------------------------------------------------------


def apply_place_ack(
    session: Session,
    order: Order,
    *,
    ordno: str | None,
    broker_order_id: str | None,
    reservation_id: str | None,
) -> None:
    """place 成功（含 late）：補 ordno/broker_order_id，狀態→submitted；confirm 對應保留
    配額（`reservation_id=None` 時不 confirm——無 risk_guard 或此筆無保留列）。與既有
    `ShioajiAdapter.place` 成功寫回逐位元相同語意：`set_order_ack` 直接覆寫（不受狀態偏序
    守衛限制），unknown→submitted 是合法且預期的收斂（late ack 主場景）。"""
    brepo.set_order_ack(
        session, order.id, broker_order_id=broker_order_id or "", ordno=ordno, status="submitted"
    )
    if reservation_id is not None:
        brepo.confirm_quota(session, reservation_id=reservation_id)


def apply_place_failure(
    session: Session,
    order: Order,
    *,
    reservation_id: str | None,
    status: Literal["failed", "unknown"],
) -> None:
    """place 失敗寫回：`status="failed"`（明確拒絕/expired/scope_mismatch/never-dispatched）
    額外 release 保留配額；`status="unknown"`（timeout，結果不明）只標狀態、配額原封不動
    （fail-safe，watchdog/人工終結程序另行收斂，見 spec D4/D8）。"""
    brepo.mark_order_status(session, order, status=status)
    if status == "failed" and reservation_id is not None:
        brepo.release_quota(session, reservation_id=reservation_id)


def apply_cancel_ack(session: Session, order: Order) -> None:
    """cancel 成功（含 late）：記委託已取消。`mark_order_status` 本身的狀態偏序守衛
    （`_order_status_transition_allowed`）就是「Order 已終態則 no-op」——終態仍由回報推進
    的既有原則因此自然成立，不需要額外分支去判斷 Order 是否已經是終態。cancel 無 quota
    效果（D4 轉移表）。"""
    brepo.mark_order_status(session, order, status="cancelled")


def apply_update_ack(
    session: Session,
    order: Order,
    *,
    new_price,
    new_qty: int,
    reservation_id: str | None,
) -> None:
    """update 成功（含 late）：原子寫新 price/qty，狀態→submitted（經
    `mark_order_status` 既有狀態偏序守衛，逐位元同既有 in-process 寫回）；confirm delta
    保留配額（`reservation_id=None` 時不 confirm——無增量保留列）。"""
    order.price = new_price
    order.qty = new_qty
    brepo.mark_order_status(session, order, status="submitted")
    if reservation_id is not None:
        brepo.confirm_quota(session, reservation_id=reservation_id)


# ---------------------------------------------------------------------------
# 兩維 CAS applier（agent_ws 的 UpCmdAck handler 唯一效果套用入口）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AppliedOutcome:
    """`apply_command_ack` 的結果，供 `agent_ws.py` 決定：要不要 `channel.resolve_ack`、
    要不要記警告 log。"""

    found: bool  # cmd_id 是否存在（不論是否屬於這個 user）
    user_mismatch: bool  # 存在但屬於別的 user（R1-5：拒絕＋告警，完全不觸碰）
    transport_won: bool  # 這次呼叫是否贏得 transport CAS（True＝這是這個 cmd 第一次被記錄）
    already_resolved: bool  # 業務維落空（resolved_at 已非 NULL——遲到 ack）
    applied: bool  # 這次呼叫是否實際套用了 Order/quota 效果
    outcome: str | None  # 呼叫後 AgentCommand.outcome 的值
    resolved: bool  # 呼叫後 AgentCommand 是否已 resolved


def _empty_outcome(*, found: bool, user_mismatch: bool, outcome=None, resolved=False) -> AppliedOutcome:
    return AppliedOutcome(
        found=found, user_mismatch=user_mismatch, transport_won=False,
        already_resolved=False, applied=False, outcome=outcome, resolved=resolved,
    )


def _mark_transport_acked(session: Session, *, cmd_id: str, user_id: int) -> bool:
    """transport 維 CAS（D4/R4-2）：`WHERE cmd_id=? AND user_id=? AND transport_acked_at
    IS NULL`——帶 `user_id`（連線認證身分）防跨 user 終結他人指令（codex R1-5）。"""
    t = AgentCommand.__table__
    stmt = (
        sa_update(t)
        .where(t.c.cmd_id == cmd_id, t.c.user_id == user_id, t.c.transport_acked_at.is_(None))
        .values(transport_acked_at=_utcnow())
    )
    result = session.exec(stmt)  # type: ignore[call-overload]
    return result.rowcount == 1


def _ack_result_payload(ack: UpCmdAck) -> str:
    payload = dict(ack.result or {})
    if ack.error_kind is not None:
        payload["error_kind"] = ack.error_kind
    if ack.message is not None:
        payload["message"] = ack.message
    return json.dumps(payload, ensure_ascii=False)


def _resolve(row: AgentCommand, *, outcome: str, resolved_via: str, result_json: str) -> None:
    row.outcome = outcome
    row.resolved_via = resolved_via
    row.resolved_at = _utcnow()
    row.result = result_json


def apply_command_ack(session_factory, *, cmd_id: str, user_id: int, ack: UpCmdAck) -> AppliedOutcome:
    """agent 模式**唯一**的 UpCmdAck 效果套用入口（`agent_ws.py` 呼叫）。單一交易內完成：

    1. 存在性／ownership 檢查（不存在或屬於別的 user → 立即返回，不寫入）。
    2. transport 維 CAS——只有**第一次**看到這個 cmd_id 的 ack 才會繼續套效果（第二次/
       重複 ack 在這一步就落空，天然滿足「重複 ack no-op」，S#2）。
    3. 業務維守衛——`resolved_at IS NOT NULL`（已被別的路徑終結，如未來的 report-based
       cancel resolver）→ 只補 transport（上一步已做），不改 outcome、不重套效果（D4 規則
       5：遲到 ack 遇已 resolved）。
    4. 依 `row.kind` × `ack` 分派 D4 的 kind×outcome 轉移表，套用 Order/quota 效果並視情況
       resolve；place 成功時額外解除該 user 的 `association_pending` quarantine。
    """
    with session_factory() as session:
        row = session.get(AgentCommand, cmd_id)
        if row is None:
            return _empty_outcome(found=False, user_mismatch=False)
        if row.user_id != user_id:
            return _empty_outcome(found=True, user_mismatch=True, outcome=row.outcome,
                                   resolved=row.resolved_at is not None)

        transport_won = _mark_transport_acked(session, cmd_id=cmd_id, user_id=user_id)
        if not transport_won:
            # 重複 ack（同 cmd_id 第二次以後）——不論業務維目前狀態為何，一律 no-op。
            session.commit()
            return AppliedOutcome(
                found=True, user_mismatch=False, transport_won=False, already_resolved=False,
                applied=False, outcome=row.outcome, resolved=row.resolved_at is not None,
            )
        row.transport_acked_at = _utcnow()  # 讓 ORM 物件與剛才的 raw UPDATE 保持同步

        if row.resolved_at is not None:
            # D4 規則 5：遲到 ack 遇已 resolved——只補 transport（上面已寫），不改 outcome。
            session.add(row)
            session.commit()
            return AppliedOutcome(
                found=True, user_mismatch=False, transport_won=True, already_resolved=True,
                applied=False, outcome=row.outcome, resolved=True,
            )

        _apply_effects(session, row, ack)
        session.add(row)
        session.commit()
        return AppliedOutcome(
            found=True, user_mismatch=False, transport_won=True, already_resolved=False,
            applied=True, outcome=row.outcome, resolved=row.resolved_at is not None,
        )


def _apply_effects(session: Session, row: AgentCommand, ack: UpCmdAck) -> None:
    result_json = _ack_result_payload(ack)
    if row.kind == "place":
        _apply_place(session, row, ack, result_json)
    elif row.kind == "cancel":
        _apply_cancel(session, row, ack, result_json)
    elif row.kind == "update":
        _apply_update(session, row, ack, result_json)
    else:  # pragma: no cover - kind 由決策段的 Literal 保證，防禦性分支
        row.outcome = "unknown"
        row.result = result_json


def _apply_place(session: Session, row: AgentCommand, ack: UpCmdAck, result_json: str) -> None:
    order = brepo.find_order_by_client_order_id(session, row.client_order_id)
    if ack.ok:
        result = ack.result or {}
        if order is not None:
            apply_place_ack(
                session, order, ordno=result.get("ordno"), broker_order_id=result.get("broker_order_id"),
                reservation_id=row.reservation_id,
            )
        _resolve(row, outcome="ok", resolved_via="ack", result_json=result_json)
        # D4：applier 成功補 ordno 後解除該 user 的 association_pending quarantine，讓
        # worker 用新 ordno 重試匹配（既有 unquarantine 機制，bounded per-user）。
        # `older_than=now` 效果等同「立即解除全部符合條件的列」（同 watchdog 既有呼叫慣例，
        # 只是把 age 門檻換成 0）。
        brepo.unquarantine_stale_raw_inbox(session, older_than=_utcnow(), user_id=row.user_id)
    elif ack.error_kind not in _EXPLICIT_REJECT_KINDS:
        # "timeout" 或任何未涵蓋值——fail-safe 落 acked_unknown（非終結）。
        if order is not None:
            apply_place_failure(session, order, reservation_id=None, status="unknown")
        row.outcome = "unknown"
        row.result = result_json
    else:
        if order is not None:
            apply_place_failure(session, order, reservation_id=row.reservation_id, status="failed")
        _resolve(row, outcome="error", resolved_via="ack", result_json=result_json)


def _apply_cancel(session: Session, row: AgentCommand, ack: UpCmdAck, result_json: str) -> None:
    order = brepo.find_order_by_ordno(
        session, broker=row.broker, account=row.account, mode=row.mode, ordno=row.ordno
    )
    if ack.ok:
        if order is not None:
            apply_cancel_ack(session, order)
        _resolve(row, outcome="ok", resolved_via="ack", result_json=result_json)
    elif ack.error_kind not in _EXPLICIT_REJECT_KINDS:
        # timeout/未涵蓋值——不改 Order，不 resolved。
        row.outcome = "unknown"
        row.result = result_json
    else:
        # 明確拒絕/expired/scope_mismatch——不改 Order（交由 reconcile/回報收斂）＋audit。
        brepo.append_audit(
            session, actor_user_id=row.user_id, mode=row.mode, action="cancel",
            payload_hash=row.cmd_id, result="rejected", rule="agent_cmd_ack",
            detail=ack.message or ack.error_kind,
        )
        _resolve(row, outcome="error", resolved_via="ack", result_json=result_json)


def _apply_update(session: Session, row: AgentCommand, ack: UpCmdAck, result_json: str) -> None:
    order = brepo.find_order_by_ordno(
        session, broker=row.broker, account=row.account, mode=row.mode, ordno=row.ordno
    )
    if ack.ok:
        payload = json.loads(row.payload)
        new_price = payload.get("price")
        if order is not None:
            price_value = Decimal(new_price) if new_price is not None else order.price
            apply_update_ack(
                session, order, new_price=price_value, new_qty=payload["qty"],
                reservation_id=row.reservation_id,
            )
        _resolve(row, outcome="ok", resolved_via="ack", result_json=result_json)
    elif ack.error_kind not in _EXPLICIT_REJECT_KINDS:
        # timeout/未涵蓋值——不改 Order，delta 保留（D8 保護）。
        row.outcome = "unknown"
        row.result = result_json
    else:
        # 明確拒絕/expired/scope_mismatch——**不得標 failed**（整張單仍有效），只 release delta。
        if row.reservation_id is not None:
            brepo.release_quota(session, reservation_id=row.reservation_id)
        _resolve(row, outcome="error", resolved_via="ack", result_json=result_json)
