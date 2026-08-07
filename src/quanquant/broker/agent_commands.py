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
from quanquant.db.models import AgentCommand, Order, QuotaReservation

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


def _cas_resolve(
    session: Session, *, cmd_id: str, outcome: str, resolved_via: str, result_json: str
) -> bool:
    """resolver 專用的原子 resolve CAS（Task 11 修復回合 1，發現 1／codex R4-2）：`WHERE
    resolved_at IS NULL`，寫法比照 `resolve_never_dispatched`（route 本地終結）既有範例。

    **只給 resolver 呼叫**（`resolve_update_via_query_qty`/`resolve_unresolved_cancel_via_report`
    ／`resolve_one_unresolved_*`）——resolver 與 ack path（`apply_command_ack`）可能在不同交易
    搶著終結同一筆 ledger 列（agent 模式 unknown-resolver 與遲到 ack 的真實跨交易競態；
    Task 11 之後又多了 watchdog 週期掃描 vs. worker 終態掛載點兩個 resolver 觸發點互搶的
    情境，見發現 2），純 ORM 屬性賦值（`_resolve`）在 Postgres READ COMMITTED 下是
    unconditional `UPDATE ... WHERE cmd_id=?`（無視目前 `resolved_at`），會被後寫者悄悄
    覆寫掉先寫者已經 commit 的結果、稽核欄位失真。CAS 版本：贏家（`rowcount==1`）才可以繼續
    對 Order/quota 套用效果；輸家（另一路徑已搶先 resolve）no-op，呼叫端不得再套用任何
    Order/quota 效果——即使那些效果本身是冪等的（`confirm_quota`/`release_quota` 都是
    `WHERE state='reserved'` 的一次性轉移），`outcome`/`resolved_via`/`result` 這幾個純稽核
    欄位不是冪等寫入，沒有這道 CAS 保護一樣會被錯誤覆寫。"""
    t = AgentCommand.__table__
    stmt = (
        sa_update(t)
        .where(t.c.cmd_id == cmd_id, t.c.resolved_at.is_(None))
        .values(outcome=outcome, resolved_via=resolved_via, resolved_at=_utcnow(), result=result_json)
    )
    result = session.exec(stmt)  # type: ignore[call-overload]
    won = result.rowcount == 1
    if won:
        session.flush()
    return won


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


# ---------------------------------------------------------------------------
# G3 unknown-resolver（D4 unknown-resolver bullet＋D8，Task 11）
# ---------------------------------------------------------------------------
#
# 適用集合（update／cancel 各自）一律是 `resolved_at IS NULL`——**created/sent（尚無
# transport 回覆，`outcome IS NULL`）絕不碰**，繼續等 ack／重連重播；其明確未執行 ack
# （expired/scope_mismatch/failstop/明確拒絕）仍依 `_apply_update`/`_apply_cancel` 既有轉移
# 表 release delta。
#
# Task 11 修復回合 1（發現 1／codex R4-2）：resolver 與 ack-path「用不相交的 outcome 值域
# 天然互斥」只在單一交易內成立——resolver 讀到適用集合（`resolved_at IS NULL`）之後、真正
# 落地 resolve 之前，遲到 ack 完全可能在**別的交易**搶先把同一列 resolve 掉（反之亦然：
# resolver 先贏，遲到 ack 才到），這是真實跨交易競態，光靠 outcome 值域不相交防不住。
# resolver 的每個 resolve 寫入點因此一律經 `_cas_resolve`（`WHERE resolved_at IS NULL`）
# 才是最終防線——輸家 no-op，不套用任何 Order/quota 效果；watchdog 的
# `resolve_one_unresolved_*`／worker 的終態掛載點（`inbox_worker._process_order_report`，
# 發現 2）都經同一份 CAS，兩個掛載點互搶同一筆列時也是誰先誰贏、輸家 no-op，不會重複套效果。

# Task 11 修復回合 1（發現 2）起改成公開名稱（原為 `_ORDER_TERMINAL_FOR_UNKNOWN_RESOLVER`）：
# `inbox_worker.py` 的終態掛載點也需要引用同一份終態集合，避免兩處各自定義而漂移不同步。
ORDER_TERMINAL_FOR_UNKNOWN_RESOLVER = frozenset({"cancelled", "failed", "filled"})


def list_unresolved_unknown_updates(session: Session, *, user_id: int) -> list[AgentCommand]:
    """update unknown-resolver 的適用集合（D4）：`kind='update' AND outcome='unknown' AND
    resolved_at IS NULL`——`outcome='unknown'` 只可能由 `_apply_update` 的 timeout/未涵蓋值
    fail-safe 分支寫入，結構上已蘊含 `transport_acked_at IS NOT NULL`（never-acked 的
    created/sent 列 `outcome` 恆為 NULL，不會出現在這個查詢），不需要額外過濾。"""
    stmt = (
        select(AgentCommand)
        .where(
            AgentCommand.user_id == user_id,
            AgentCommand.kind == "update",
            AgentCommand.outcome == "unknown",
            AgentCommand.resolved_at.is_(None),
        )
        .order_by(AgentCommand.created_at)
    )
    return list(session.exec(stmt))


def list_unresolved_cancels(session: Session, *, user_id: int) -> list[AgentCommand]:
    """cancel 的 state-based resolver 掃描集合（D4）：`kind='cancel' AND resolved_at IS
    NULL`——涵蓋 timeout（outcome='unknown'）與尚未收到任何 ack 的 created/sent 列（cancel
    無 quota 效果，不需要像 update 一樣嚴格排除 created/sent；Order 進終態就足以誠實收斂
    「回報已終結曝險、指令本身效果不可知」，見 `resolve_unresolved_cancel_via_report`）。"""
    stmt = (
        select(AgentCommand)
        .where(
            AgentCommand.user_id == user_id,
            AgentCommand.kind == "cancel",
            AgentCommand.resolved_at.is_(None),
        )
        .order_by(AgentCommand.created_at)
    )
    return list(session.exec(stmt))


def list_unresolved_unknown_updates_for_ordno(
    session: Session, *, user_id: int, ordno: str
) -> list[AgentCommand]:
    """同 `list_unresolved_unknown_updates`，額外鎖定單一委託（Task 11 修復回合 1，發現 2：
    `inbox_worker._process_order_report` 的終態掛載點用）——Order 剛被這筆回報推進終態時，
    只需要收斂『這張委託』的 unresolved update 列，不必像 watchdog 週期掃描那樣掃這個 user
    名下全部委託。"""
    stmt = (
        select(AgentCommand)
        .where(
            AgentCommand.user_id == user_id,
            AgentCommand.kind == "update",
            AgentCommand.outcome == "unknown",
            AgentCommand.resolved_at.is_(None),
            AgentCommand.ordno == ordno,
        )
        .order_by(AgentCommand.created_at)
    )
    return list(session.exec(stmt))


def list_unresolved_cancels_for_ordno(session: Session, *, user_id: int, ordno: str) -> list[AgentCommand]:
    """同 `list_unresolved_cancels`，額外鎖定單一委託，理由同上（worker 終態掛載點用）。"""
    stmt = (
        select(AgentCommand)
        .where(
            AgentCommand.user_id == user_id,
            AgentCommand.kind == "cancel",
            AgentCommand.resolved_at.is_(None),
            AgentCommand.ordno == ordno,
        )
        .order_by(AgentCommand.created_at)
    )
    return list(session.exec(stmt))


def resolve_update_via_query_qty(
    session: Session, *, row: AgentCommand, order: Order, real_qty: int | None,
) -> str:
    """update unknown-resolver 的核心比對（D4/D8），供 watchdog（`_reconcile_unknown_quota_
    agent`）在同一交易內呼叫。`real_qty` 由呼叫端先 `await gateway.query_qty(row.ordno)`
    取得（本函式純同步、不碰 native/網路）。呼叫端責任：`row` 必須屬於這個 user、
    `row.kind == 'update'`、`row.outcome == 'unknown'`、`row.resolved_at is None`（見
    `list_unresolved_unknown_updates`），`order` 是依 `row.ordno` 複合 scope 查到的同一張委託。

    三／四分支（R3-1 主場景＋R5-3/R6-2 終態收尾）：
      - **只在這筆 update 仍有一筆 `state='reserved'` 的 delta 保留列時**才做二分判定
        （同 in-process `_reconcile_unknown_quota_blocking` 既有精神：`qty` 減量/純改價的
        update 本就不建立保留列，`target_qty` 若沒有 delta 會與 `original_qty` 相同、無法
        用口數區分「改單生效」與「改單沒生效」兩種情境，寧可不猜——落到下面的終態/留待
        下一輪分支）：
        - `real_qty == 改後目標值`（`order.qty + delta`）→ 改單其實生效：resolve(ok,
          via=query_qty)＋原子寫 Order 新 price/qty＋confirm delta。
        - `real_qty == 改單前原值`（`order.qty`，unknown 分支未覆寫）→ 改單其實沒生效：
          resolve(error, via=query_qty)＋release delta。
      - 其餘（含 `real_qty is None`、沒有可比對的 delta 保留列、或口數兩者皆不符）：
        - `order.status` 已終態（cancelled/failed/filled）→ **終態 resolver**（R5-3/R6-2）：
          無法判斷最終 qty，resolve(unknown, via=report)＋delta **保守 confirm、不
          release**（低估可能已執行的增量比高估危險；沒有保留列時這一步是 no-op，但仍要
          resolve 這筆 ledger 列本身，避免永久卡住 update 單飛鎖），不改寫 Order price/qty
          （不知道真正執行了什麼，不可亂寫）。
        - `order.status` 非終態 → 留待下一輪（不動任何東西，watchdog 之後重跑會重新查）。

    回傳值供呼叫端/測試判斷實際採取的動作：`"confirmed"` / `"released"` /
    `"conservative_confirmed"` / `"left_pending"` / `"race_lost"`（Task 11 修復回合 1，
    發現 1：CAS 輸給了另一個同時搶著 resolve 這筆列的路徑——遲到 ack，或 Task 11 發現 2
    新增的另一個 resolver 掛載點；`_cas_resolve` 只在贏得 `WHERE resolved_at IS NULL` 這道
    原子條件時才繼續套用 Order/quota 效果，輸家完全不觸碰 Order/quota，也不改 outcome）。"""
    reservation: QuotaReservation | None = None
    if row.reservation_id is not None:
        reservation = session.exec(
            select(QuotaReservation).where(QuotaReservation.reservation_id == row.reservation_id)
        ).first()
    has_active_delta = reservation is not None and reservation.state == "reserved"
    original_qty = order.qty
    target_qty = original_qty + (reservation.qty if has_active_delta else 0)

    if has_active_delta and real_qty is not None and real_qty == target_qty:
        if not _cas_resolve(session, cmd_id=row.cmd_id, outcome="ok", resolved_via="query_qty",
                             result_json=json.dumps({"real_qty": real_qty}, ensure_ascii=False)):
            return "race_lost"
        payload = json.loads(row.payload)
        new_price = payload.get("price")
        price_value = Decimal(new_price) if new_price is not None else order.price
        apply_update_ack(
            session, order, new_price=price_value, new_qty=payload["qty"],
            reservation_id=row.reservation_id,
        )
        return "confirmed"

    if has_active_delta and real_qty is not None and real_qty == original_qty:
        if not _cas_resolve(session, cmd_id=row.cmd_id, outcome="error", resolved_via="query_qty",
                             result_json=json.dumps({"real_qty": real_qty}, ensure_ascii=False)):
            return "race_lost"
        brepo.release_quota(session, reservation_id=row.reservation_id)
        return "released"

    if order.status in ORDER_TERMINAL_FOR_UNKNOWN_RESOLVER:
        if not _cas_resolve(session, cmd_id=row.cmd_id, outcome="unknown", resolved_via="report",
                             result_json=json.dumps({"real_qty": real_qty}, ensure_ascii=False)):
            return "race_lost"
        if row.reservation_id is not None:
            brepo.confirm_quota(session, reservation_id=row.reservation_id)
        return "conservative_confirmed"

    return "left_pending"


def resolve_unresolved_cancel_via_report(session: Session, *, row: AgentCommand, order: Order) -> bool:
    """cancel 的 state-based resolver（D4 R4-4）：`order` 已進終態（cancelled/failed/
    filled）時，同一交易把這筆尚未 resolved 的 cancel 指令收斂為「回報已終結曝險、指令本身
    效果不可知」的誠實紀錄——`outcome='unknown'` 但 `resolved_at` 非 NULL（**`resolved` 唯一
    由 `resolved_at` 定義**，見 `AgentCommand` docstring；這是 cancel 專屬合法的
    unknown-且-resolved 組合）。cancel 無 quota 效果，不觸碰任何 QuotaReservation；也不改寫
    Order（終態已經是終態，不需要也不可以再改）。

    `order` 未達終態時回 False（no-op，呼叫端不需要 commit）——state-based 週期掃描不依賴
    「進入終態」的單一事件，之後重跑會再檢查一次，不會遺漏。

    Task 11 修復回合 1（發現 1）：回傳 False 還有第二種原因——CAS（`_cas_resolve`）輸給了
    另一個同時搶著 resolve 這筆列的路徑（遲到 ack，或發現 2 新增的另一個 resolver 掛載點）。
    兩種 False 對呼叫端的處置完全相同（no-op，不需要 commit），不需要區分。"""
    if order.status not in ORDER_TERMINAL_FOR_UNKNOWN_RESOLVER:
        return False
    return _cas_resolve(session, cmd_id=row.cmd_id, outcome="unknown", resolved_via="report",
                         result_json=json.dumps({}, ensure_ascii=False))


# ---------------------------------------------------------------------------
# 單筆 resolver 收斂（Task 11 修復回合 1，發現 2）：終態 resolver 的兩個掛載點共用同一份
# 讀-判-寫，不各自實作一份——`watchdog.py` 的 `_apply_update_resolution_blocking`/
# `_resolve_unresolved_cancel_cmd_blocking`（自開 session，週期掃描）與
# `inbox_worker._process_order_report`（借用既有交易，Order 推進終態的同一交易內立即收斂）
# 都呼叫這兩個函式；`session` 的生命週期／commit 時機一律交呼叫端決定，這裡只做
# 讀-判-寫，不 commit。
# ---------------------------------------------------------------------------


def resolve_one_unresolved_update(session: Session, *, cmd_id: str, real_qty: int | None) -> bool:
    """單筆 update-unknown-resolver 收斂：重新讀 `row`/`order`（不接受呼叫端傳入可能過期的
    ORM 物件——`row`/`order` 一律讀取呼叫當下的最新 DB 狀態，CAS 之外再上一道防線），核心
    比對交給 `resolve_update_via_query_qty`。回傳 True 僅代表「這次呼叫真的套用了效果」
    （`"left_pending"`/`"race_lost"` 都算 False——後者見 `resolve_update_via_query_qty`
    docstring）。"""
    row = session.get(AgentCommand, cmd_id)
    if row is None or row.resolved_at is not None:
        return False
    order = brepo.find_order_by_ordno(
        session, broker=row.broker, account=row.account, mode=row.mode, ordno=row.ordno
    )
    if order is None:
        return False
    action = resolve_update_via_query_qty(session, row=row, order=order, real_qty=real_qty)
    return action not in ("left_pending", "race_lost")


def resolve_one_unresolved_cancel(session: Session, *, cmd_id: str) -> bool:
    """單筆 cancel state-based resolver 收斂，理由與讀-判-寫慣例同
    `resolve_one_unresolved_update`。"""
    row = session.get(AgentCommand, cmd_id)
    if row is None or row.resolved_at is not None or row.ordno is None:
        return False
    order = brepo.find_order_by_ordno(
        session, broker=row.broker, account=row.account, mode=row.mode, ordno=row.ordno
    )
    if order is None:
        return False
    return resolve_unresolved_cancel_via_report(session, row=row, order=order)
