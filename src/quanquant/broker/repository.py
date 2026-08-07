"""Order/Deal/RawInbox/BrokerPosition/OrderAudit/ConfirmToken/QuotaReservation 倉儲。

雙方言可攜：CAS/upsert 一律走 session.get_bind().dialect.name 分流
sqlalchemy.dialects.{sqlite,postgresql}.insert（candles/repo.py 既有慣例），其餘用 SQLModel/select。

冪等/去重的 IntegrityError 精確化（round3 B5）：任何「撞唯一鍵即重播」的 catch，一律先
rollback() 後重新查詢「確認命中的正是該唯一鍵本身（用組成該唯一鍵的欄位重新查詢）」才回
既有列；其餘 IntegrityError（FK/NULL 等）一律重新拋出，不吞。

交易邊界（重要，本檔所有寫入函式的統一政策）：一律只 flush，不 commit——commit 的時機
交由呼叫端決定（Task 5 的 raw-inbox worker 要把「Deal insert + 部位帳務 + Order 狀態更新 +
Trade 寫入 + RawInbox.processed=true」包在同一個交易；Task 7 的 place 要把「quota reserve +
create_order」包在同一個交易）。純讀取函式本就不寫，不受此政策影響。

模組內 `select` 特別注意：本檔混用兩種 select——
  - `sqlmodel.select`（模組層級 import 的 `select` 名稱）：查詢 ORM entity（如
    `select(Order)`），`session.exec(...).first()` 直接拿到 mapped instance。
  - `sqlalchemy.select`（本檔另外取名 `sa_select` import）：組 Core 層級的純欄位/聚合
    子查詢（如 QuotaReservation 的 SUM 聚合、INSERT...SELECT 的來源 select），
    `session.exec(...)` 回傳的是 CursorResult/Row，不會被當成 ORM entity 水合。
  兩者絕不可混淆（混用會拿到 Row 而非 model instance，例如 `.state` 會 AttributeError）。
"""
import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import delete, func, insert, literal, or_, update
from sqlalchemy import select as sa_select
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from quanquant.db.models import (
    AgentAccountBinding,
    AgentCommand,
    BrokerPosition,
    BrokerReconcileCursor,
    ConfirmToken,
    Deal,
    Order,
    OrderAudit,
    QuotaReservation,
    RawInbox,
)

_CST = timezone(timedelta(hours=8))
_ACTIVE_QUOTA_STATES = ("reserved", "confirmed")

# ---- Order 狀態偏序（round3 #12：晚到/重播的狀態回報不得回退已達成的進度） ----
_ORDER_STATUS_RANK = {"pending": 0, "sending": 1, "submitted": 2, "partfilled": 3, "filled": 4}
_ORDER_TERMINAL_STATUSES = frozenset({"cancelled", "failed"})


class PositionConcurrencyError(Exception):
    """BrokerPosition 樂觀鎖 version CAS 衝突（見 cas_update_broker_position）：代表寫入當下
    版本已被別的寫入者搶先更動，呼叫端應重新讀取整筆 BrokerPosition 後重試，不可假裝成功、
    也不可直接覆寫（那正是 CAS 要防的 lost update）。"""


def _order_status_transition_allowed(current: str, new: str) -> bool:
    """狀態偏序守衛（round3 #12）：pending<sending<submitted<partfilled<filled；
    cancelled/failed 為終態。規則：
    - 終態（cancelled/failed）一旦達成即不可逆——任何後續狀態回報一律忽略。
    - 新狀態若是終態，只有在目前**尚未完全成交**（current != "filled"）時才允許
      （避免晚到/重播的 cancelled 覆蓋掉已經 filled 的委託，讓正式紀錄失真）。
    - 兩者皆是進度性狀態時，只有嚴格前進（new 的 rank 高於 current）才允許——
      避免晚到/重播的 Submitted 把已經 partfilled/filled 的委託往回蓋。

    Task 6 補充：`"unknown"`（native 呼叫失敗但不確定是否已送達券商，見
    ShioajiAdapter.place 的 except 分支）不在 `_ORDER_STATUS_RANK` 之列，若不特判，
    fallback 的 rank 比較會讓 pending→unknown 被誤判為「沒有嚴格前進」而擋下
    （`_ORDER_STATUS_RANK.get("unknown", -1)` 恆為 -1）。`"unknown"` 是待 watchdog
    reconcile 決議的暫態、不是終態，也不是進度序列的一部分：
    - 只要目前**尚未完全成交**都可以標成 unknown（同終態保護精神，已成交不可回退成不明）。
    - 從 unknown 之後，任何進度/終態回報都視為 reconcile 決議完成，一律放行覆蓋。
    """
    if current == new:
        return False
    if current in _ORDER_TERMINAL_STATUSES:
        return False
    if new in _ORDER_TERMINAL_STATUSES:
        return current != "filled"
    if new == "unknown":
        return current != "filled"
    if current == "unknown":
        return True
    return _ORDER_STATUS_RANK.get(new, -1) > _ORDER_STATUS_RANK.get(current, -1)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def trading_day_for(ts_ms: int) -> str:
    """成交 epoch-ms UTC → CST 日曆日期字串（Deal/Order/QuotaReservation scope 組成之一）。
    刻意用「日曆日」而非交易時段語意（candles.trading_date 的夜盤跨日規則）——這裡只需要
    一個穩定、可重現的值防止 fill_id 理論上跨日碰撞，不需要交易時段判斷。"""
    return datetime.fromtimestamp(ts_ms / 1000, _CST).strftime("%Y-%m-%d")


# ---- Order ----

def create_order(
    session: Session,
    *,
    client_order_id: str,
    request_hash: str,
    user_id: int,
    mode: str,
    broker: str,
    account: str,
    symbol: str,
    action: str,
    qty: int,
    price,
    price_type: str,
    order_type: str,
    octype: str,
    trading_day: str,
) -> Order:
    """冪等建委託：同 client_order_id 已存在 → request_hash 相符回既有列，不符則拒絕（V3-3）。

    round3 B5：IntegrityError 精確化——撞鍵後重新以 client_order_id（本函式插入時唯一會撞
    到的鍵，ordno/broker_order_id 建單當下皆為 NULL、SQL NULL 不觸發 UNIQUE 衝突）查詢，
    確認命中的正是這個鍵才當重播回既有列；查無則代表 IntegrityError 另有原因（FK/NULL 等），
    重新拋出，不靜默吞。
    """
    existing = find_order_by_client_order_id(session, client_order_id)
    if existing is not None:
        if existing.request_hash != request_hash:
            raise ValueError(
                f"client_order_id={client_order_id!r} 已存在但 payload 不同"
                "（冪等鍵不可變更內容，疑似竄改或用錯 client_order_id）"
            )
        return existing
    order = Order(
        client_order_id=client_order_id, request_hash=request_hash, user_id=user_id, mode=mode,
        broker=broker, account=account, symbol=symbol, action=action, qty=qty, price=price,
        price_type=price_type, order_type=order_type, octype=octype, trading_day=trading_day,
    )
    session.add(order)
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        existing = find_order_by_client_order_id(session, client_order_id)
        if existing is not None:
            if existing.request_hash != request_hash:
                raise ValueError(
                    f"client_order_id={client_order_id!r} 已存在但 payload 不同"
                ) from None
            return existing  # 競態：另一請求先插入，確認撞的正是 client_order_id 唯一鍵
        raise  # 非本鍵造成的 IntegrityError（FK/NULL 等）→ 不吞，拋出
    return order


def find_order_by_client_order_id(session: Session, client_order_id: str) -> Order | None:
    return session.exec(select(Order).where(Order.client_order_id == client_order_id)).first()


def find_order_by_ordno(
    session: Session, *, broker: str, account: str, mode: str, ordno: str
) -> Order | None:
    """複合 scope 查詢（round3 #3）——修 BLOCKER#3：裸鍵 `.first()` 在多帳戶/多 mode 下
    可能撞到別人的委託，解析 fill→order 一律不得用裸 ordno/broker_order_id 查詢。"""
    stmt = select(Order).where(
        Order.broker == broker, Order.account == account, Order.mode == mode, Order.ordno == ordno
    )
    return session.exec(stmt).first()


def find_order_by_broker_id(
    session: Session, *, broker: str, account: str, mode: str, broker_order_id: str
) -> Order | None:
    """複合 scope 查詢（round3 #3），同上但用 broker_order_id 當關聯鍵。"""
    stmt = select(Order).where(
        Order.broker == broker, Order.account == account, Order.mode == mode,
        Order.broker_order_id == broker_order_id,
    )
    return session.exec(stmt).first()


def set_order_sending(session: Session, order_id: int) -> Order | None:
    order = session.get(Order, order_id)
    if order is None:
        return None
    order.status = "sending"
    order.updated_at = _utcnow()
    session.add(order)
    session.flush()
    return order


def set_order_ack(
    session: Session, order_id: int, *, broker_order_id: str, ordno: str | None, status: str
) -> Order | None:
    order = session.get(Order, order_id)
    if order is None:
        return None
    order.broker_order_id = broker_order_id
    order.ordno = ordno
    order.status = status
    order.updated_at = _utcnow()
    session.add(order)
    session.flush()
    return order


def apply_order_fill(
    session: Session, order: Order, *, fill_qty: int, fill_price, terminal_status: str | None = None
) -> Order:
    """單調累加 filled_qty + 加權更新 avg_fill_price（V3-2「Order 狀態由 fill 更新」）。

    數學上安全：filled_qty 只增不減（一張委託的成交只會累加，不像 BrokerPosition 有
    「中途平倉」這種會讓歷史貢獻被錯誤覆寫的操作），故 (old_avg*old_qty + new_price*new_qty)/(old+new)
    是精確的加權平均，不是估計。

    round3 #12：filled_qty/avg_fill_price 永遠累加（真實成交量沒有「回退」這件事），但
    `status` 的寫入受 `_order_status_transition_allowed` 守衛——晚到/重播的低序狀態
    （或非法把已 filled 覆寫成 cancelled）一律忽略，不回退。"""
    prior_qty = order.filled_qty
    prior_notional = (order.avg_fill_price or Decimal(0)) * prior_qty
    new_qty = prior_qty + fill_qty
    order.avg_fill_price = (prior_notional + fill_price * fill_qty) / new_qty
    order.filled_qty = new_qty
    candidate_status = terminal_status or ("filled" if new_qty >= order.qty else "partfilled")
    if _order_status_transition_allowed(order.status, candidate_status):
        order.status = candidate_status
    order.updated_at = _utcnow()
    session.add(order)
    session.flush()
    return order


def mark_order_status(session: Session, order: Order, *, status: str) -> Order:
    """純狀態回報（委託回報 callback，如 cancelled/failed/submitted），不動 filled_qty。

    round3 #12：狀態寫入受 `_order_status_transition_allowed` 守衛，晚到/重播的回報
    不得回退已達成的進度（不符合的呼叫直接 no-op，不寫入、不 flush）。"""
    if not _order_status_transition_allowed(order.status, status):
        return order
    order.status = status
    order.updated_at = _utcnow()
    session.add(order)
    session.flush()
    return order


def list_orders(session: Session, *, user_id: int, mode: str, limit: int = 100) -> list[Order]:
    stmt = (
        select(Order)
        .where(Order.user_id == user_id, Order.mode == mode)
        .order_by(Order.created_at.desc())  # type: ignore[union-attr]
        .limit(limit)
    )
    return list(session.exec(stmt))


def count_orders_today(session: Session, *, user_id: int, mode: str, trading_day: str) -> int:
    stmt = select(Order).where(
        Order.user_id == user_id, Order.mode == mode, Order.trading_day == trading_day
    )
    return len(list(session.exec(stmt)))


_OPEN_ORDER_STATUSES = ("submitted", "partfilled")


def count_open_orders(session: Session, *, mode: str | None = None) -> int:
    """kill switch 告警用：粗略計算「當下仍掛在券商、未成交」的委託數（submitted/partfilled，
    與 order_table.html 判定可取消/改單的 live-open 集合一致）。全域（不分 user）、best-effort，
    供人工決定是否手動撤單參考；`mode` 指定時只計該執行 mode（real/sim）。"""
    stmt = select(Order).where(Order.status.in_(_OPEN_ORDER_STATUSES))
    if mode is not None:
        stmt = stmt.where(Order.mode == mode)
    return len(list(session.exec(stmt)))


def sum_qty_today(session: Session, *, user_id: int, mode: str, trading_day: str) -> int:
    stmt = select(Order).where(
        Order.user_id == user_id, Order.mode == mode, Order.trading_day == trading_day
    )
    return sum(o.qty for o in session.exec(stmt))


def list_unknown_orders_older_than(
    session: Session, *, older_than: datetime, limit: int = 200
) -> list[Order]:
    """Task 8 watchdog「quota unknown reconcile」用：找出送單結果不明（status="unknown"，見
    ShioajiAdapter.place/update 的 except 分支）且已經卡了一段時間（updated_at < older_than，
    給 reconcile() 對帳流程一段時間自然解決）的委託，交給 watchdog 依券商真實狀態決議。"""
    stmt = (
        select(Order)
        .where(Order.status == "unknown", Order.updated_at < older_than)
        .order_by(Order.id)
        .limit(limit)
    )
    return list(session.exec(stmt))


def list_pending_orphans_older_than(
    session: Session, *, older_than: datetime, limit: int = 200
) -> list[Order]:
    """T0.2 孤兒單偵測：找出卡在 pending/sending、且**從未拿到任何券商識別碼**（ordno 與
    broker_order_id 皆 NULL）、又卡了一段時間（updated_at < older_than）的委託。

    這代表 process 曾在「create_order/reserve_quota 之後、set_order_ack 落地之前」崩潰。
    與 unknown+NULL 不同（那是 native 呼叫**拋例外**、幾乎確定沒送達），pending 孤兒是
    native place_order() **可能已成功回傳**（券商已收單）後才崩潰——因此**不可**自動判
    failed/釋放配額（會少算曝險→過度交易）。本函式只用來 surface 給人工/reconcile 依券商
    真相處理；對應 QuotaReservation 仍卡 reserved，配額收尾需券商端關聯確認後才做。"""
    stmt = (
        select(Order)
        .where(
            Order.status.in_(("pending", "sending")),  # type: ignore[union-attr]
            Order.ordno.is_(None),  # type: ignore[union-attr]
            Order.broker_order_id.is_(None),  # type: ignore[union-attr]
            Order.updated_at < older_than,
        )
        .order_by(Order.id)
        .limit(limit)
    )
    return list(session.exec(stmt))


# ---- RawInbox（durable callback spool，V3-2；Inc1 D5：per-user scope 蓋章） ----

def stage_raw_inbox(
    session: Session,
    *,
    kind: str,
    broker: str,
    payload: str,
    user_id: int | None = None,
    account: str | None = None,
    mode: str | None = None,
) -> RawInbox:
    """Inc1 D5：`user_id`/`account`/`mode` 皆為上行來源在事件產生當下蓋章的 immutable scope
    （見 `inbox_worker.stage_scoped_raw_inbox`/`commit_raw_callback`——唯一 scoped staging API，
    本函式是它們共用的底層 insert）。預設全 None 以相容既有直接呼叫端（測試／尚未蓋章的呼叫
    路徑），語意等同「未蓋章的舊列」，不強制呼叫端一定要指定。"""
    row = RawInbox(kind=kind, broker=broker, payload=payload, user_id=user_id, account=account, mode=mode)
    session.add(row)
    session.flush()
    return row


def find_account_binding(session: Session, *, broker: str, account: str) -> AgentAccountBinding | None:
    """D10/D5：查 `(broker,account)` 目前綁定的 user（Task 6 才建立寫入/backfill 邏輯，本 task
    只讀）。查無列＝該帳號尚未綁定任何人。"""
    stmt = select(AgentAccountBinding).where(
        AgentAccountBinding.broker == broker, AgentAccountBinding.account == account,
    )
    return session.exec(stmt).first()


def list_unprocessed_raw_inbox(
    session: Session, *, limit: int = 200, user_id: int | None = None
) -> list[RawInbox]:
    """Inc1 D6/D9：`user_id=None`（預設）＝不加篩選，與既有 in-process 單一 worker 行為
    位元級一致；agent 模式 per-slot `RawInboxWorker` 傳自己的 `slot.user_id` 精確篩
    `RawInbox.user_id == user_id`（不是 `IS NULL OR =`）——歷史未蓋章的 NULL 列因此永遠不會
    被 agent per-slot worker 撿走，只由 in-process worker（不帶 `user_id`）處理，見 D5。"""
    conditions = [RawInbox.processed.is_(False), RawInbox.quarantine.is_(False)]  # type: ignore[union-attr]
    if user_id is not None:
        conditions.append(RawInbox.user_id == user_id)
    stmt = select(RawInbox).where(*conditions).order_by(RawInbox.id).limit(limit)
    return list(session.exec(stmt))


def mark_raw_inbox_processed(session: Session, row: RawInbox) -> None:
    row.processed = True
    row.processed_at = _utcnow()
    session.add(row)
    session.flush()


# R2-6：三個永久 dead-letter reason——association_pending（預設，可重試）以外的都不進
# unquarantine/重試迴圈，退出所有 unprocessed 計數與換帳號 guard（見 quarantine_raw_inbox）。
DEAD_LETTER_QUARANTINE_REASONS = frozenset({"scope_violation", "payload_mismatch", "user_mismatch"})


def quarantine_raw_inbox(
    session: Session, row: RawInbox, *, error: str, reason: str = "association_pending"
) -> None:
    """R2-6 quarantine 分級：`reason="association_pending"`（預設，既有 ValueError/
    PositionMismatchError 路徑）代表可重試——watchdog `unquarantine_stale_raw_inbox` 之後會
    給它機會；`reason` 為 `DEAD_LETTER_QUARANTINE_REASONS` 三者之一時是**永久** dead-letter——
    連同 `quarantine=True` 一併把 `processed=True`（＋`processed_at`）落地，讓這列同時退出
    `unquarantine_stale_raw_inbox` 的重試迴圈與換帳號 guard 的 unprocessed 計數（那個計數只看
    `processed==False`，不看 `quarantine`），但保留列本身（`error`/`reason`）供稽核，不是丟棄。"""
    row.quarantine = True
    row.error = error
    row.quarantine_reason = reason
    if reason in DEAD_LETTER_QUARANTINE_REASONS:
        row.processed = True
        row.processed_at = _utcnow()
    session.add(row)
    session.flush()


def unquarantine_stale_raw_inbox(
    session: Session, *, older_than: datetime, limit: int = 200, user_id: int | None = None
) -> int:
    """把 quarantine 超過 older_than 的列解除隔離，回到一般佇列重新嘗試一次
    （Task 8 watchdog 以較慢週期呼叫——給「當時解不到委託關聯」的列一個補救機會，
    不會無限重試：解除後若原因仍不變會再次被 quarantine，只是白工，不會誤判成功）。

    R2-6：只解除 `quarantine_reason` 為 `NULL`（既有列／尚未蓋章 reason 的舊資料，保守視為可
    重試）或 `'association_pending'` 的列——`DEAD_LETTER_QUARANTINE_REASONS` 三者是永久
    dead-letter，永不進這個重試迴圈（否則會把已經 fail-closed 判定的列重新丟回處理管線，
    製造無限重試/告警洪水）。

    Inc1 D6：`user_id=None`（預設）＝不篩，與既有 in-process 單一 watchdog 行為位元級一致；
    agent 模式 per-slot watchdog 傳自己的 `slot.user_id`——`RawInbox.user_id == user_id` 精確
    比對（NULL 列不會被任何 user_id 值命中，天然把未蓋章的歷史殘留留給人工／in-process 處理，
    不會被某個 agent slot 誤認領，同 `list_unprocessed_raw_inbox` 的篩選原則）。"""
    conditions = [
        RawInbox.quarantine.is_(True),  # type: ignore[union-attr]
        RawInbox.received_at < older_than,
        or_(
            RawInbox.quarantine_reason.is_(None),  # type: ignore[union-attr]
            RawInbox.quarantine_reason == "association_pending",
        ),
    ]
    if user_id is not None:
        conditions.append(RawInbox.user_id == user_id)
    stmt = select(RawInbox).where(*conditions).order_by(RawInbox.id).limit(limit)
    rows = list(session.exec(stmt))
    for row in rows:
        row.quarantine = False
        row.error = None
        row.quarantine_reason = None
        session.add(row)
    session.flush()
    return len(rows)


# ---- AgentAccountBinding（D10：帳號↔使用者唯一綁定；Task 6：寫入/backfill/UpLogin guard v2）----
#
# I9 不變式：一個 (broker,account) 至多屬於一個 user（不分 mode，codex R2-7——Inc1 只會有
# sim 登入，但綁定與檢查涵蓋全部 mode，避免「同 user 的 sim/real 兩列撞 PK」與 mode 欄語意
# 含糊）。`find_account_binding`（見上方 RawInbox 段）是既有唯讀查詢（Task 5，供
# `inbox_worker._validate_report_scope` 用）；以下補上寫入（`bind_account`/
# `backfill_account_bindings`）與 UpLogin 專用 guard 查詢（`count_unprocessed_for_login`/
# `has_unresolved_risky_commands_other_account`），供 `agent_ws._check_uplogin`（Task 6）使用。


class BackfillConflictError(Exception):
    """D10/R1-8/R2-7：backfill 掃到同一 `(broker,account)` 歷史上同時屬於多個 user（或既有
    綁定列與 Order 歷史 owner 不符）——不能讓「先綁先贏」隨機挑一個覆蓋既有 ownership，必須
    人工裁決。呼叫端（`web/app.py` agent 分支啟動）收到此例外應讓下單子系統整體拒啟
    （fail closed），不可吞掉或忽略。"""


def bind_account(session: Session, *, broker: str, account: str, user_id: int) -> bool:
    """D10：UpLogin 用的先綁先贏寫入。查無列 → 建新綁定，回 True；已綁同一 user → no-op，
    回 True（冪等，允許同帳號重連/重試）；已綁別的 user → 回 False（呼叫端據此拒登，不動
    這一列）。

    本函式只 flush，不 commit（見本檔頂部交易邊界政策）——是否真正落地由呼叫端的交易決定
    （Task 6：`agent_ws._check_uplogin` 把這個 flush 跟後面幾步 guard 查詢包在同一個交易，
    全過才 commit；任一步被擋，呼叫端 rollback，這裡的寫入不會留下殘影）。round3 B5 慣例：
    撞唯一鍵時 rollback 後以該唯一鍵重新查詢，確認命中的正是這個鍵才決定 True/False；其餘
    IntegrityError 不吞、往上拋。"""
    existing = find_account_binding(session, broker=broker, account=account)
    if existing is not None:
        return existing.user_id == user_id
    binding = AgentAccountBinding(broker=broker, account=account, user_id=user_id)
    session.add(binding)
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        existing = find_account_binding(session, broker=broker, account=account)
        if existing is not None:
            return existing.user_id == user_id
        raise
    return True


def account_owned_by_other_user_in_orders(
    session: Session, *, broker: str, account: str, user_id: int
) -> bool:
    """D10/R1-8：UpLogin 第二道防線——不只信 `agent_account_bindings` 新表，直接核對 `Order`
    歷史紀錄：這個 `(broker,account)` 是否存在別的 user 建立過的委託（掃全部 mode）。
    True → 呼叫端應拒登。正常情況下這個分支不該獨立命中（backfill 已在啟動時把歷史
    ownership 灌進綁定表，`bind_account` 那一步就會先擋下）；這裡是「不能只信新表」的
    belt-and-suspenders，涵蓋 backfill 未執行/資料落後等異常情境。"""
    stmt = select(Order.id).where(
        Order.broker == broker, Order.account == account, Order.user_id != user_id,
    ).limit(1)
    return session.exec(stmt).first() is not None


def backfill_account_bindings(session_factory) -> None:
    """D10/R1-8/R2-7：agent 模式啟動時呼叫（`web/app.py` `_start_agent_channel_subsystem`）。
    掃描既有 `Order`（distinct `(broker,account)` → 該帳號歷史上出現過的所有 user_id，
    **合併全部 mode**——R2-7 綁定不分 mode）灌進 `agent_account_bindings`。

    Fail closed（R1-8）：任一 `(broker,account)` 歷史上同時屬於多個 user，或既有綁定列與
    Order 歷史 owner 不符（如人工誤改 DB），一律 raise `BackfillConflictError`、**整批不寫入
    任何一列**。實作上分兩層：跨 user 的 Order 歷史衝突在迴圈開始前**預掃**一次性抓出（見
    下方 `conflicts` 計算）；既有綁定列與 Order 歷史 owner 不符則是在逐 `(broker,account)`
    寫入迴圈中才發現（`elif existing.user_id != owner_user_id`）。兩者都只 `session.add`、
    不逐筆 commit——真正落地靠迴圈結束後**單一次** `session.commit()`，衝突中途 raise 時
    尚未 commit 的 add 都隨例外傳播、session 生命週期結束而失效，不會有「部分帳號已寫入、
    衝突的那個沒寫」的半途狀態，呼叫端據此讓整個 agent 子系統拒啟，不能隨機挑一個 user
    覆蓋既有 ownership。

    冪等：已有正確綁定的 `(broker,account)` 重跑無副作用（no-op）；只在缺列時補寫。"""
    with session_factory() as session:
        rows = session.exec(sa_select(Order.broker, Order.account, Order.user_id).distinct()).all()
        owners: dict[tuple[str, str], set[int]] = {}
        for broker, account, uid in rows:
            owners.setdefault((broker, account), set()).add(uid)

        conflicts = {key: uids for key, uids in owners.items() if len(uids) > 1}
        if conflicts:
            detail = "; ".join(
                f"{broker}/{account}→users={sorted(uids)}"
                for (broker, account), uids in sorted(conflicts.items())
            )
            raise BackfillConflictError(
                f"帳號綁定 backfill 偵測到跨 user 歷史 ownership 衝突，fail closed：{detail}"
            )

        for (broker, account), uids in owners.items():
            (owner_user_id,) = uids
            existing = find_account_binding(session, broker=broker, account=account)
            if existing is None:
                session.add(AgentAccountBinding(broker=broker, account=account, user_id=owner_user_id))
            elif existing.user_id != owner_user_id:
                raise BackfillConflictError(
                    f"{broker}/{account} 既有綁定 user_id={existing.user_id} 與 Order 歷史 "
                    f"owner user_id={owner_user_id} 不符，fail closed"
                )
        session.commit()


def count_unprocessed_for_login(session: Session, *, user_id: int, account: str) -> int:
    """Task 6（S2 per-user 化，取代舊版全域 `agent_ws._count_unprocessed_raw_inbox`）：這個
    user 名下未處理（`processed==False`，不論 quarantine——同 codex round4 修正，dead-letter
    已在 `quarantine_raw_inbox` 內把 processed 設 True，天然被排除，不需要另外濾 quarantine）
    的 `RawInbox` 中，`account` 與這次登入帳號不同、或未蓋章（NULL）的列數。>0 代表這個 user
    還有可能被之後 worker 用「已被新帳號覆蓋的 mutable adapter.account」錯配處理的殘留，
    UpLogin 應拒登（codex round2 fix2 的原始理由，Task 6 改成 per-user scope）。同帳號重連
    的殘留（account 與這次登入帳號相同）不計入——那是正常在途處理，不是換帳號風險。"""
    stmt = select(func.count()).where(
        RawInbox.processed == False,  # noqa: E712 - SQLAlchemy 表達式需字面 == 比較
        RawInbox.user_id == user_id,
        or_(RawInbox.account != account, RawInbox.account.is_(None)),
    )
    return session.exec(stmt).one()


_RISKY_COMMAND_KINDS = ("place", "update")


def has_unresolved_risky_commands_other_account(
    session: Session, *, user_id: int, account: str
) -> bool:
    """R1-2：這個 user 在別的帳號（`account` 不等於這次登入帳號）是否還有未 resolved
    （`resolved_at IS NULL`）的曝險指令——只算 `place`/`update`（cancel 不是新增曝險，讓
    cancel 收斂不擋換帳號，R3-1 #29）。True → UpLogin 應拒登，要求先用原帳號連線收斂
    （或走人工終結程序）。"""
    stmt = select(AgentCommand.cmd_id).where(
        AgentCommand.user_id == user_id,
        AgentCommand.account != account,
        AgentCommand.kind.in_(_RISKY_COMMAND_KINDS),
        AgentCommand.resolved_at.is_(None),
    ).limit(1)
    return session.exec(stmt).first() is not None


def has_unresolved_update_command(session: Session, *, client_order_id: str) -> bool:
    """Task 8：`ShioajiAdapter.update` 的 ledger insert 撞到 `uq_agent_cmd_update_singleflight`
    partial unique index 後，用這個查詢確認撞的正是這個單飛鍵（同一 `client_order_id` 已有
    一筆 `kind='update' AND resolved_at IS NULL` 的列）——確認命中才轉「前一筆改單結果未定」
    的友善訊息，查無則代表 IntegrityError 另有原因（如 cmd_id 這種天文數字機率的 uuid4
    碰撞），呼叫端應原樣拋出（codex R6-3：精確辨認單飛 index，不可把所有 IntegrityError
    都吞成同一句話）。"""
    stmt = select(AgentCommand.cmd_id).where(
        AgentCommand.client_order_id == client_order_id,
        AgentCommand.kind == "update",
        AgentCommand.resolved_at.is_(None),
    ).limit(1)
    return session.exec(stmt).first() is not None


# ---- Deal（fill 去重帳本） ----

def _find_deal(
    session: Session, *, broker: str, mode: str, account: str, trading_day: str, fill_id: str
) -> Deal | None:
    stmt = select(Deal).where(
        Deal.broker == broker, Deal.mode == mode, Deal.account == account,
        Deal.trading_day == trading_day, Deal.fill_id == fill_id,
    )
    return session.exec(stmt).first()


def stage_deal(
    session: Session,
    *,
    broker: str,
    account: str,
    mode: str,
    trading_day: str,
    fill_id: str,
    ordno: str | None,
    broker_order_id: str | None,
    order_id: int | None,
    user_id: int | None,
    symbol: str,
    action: str,
    price,
    qty: int,
    fee,
    octype: str,
    ts: int,
    raw_inbox_id: int | None,
) -> Deal | None:
    """把成交插進去重帳本；只 flush 不 commit（呼叫端負責交易邊界）。
    撞唯一鍵（重播）→ 回 None，不重寫；其餘 IntegrityError 不吞、往上拋（round3 B5：
    重新以 (broker,mode,account,trading_day,fill_id) 這組唯一鍵本身查詢確認命中）。"""
    existing = _find_deal(session, broker=broker, mode=mode, account=account,
                          trading_day=trading_day, fill_id=fill_id)
    if existing is not None:
        return None
    deal = Deal(
        broker=broker, account=account, mode=mode, trading_day=trading_day, fill_id=fill_id,
        ordno=ordno, broker_order_id=broker_order_id, order_id=order_id, user_id=user_id,
        symbol=symbol, action=action, price=price, qty=qty, fee=fee, octype=octype, ts=ts,
        raw_inbox_id=raw_inbox_id,
    )
    session.add(deal)
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        existing = _find_deal(session, broker=broker, mode=mode, account=account,
                              trading_day=trading_day, fill_id=fill_id)
        if existing is not None:
            return None  # 競態：另一 worker 先插入，確認撞的正是本唯一鍵
        raise
    return deal


# ---- BrokerPosition ----

def find_open_position(
    session: Session, *, user_id: int, broker: str, account: str, mode: str, symbol: str, direction: str
) -> BrokerPosition | None:
    stmt = select(BrokerPosition).where(
        BrokerPosition.user_id == user_id, BrokerPosition.broker == broker,
        BrokerPosition.account == account, BrokerPosition.mode == mode,
        BrokerPosition.symbol == symbol, BrokerPosition.direction == direction,
        BrokerPosition.status == "open",
    )
    return session.exec(stmt).first()


def list_open_positions(
    session: Session, *, user_id: int, broker: str, account: str, mode: str, symbol: str
) -> list[BrokerPosition]:
    stmt = select(BrokerPosition).where(
        BrokerPosition.user_id == user_id, BrokerPosition.broker == broker,
        BrokerPosition.account == account, BrokerPosition.mode == mode,
        BrokerPosition.symbol == symbol, BrokerPosition.status == "open",
    )
    return list(session.exec(stmt))


def remaining_qty(pos: BrokerPosition) -> int:
    return pos.total_opened_qty - pos.closed_qty


def avg_entry_price(pos: BrokerPosition) -> Decimal:
    return pos.entry_notional / pos.total_opened_qty


def avg_exit_price(pos: BrokerPosition) -> Decimal:
    return pos.exit_notional / pos.closed_qty


def cas_update_broker_position(session: Session, pos: BrokerPosition, **values: object) -> BrokerPosition:
    """樂觀鎖版本 CAS 更新 BrokerPosition（round3 #15 version 欄位的實際用法）：呼叫端在
    Python 端算好新的絕對值（Decimal 算術一律在 Python 端做——`entry_notional`/`exit_notional`
    等欄位是 DecimalText/TEXT column，SQL 層級做加法不安全，見 db/models.py DecimalText），
    本函式只負責用單一 `UPDATE ... WHERE id=? AND version=?` 陳述式原子地把它們寫入並把
    version 推進一格；`rowcount != 1` 代表寫入當下已被別的寫入者搶先更動版本（並發競態），
    拋 `PositionConcurrencyError`，呼叫端應重新讀取整筆 BrokerPosition 後重試，不可假裝成功。

    成功後直接把新值 setattr 回傳入的 `pos`（同時把它在 session identity map 的已知快照
    對齊），避免呼叫端另外重新查詢一次；不可讓 `pos` 之後又被 ORM 預設 flush 覆寫回舊值。"""
    t = BrokerPosition.__table__
    current_version = pos.version
    payload = dict(values)
    payload["version"] = current_version + 1
    payload.setdefault("updated_at", _utcnow())
    stmt = (
        update(t)
        .where(t.c.id == pos.id, t.c.version == current_version)
        .values(**payload)
    )
    result = session.exec(stmt)  # type: ignore[call-overload]
    if result.rowcount != 1:
        raise PositionConcurrencyError(
            f"BrokerPosition id={pos.id} 版本衝突（預期 version={current_version}），"
            "疑似並發寫入，請重新讀取後重試"
        )
    session.flush()
    for key, value in payload.items():
        setattr(pos, key, value)
    return pos


# ---- OrderAudit ----

def audit_reference_hash(*parts: object) -> str:
    """純稽核用途的參考摘要（如 cancel/reconnect 沒有完整可執行 payload 時）。
    **不是** broker.types.canonical_payload_hash，不可拿來做 confirm token 簽發/驗證。"""
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def append_audit(
    session: Session,
    *,
    actor_user_id: int | None,
    mode: str,
    action: str,
    payload_hash: str,
    result: str,
    rule: str | None = None,
    detail: str | None = None,
    now_ms: int | None = None,
) -> OrderAudit:
    audit = OrderAudit(
        ts=now_ms if now_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000),
        actor_user_id=actor_user_id, mode=mode, action=action,
        payload_hash=payload_hash, rule=rule, result=result, detail=detail,
    )
    session.add(audit)
    session.flush()
    return audit


# ---- ConfirmToken（round3 #16：TTL+JTI DB 列，claim 為原子 UPDATE） ----

def create_confirm_token_row(
    session: Session, *, jti: str, actor_user_id: int, payload_hash: str, expires_at: datetime
) -> ConfirmToken:
    row = ConfirmToken(jti=jti, actor_user_id=actor_user_id, payload_hash=payload_hash, expires_at=expires_at)
    session.add(row)
    session.flush()
    return row


def claim_confirm_token(
    session: Session, *, jti: str, actor_user_id: int, payload_hash: str, now: datetime
) -> bool:
    """原子 UPDATE：consumed_at IS NULL AND 未過期 AND actor/payload 相符 才能 claim 成功。
    rowcount==1 → True（單次呼叫最多消費一次，重放/競態下第二次一定拿到 False）。"""
    stmt = (
        update(ConfirmToken.__table__)
        .where(
            ConfirmToken.__table__.c.jti == jti,
            ConfirmToken.__table__.c.actor_user_id == actor_user_id,
            ConfirmToken.__table__.c.payload_hash == payload_hash,
            ConfirmToken.__table__.c.consumed_at.is_(None),
            ConfirmToken.__table__.c.expires_at > now,
        )
        .values(consumed_at=now)
    )
    result = session.exec(stmt)  # type: ignore[call-overload]
    ok = result.rowcount == 1
    if ok:
        session.flush()
    return ok


def cleanup_expired_confirm_tokens(session: Session, *, now: datetime) -> int:
    """round3 #16：定期清理過期或已消費的 ConfirmToken 列，避免 DB 無界成長（token 本身沒有
    保留價值——過期的驗證不過、已消費的不能重放，兩者都是純垃圾）。回傳刪除筆數。"""
    t = ConfirmToken.__table__
    stmt = delete(t).where(or_(t.c.expires_at < now, t.c.consumed_at.is_not(None)))
    result = session.exec(stmt)  # type: ignore[call-overload]
    session.flush()
    return result.rowcount


# ---- BrokerReconcileCursor（round3 #2：持久對帳 watermark） ----

def get_reconcile_cursor(
    session: Session, *, broker: str, account: str, mode: str
) -> datetime | None:
    stmt = select(BrokerReconcileCursor).where(
        BrokerReconcileCursor.broker == broker, BrokerReconcileCursor.account == account,
        BrokerReconcileCursor.mode == mode,
    )
    row = session.exec(stmt).first()
    return row.last_reconciled_at if row is not None else None


def upsert_reconcile_cursor(
    session: Session, *, broker: str, account: str, mode: str, at: datetime
) -> None:
    """單一寫入者（watchdog，經 supervisor.lock 序列化），不需要 CAS——單純
    select-then-write 已足夠安全，比照本檔其餘僅低頻背景寫入的慣例（非配額/委託身分那類
    高併發路徑）。"""
    stmt = select(BrokerReconcileCursor).where(
        BrokerReconcileCursor.broker == broker, BrokerReconcileCursor.account == account,
        BrokerReconcileCursor.mode == mode,
    )
    row = session.exec(stmt).first()
    if row is None:
        row = BrokerReconcileCursor(broker=broker, account=account, mode=mode, last_reconciled_at=at)
    else:
        row.last_reconciled_at = at
        row.updated_at = _utcnow()
    session.add(row)
    session.flush()


# ---- QuotaReservation（round3 #4：per-reservation id + 狀態機，CAS 原語） ----
#
# 「日額聚合用 CAS（條件 UPDATE）」的字面意思在單列聚合設計下是 reserved_qty 的
# conditional UPDATE；本設計把聚合改成「多列各自狀態」，等價的原子寫入原語變成
# INSERT...SELECT...WHERE（reserve_quota）與 UPDATE...WHERE state='reserved'
# （confirm_quota/release_quota）——三者都是「單一 SQL 陳述式內完成條件判斷與寫入」，
# 與舊版 conditional UPDATE 同一等級的原子性保證，只是聚合來源從單列的欄位值改成
# 多列的 SUM(...)。

def reserve_quota(
    session: Session,
    *,
    reservation_id: str,
    user_id: int,
    mode: str,
    trading_day: str,
    qty: int,
    daily_limit: int,
) -> bool:
    """CAS 保留配額：用單一 INSERT ... SELECT ... WHERE 陳述式，在同一條 SQL 內完成
    「讀目前已用配額（reserved+confirmed 狀態列的 SUM(qty)）」與「若未超限才寫入新保留列」，
    不存在應用層讀寫之間的 TOCTOU 窗口——單一陳述式的執行與寫入衝突序列化由 DB 引擎保證
    （SQLite 對衝突寫入交易序列化重試/擋 SQLITE_BUSY，Postgres 用列鎖+MVCC），已用真實
    多執行緒 + 檔案 DB 測試驗證（見 tests/test_broker_repo.py 對應測試）。

    reservation_id 為呼叫端提供的冪等鍵（place 用 client_order_id）：重放同一 reservation_id
    直接回報該列目前是否仍佔用配額（reserved/confirmed=True，released=False），不重複計入
    配額、不重複插入列。
    """
    existing = session.exec(
        select(QuotaReservation).where(QuotaReservation.reservation_id == reservation_id)
    ).first()
    if existing is not None:
        return existing.state in _ACTIVE_QUOTA_STATES

    now = _utcnow()
    t = QuotaReservation.__table__
    used = (
        sa_select(func.coalesce(func.sum(t.c.qty), 0))
        .where(
            t.c.user_id == user_id, t.c.mode == mode, t.c.trading_day == trading_day,
            t.c.state.in_(_ACTIVE_QUOTA_STATES),
        )
        .scalar_subquery()
    )
    src = sa_select(
        literal(reservation_id).label("reservation_id"),
        literal(user_id).label("user_id"),
        literal(mode).label("mode"),
        literal(trading_day).label("trading_day"),
        literal(qty).label("qty"),
        literal("reserved").label("state"),
        literal(now).label("created_at"),
        literal(now).label("updated_at"),
    ).where((used + qty) <= daily_limit)
    stmt = insert(t).from_select(
        ["reservation_id", "user_id", "mode", "trading_day", "qty", "state", "created_at", "updated_at"],
        src,
    )
    try:
        result = session.exec(stmt)  # type: ignore[call-overload]
    except IntegrityError:
        # 競態：另一請求以相同 reservation_id 搶先插入（同一冪等鍵重送）。
        session.rollback()
        existing = session.exec(
            select(QuotaReservation).where(QuotaReservation.reservation_id == reservation_id)
        ).first()
        if existing is not None:
            return existing.state in _ACTIVE_QUOTA_STATES
        raise
    ok = result.rowcount == 1
    if ok:
        session.flush()
    return ok


def confirm_quota(session: Session, *, reservation_id: str) -> bool:
    """原子一次性轉移 reserved → confirmed（`UPDATE ... WHERE state='reserved'`，
    rowcount==1 才算成功）；委託已確認送出後呼叫，代表這筆保留永久計入當日已用配額，
    往後對它呼叫 release_quota 一律回 False（confirmed 是終態）。"""
    t = QuotaReservation.__table__
    stmt = (
        update(t)
        .where(t.c.reservation_id == reservation_id, t.c.state == "reserved")
        .values(state="confirmed", updated_at=_utcnow())
    )
    result = session.exec(stmt)  # type: ignore[call-overload]
    ok = result.rowcount == 1
    if ok:
        session.flush()
    return ok


def release_quota(session: Session, *, reservation_id: str) -> bool:
    """原子一次性轉移 reserved → released（`UPDATE ... WHERE state='reserved'`）；
    委託送出失敗/未送出即取消時呼叫，退還配額（released 列的 qty 不再計入 SUM）。
    對 confirmed 或已 released 的列呼叫一律回 False（終態不可逆、不可重複生效）。"""
    t = QuotaReservation.__table__
    stmt = (
        update(t)
        .where(t.c.reservation_id == reservation_id, t.c.state == "reserved")
        .values(state="released", updated_at=_utcnow())
    )
    result = session.exec(stmt)  # type: ignore[call-overload]
    ok = result.rowcount == 1
    if ok:
        session.flush()
    return ok


def reservation_id_for_update(*, client_order_id: str, request_hash: str) -> str:
    """update 專用配額保留冪等鍵（round3 #4）：place 直接用 client_order_id 本身當
    reservation_id；update 額外併入 request_hash，讓「同一委託改成不同內容」的每次改單
    嘗試各自佔用獨立保留列，不與 place 的保留或彼此互撞——同一次重試（相同
    client_order_id+相同 request_hash）則視為同一筆保留（reserve_quota 本身的冪等保證）。

    Task 7 `RiskGuard.check_update` 與 Task 6 `ShioajiAdapter.update` 都呼叫本函式（而非
    各自各寫一份字串樣板），確保兩端算出同一個 reservation_id，quota confirm/release
    才能命中正確的列——這正是本函式放在 repository.py（兩者共同已依賴的模組）而不是
    risk.py 的原因：shioaji_adapter.py 刻意不 import risk.py（見該檔頂部說明），放在
    repository.py 才能讓兩邊零額外耦合地共用同一套推導公式。
    """
    return f"{client_order_id}:update:{request_hash}"


def _escape_like(value: str) -> str:
    """LIKE pattern 逐字比對安全化：client_order_id 為 URL-safe token（可能含 `_`，恰是 LIKE
    的單字元萬用字元），不逃脫的話理論上可能被另一個「恰好在同一位置差一字元」的
    client_order_id 誤配到（機率極低但非零）——這裡一律逃脫，讓比對是純字面值。"""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def list_reserved_update_reservations(session: Session, *, client_order_id: str) -> list[QuotaReservation]:
    """Task 8 watchdog「quota unknown reconcile」收尾用（round3 殘留1）：找出這筆委託目前
    仍是 `reserved` 的 update-path 保留列（reservation_id 格式見 reservation_id_for_update，
    `f"{client_order_id}:update:{request_hash}"`）。每次改單嘗試各自帶不同 request_hash，
    理論上同一委託可能同時卡著多列（連續多次改單皆逾時未決議）——回傳全部，呼叫端逐列
    各自依券商真實狀態決議，不假設只有一列。"""
    prefix = f"{_escape_like(client_order_id)}:update:"
    stmt = (
        select(QuotaReservation)
        .where(
            QuotaReservation.state == "reserved",
            QuotaReservation.reservation_id.like(f"{prefix}%", escape="\\"),  # type: ignore[union-attr]
        )
        .order_by(QuotaReservation.id)
    )
    return list(session.exec(stmt))


def quota_used_today(session: Session, *, user_id: int, mode: str, trading_day: str) -> int:
    """目前已用配額（reserved+confirmed 加總）。純讀取、不具 CAS 保證——供 Task 7 guard
    做預檢/UI 顯示用；真正決定「這筆能不能保留」一律要呼叫 reserve_quota（單一陳述式
    判斷+寫入），不可只憑這個函式的結果自行判斷再另外插入列（那正是 TOCTOU）。"""
    t = QuotaReservation.__table__
    stmt = sa_select(func.coalesce(func.sum(t.c.qty), 0)).where(
        t.c.user_id == user_id, t.c.mode == mode, t.c.trading_day == trading_day,
        t.c.state.in_(_ACTIVE_QUOTA_STATES),
    )
    return session.exec(stmt).scalar_one()  # type: ignore[call-overload]
