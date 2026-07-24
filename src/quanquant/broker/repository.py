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

from sqlalchemy import func, insert, literal, update
from sqlalchemy import select as sa_select
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from quanquant.db.models import (
    BrokerPosition,
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


def sum_qty_today(session: Session, *, user_id: int, mode: str, trading_day: str) -> int:
    stmt = select(Order).where(
        Order.user_id == user_id, Order.mode == mode, Order.trading_day == trading_day
    )
    return sum(o.qty for o in session.exec(stmt))


# ---- RawInbox（durable callback spool，V3-2） ----

def stage_raw_inbox(session: Session, *, kind: str, broker: str, payload: str) -> RawInbox:
    row = RawInbox(kind=kind, broker=broker, payload=payload)
    session.add(row)
    session.flush()
    return row


def list_unprocessed_raw_inbox(session: Session, *, limit: int = 200) -> list[RawInbox]:
    stmt = (
        select(RawInbox)
        .where(RawInbox.processed.is_(False), RawInbox.quarantine.is_(False))  # type: ignore[union-attr]
        .order_by(RawInbox.id)
        .limit(limit)
    )
    return list(session.exec(stmt))


def mark_raw_inbox_processed(session: Session, row: RawInbox) -> None:
    row.processed = True
    row.processed_at = _utcnow()
    session.add(row)
    session.flush()


def quarantine_raw_inbox(session: Session, row: RawInbox, *, error: str) -> None:
    row.quarantine = True
    row.error = error
    session.add(row)
    session.flush()


def unquarantine_stale_raw_inbox(session: Session, *, older_than: datetime, limit: int = 200) -> int:
    """把 quarantine 超過 older_than 的列解除隔離，回到一般佇列重新嘗試一次
    （Task 8 watchdog 以較慢週期呼叫——給「當時解不到委託關聯」的列一個補救機會，
    不會無限重試：解除後若原因仍不變會再次被 quarantine，只是白工，不會誤判成功）。"""
    stmt = (
        select(RawInbox)
        .where(RawInbox.quarantine.is_(True), RawInbox.received_at < older_than)  # type: ignore[union-attr]
        .order_by(RawInbox.id)
        .limit(limit)
    )
    rows = list(session.exec(stmt))
    for row in rows:
        row.quarantine = False
        row.error = None
        session.add(row)
    session.flush()
    return len(rows)


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
