"""CRUD + filtering for trades. P&L is auto-computed unless manually overridden."""
from datetime import datetime, timezone

from sqlmodel import Session, select

from quanquant.db.models import Trade
from quanquant.journal.pnl import compute_pnl
from quanquant.journal.schemas import TradeCreate, TradeUpdate, join_tags, split_tags


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _recompute_pnl(trade: Trade) -> None:
    if not trade.pnl_is_manual:
        trade.pnl = compute_pnl(
            trade.direction,
            trade.entry_price,
            trade.exit_price,
            trade.size,
            trade.point_value,
            trade.fee,
        )


def create_trade(session: Session, data: TradeCreate, *, user_id: int, commit: bool = True) -> Trade:
    """`commit=False`（Task 4）：只 flush，不 commit/refresh——供 broker fill 交易把
    「BrokerPosition 帳務 + 這筆 Trade」包進呼叫端自己的同一個 commit（round-trip 完成時）。
    預設 `True` 保持既有（手動日誌 CRUD）行為不回歸。"""
    trade = Trade(
        user_id=user_id,
        symbol=data.symbol,
        direction=data.direction,
        entry_time=data.entry_time,
        entry_price=data.entry_price,
        exit_time=data.exit_time,
        exit_price=data.exit_price,
        stop_loss_price=data.stop_loss_price,
        take_profit_strategy=data.take_profit_strategy,
        size=data.size,
        point_value=data.point_value,
        fee=data.fee,
        note=data.note,
        tags=join_tags(data.tags),
        mode=data.mode,
        source=data.source,
        pnl_is_manual=data.pnl is not None,
        pnl=data.pnl,
    )
    _recompute_pnl(trade)
    session.add(trade)
    if commit:
        session.commit()
        session.refresh(trade)
    else:
        session.flush()
    return trade


def get_trade(session: Session, trade_id: int, *, user_id: int) -> Trade | None:
    trade = session.get(Trade, trade_id)
    if trade is None or trade.user_id != user_id:
        return None  # not found OR someone else's — identical from the caller's view
    return trade


def update_trade(session: Session, trade_id: int, data: TradeUpdate, *, user_id: int) -> Trade | None:
    trade = get_trade(session, trade_id, user_id=user_id)
    if trade is None:
        return None

    fields = data.model_dump(exclude_unset=True)
    if "tags" in fields:
        trade.tags = join_tags(fields.pop("tags"))
    pnl_provided = "pnl" in fields
    for key, value in fields.items():
        setattr(trade, key, value)

    if (trade.exit_time is None) != (trade.exit_price is None):
        raise ValueError("exit_time 與 exit_price 必須同時填寫或同時留空")

    if pnl_provided:
        trade.pnl_is_manual = trade.pnl is not None
    _recompute_pnl(trade)
    trade.updated_at = _utcnow()

    session.add(trade)
    session.commit()
    session.refresh(trade)
    return trade


def delete_trade(session: Session, trade_id: int, *, user_id: int) -> bool:
    trade = get_trade(session, trade_id, user_id=user_id)
    if trade is None:
        return False
    session.delete(trade)
    session.commit()
    return True


def list_trades(
    session: Session,
    *,
    user_id: int,
    mode: str = "real",
    symbol: str | None = None,
    tag: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    status: str = "all",  # "all" | "open" | "closed"
) -> list[Trade]:
    stmt = select(Trade).where(Trade.user_id == user_id, Trade.mode == mode)
    if symbol:
        stmt = stmt.where(Trade.symbol == symbol)
    if status == "open":
        stmt = stmt.where(Trade.exit_time.is_(None))  # type: ignore[union-attr]
    elif status == "closed":
        stmt = stmt.where(Trade.exit_time.is_not(None))  # type: ignore[union-attr]
    if date_from:
        stmt = stmt.where(Trade.entry_time >= date_from)
    if date_to:
        stmt = stmt.where(Trade.entry_time <= date_to)
    stmt = stmt.order_by(Trade.entry_time.desc())  # type: ignore[union-attr]

    trades = list(session.exec(stmt))
    if tag:
        trades = [t for t in trades if tag in split_tags(t.tags)]
    return trades


def list_for_stats(
    session: Session,
    *,
    user_id: int,
    mode: str = "real",
    symbol: str | None = None,
    tag: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> list[Trade]:
    """Closed trades matching the filters, sorted by exit_time ascending."""
    trades = list_trades(
        session,
        user_id=user_id,
        mode=mode,
        symbol=symbol,
        tag=tag,
        date_from=date_from,
        date_to=date_to,
        status="closed",
    )
    return sorted(trades, key=lambda t: (t.exit_time or t.entry_time))


def list_symbols(session: Session, *, user_id: int) -> list[str]:
    rows = session.exec(select(Trade.symbol).where(Trade.user_id == user_id).distinct())
    return sorted(set(rows))


def list_all_tags(session: Session, *, user_id: int) -> list[str]:
    rows = session.exec(select(Trade.tags).where(Trade.user_id == user_id))
    tags: set[str] = set()
    for raw in rows:
        tags.update(split_tags(raw))
    return sorted(tags)
