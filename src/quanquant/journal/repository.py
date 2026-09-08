"""CRUD + filtering for trades. P&L is auto-computed unless manually overridden."""
from datetime import date, datetime, timezone

from sqlmodel import Session, select

from quanquant.db.models import Trade
from quanquant.journal.pnl import compute_pnl
from quanquant.journal.schemas import TradeCreate, TradeUpdate, join_tags, split_tags
from quanquant.journal.trading_day import trading_day_bounds


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


def list_closed_for_period(
    session: Session,
    *,
    user_id: int,
    mode: str = "real",
    date_from: date,
    date_to: date,
    symbol: str | None = None,
    tag: str | None = None,
    result: str | None = None,  # "win" | "loss" | None（全部）
    include_manual: bool = False,
) -> list[Trade]:
    """008 交易績效頁專用：已平倉逐筆明細，依 trading_day 區間（含前一夜盤，見
    `journal.trading_day.trading_day_bounds`）篩選，排序為平倉時間新到舊（畫面順序）。

    與 `list_for_stats`/`list_trades`（entry_time 為界、不分來源）刻意分開——那兩支供
    交易日記頁與既有 `/stats/data` API 使用，語意不變；本函式是 008 新增的獨立查詢路徑，
    避免任何一邊的行為被另一邊的新需求牽動（既有測試零回歸風險）。

    來源規則（008）：預設 `include_manual=False` 只計 `source="shioaji"`；勾選後納入
    `source="manual"`。結果篩選（008）：`result="win"` 只留 `pnl>0`，`"loss"` 只留
    `pnl<0`，打平（`pnl==0`）與尚未平倉（不會出現於此查詢）兩者皆不計入任一邊。
    """
    lower, upper = trading_day_bounds(date_from, date_to)
    stmt = select(Trade).where(
        Trade.user_id == user_id,
        Trade.mode == mode,
        Trade.exit_time.is_not(None),  # type: ignore[union-attr]
        Trade.exit_time >= lower,  # type: ignore[operator]
        Trade.exit_time <= upper,  # type: ignore[operator]
    )
    if symbol:
        stmt = stmt.where(Trade.symbol == symbol)
    if not include_manual:
        stmt = stmt.where(Trade.source == "shioaji")
    stmt = stmt.order_by(Trade.exit_time.desc())  # type: ignore[union-attr]

    trades = list(session.exec(stmt))
    if tag:
        trades = [t for t in trades if tag in split_tags(t.tags)]
    if result == "win":
        trades = [t for t in trades if t.pnl is not None and t.pnl > 0]
    elif result == "loss":
        trades = [t for t in trades if t.pnl is not None and t.pnl < 0]
    return trades


def list_symbols(session: Session, *, user_id: int) -> list[str]:
    rows = session.exec(select(Trade.symbol).where(Trade.user_id == user_id).distinct())
    return sorted(set(rows))


def list_all_tags(session: Session, *, user_id: int) -> list[str]:
    rows = session.exec(select(Trade.tags).where(Trade.user_id == user_id))
    tags: set[str] = set()
    for raw in rows:
        tags.update(split_tags(raw))
    return sorted(tags)
