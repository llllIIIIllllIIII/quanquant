"""Broker 自動部位帳務：Fill → BrokerPosition 累計加權帳本 → round-trip 完成寫 Trade。

與手動 Trade 完全隔離：自動流程只讀寫 BrokerPosition，只在 remaining_qty 歸零時才呼叫
journal_repo.create_trade(..., source="shioaji", commit=False) 寫一筆日誌。手動/自動部位
查找一律走 BrokerPosition 表（find_open_position 已是這張表的原生 scope），不會碰到
Trade.source == "manual" 的列。

V3-1（BLOCKER#10）：round-trip 結算的 entry/exit 用 BrokerPosition 的累計欄位
（entry_notional/total_opened_qty、exit_notional/closed_qty）算真實加權平均，
不用「剩餘口數」回推——理由見 db/models.py 的 BrokerPosition docstring。

round3 修正（本模組落地）：
- #15：Cover 超額轉開反向部位時，fee 依 consumed/excess 比例拆分（恆等式：
  fee_consumed = fee_total - fee_excess，不 quantize，保證兩者相加永遠等於原 fee）。
- 新 HIGH（real 缺 fee 絕不記 0）：real fill 缺 fee 一律 fail closed（PositionMismatchError，
  呼叫端 quarantine），不得 `fill.fee or Decimal(0)`——那會讓正式 PnL 永久低估成本。
  只有 sim 才用 `_SIM_FEE_PER_LOT` 這個設定值估算缺值 fee（sim 模擬單常缺 fee 回報）。
- #12：round-trip 完成後同一交易內呼叫 broker.repository.apply_order_fill 單調更新對應
  Order 的 filled_qty/avg_fill_price/status（該函式內部已用狀態偏序守衛拒絕回退）。
- #3-new：解析 fill→order 時，若 payload 同時帶 ordno 與 broker_order_id，兩者各自
  scoped 查詢後必須指向同一張 Order，矛盾一律 fail closed（PositionMismatchError）。

無狀態：不快取任何部位於記憶體，每次 apply_fill 都重新從 DB 讀寫 BrokerPosition，
天然支撐跨重啟續平（DB 是唯一真相來源）。
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy.exc import IntegrityError
from sqlmodel import Session

from quanquant.broker import repository as brepo
from quanquant.broker.types import Fill
from quanquant.db.models import BrokerPosition, Order
from quanquant.journal import repository as journal_repo
from quanquant.journal.schemas import TradeCreate

_CST = timezone(timedelta(hours=8))
_POINT_VALUES = {"TXF": Decimal("200"), "MXF": Decimal("50")}

# sim 模擬單成交常缺 fee 回報；缺值時依此設定估算（qty * 每口費用），不留 None/0。
# real 缺 fee 絕不套用此估算——真錢的成本必須是真實回報值，缺值直接 fail closed。
_SIM_FEE_PER_LOT = Decimal("20")


class PositionMismatchError(Exception):
    """fail-closed 例外，涵蓋三類情況（呼叫端＝Task 5 worker 捕捉後 quarantine 對應的
    Deal/RawInbox，不得留下部分寫入——本模組任何會 raise 這個例外的路徑，都保證 raise
    之前沒有做過任何 session.add/flush）：
    1. Cover 找不到對應開倉部位。
    2. Auto 雙向同時 open 導致歧義，無法判斷該開或該平。
    3. real fill 缺 fee，或 fill 的 ordno/broker_order_id 兩把鍵分別指向不同 Order。
    """


def _point_value(symbol: str) -> Decimal:
    return _POINT_VALUES.get(symbol, Decimal("200"))


def _to_dt(ts_ms: int) -> datetime:
    """epoch-ms UTC → naive UTC datetime（給 BrokerPosition.opened_at 等欄位儲存）。"""
    return datetime.fromtimestamp(ts_ms / 1000, timezone.utc).replace(tzinfo=None)


def _utc_dt_to_cst(value: datetime) -> datetime:
    """naive UTC → naive CST（Trade.entry_time/exit_time 是 naive local 值）。"""
    aware = value.replace(tzinfo=timezone.utc)
    return aware.astimezone(_CST).replace(tzinfo=None)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class PositionTracker:
    """無狀態；`apply_fill(session, fill, *, user_id)` 是唯一入口。"""

    def apply_fill(self, session: Session, fill: Fill, *, user_id: int) -> None:
        # 兩個前置檢查（缺 fee / 雙鍵矛盾）都是純讀取或純例外，保證 raise 之前不寫入任何列。
        fee = self._resolve_fee(fill)
        order = self._resolve_order(session, fill)

        if fill.octype == "New":
            direction = "long" if fill.action == "Buy" else "short"
            self._open(session, fill, user_id=user_id, direction=direction, fee=fee)
        elif fill.octype == "Cover":
            target_direction = "long" if fill.action == "Sell" else "short"
            self._close(session, fill, user_id=user_id, target_direction=target_direction, fee=fee)
        else:  # "Auto"（型別層已擋非法 octype）
            self._auto(session, fill, user_id=user_id, fee=fee)

        if order is not None:
            # round3 #12：filled_qty/avg_fill_price 永遠累加，status 寫入受狀態偏序守衛
            # （brepo.apply_order_fill 內部處理），晚到/重播不會回退已達成的進度。
            brepo.apply_order_fill(session, order, fill_qty=fill.qty, fill_price=fill.price)

    # ---- 前置解析（不寫入，只讀/只 raise） ----

    def _resolve_fee(self, fill: Fill) -> Decimal:
        if fill.fee is not None:
            return fill.fee
        if fill.mode == "real":
            raise PositionMismatchError(
                f"real fill 缺 fee（fill_id={fill.fill_id}）：正式 PnL 不得靜默記 0，"
                "fail closed 進 quarantine，待人工核實實際手續費後補值重放"
            )
        return _SIM_FEE_PER_LOT * fill.qty  # sim 缺 fee 才用設定估算

    def _resolve_order(self, session: Session, fill: Fill) -> Order | None:
        """round3 #3-new：ordno/broker_order_id 各自 scoped 查詢，若兩者都有命中但
        指向不同 Order，矛盾即 fail closed（不任選其一）。任一鍵缺值或查無視為未提供。"""
        by_ordno = (
            brepo.find_order_by_ordno(
                session, broker=fill.broker, account=fill.account, mode=fill.mode, ordno=fill.ordno
            )
            if fill.ordno
            else None
        )
        by_broker_id = (
            brepo.find_order_by_broker_id(
                session, broker=fill.broker, account=fill.account, mode=fill.mode,
                broker_order_id=fill.broker_order_id,
            )
            if fill.broker_order_id
            else None
        )
        if by_ordno is not None and by_broker_id is not None and by_ordno.id != by_broker_id.id:
            raise PositionMismatchError(
                f"fill_id={fill.fill_id} 的 ordno={fill.ordno!r} 與 broker_order_id="
                f"{fill.broker_order_id!r} 分別指向不同 Order（id={by_ordno.id} vs id={by_broker_id.id}），"
                "矛盾，fail closed 進 quarantine"
            )
        return by_ordno or by_broker_id

    # ---- New/Cover/Auto 分派 ----

    def _auto(self, session: Session, fill: Fill, *, user_id: int, fee: Decimal) -> None:
        covers_direction = "long" if fill.action == "Sell" else "short"
        news_direction = "short" if fill.action == "Sell" else "long"
        covers_open = brepo.find_open_position(
            session, user_id=user_id, broker=fill.broker, account=fill.account,
            mode=fill.mode, symbol=fill.symbol, direction=covers_direction,
        )
        news_open = brepo.find_open_position(
            session, user_id=user_id, broker=fill.broker, account=fill.account,
            mode=fill.mode, symbol=fill.symbol, direction=news_direction,
        )
        if covers_open is not None and news_open is not None:
            raise PositionMismatchError(
                f"Auto 歧義：{fill.symbol} 同時有 {covers_direction}/{news_direction} 兩個 open 部位，"
                "無法判斷本筆 Auto fill 該開或該平，fail closed"
            )
        if covers_open is not None:
            self._close(session, fill, user_id=user_id, target_direction=covers_direction, fee=fee)
        else:
            self._open(session, fill, user_id=user_id, direction=news_direction, fee=fee)

    def _open(self, session: Session, fill: Fill, *, user_id: int, direction: str, fee: Decimal) -> None:
        self._open_qty(
            session, user_id=user_id, broker=fill.broker, account=fill.account, mode=fill.mode,
            symbol=fill.symbol, direction=direction, qty=fill.qty, price=fill.price,
            fee=fee, opened_at=_to_dt(fill.ts),
        )
        brepo.append_audit(
            session, actor_user_id=user_id, mode=fill.mode, action="fill",
            payload_hash=brepo.audit_reference_hash(fill.fill_id, fill.octype, fill.qty, fill.price),
            result="ok", rule="open",
        )

    def _open_qty(
        self, session: Session, *, user_id: int, broker: str, account: str, mode: str, symbol: str,
        direction: str, qty: int, price: Decimal, fee: Decimal, opened_at: datetime,
    ) -> BrokerPosition:
        pos = brepo.find_open_position(
            session, user_id=user_id, broker=broker, account=account, mode=mode,
            symbol=symbol, direction=direction,
        )
        if pos is None:
            pos = BrokerPosition(
                user_id=user_id, broker=broker, account=account, mode=mode, symbol=symbol,
                direction=direction, total_opened_qty=qty, entry_notional=price * qty,
                open_fee_total=fee, opened_at=opened_at, updated_at=_now_utc(), version=0,
            )
            session.add(pos)
            try:
                session.flush()
                return pos
            except IntegrityError:
                # 競態：另一 fill 搶先插入同 active-scope open 列（partial unique index），
                # rollback 後改走下面的 CAS 更新既有列路徑。
                session.rollback()
                pos = brepo.find_open_position(
                    session, user_id=user_id, broker=broker, account=account, mode=mode,
                    symbol=symbol, direction=direction,
                )
                if pos is None:
                    raise

        new_total = pos.total_opened_qty + qty
        new_notional = pos.entry_notional + price * qty
        new_fee = pos.open_fee_total + fee
        return brepo.cas_update_broker_position(
            session, pos, total_opened_qty=new_total, entry_notional=new_notional, open_fee_total=new_fee,
        )

    def _close(
        self, session: Session, fill: Fill, *, user_id: int, target_direction: str, fee: Decimal
    ) -> None:
        pos = brepo.find_open_position(
            session, user_id=user_id, broker=fill.broker, account=fill.account,
            mode=fill.mode, symbol=fill.symbol, direction=target_direction,
        )
        if pos is None:
            raise PositionMismatchError(
                f"Cover fill 找不到對應開倉部位（{fill.symbol}/{target_direction}），fail closed 進 quarantine"
            )
        remaining = brepo.remaining_qty(pos)
        consumed = min(fill.qty, remaining)
        excess = fill.qty - consumed
        if excess > 0:
            # round3 #15：依 consumed/excess 比例拆分，恆等式（不 quantize）保證相加等於原 fee。
            fee_excess = fee * excess / fill.qty
            fee_consumed = fee - fee_excess
        else:
            fee_consumed, fee_excess = fee, Decimal(0)

        new_closed = pos.closed_qty + consumed
        new_exit_notional = pos.exit_notional + fill.price * consumed
        new_close_fee = pos.close_fee_total + fee_consumed
        finalized = new_closed >= pos.total_opened_qty
        pos = brepo.cas_update_broker_position(
            session, pos,
            closed_qty=new_closed, exit_notional=new_exit_notional, close_fee_total=new_close_fee,
            status="closed" if finalized else pos.status,
        )

        if finalized:
            self._finalize(session, pos, fill, user_id=user_id)

        brepo.append_audit(
            session, actor_user_id=user_id, mode=fill.mode, action="fill",
            payload_hash=brepo.audit_reference_hash(fill.fill_id, fill.octype, consumed, fill.price),
            result="ok", rule="close",
        )

        if excess > 0:
            reversal_direction = "short" if fill.action == "Sell" else "long"
            self._open_qty(
                session, user_id=user_id, broker=fill.broker, account=fill.account, mode=fill.mode,
                symbol=fill.symbol, direction=reversal_direction, qty=excess, price=fill.price,
                fee=fee_excess, opened_at=_to_dt(fill.ts),
            )
            brepo.append_audit(
                session, actor_user_id=user_id, mode=fill.mode, action="fill",
                payload_hash=brepo.audit_reference_hash(fill.fill_id, "cover_excess_reversal", excess, fill.price),
                result="ok", rule="cover_excess_reversal",
                detail=(
                    f"Cover fill qty={fill.qty} 超過剩餘 {remaining}，excess={excess} "
                    f"轉開 {reversal_direction} 部位（fee 依比例拆分，避免重複計）"
                ),
            )

    def _finalize(self, session: Session, pos: BrokerPosition, fill: Fill, *, user_id: int) -> None:
        entry = brepo.avg_entry_price(pos)
        exit_ = brepo.avg_exit_price(pos)
        fee = pos.open_fee_total + pos.close_fee_total
        trade = journal_repo.create_trade(
            session,
            TradeCreate(
                symbol=pos.symbol,
                direction=pos.direction,
                entry_time=_utc_dt_to_cst(pos.opened_at),
                entry_price=entry,
                exit_time=_utc_dt_to_cst(_to_dt(fill.ts)),
                exit_price=exit_,
                size=pos.total_opened_qty,
                point_value=_point_value(pos.symbol),
                fee=fee,
                mode=pos.mode,
                source="shioaji",
            ),
            user_id=user_id,
            commit=False,
        )
        brepo.cas_update_broker_position(session, pos, trade_id=trade.id)
