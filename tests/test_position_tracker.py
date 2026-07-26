"""PositionTracker：累計加權 BrokerPosition 帳務。

V3-1 核心回歸：開2@100→平1→再開1@200→全平，結算 entry 必須是真實加權平均(400/3)，
不是用『剩餘量』回推的錯誤值(150)——這是 BLOCKER#10 的專屬回歸測試。

round3 額外落地（超出計畫原稿 pseudocode，補上覆核要求的修正）：
- 新 HIGH：real fill 缺 fee 不得 zero-coerce，fail closed；sim 缺 fee 用設定估算。
- #12：round-trip 完成後同一交易內單調更新對應 Order（晚到的 Submitted 不回退 filled）。
- #3-new：fill 的 ordno/broker_order_id 兩把鍵矛盾（分別指向不同 Order）fail closed。
- 雙向 open 各自配對：多空各自持有 open 部位時，明確 Cover 各自配對到自己的方向，不互污。
"""
from decimal import Decimal

import pytest
from sqlmodel import select

from quanquant.broker import repository as brepo
from quanquant.broker.position_tracker import PositionMismatchError, PositionTracker
from quanquant.broker.types import Fill
from quanquant.db.models import BrokerPosition, Trade
from quanquant.journal import repository as journal_repo

from tests.conftest import make_create


def _fill(**over):
    base = dict(
        broker="shioaji", fill_id="F1", ordno="O1", broker_order_id="B1", symbol="TXF",
        action="Buy", price=Decimal("18000"), qty=1, fee=Decimal("20"), octype="New",
        ts=1_780_000_000_000, account="F1", mode="sim", user_id=1,
    )
    base.update(over)
    return Fill(**base)


def _positions(session):
    return list(session.exec(select(BrokerPosition)))


def _order_kwargs(**over):
    base = dict(
        client_order_id="C1", request_hash="H1", user_id=1, mode="sim", broker="shioaji",
        account="F1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", trading_day="2026-06-16",
    )
    base.update(over)
    return base


def test_new_fill_opens_broker_position(session):
    tracker = PositionTracker()
    tracker.apply_fill(session, _fill(fill_id="F1", octype="New", action="Buy", qty=2, price=Decimal("18000")), user_id=1)
    session.commit()
    pos = _positions(session)
    assert len(pos) == 1
    assert pos[0].direction == "long" and pos[0].total_opened_qty == 2 and pos[0].status == "open"


def test_aggregating_new_fills_weighted_avg(session):
    tracker = PositionTracker()
    tracker.apply_fill(session, _fill(fill_id="F1", qty=1, price=Decimal("18000")), user_id=1)
    tracker.apply_fill(session, _fill(fill_id="F2", qty=1, price=Decimal("18100")), user_id=1)
    session.commit()
    pos = _positions(session)[0]
    assert pos.total_opened_qty == 2
    assert brepo.avg_entry_price(pos) == Decimal("18050")  # (18000+18100)/2


def test_lot_ledger_weighted_entry_survives_partial_cover_then_reopen(session, user):
    """核心 V3-1 回歸：開2@100→平1@105→再開1@200→平2@110 → entry 必須是 (2*100+1*200)/3=133.33...，
    非用剩餘量推估的 150；fee 全程守恆（合計等於所有 fill fee 總和）。"""
    tracker = PositionTracker()
    total_fee_in = Decimal(0)

    f1 = _fill(fill_id="F1", octype="New", action="Buy", qty=2, price=Decimal("100"), fee=Decimal("6"), user_id=user.id)
    tracker.apply_fill(session, f1, user_id=user.id)
    total_fee_in += f1.fee

    f2 = _fill(fill_id="F2", octype="Cover", action="Sell", qty=1, price=Decimal("105"), fee=Decimal("3"), user_id=user.id)
    tracker.apply_fill(session, f2, user_id=user.id)
    total_fee_in += f2.fee

    f3 = _fill(fill_id="F3", octype="New", action="Buy", qty=1, price=Decimal("200"), fee=Decimal("3"), user_id=user.id)
    tracker.apply_fill(session, f3, user_id=user.id)
    total_fee_in += f3.fee

    f4 = _fill(fill_id="F4", octype="Cover", action="Sell", qty=2, price=Decimal("110"), fee=Decimal("6"), user_id=user.id)
    tracker.apply_fill(session, f4, user_id=user.id)
    total_fee_in += f4.fee
    session.commit()

    trades = list(session.exec(select(Trade).where(Trade.source == "shioaji")))
    assert len(trades) == 1
    t = trades[0]
    assert t.size == 3
    assert t.entry_price == Decimal(400) / 3          # 真實加權平均，非 150
    assert t.exit_price == (Decimal("105") + Decimal("220")) / 3  # (105*1+110*2)/3
    assert t.fee == total_fee_in                       # fee 全程守恆


def test_partial_cover_persists_progress_across_tracker_restart(session, user):
    tracker_a = PositionTracker()
    tracker_a.apply_fill(session, _fill(fill_id="F1", octype="New", qty=3, price=Decimal("100"), user_id=user.id), user_id=user.id)
    tracker_a.apply_fill(session, _fill(fill_id="F2", octype="Cover", action="Sell", qty=1, price=Decimal("110"), user_id=user.id), user_id=user.id)
    session.commit()

    tracker_b = PositionTracker()  # 模擬重啟：全新 instance，無記憶體快取
    tracker_b.apply_fill(session, _fill(fill_id="F3", octype="Cover", action="Sell", qty=2, price=Decimal("120"), user_id=user.id), user_id=user.id)
    session.commit()

    pos = _positions(session)[0]
    assert pos.status == "closed" and pos.closed_qty == 3
    trades = list(session.exec(select(Trade).where(Trade.source == "shioaji")))
    assert len(trades) == 1 and trades[0].size == 3


def test_cover_excess_reverses_into_new_position_and_splits_fee(session, user):
    tracker = PositionTracker()
    tracker.apply_fill(session, _fill(fill_id="F1", octype="New", action="Buy", qty=1, price=Decimal("100"), fee=Decimal("9"), user_id=user.id), user_id=user.id)
    tracker.apply_fill(session, _fill(fill_id="F2", octype="Cover", action="Sell", qty=3, price=Decimal("110"), fee=Decimal("9"), user_id=user.id), user_id=user.id)
    session.commit()

    closed = [p for p in _positions(session) if p.status == "closed"]
    opened = [p for p in _positions(session) if p.status == "open"]
    assert len(closed) == 1 and closed[0].closed_qty == 1
    assert len(opened) == 1 and opened[0].direction == "short" and opened[0].total_opened_qty == 2
    # fee 3 等分：consumed=1/3 → fee=3；excess=2/3 → fee=6；相加等於原 fee=9
    assert closed[0].close_fee_total + opened[0].open_fee_total == Decimal("9")


def test_auto_infers_cover_when_opposite_direction_open(session, user):
    tracker = PositionTracker()
    tracker.apply_fill(session, _fill(fill_id="F1", octype="New", action="Buy", qty=1, price=Decimal("100"), user_id=user.id), user_id=user.id)
    tracker.apply_fill(session, _fill(fill_id="F2", octype="Auto", action="Sell", qty=1, price=Decimal("110"), user_id=user.id), user_id=user.id)
    session.commit()
    pos = _positions(session)[0]
    assert pos.status == "closed"  # Auto 被推斷為 Cover


def test_auto_infers_new_when_no_opposite_direction_open(session, user):
    tracker = PositionTracker()
    tracker.apply_fill(session, _fill(fill_id="F1", octype="Auto", action="Buy", qty=1, price=Decimal("100"), user_id=user.id), user_id=user.id)
    session.commit()
    pos = _positions(session)[0]
    assert pos.status == "open" and pos.direction == "long"  # Auto 被推斷為 New


def test_auto_ambiguous_with_both_directions_open_fails_closed(session, user):
    tracker = PositionTracker()
    tracker.apply_fill(session, _fill(fill_id="F1", octype="New", action="Buy", qty=1, price=Decimal("100"), user_id=user.id), user_id=user.id)
    tracker.apply_fill(session, _fill(fill_id="F2", octype="New", action="Sell", qty=1, price=Decimal("100"), user_id=user.id), user_id=user.id)
    session.commit()
    with pytest.raises(PositionMismatchError):
        tracker.apply_fill(session, _fill(fill_id="F3", octype="Auto", action="Sell", qty=1, price=Decimal("110"), user_id=user.id), user_id=user.id)


def test_cover_without_open_position_raises_for_quarantine(session, user):
    tracker = PositionTracker()
    with pytest.raises(PositionMismatchError):
        tracker.apply_fill(session, _fill(fill_id="F1", octype="Cover", action="Sell", qty=1, price=Decimal("100"), user_id=user.id), user_id=user.id)
    session.rollback()
    assert _positions(session) == []  # 沒有部分寫入殘留


def test_manual_trade_and_broker_round_trip_do_not_cross_pollute(session, user):
    manual = journal_repo.create_trade(session, make_create(symbol="TXF"), user_id=user.id)  # source="manual"
    assert manual.source == "manual"

    tracker = PositionTracker()
    tracker.apply_fill(session, _fill(fill_id="F1", octype="New", action="Buy", qty=1, price=Decimal("100"), user_id=user.id), user_id=user.id)
    tracker.apply_fill(session, _fill(fill_id="F2", octype="Cover", action="Sell", qty=1, price=Decimal("110"), user_id=user.id), user_id=user.id)
    session.commit()

    trades = list(session.exec(select(Trade)))
    assert len(trades) == 2
    sources = {t.source for t in trades}
    assert sources == {"manual", "shioaji"}


def test_bidirectional_open_positions_each_cover_matches_own_direction(session, user):
    """雙向 open 各自配對：多空各自持有 open 部位時，明確 Cover（非 Auto）依 action 推出的
    target_direction 各自配對到自己的方向，entry/exit 不互相污染。"""
    tracker = PositionTracker()
    tracker.apply_fill(session, _fill(fill_id="F1", octype="New", action="Buy", qty=1, price=Decimal("100"), user_id=user.id), user_id=user.id)   # 開多
    tracker.apply_fill(session, _fill(fill_id="F2", octype="New", action="Sell", qty=1, price=Decimal("110"), user_id=user.id), user_id=user.id)  # 開空
    tracker.apply_fill(session, _fill(fill_id="F3", octype="Cover", action="Sell", qty=1, price=Decimal("120"), user_id=user.id), user_id=user.id)  # 平多
    tracker.apply_fill(session, _fill(fill_id="F4", octype="Cover", action="Buy", qty=1, price=Decimal("90"), user_id=user.id), user_id=user.id)    # 平空
    session.commit()

    trades = {t.direction: t for t in session.exec(select(Trade).where(Trade.source == "shioaji"))}
    assert trades["long"].entry_price == Decimal("100") and trades["long"].exit_price == Decimal("120")
    assert trades["short"].entry_price == Decimal("110") and trades["short"].exit_price == Decimal("90")


# ---- round3 新 HIGH：real 缺 fee 絕不記 0 ----

def test_real_fill_missing_fee_fails_closed_not_zero_coerced(session, user):
    tracker = PositionTracker()
    bad = _fill(fill_id="F1", octype="New", action="Buy", qty=1, price=Decimal("18000"), fee=None, mode="real", user_id=user.id)
    with pytest.raises(PositionMismatchError):
        tracker.apply_fill(session, bad, user_id=user.id)
    session.rollback()
    assert _positions(session) == []  # 沒有以 fee=0 留下的部分寫入


def test_sim_fill_missing_fee_uses_configured_estimate(session, user):
    tracker = PositionTracker()
    fill = _fill(fill_id="F1", octype="New", action="Buy", qty=3, price=Decimal("18000"), fee=None, mode="sim", user_id=user.id)
    tracker.apply_fill(session, fill, user_id=user.id)
    session.commit()
    pos = _positions(session)[0]
    assert pos.open_fee_total == Decimal("60")  # _SIM_FEE_PER_LOT(20) * qty(3)，非 0/None


# ---- round3 #3-new：ordno/broker_order_id 雙鍵矛盾 fail closed ----

def test_fill_with_conflicting_ordno_and_broker_order_id_fails_closed(session, user):
    order_a = brepo.create_order(session, **_order_kwargs(client_order_id="CA", request_hash="HA", user_id=user.id))
    brepo.set_order_ack(session, order_a.id, broker_order_id="BA", ordno="OA", status="submitted")
    order_b = brepo.create_order(session, **_order_kwargs(client_order_id="CB", request_hash="HB", user_id=user.id))
    brepo.set_order_ack(session, order_b.id, broker_order_id="BB", ordno="OB", status="submitted")
    session.commit()

    tracker = PositionTracker()
    bad_fill = _fill(
        fill_id="F1", octype="New", action="Buy", qty=1, price=Decimal("18000"),
        ordno="OA", broker_order_id="BB", user_id=user.id,  # ordno→A, broker_order_id→B：矛盾
    )
    with pytest.raises(PositionMismatchError):
        tracker.apply_fill(session, bad_fill, user_id=user.id)
    session.rollback()
    assert _positions(session) == []  # 沒有部分寫入殘留


# ---- round3 #12：Order 狀態單調，晚到的 Submitted 不回退已 filled ----

def test_order_status_does_not_regress_on_late_report_after_fully_filled(session, user):
    order = brepo.create_order(session, **_order_kwargs(qty=1))
    brepo.set_order_ack(session, order.id, broker_order_id="B1", ordno="O1", status="submitted")
    session.commit()

    tracker = PositionTracker()
    tracker.apply_fill(
        session,
        _fill(fill_id="F1", octype="New", action="Buy", qty=1, price=Decimal("18000"),
              ordno="O1", broker_order_id="B1", user_id=user.id),
        user_id=user.id,
    )
    session.commit()
    session.refresh(order)
    assert order.status == "filled" and order.filled_qty == 1

    # 晚到/重播的 Submitted 不得把已 filled 的委託回退
    brepo.mark_order_status(session, order, status="submitted")
    session.commit()
    session.refresh(order)
    assert order.status == "filled"

    # 晚到/重播的 cancelled 也不得覆蓋已成交的委託
    brepo.mark_order_status(session, order, status="cancelled")
    session.commit()
    session.refresh(order)
    assert order.status == "filled"
