"""RiskGuard：owner 授權先跑、kill switch 即時可切、白名單/單筆/單日上限、per-reservation
CAS 配額（round3 #4：reserve_quota/confirm_quota/release_quota 皆用 reservation_id 導向，
非舊版單列 reserved_qty 聚合）、real 兩階段確認（token 綁 (actor_user_id,payload_hash)，
一次性、TTL）、check_update 用『套用變更後的完整新內容』canonical hash（BLOCKER#1 回歸：
real update 必須能成功，不因用錯 hash 內容而永遠鎖死）、check_update 只對「變動量」
（new_qty-order.qty，且只在增加時）走 reserve_quota、逐欄 mutation 令 token 失效、
攔截與通過皆記 audit（含 owner 授權失敗）。

round3 #4 澄清：RiskGuard 只負責「reserve」（建立 state='reserved' 的保留列），不在
check_place/check_update 內自動 confirm——確認送出成功後才 confirm、送出失敗/被擋才
release，是 ShioajiAdapter（Task 6）收到 broker 回覆之後的職責（見 test_shioaji_adapter.py
新增的 wiring 測試），RiskGuard 本身測試只驗證「reserved 列有沒有正確建立/回滾」。
"""
import asyncio
from decimal import Decimal

import pytest
from sqlmodel import select

from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, RiskError
from quanquant.broker.risk import RiskGuard, parse_owner_ids, parse_whitelist
from quanquant.broker.types import OrderRequest, canonical_payload_hash
from quanquant.db.models import Order, OrderAudit, QuotaReservation


def _req(**over):
    base = dict(
        client_order_id="C1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", user_id=1,
    )
    base.update(over)
    return OrderRequest(**base)


def _guard(*, session_factory, **over):
    base = dict(
        secret="test-secret", owner_user_ids=frozenset({1}), symbol_whitelist=frozenset({"TXF"}),
        max_qty_per_order=5, max_qty_per_day=10, max_orders_per_day=3, confirm_token_ttl_seconds=120,
    )
    base.update(over)
    return RiskGuard(session_factory=session_factory, **base)


def test_parse_owner_ids_and_whitelist_helpers():
    assert parse_owner_ids("1, 2 ,3") == frozenset({1, 2, 3})
    assert parse_owner_ids("") == frozenset()
    assert parse_whitelist(" TXF ,MXF") == frozenset({"TXF", "MXF"})


def test_assert_owner_allows_listed_user(session):
    guard = _guard(session_factory=lambda: session)
    guard.assert_owner(1)  # 不 raise


def test_assert_owner_rejects_non_owner(session):
    guard = _guard(session_factory=lambda: session)
    with pytest.raises(AuthorizationError):
        guard.assert_owner(99)


def _hash_for(req, *, account="F1", mode="sim"):
    return canonical_payload_hash(
        symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
        price_type=req.price_type, order_type=req.order_type, octype=req.octype,
        account=account, mode=mode,
    )


def test_check_place_passes_within_limits_sim(session):
    guard = _guard(session_factory=lambda: session)
    req = _req()
    order = guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                              account="F1", request_hash=_hash_for(req))
    assert order.status == "pending" and order.mode == "sim"


def test_check_place_owner_check_runs_first(session):
    guard = _guard(session_factory=lambda: session)
    req = _req()
    with pytest.raises(AuthorizationError):
        guard.check_place(session, req, actor_user_id=99, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req))
    assert session.exec(select(Order)).first() is None  # 沒有半成品委託殘留
    assert session.exec(select(QuotaReservation)).first() is None  # 也沒有半成品配額保留殘留


def test_check_place_kill_switch_blocks_and_is_live_toggleable(session):
    guard = _guard(session_factory=lambda: session)
    guard.set_kill_switch(True)
    req = _req()
    with pytest.raises(RiskError):
        guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req))
    guard.set_kill_switch(False)  # 即時可切，非啟動快照
    guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                      account="F1", request_hash=_hash_for(req))


def test_check_place_symbol_not_whitelisted(session):
    guard = _guard(session_factory=lambda: session)
    req = _req(symbol="MXF", client_order_id="C-X")
    with pytest.raises(RiskError):
        guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req))


def test_check_place_per_order_qty_limit(session):
    guard = _guard(session_factory=lambda: session)
    req = _req(qty=6, client_order_id="C-Q")  # 上限 5
    with pytest.raises(RiskError):
        guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req))


def test_check_place_per_day_qty_limit_uses_cas(session):
    guard = _guard(session_factory=lambda: session)
    for i in range(2):  # 5+5=10（上限），第三筆超過
        req = _req(qty=5, client_order_id=f"C-{i}")
        guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req))
    req3 = _req(qty=1, client_order_id="C-over")
    with pytest.raises(RiskError):
        guard.check_place(session, req3, actor_user_id=1, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req3))


def test_check_place_per_day_order_count_limit(session):
    guard = _guard(session_factory=lambda: session)
    for i in range(3):  # 上限 3
        req = _req(qty=1, client_order_id=f"CC-{i}")
        guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req))
    req4 = _req(qty=1, client_order_id="CC-over")
    with pytest.raises(RiskError):
        guard.check_place(session, req4, actor_user_id=1, mode="sim", broker="shioaji",
                          account="F1", request_hash=_hash_for(req4))


def test_check_place_sim_never_needs_confirm_token(session):
    guard = _guard(session_factory=lambda: session)
    req = _req()
    order = guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                              account="F1", request_hash=_hash_for(req), confirm_token=None)
    assert order is not None  # sim 不需要 token 就成功


def test_check_place_real_requires_valid_confirm_token(session):
    guard = _guard(session_factory=lambda: session)
    req = _req()
    rh = _hash_for(req, mode="real")
    with pytest.raises(RiskError) as exc_info:
        guard.check_place(session, req, actor_user_id=1, mode="real", broker="shioaji",
                          account="F1", request_hash=rh, confirm_token=None)
    assert exc_info.value.needs_confirm is True

    token = guard.issue_confirm_token(session, actor_user_id=1, payload_hash=rh)
    order = guard.check_place(session, req, actor_user_id=1, mode="real", broker="shioaji",
                              account="F1", request_hash=rh, confirm_token=token)
    assert order.mode == "real"


def test_check_place_real_token_is_one_time_use(session):
    guard = _guard(session_factory=lambda: session)
    req = _req()
    rh = _hash_for(req, mode="real")
    token = guard.issue_confirm_token(session, actor_user_id=1, payload_hash=rh)
    guard.check_place(session, req, actor_user_id=1, mode="real", broker="shioaji",
                      account="F1", request_hash=rh, confirm_token=token)
    req2 = _req(client_order_id="C2")
    rh2 = _hash_for(req2, mode="real")
    with pytest.raises(RiskError):  # 同一個 token 不得重放給另一張委託
        guard.check_place(session, req2, actor_user_id=1, mode="real", broker="shioaji",
                          account="F1", request_hash=rh2, confirm_token=token)


def test_check_place_records_audit_on_reject_and_ok(session):
    guard = _guard(session_factory=lambda: session)
    req = _req()
    guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                      account="F1", request_hash=_hash_for(req))
    with pytest.raises(AuthorizationError):
        guard.check_place(session, _req(client_order_id="C-rej"), actor_user_id=99, mode="sim",
                          broker="shioaji", account="F1", request_hash=_hash_for(req))
    audits = list(session.exec(select(OrderAudit)))
    actions = [(a.action, a.result) for a in audits]
    assert ("place", "ok") in actions
    assert ("risk_reject", "rejected") in actions


def test_check_place_reserves_quota_row_in_reserved_state_not_confirmed(session):
    """round3 #4：RiskGuard 只建立 state='reserved' 的保留列——confirm 是下單送出成功後
    ShioajiAdapter 的職責（見 test_shioaji_adapter.py），RiskGuard 本身不越權提前 confirm。"""
    guard = _guard(session_factory=lambda: session)
    req = _req(qty=3)
    guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                      account="F1", request_hash=_hash_for(req))
    row = session.exec(select(QuotaReservation)).first()
    assert row is not None
    assert row.reservation_id == "C1"  # place 用 client_order_id 當 reservation_id
    assert row.state == "reserved"
    assert row.qty == 3


def test_check_update_real_round_trip_succeeds_with_canonical_hash(session):
    """BLOCKER#1 回歸：real update 用『套用變更後的完整新內容』canonical hash 簽發+驗證，必須能成功。"""
    guard = _guard(session_factory=lambda: session)
    req = _req()
    rh = _hash_for(req, mode="real")
    token = guard.issue_confirm_token(session, actor_user_id=1, payload_hash=rh)
    order = guard.check_place(session, req, actor_user_id=1, mode="real", broker="shioaji",
                              account="F1", request_hash=rh, confirm_token=token)

    new_hash = canonical_payload_hash(
        symbol=order.symbol, action=order.action, qty=2, price=Decimal("18500"),
        price_type=order.price_type, order_type=order.order_type, octype=order.octype,
        account="F1", mode="real",
    )
    update_token = guard.issue_confirm_token(session, actor_user_id=1, payload_hash=new_hash)
    guard.check_update(session, order, actor_user_id=1, new_qty=2, new_price=Decimal("18500"),
                       request_hash=new_hash, confirm_token=update_token)  # 不得 raise


def test_check_update_field_mutation_invalidates_token(session):
    guard = _guard(session_factory=lambda: session)
    req = _req()
    rh = _hash_for(req, mode="real")
    token = guard.issue_confirm_token(session, actor_user_id=1, payload_hash=rh)
    order = guard.check_place(session, req, actor_user_id=1, mode="real", broker="shioaji",
                              account="F1", request_hash=rh, confirm_token=token)

    hash_for_qty2 = canonical_payload_hash(
        symbol=order.symbol, action=order.action, qty=2, price=order.price,
        price_type=order.price_type, order_type=order.order_type, octype=order.octype,
        account="F1", mode="real",
    )
    update_token = guard.issue_confirm_token(session, actor_user_id=1, payload_hash=hash_for_qty2)
    hash_for_qty3 = canonical_payload_hash(  # 拿著 qty=2 的 token，卻改送 qty=3
        symbol=order.symbol, action=order.action, qty=3, price=order.price,
        price_type=order.price_type, order_type=order.order_type, octype=order.octype,
        account="F1", mode="real",
    )
    trading_day = order.trading_day  # 先讀出來——check_update 失敗會經 _audit_reject 另開/關
    # session，讓 order 這個 ORM instance 變成 detached，之後不能再存取它的屬性。
    with pytest.raises(RiskError):
        guard.check_update(session, order, actor_user_id=1, new_qty=3, new_price=order.price,
                           request_hash=hash_for_qty3, confirm_token=update_token)
    # token 失效攔截時，這次改單嘗試多保留的 delta 配額也一併回滾，不留殘留列。
    assert brepo.quota_used_today(session, user_id=1, mode="real", trading_day=trading_day) == 1


def test_check_update_reruns_all_limits_with_new_qty(session):
    guard = _guard(session_factory=lambda: session)
    req = _req(qty=3)
    order = guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                              account="F1", request_hash=_hash_for(req))
    with pytest.raises(RiskError):  # 改到 6 超過單筆上限 5
        guard.check_update(session, order, actor_user_id=1, new_qty=6, new_price=order.price,
                           request_hash="irrelevant-sim-no-token-needed")


def test_check_update_only_charges_quota_for_delta(session):
    guard = _guard(session_factory=lambda: session)
    req = _req(qty=3)
    order = guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                              account="F1", request_hash=_hash_for(req))
    # 上限 10、單筆上限 5；已用 3；改到 5（delta=+2）應該過。
    guard.check_update(session, order, actor_user_id=1, new_qty=5, new_price=order.price,
                       request_hash="n/a")  # sim 不需要 token
    # 已用配額只加了 delta(2)，不是整筆 5 重複加（3+2=5，不是 3+5=8）——per-reservation
    # 設計下這是兩列各自 reserved 的 SUM，不是單列覆寫。
    assert brepo.quota_used_today(session, user_id=1, mode="sim", trading_day=order.trading_day) == 5
    rows = list(session.exec(select(QuotaReservation)))
    assert len(rows) == 2
    assert {r.qty for r in rows} == {3, 2}


def test_check_update_negative_delta_does_not_reserve_more(session):
    """qty 減少（delta<0）不需要額外保留配額——降低用量本來就不會超限。"""
    guard = _guard(session_factory=lambda: session)
    req = _req(qty=5)
    order = guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                              account="F1", request_hash=_hash_for(req))
    guard.check_update(session, order, actor_user_id=1, new_qty=2, new_price=order.price, request_hash="n/a")
    rows = list(session.exec(select(QuotaReservation)))
    assert len(rows) == 1  # 沒有為負 delta 多開一列
    assert brepo.quota_used_today(session, user_id=1, mode="sim", trading_day=order.trading_day) == 5


def test_check_update_nonpositive_qty_or_price_rejected(session):
    # 各自用獨立的 order：check_update 失敗會經 _audit_reject 另開/關 session，讓傳入的
    # order 這個 ORM instance 變成 detached，不能在同一個失敗的 order 上再呼叫第二次。
    guard = _guard(session_factory=lambda: session)
    order_a = guard.check_place(session, _req(client_order_id="C-A"), actor_user_id=1, mode="sim",
                                broker="shioaji", account="F1", request_hash=_hash_for(_req(client_order_id="C-A")))
    with pytest.raises(RiskError):
        guard.check_update(session, order_a, actor_user_id=1, new_qty=0, new_price=order_a.price, request_hash="n/a")

    order_b = guard.check_place(session, _req(client_order_id="C-B"), actor_user_id=1, mode="sim",
                                broker="shioaji", account="F1", request_hash=_hash_for(_req(client_order_id="C-B")))
    with pytest.raises(RiskError):
        guard.check_update(session, order_b, actor_user_id=1, new_qty=1, new_price=Decimal("0"), request_hash="n/a")


def test_check_update_allows_zero_price_for_mkt_order(session):
    """bug 2：check_update 的 new_price<=0 檢查只對 LMT 生效——MKT 委託改單時 price=0
    不應被擋（order.price_type 才是這張委託真正的價格類型，改單不能改變它）。"""
    guard = _guard(session_factory=lambda: session)
    req = _req(price=Decimal("0"), price_type="MKT", order_type="IOC", client_order_id="C-MKT")
    order = guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                              account="F1", request_hash=_hash_for(req))
    guard.check_update(session, order, actor_user_id=1, new_qty=2, new_price=Decimal("0"),
                       request_hash="n/a")  # 不應 raise
    # check_update 本身不寫 order.qty（那是 ShioajiAdapter.update 送出成功後才做的事）；
    # 這裡確認流程真的跑完到底（delta 配額有正確保留），而不是半途被別的檢查擋下。
    assert brepo.quota_used_today(session, user_id=1, mode="sim", trading_day=order.trading_day) == 2


def test_check_update_rejects_negative_price_even_for_mkt_order(session):
    guard = _guard(session_factory=lambda: session)
    req = _req(price=Decimal("0"), price_type="MKT", order_type="IOC", client_order_id="C-MKT2")
    order = guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                              account="F1", request_hash=_hash_for(req))
    with pytest.raises(RiskError):
        guard.check_update(session, order, actor_user_id=1, new_qty=2, new_price=Decimal("-1"),
                           request_hash="n/a")


def test_check_update_rejects_non_owner_of_order_even_if_in_owner_allowlist(session):
    """round3 #6：owner allowlist 過了不代表這張委託是這個 owner 的——多 owner 情境下仍要
    以委託實際 user_id 驗真正所有權，不能只驗「是不是某個 owner」。"""
    guard = _guard(session_factory=lambda: session, owner_user_ids=frozenset({1, 2}))
    req = _req()
    order = guard.check_place(session, req, actor_user_id=1, mode="sim", broker="shioaji",
                              account="F1", request_hash=_hash_for(req))
    with pytest.raises(AuthorizationError):
        guard.check_update(session, order, actor_user_id=2, new_qty=2, new_price=order.price, request_hash="n/a")
    audits = list(session.exec(select(OrderAudit)))
    actions = [(a.action, a.result) for a in audits]
    assert ("risk_reject", "rejected") in actions


def test_cas_quota_blocks_one_of_two_concurrent_places(tmp_path):
    """V3-3：兩並行 place 併發時日限額不被突破（一過一擋）——真 asyncio 併發，非序列呼叫。

    用檔案 SQLite（而非 in-memory + StaticPool）：StaticPool 讓所有 Session 共用單一底層
    連線，兩執行緒同時對同一顆連線 flush 會產生與本測試主旨無關的 identity-map 交錯錯誤
    （flaky），測不出跨連線的寫入序列化行為；比照 tests/test_broker_repo.py 既有的
    reserve_quota 併發測試，改用各自獨立連線打同一個檔案 DB 才是真正驗證 CAS 語意的作法。
    """
    from sqlmodel import Session, SQLModel, create_engine

    db_path = tmp_path / "risk_guard_concurrency.db"
    eng = create_engine(f"sqlite:///{db_path}", connect_args={"timeout": 30})
    SQLModel.metadata.create_all(eng)
    guard = RiskGuard(
        session_factory=lambda: Session(eng), secret="s", owner_user_ids=frozenset({1}),
        symbol_whitelist=frozenset({"TXF"}), max_qty_per_order=10, max_qty_per_day=5, max_orders_per_day=10,
    )

    results = []

    async def _attempt(i):
        with Session(eng) as s:
            req = _req(qty=5, client_order_id=f"P{i}")
            try:
                await asyncio.to_thread(
                    guard.check_place, s, req, actor_user_id=1, mode="sim", broker="shioaji",
                    account="F1", request_hash=_hash_for(req),
                )
                results.append("ok")
            except RiskError:
                results.append("blocked")

    async def scenario():
        await asyncio.gather(_attempt(1), _attempt(2))

    asyncio.run(scenario())
    assert sorted(results) == ["blocked", "ok"]  # 一過一擋，5+5>5 的日限額不被突破
