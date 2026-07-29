"""RiskGuard：Task 6 `_RiskGuardLike` 的真正實作。

check_place/check_update 內任何攔截（AuthorizationError 或 RiskError，含 owner 授權失敗）
都 rollback 目前交易（未 commit 的 quota reserve/create_order 一併撤銷）後在新交易補一筆
append_audit(action="risk_reject")；通過則與 create_order（place）或狀態更新（update）
同一交易 commit（V3-3：quota reserve 與建單同一交易）。

round3 #4（quota reservation 閉環，重要澄清）：check_place/check_update 只負責用
`repository.reserve_quota` 建立 state='reserved' 的保留列（CAS，同一交易內與 create_order
一起 commit/rollback）——**不**在這裡自動 confirm。委託是否真的成功送到券商是
ShioajiAdapter（Task 6）收到 native 呼叫結果之後才知道的事，所以「送出成功→confirm_quota、
送出失敗/被擋→release_quota、結果不明→不動（留給 Task 8 watchdog reconcile 決議）」的收尾
屬於 adapter 職責，見 shioaji_adapter.py 的 place/update。check_update 對「變動量」
（new_qty-order.qty）只在**增加**時才走 reserve_quota；減少不需要多保留（降低用量本來就
不會超限，也不去動原本 place 時保留的那一列——那一列的 confirm/release 仍歸 place 的
送出結果決定，不因 update 而改變）。

兩階段確認：itsdangerous 簽出的 token 字串本身防偽造/篡改/過期；ConfirmToken DB 列另外
保證一次性（claim 是原子 UPDATE，見 broker/repository.py::claim_confirm_token）。token 綁
(actor_user_id,payload_hash)，payload_hash 一律來自呼叫端傳入的 canonical_payload_hash
結果（place 用即將送出的內容、update 用套用變更後的完整新內容）——本檔不重算，只驗證/消費，
這正是 BLOCKER#1 的修法：place/update 共用同一個 hash helper，update 簽發/驗證用的是
「這次改單後真正會送給券商的完整內容」而非委託身分鍵，round-trip 才會成功。
"""
from datetime import datetime, timedelta, timezone

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from sqlmodel import Session

from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, RiskError
from quanquant.broker.types import OrderRequest
from quanquant.db.models import Order


def parse_owner_ids(raw: str) -> frozenset[int]:
    return frozenset(int(x.strip()) for x in raw.split(",") if x.strip())


def parse_whitelist(raw: str) -> frozenset[str]:
    return frozenset(x.strip() for x in raw.split(",") if x.strip())


def _now_naive_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class RiskGuard:
    def __init__(
        self,
        *,
        session_factory,
        secret: str,
        owner_user_ids: frozenset[int],
        symbol_whitelist: frozenset[str],
        max_qty_per_order: int,
        max_qty_per_day: int,
        max_orders_per_day: int,
        confirm_token_ttl_seconds: int = 120,
        kill_switch_initial: bool = False,
    ) -> None:
        self._session_factory = session_factory
        self._serializer = URLSafeTimedSerializer(secret, salt="order-confirm-token")
        self._owner_user_ids = owner_user_ids
        self._symbol_whitelist = symbol_whitelist
        self._max_qty_per_order = max_qty_per_order
        self._max_qty_per_day = max_qty_per_day
        self._max_orders_per_day = max_orders_per_day
        self._confirm_token_ttl_seconds = confirm_token_ttl_seconds
        self._kill_switch = kill_switch_initial

    @property
    def kill_switch(self) -> bool:
        return self._kill_switch

    def set_kill_switch(self, value: bool) -> None:
        """即時可切（非啟動快照）：緊貼送單前的 ShioajiAdapter._send_gate 讀的就是這個
        property 當下的值，Task 8 的 admin 開關/緊急停止直接呼叫本方法即可立刻生效。"""
        self._kill_switch = value

    def is_owner(self, actor_user_id: int) -> bool:
        """非 raise 版的 owner 判定（`assert_owner` 是 raise 版）：供模板/UI 決定是否顯示
        owner-only 控制（如 kill switch）。server 端的授權仍一律經 `assert_owner`。"""
        return actor_user_id in self._owner_user_ids

    def assert_owner(self, actor_user_id: int) -> None:
        if actor_user_id not in self._owner_user_ids:
            raise AuthorizationError(f"user_id={actor_user_id} 不是 owner")

    def issue_confirm_token(self, session: Session, *, actor_user_id: int, payload_hash: str) -> str:
        self.assert_owner(actor_user_id)
        import secrets

        jti = secrets.token_urlsafe(16)
        expires_at = _now_naive_utc() + timedelta(seconds=self._confirm_token_ttl_seconds)
        brepo.create_confirm_token_row(
            session, jti=jti, actor_user_id=actor_user_id, payload_hash=payload_hash, expires_at=expires_at
        )
        session.commit()
        return self._serializer.dumps({"jti": jti, "actor_user_id": actor_user_id, "payload_hash": payload_hash})

    def _claim_confirm_token(
        self, session: Session, *, actor_user_id: int, payload_hash: str, token: str | None
    ) -> bool:
        if not token:
            return False
        try:
            data = self._serializer.loads(token, max_age=self._confirm_token_ttl_seconds)
        except (BadSignature, SignatureExpired):
            return False
        if data.get("actor_user_id") != actor_user_id or data.get("payload_hash") != payload_hash:
            return False
        return brepo.claim_confirm_token(
            session, jti=data["jti"], actor_user_id=actor_user_id, payload_hash=payload_hash, now=_now_naive_utc()
        )

    def check_place(
        self,
        session: Session,
        req: OrderRequest,
        *,
        actor_user_id: int,
        mode: str,
        broker: str,
        account: str,
        request_hash: str,
        confirm_token: str | None = None,
    ) -> Order:
        trading_day = brepo.trading_day_for(int(_now_naive_utc().timestamp() * 1000))
        try:
            self.assert_owner(actor_user_id)
            if self.kill_switch:
                raise RiskError("kill switch 已啟動")
            if req.symbol not in self._symbol_whitelist:
                raise RiskError(f"{req.symbol} 不在白名單")
            if req.qty > self._max_qty_per_order:
                raise RiskError(f"單筆口數 {req.qty} 超過上限 {self._max_qty_per_order}")
            if brepo.count_orders_today(
                session, user_id=actor_user_id, mode=mode, trading_day=trading_day
            ) >= self._max_orders_per_day:
                raise RiskError("今日委託次數已達上限")
            # round3 #4：reservation_id 用 client_order_id（呼叫端冪等鍵），CAS 建立
            # state='reserved' 列；只 reserve，不在這裡 confirm（見模組頂部說明）。
            if not brepo.reserve_quota(
                session, reservation_id=req.client_order_id, user_id=actor_user_id, mode=mode,
                trading_day=trading_day, qty=req.qty, daily_limit=self._max_qty_per_day,
            ):
                raise RiskError("今日口數配額已滿")

            if mode == "real" and not self._claim_confirm_token(
                session, actor_user_id=actor_user_id, payload_hash=request_hash, token=confirm_token
            ):
                raise RiskError("real 下單需要有效的兩階段確認", needs_confirm=True)

            order = brepo.create_order(
                session, client_order_id=req.client_order_id, request_hash=request_hash,
                user_id=actor_user_id, mode=mode, broker=broker, account=account,
                symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
                price_type=req.price_type, order_type=req.order_type, octype=req.octype,
                trading_day=trading_day,
            )
            brepo.append_audit(session, actor_user_id=actor_user_id, mode=mode, action="place",
                               payload_hash=request_hash, result="ok")
            session.commit()
            return order
        except (RiskError, AuthorizationError) as exc:
            session.rollback()
            self._audit_reject(actor_user_id, mode, "place", request_hash, exc)
            raise

    def check_update(
        self,
        session: Session,
        order: Order,
        *,
        actor_user_id: int,
        new_qty: int,
        new_price,
        request_hash: str,
        confirm_token: str | None = None,
    ) -> None:
        try:
            self.assert_owner(actor_user_id)
            if order.user_id != actor_user_id:
                raise AuthorizationError("非委託所有人不得改單")
            if new_qty <= 0:
                raise RiskError(f"qty 必須 > 0，收到 {new_qty}")
            # price>0 檢查改成 price_type 感知（bug 2）：order.price_type 才是「這張委託」的
            # 價格類型（改單不能改變 price_type），MKT 不需要價格、允許 0，只有 LMT 才要求
            # price > 0；兩者皆不可為負。
            if new_price is not None:
                if order.price_type == "LMT" and new_price <= 0:
                    raise RiskError(f"price 必須 > 0，收到 {new_price}")
                if order.price_type != "LMT" and new_price < 0:
                    raise RiskError(f"price 不可為負，收到 {new_price}")
            if self.kill_switch:
                raise RiskError("kill switch 已啟動")
            if order.symbol not in self._symbol_whitelist:
                raise RiskError(f"{order.symbol} 不在白名單")
            if new_qty > self._max_qty_per_order:
                raise RiskError(f"單筆口數 {new_qty} 超過上限 {self._max_qty_per_order}")

            delta = new_qty - order.qty
            if delta > 0:
                # 只對「增加」的變動量另開一列保留（減少不需要、也不動 place 時保留的那列，
                # 那列的 confirm/release 仍由 place 的送出結果決定）。reservation_id 用
                # repository.reservation_id_for_update 推導，與 ShioajiAdapter.update 共用
                # 同一公式，confirm/release 才能命中正確的列。
                reservation_id = brepo.reservation_id_for_update(
                    client_order_id=order.client_order_id, request_hash=request_hash
                )
                if not brepo.reserve_quota(
                    session, reservation_id=reservation_id, user_id=actor_user_id, mode=order.mode,
                    trading_day=order.trading_day, qty=delta, daily_limit=self._max_qty_per_day,
                ):
                    raise RiskError("今日口數配額已滿")

            if order.mode == "real" and not self._claim_confirm_token(
                session, actor_user_id=actor_user_id, payload_hash=request_hash, token=confirm_token
            ):
                raise RiskError("real 改單需要有效的兩階段確認", needs_confirm=True)

            brepo.append_audit(session, actor_user_id=actor_user_id, mode=order.mode, action="update",
                               payload_hash=request_hash, result="ok")
            session.commit()
        except (RiskError, AuthorizationError) as exc:
            session.rollback()
            self._audit_reject(actor_user_id, order.mode, "update", request_hash, exc)
            raise

    def _audit_reject(self, actor_user_id: int, mode: str, action: str, payload_hash: str, exc: Exception) -> None:
        with self._session_factory() as session:
            brepo.append_audit(
                session, actor_user_id=actor_user_id, mode=mode, action="risk_reject",
                payload_hash=payload_hash, result="rejected", rule=action, detail=str(exc),
            )
            session.commit()
