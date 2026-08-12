"""Device-code flow（spec §4）發起端：`user_code` 產生、device code 建立與 hash、
per-IP／全域 active-pending 計數、過期列清理。

比照 `agent_tokens.py` 的模式：`create_device_code` 回傳的明文（device_code）只在這一
次呼叫存在，DB 只存 `_hash_device_code` 產生的 sha256 hash（見 `AgentDeviceCode.
device_code_hash`）。`_hash_device_code` 與 `agent_tokens._hash` 邏輯相同但不共用——
模組邊界各自獨立，比照既有風格（`agent_tokens.py` 模組頂部說明）。

`DEVICE_CODE_TTL_SECONDS`/`POLL_INTERVAL_SECONDS` 為模組常數，比照 `broker/
agent_commands.py` 的 `DEFAULT_COMMAND_EXPIRY_SECONDS` 先例：`config.py` 已新增對應的
`agent_device_code_ttl_seconds`/`agent_device_code_poll_interval_seconds` 兩個 Settings
鍵，但尚未接線——本 task 先用常數，供 Task 4/5/6 直接 import 使用；`agent_device_code_
rate_per_minute`/`_rate_burst`/`_max_active_per_ip`/`_max_active_global` 四個 Settings 鍵
同理留給 Task 5（router）接線，本模組不讀取。

限流判斷（token bucket、per-IP/全域上限）不在本模組——`create_device_code` 只負責『建立
一筆 pending device code』，呼叫端（Task 5 router）必須先用 `count_active_pending_for_ip`/
`count_active_pending_global` 完成限流檢查才呼叫本函式。"""
import hashlib
import hmac
import secrets
from datetime import timedelta

from sqlalchemy import delete, func
from sqlalchemy import update as sa_update
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from quanquant.auth.agent_tokens import stage_rotation
from quanquant.db.models import AgentDeviceCode, AgentToken, User, _utcnow

DEVICE_CODE_TTL_SECONDS = 600
POLL_INTERVAL_SECONDS = 5
_USER_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # 剔除 0/O/1/I（spec §4.1）
_USER_CODE_MAX_RETRIES = 3  # user_code 唯一鍵撞鍵重試上限（32^8 空間，機率極低，防禦性程式碼）


def _hash_device_code(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def generate_user_code() -> str:
    """8 碼 XXXX-XXXX，大寫、剔除混淆字元。secrets.choice——每碼獨立均勻取樣。"""
    chars = [secrets.choice(_USER_CODE_ALPHABET) for _ in range(8)]
    return "".join(chars[:4]) + "-" + "".join(chars[4:])


def count_active_pending_for_ip(session: Session, *, request_ip: str) -> int:
    """status='pending' 且 expires_at>now 的列數（§4.3 per-IP 限流用）。"""
    return session.exec(
        select(func.count()).where(
            AgentDeviceCode.request_ip == request_ip,
            AgentDeviceCode.status == "pending",
            AgentDeviceCode.expires_at > _utcnow(),
        )
    ).one()


def count_active_pending_global(session: Session) -> int:
    """同上，不篩 request_ip（全域上限用）。"""
    return session.exec(
        select(func.count()).where(
            AgentDeviceCode.status == "pending",
            AgentDeviceCode.expires_at > _utcnow(),
        )
    ).one()


def create_device_code(
    session: Session, *, request_ip: str, code_challenge: str,
) -> tuple[str, AgentDeviceCode]:
    """建立一筆 pending device code；回傳 (device_code 明文, row)。呼叫端（Task 5 router）
    必須先完成限流檢查（token bucket＋上面兩個計數函式）才呼叫本函式——本函式不做限流
    判斷，純粹『建立一筆』。副作用：同一交易內順手 DELETE expires_at < now-1day 的過期列
    （spec §4.2 清理策略，不加背景任務）。user_code 撞唯一鍵時重試（機率極低，32^8 空間），
    上限 3 次，仍撞則往外拋 IntegrityError（不可能發生，防禦性程式碼）。"""
    session.exec(  # type: ignore[call-overload]
        delete(AgentDeviceCode).where(AgentDeviceCode.expires_at < _utcnow() - timedelta(days=1))
    )
    for attempt in range(_USER_CODE_MAX_RETRIES):
        raw = secrets.token_urlsafe(32)
        row = AgentDeviceCode(
            device_code_hash=_hash_device_code(raw),
            code_challenge=code_challenge,
            user_code=generate_user_code(),
            request_ip=request_ip,
            expires_at=_utcnow() + timedelta(seconds=DEVICE_CODE_TTL_SECONDS),
            current_interval=POLL_INTERVAL_SECONDS,
        )
        session.add(row)
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            if attempt == _USER_CODE_MAX_RETRIES - 1:
                raise
            continue
        session.refresh(row)
        return raw, row
    raise AssertionError("unreachable")  # pragma: no cover


def claim_and_issue_device_token(
    session: Session, *, device_code_id: int, user_id: int, ttl_days: int,
) -> tuple[str, AgentToken] | None:
    """claim 交易（spec §4.1 step⑤ 核心）：①conditional UPDATE 搶占 consumed_at
    （WHERE id=:id AND user_id=:user_id AND consumed_at IS NULL AND status='approved'）
    ②呼叫 stage_rotation() ③單次 commit。任何 IntegrityError rollback 後必須從步驟①
    重新開始（搶占與簽發同生共死——絕不能出現『device code 已標 consumed 但 token 沒發
    出』的狀態）。回傳 None＝搶占失敗（rowcount=0，代表已被另一併發輪詢搶走，或狀態已
    變更）——呼叫端據此重讀最新狀態決定回應。"""
    t = AgentDeviceCode.__table__
    for attempt in range(2):
        now = _utcnow()
        stmt = (
            sa_update(t)
            .where(t.c.id == device_code_id, t.c.user_id == user_id,
                   t.c.consumed_at.is_(None), t.c.status == "approved")
            .values(consumed_at=now)
        )
        result = session.exec(stmt)  # type: ignore[call-overload]
        if result.rowcount == 0:
            session.rollback()
            return None
        raw, token = stage_rotation(session, user_id=user_id, ttl_days=ttl_days)
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            if attempt == 1:
                raise
            continue
        session.refresh(token)
        return raw, token
    raise AssertionError("unreachable")  # pragma: no cover


def _apply_poll_throttle(session: Session, *, row: AgentDeviceCode, now) -> dict | None:
    """單一 conditional UPDATE（spec §4.1 step④）。以樂觀鎖（WHERE last_polled_at=
    當初讀到的值）保證『讀→算新值→寫回』對同一列的併發輪詢是原子的——輸家 rowcount=0，
    重讀最新列狀態直接回應，不重算（避免雙重懲罰同一次遲到請求）。回傳 None＝不節流
    （放行到 claim 步驟）。"""
    if row.blocked_until is not None and row.blocked_until > now:
        return {"state": "slow_down", "interval": row.current_interval,
                "blocked_until": row.blocked_until.isoformat()}

    # 從未記錄過 last_polled_at，理論上等同「還沒有基準可比較」，一律放行；唯一例外：
    # current_interval 已經高於預設值（POLL_INTERVAL_SECONDS）——代表這列早就處於
    # 「已被節流過」的狀態（只是這次巧合沒有 last_polled_at 可比對，例如程序重啟後欄位
    # 不同步、或本模組測試以外的資料操作直接改列），保守起見視為仍在違規節奏內，避免
    # 「巧合缺基準」變成繞過節流的漏洞。
    too_fast = (
        (row.last_polled_at is not None and (now - row.last_polled_at).total_seconds() < row.current_interval)
        or (row.last_polled_at is None and row.current_interval > POLL_INTERVAL_SECONDS)
    )
    if too_fast:
        new_interval = min(row.current_interval + 5, 30)
        new_violations = row.consecutive_violations + 1
        if new_violations >= 5:
            new_blocked_until, new_violations = now + timedelta(seconds=60), 0
        else:
            new_blocked_until = row.blocked_until
    else:
        new_interval, new_violations, new_blocked_until = row.current_interval, 0, row.blocked_until

    t = AgentDeviceCode.__table__
    prev = row.last_polled_at
    where_prev = t.c.last_polled_at.is_(None) if prev is None else t.c.last_polled_at == prev
    stmt = (
        sa_update(t).where(t.c.id == row.id, where_prev)
        .values(last_polled_at=now, current_interval=new_interval,
                consecutive_violations=new_violations, blocked_until=new_blocked_until)
    )
    result = session.exec(stmt)  # type: ignore[call-overload]
    session.commit()
    if result.rowcount == 0:
        session.refresh(row)
        return {"state": "slow_down", "interval": row.current_interval} if too_fast else None
    row.last_polled_at, row.current_interval = now, new_interval
    row.consecutive_violations, row.blocked_until = new_violations, new_blocked_until
    return {"state": "slow_down", "interval": new_interval} if too_fast else None


def poll_device_token(
    session: Session, *, device_code: str, code_verifier: str, ttl_days: int,
) -> dict:
    """輪詢五步入口（spec §4.1 step2 全部語意）：①hash 查列 ②constant-time 驗
    code_verifier，失敗零副作用 ③唯讀 terminal 態（consumed/denied/expired） ④blocked/
    slow_down 原子節流 ⑤pending／approved claim。"""
    now = _utcnow()
    row = session.exec(
        select(AgentDeviceCode).where(AgentDeviceCode.device_code_hash == _hash_device_code(device_code))
    ).first()
    # ① hash 查無
    if row is None:
        return {"state": "invalid"}
    # ② constant-time 驗 code_verifier；失敗零副作用——這裡之前絕不能有任何 session.add/commit
    challenge = hashlib.sha256(code_verifier.encode("utf-8")).hexdigest()
    if not hmac.compare_digest(challenge, row.code_challenge):
        return {"state": "invalid"}
    # ③ 唯讀 terminal 狀態，恆同冪等，不寫入
    if row.consumed_at is not None:
        return {"state": "consumed"}
    if row.status == "denied":
        return {"state": "denied"}
    if row.expires_at <= now:
        return {"state": "expired"}
    # ④ slow_down／封鎖判定與更新
    throttled = _apply_poll_throttle(session, row=row, now=now)
    if throttled is not None:
        return throttled
    # ⑤ pending／approved claim
    if row.status == "pending":
        return {"state": "pending", "interval": row.current_interval}
    claimed = claim_and_issue_device_token(
        session, device_code_id=row.id, user_id=row.user_id, ttl_days=ttl_days,
    )
    if claimed is None:
        session.refresh(row)
        if row.consumed_at is not None:
            return {"state": "consumed"}
        if row.status == "denied":
            return {"state": "denied"}
        if row.expires_at <= now:
            return {"state": "expired"}
        return {"state": "pending"}
    raw, token = claimed
    owner = session.get(User, row.user_id)
    return {
        "state": "approved", "token": raw, "profile_id": str(row.user_id),
        "username": owner.username if owner else "", "token_expires_at": token.expires_at.isoformat(),
    }
