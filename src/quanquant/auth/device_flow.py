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
import secrets
from datetime import timedelta

from sqlalchemy import delete, func
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from quanquant.db.models import AgentDeviceCode, _utcnow

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
