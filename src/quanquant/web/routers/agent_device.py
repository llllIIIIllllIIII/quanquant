"""Device-code flow（spec §4）對外 HTTP 端點——把 `auth/device_flow.py` 的 service 層接
上 HTTP，供本機 agent（CLI）與瀏覽器（`/agent/authorize`，Task 6）使用。

兩支 endpoint 都掛在 `app.py` 的 public 區（比照 `/ws/agent`，不經 `protected` 依賴）：
device-code 發起本來就發生在使用者登入之前（agent 端還沒有任何憑證），poll 端點靠
`device_code`＋PoP（`code_verifier`）自證，不需要 cookie session。

鐵律（spec §4.3）：trusted IP 只讀 `request.client.host`——uvicorn 的
`ProxyHeadersMiddleware`（`forwarded_allow_ips`，見 Task 16）已經把可信來源的
`X-Forwarded-For` 換算好放進 `request.client.host`，這裡絕不自行解析任何
`X-Forwarded-*` 標頭（否則不受信任的來源可以偽造標頭繞過限流／偽冒 IP）。

限流兩層，序列化在同一顆 `app.state.device_code_lock`（`asyncio.Lock`）內，順序固定
「先過 token bucket → 再查 DB active 上限 → 才 insert」：
  ①`app.state.device_code_bucket`（`TokenBucket`）：每 IP 頻率限制（10 次/分、burst 5），
    超限 429，純記憶體、不查 DB，擋掉真正的暴衝。
  ②`count_active_pending_for_ip`/`count_active_pending_global`：DB 查詢的持久上限（同 IP
    同時最多幾筆 pending、全站同時最多幾筆 pending），超限同樣 429。lock 序列化「計數→
    insert」這段，避免兩個併發請求各自查到「還沒超限」而一起插入、一起衝破上限
    （TOCTOU）。

回應不透露細節：`poll_device_code` 對 `state == "invalid"`（hash 查無列，或
code_verifier 驗證失敗）一律回 404 且不帶額外資訊——不讓外部呼叫者用回應差異去猜測
「device_code 到底存不存在」還是「PoP 驗證失敗」。其餘狀態（`expired`/`denied`/
`consumed`/`pending`/`slow_down`/`approved`）都是唯讀或已授權後的合法終態／中繼態，
直接回 200，body 帶 `poll_device_token()` 原始 dict（FastAPI 對 dict 回傳自動轉
JSON）。secrets（明文 device_code／token）只在回應 body 出現一次，不落 log。
"""
import asyncio

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlmodel import Session

from quanquant.auth.device_flow import (
    DEVICE_CODE_TTL_SECONDS,
    POLL_INTERVAL_SECONDS,
    count_active_pending_for_ip,
    count_active_pending_global,
    create_device_code,
    poll_device_token,
)
from quanquant.config import get_settings
from quanquant.web.deps import get_session
from quanquant.web.rate_limit import TokenBucket

router = APIRouter()

_VERIFICATION_PATH = "/agent/authorize"  # 固定路徑（spec §4.1）——瀏覽器端授權頁（Task 6）


class DeviceCodeInitiateRequest(BaseModel):
    code_challenge: str


class DeviceCodeInitiateResponse(BaseModel):
    device_code: str
    user_code: str
    verification_path: str
    interval: int
    expires_in: int


class DeviceTokenPollRequest(BaseModel):
    device_code: str
    code_verifier: str


def client_ip(request: Request) -> str:
    """trusted IP 讀取：只用 `request.client.host`——uvicorn 的 `ProxyHeadersMiddleware`
    （`forwarded_allow_ips`，見 Task 16）已經把可信來源的 `X-Forwarded-For` 換算好，這裡
    絕不自行解析任何 `X-Forwarded-*` 標頭（spec §4.3 鐵律）。"""
    return request.client.host if request.client is not None else ""


def _device_code_bucket(request: Request) -> TokenBucket:
    """Lazy-init、快取在 `app.state`（每個 app 實例一份）。刻意延到第一次真正呼叫這支
    endpoint 才讀 `get_settings()`／建構——而不是在 `create_app()` 當下就建——這樣測試（或
    部署）在啟動之後才調整的 `AGENT_DEVICE_CODE_RATE_*` 環境變數＋`get_settings.
    cache_clear()` 才會確實反映在限流參數上（`create_app()` 早於任何請求執行，若在那裡就
    綁死 rate/burst，之後的設定調整永遠讀不到）。"""
    bucket = getattr(request.app.state, "device_code_bucket", None)
    if bucket is None:
        settings = get_settings()
        bucket = TokenBucket(
            rate_per_minute=settings.agent_device_code_rate_per_minute,
            burst=settings.agent_device_code_rate_burst,
        )
        request.app.state.device_code_bucket = bucket
    return bucket


def _device_code_lock(request: Request) -> asyncio.Lock:
    """`create_app()` 已經建構好一份放在 `app.state`（比照 `agent_registry` 的掛法）；這裡
    仍用 `getattr` 防禦性 fallback（測試若繞過 `create_app()` 直接組 app 時不必因此炸掉）。"""
    lock = getattr(request.app.state, "device_code_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        request.app.state.device_code_lock = lock
    return lock


@router.post("/api/agent/device-code", response_model=DeviceCodeInitiateResponse)
async def initiate_device_code(
    request: Request,
    body: DeviceCodeInitiateRequest,
    session: Session = Depends(get_session),
) -> DeviceCodeInitiateResponse:
    settings = get_settings()
    ip = client_ip(request)

    bucket = _device_code_bucket(request)
    if not bucket.allow(ip):
        raise HTTPException(status_code=429, detail="請求過於頻繁，請稍後再試")

    async with _device_code_lock(request):
        if count_active_pending_for_ip(session, request_ip=ip) >= settings.agent_device_code_max_active_per_ip:
            raise HTTPException(status_code=429, detail="此 IP 進行中的 device code 已達上限")
        if count_active_pending_global(session) >= settings.agent_device_code_max_active_global:
            raise HTTPException(status_code=429, detail="進行中的 device code 已達全站上限")
        raw_device_code, row = create_device_code(session, request_ip=ip, code_challenge=body.code_challenge)

    return DeviceCodeInitiateResponse(
        device_code=raw_device_code,
        user_code=row.user_code,
        verification_path=_VERIFICATION_PATH,
        interval=POLL_INTERVAL_SECONDS,
        expires_in=DEVICE_CODE_TTL_SECONDS,
    )


@router.post("/api/agent/device-token")
async def poll_device_code(
    request: Request,
    body: DeviceTokenPollRequest,
    session: Session = Depends(get_session),
) -> dict:
    settings = get_settings()
    result = poll_device_token(
        session,
        device_code=body.device_code,
        code_verifier=body.code_verifier,
        ttl_days=settings.agent_token_ttl_days,
    )
    if result["state"] == "invalid":
        raise HTTPException(status_code=404)
    return result
