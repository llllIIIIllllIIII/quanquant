"""FastAPI dependencies and small request helpers."""
from datetime import date, datetime, time
from typing import TYPE_CHECKING

from fastapi import Depends, HTTPException, Request
from sqlmodel import Session

from quanquant.auth.tokens import SESSION_COOKIE, load_session
from quanquant.broker.session_state import OrderSessionState
from quanquant.db.engine import get_session  # re-exported for routers
from quanquant.db.models import User
from quanquant.poller import QuotePoller
from quanquant.pulse.engine import PulseEngine

if TYPE_CHECKING:
    from quanquant.broker.agent_registry import UserAgentSlot

__all__ = ["get_session", "get_poller", "get_pulse", "get_order_session_state", "parse_date",
           "get_current_user", "require_admin", "get_agent_slot", "get_order_service", "resolve_mode"]


def get_poller(request: Request) -> QuotePoller | None:
    """The shared poller from app state; None if not started (e.g. in tests)."""
    return getattr(request.app.state, "poller", None)


def get_pulse(request: Request) -> PulseEngine | None:
    """The shared Market Pulse engine from app state; None if disabled / in tests."""
    return getattr(request.app.state, "pulse", None)


def get_order_session_state(request: Request) -> OrderSessionState | None:
    """下單子系統目前狀態（Task 8）；None 代表 lifespan 尚未跑過（如測試環境）——
    /healthz 對此情況一律回傳 order_subsystem=None，不視為錯誤。"""
    return getattr(request.app.state, "order_session_state", None)


def parse_date(value: str | None, *, end: bool = False) -> datetime | None:
    """Parse a 'YYYY-MM-DD' filter value into a naive datetime bound."""
    if not value:
        return None
    d = date.fromisoformat(value)
    return datetime.combine(d, time(23, 59, 59) if end else time(0, 0, 0))


def _auth_failure(request: Request) -> HTTPException:
    """Full-page loads get a 303 to /login; HTMX/API calls get 401 + HX-Redirect
    (htmx performs a full-page redirect on that header)."""
    if request.headers.get("HX-Request") or request.url.path.startswith("/api/"):
        return HTTPException(status_code=401, headers={"HX-Redirect": "/login"})
    return HTTPException(status_code=303, headers={"Location": "/login"})


def get_current_user(request: Request, session: Session = Depends(get_session)) -> User:
    """Resolve the logged-in user from the signed session cookie.

    Re-checks is_active and token_version on every request, so deactivating an
    account or changing a password revokes existing cookies immediately. Also
    stashes the user on request.state for templates (base.html user menu).
    """
    raw = request.cookies.get(SESSION_COOKIE)
    data = load_session(raw) if raw else None
    if data is None:
        raise _auth_failure(request)
    user = session.get(User, data["uid"])
    if user is None or not user.is_active or user.token_version != data["tv"]:
        raise _auth_failure(request)
    request.state.user = user
    return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="admin only")
    return user


def get_agent_slot(request: Request, user: User = Depends(get_current_user)) -> "UserAgentSlot | None":
    """Task 7（D1）：agent 模式（`app.state.agent_registry` 存在＝wiring 完成）下，這個
    請求的 user 對應的 `UserAgentSlot`——只有 owner 白名單成員才有 slot（D1 eager 建置），
    非 owner／查無一律 None。in-process 模式（無 registry）恆回 None。"""
    registry = getattr(request.app.state, "agent_registry", None)
    if registry is None:
        return None
    return registry.get(user.id)


def get_order_service(request: Request, user: User = Depends(get_current_user)):
    """Task 7（D1）：user-aware 下單服務解析——取代原本各路由檔各自定義的
    `getattr(request.app.state, "order_service", None)`。

    agent 模式（`agent_registry` 已 wiring）：只有這個 user 在 registry 裡有 slot（＝owner
    白名單成員）才回自己的 adapter；查無 slot（非 owner／unknown user）回 None，沿用既有
    「下單子系統未啟用」的下游語意——owner 呼叫下單時 adapter 內部仍會再驗一次
    `risk_guard.assert_owner`，403 語意不變（見 shioaji_adapter.py place/cancel/update）。

    in-process 模式／agent 模式尚未成功 wiring（如 backfill 衝突拒啟）：`agent_registry`
    不存在，一律 fallback 回單例 `app.state.order_service`（含 None）——與 Task 7 之前完全
    零改動的既有路徑。"""
    registry = getattr(request.app.state, "agent_registry", None)
    if registry is not None:
        slot = registry.get(user.id)
        return slot.adapter if slot is not None else None
    return getattr(request.app.state, "order_service", None)


def resolve_mode(mode: str | None, service) -> str:
    """R3-1（2026-09-10）：`mode` 分頁未帶 `?mode=` 時的預設值共用 helper——跟隨 server
    執行模式（`service.mode`），與 `orders.py` 既有三個新頁（`orders_queue_page`／
    `orders_deals_page`／`orders_holdings_page`）沿用的
    `_mode(mode or (service.mode if service is not None else None))` 慣例完全一致（見該檔
    `_mode`）。帶 `?mode=` 時一律優先，不受這裡影響。

    `service` 為 `None`（下單子系統停用、或呼叫者非 owner，見 `get_order_service`）、或
    `service.mode` 不是合法值時，fallback 回 `"sim"`——沿用 orders.py 既有慣例：寧可預設
    顯示風險較低的模擬資料，也不要在無法判斷真實執行模式時靜默落回 real。

    這輪（2026-09-10 R3-1）只套用在 `stats.py`／`trades.py`；`orders.py` 本身這次刻意不碰
    ——另一條並行開發線正在改它，待該線合併後兩邊應收斂成同一份實作（`orders.py::_mode`
    屆時改呼叫這裡）。
    """
    candidate = mode or (service.mode if service is not None else None)
    return candidate if candidate in ("sim", "real") else "sim"
