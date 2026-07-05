"""FastAPI dependencies and small request helpers."""
from datetime import date, datetime, time

from fastapi import Depends, HTTPException, Request
from sqlmodel import Session

from quanquant.auth.tokens import SESSION_COOKIE, load_session
from quanquant.db.engine import get_session  # re-exported for routers
from quanquant.db.models import User
from quanquant.poller import QuotePoller
from quanquant.pulse.engine import PulseEngine

__all__ = ["get_session", "get_poller", "get_pulse", "parse_date",
           "get_current_user", "require_admin"]


def get_poller(request: Request) -> QuotePoller | None:
    """The shared poller from app state; None if not started (e.g. in tests)."""
    return getattr(request.app.state, "poller", None)


def get_pulse(request: Request) -> PulseEngine | None:
    """The shared Market Pulse engine from app state; None if disabled / in tests."""
    return getattr(request.app.state, "pulse", None)


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
