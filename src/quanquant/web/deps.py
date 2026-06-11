"""FastAPI dependencies and small request helpers."""
from datetime import date, datetime, time

from fastapi import Request

from quanquant.db.engine import get_session  # re-exported for routers
from quanquant.poller import QuotePoller

__all__ = ["get_session", "get_poller", "parse_date"]


def get_poller(request: Request) -> QuotePoller | None:
    """The shared poller from app state; None if not started (e.g. in tests)."""
    return getattr(request.app.state, "poller", None)


def parse_date(value: str | None, *, end: bool = False) -> datetime | None:
    """Parse a 'YYYY-MM-DD' filter value into a naive datetime bound."""
    if not value:
        return None
    d = date.fromisoformat(value)
    return datetime.combine(d, time(23, 59, 59) if end else time(0, 0, 0))
