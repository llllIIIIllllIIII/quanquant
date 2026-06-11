"""Shared Jinja2 environment, template helpers, and formatting filters."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from fastapi.templating import Jinja2Templates

WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"

_CST = timezone(timedelta(hours=8))
SESSION_LABEL = {"day": "● 日盤", "night": "● 夜盤", "closed": "○ 休市"}

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _to_decimal(value) -> Decimal:
    # str() guards against float drift if a non-Decimal ever reaches these filters.
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _num(value) -> str:
    if value is None:
        return "—"
    s = f"{_to_decimal(value):,.2f}"
    if s.endswith(".00"):
        s = s[:-3]
    return s


def _signed(value) -> str:
    if value is None:
        return "—"
    s = f"{_to_decimal(value):+,.2f}"
    return s[:-3] if s.endswith(".00") else s


def _pct(value) -> str:
    return "—" if value is None else f"{value:+.2f}"


def _comma(value) -> str:
    return "—" if value is None else f"{int(value):,}"


def _cst_time(value: datetime | None) -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(_CST).strftime("%H:%M:%S")


def _dt(value: datetime | None, fmt: str = "%Y-%m-%d %H:%M") -> str:
    return value.strftime(fmt) if value else "—"


templates.env.filters["num"] = _num
templates.env.filters["signed"] = _signed
templates.env.filters["pct"] = _pct
templates.env.filters["comma"] = _comma
templates.env.filters["cst_time"] = _cst_time
templates.env.filters["dt"] = _dt


def render_partial(name: str, **context) -> str:
    """Render a template to an HTML string (for SSE payloads / HTMLResponses)."""
    return templates.env.get_template(name).render(**context)
