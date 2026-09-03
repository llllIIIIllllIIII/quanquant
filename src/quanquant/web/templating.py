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

# 003：商品代碼 → 中文品名簡稱（人看的辨識，對應交易者口語）。畫面顯示用，送單/API/資料表
# key 一律仍是代碼；未知代碼由 symbol_label filter 回代碼本身，不可顯示空白或錯誤名稱。
SYMBOL_LABELS = {"TXF": "台指", "MXF": "小台", "TMF": "微台"}

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


def _dt_cst(value: datetime | None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """終審必修 HIGH-3：naive-UTC datetime（如 `Order.created_at`，見 db/models.py::
    _utcnow）→ 台灣本地時間字串。`_dt` 只 strftime、不轉時區，套用在 naive-UTC 欄位上
    會裸印 UTC 時刻（差 8 小時）；本 filter 比照既有 `_cst_time`/`_ms_dt` 的轉換管道
    （tzinfo 缺席一律視為 UTC，`.astimezone(_CST)`），但保留完整日期＋秒（`_cst_time`
    只回 HH:MM:SS，不夠用在跨日的委託列表）。**不可**拿去套已經是 naive-local 值的欄位
    （如 `Trade.entry_time`/`exit_time`，那些本來就存 CST，見
    broker/position_tracker.py::_utc_dt_to_cst 的說明——那些欄位仍用 `_dt`，不要混用）。"""
    if value is None:
        return "—"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(_CST).strftime(fmt)


def _symbol_label(code: str | None) -> str:
    """商品代碼 → 中文品名簡稱；未知/未對映代碼一律回代碼本身，不可顯示空白或錯誤名稱。"""
    if not code:
        return code or ""
    return SYMBOL_LABELS.get(code, code)


# 006（委託頁搬家順修 P1-9）：委託狀態英文原始值 → 繁中對照；未知值一律回原字串本身
# （不可顯示空白，防未來新增狀態值時畫面消失）。
_STATUS_LABELS = {
    "pending": "待送出", "sending": "傳送中", "submitted": "已委託",
    "partfilled": "部分成交", "filled": "全部成交", "cancelled": "已取消",
    "failed": "失敗", "unknown": "狀態不明",
}

# P1-8：倉別中文化（下單表單既有的三個選項標籤同一份對照，這裡供委託列表小標籤重用）。
_OCTYPE_LABELS = {"New": "新倉", "Cover": "平倉", "Auto": "自動"}


def _status_label(value: str | None) -> str:
    if not value:
        return "—"
    return _STATUS_LABELS.get(value, value)


def _octype_label(value: str | None) -> str:
    if not value:
        return "—"
    return _OCTYPE_LABELS.get(value, value)


def _order_type_badge(price_type: str | None, order_type: str | None, octype: str | None) -> str:
    """P1-8：委託列表補「LMT/MKT・ROD/IOC/FOK・新倉/平倉」，縮成一格小標籤而非三個獨立欄，
    用法 `{{ o.price_type | order_type_badge(o.order_type, o.octype) }}`。"""
    return f"{price_type or '—'}・{order_type or '—'}・{_octype_label(octype)}"


def _ms_dt(value: int | None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """epoch-ms UTC（如 `Deal.ts`）→ 台灣本地時間字串；None 回「—」（同 `_dt` 慣例）。"""
    if value is None:
        return "—"
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).astimezone(_CST).strftime(fmt)


templates.env.filters["num"] = _num
templates.env.filters["signed"] = _signed
templates.env.filters["pct"] = _pct
templates.env.filters["comma"] = _comma
templates.env.filters["cst_time"] = _cst_time
templates.env.filters["dt"] = _dt
templates.env.filters["dt_cst"] = _dt_cst
templates.env.filters["symbol_label"] = _symbol_label
templates.env.filters["status_label"] = _status_label
templates.env.filters["octype_label"] = _octype_label
templates.env.filters["order_type_badge"] = _order_type_badge
templates.env.filters["ms_dt"] = _ms_dt


def render_partial(name: str, **context) -> str:
    """Render a template to an HTML string (for SSE payloads / HTMLResponses)."""
    return templates.env.get_template(name).render(**context)
