"""Trade journal: page, table fragment (filtered), and CRUD (HTMX-first)."""
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import ValidationError
from sqlmodel import Session

from quanquant.db.models import User
from quanquant.journal import repository as repo
from quanquant.journal import review_repository as review_repo
from quanquant.journal.pnl import unrealized_pnl
from quanquant.journal.schemas import TradeCreate, TradeUpdate, split_tags
from quanquant.poller import QuotePoller
from quanquant.web.deps import get_current_user, get_order_service, get_poller, get_session, parse_date, resolve_mode
from quanquant.web.templating import render_partial, templates

router = APIRouter()


def _mark_price(poller: QuotePoller | None) -> Decimal | None:
    return poller.last.price if poller and poller.last else None


def _rows(trades: list, mark: Decimal | None) -> list[dict]:
    out = []
    for t in trades:
        unreal = None
        if t.exit_time is None and mark is not None:
            unreal = unrealized_pnl(t.direction, t.entry_price, mark, t.size, t.point_value, t.fee)
        out.append({"t": t, "tags": split_tags(t.tags), "unrealized": unreal})
    return out


async def _clean_form(request: Request) -> dict:
    form = await request.form()

    def g(key):
        v = form.get(key)
        return v if v not in (None, "") else None

    return {
        "symbol": form.get("symbol"),
        "direction": form.get("direction"),
        "entry_time": g("entry_time"),
        "entry_price": g("entry_price"),
        "exit_time": g("exit_time"),
        "exit_price": g("exit_price"),
        "stop_loss_price": g("stop_loss_price"),
        "take_profit_strategy": g("take_profit_strategy"),
        "size": g("size"),
        "point_value": g("point_value") or "200",
        "fee": g("fee"),
        "pnl": g("pnl"),
        "note": g("note"),
        "tags": split_tags(form.get("tags")),
        "mode": form.get("mode") or "real",
    }


def _trigger_response(*, close_modal: bool) -> HTMLResponse:
    # Empty body + HX-Trigger so the filter form re-fetches the table with the
    # user's current filters intact (instead of silently clearing them on mutate).
    events = "closemodal, refreshtable" if close_modal else "refreshtable"
    return HTMLResponse("", headers={"HX-Trigger": events})


def _form_error(message: str) -> HTMLResponse:
    # Re-target into the form's error slot so the message shows on the form, not the table.
    # 200 (not 4xx) so htmx performs the swap; HX-Retarget puts it in the form's slot.
    html = render_partial("partials/form_error.html", message=message)
    return HTMLResponse(
        html, status_code=200, headers={"HX-Retarget": ".form-error-slot", "HX-Reswap": "innerHTML"}
    )


@router.get("/journal", response_class=HTMLResponse)
async def journal_page(
    request: Request,
    session: Session = Depends(get_session),
    poller: QuotePoller | None = Depends(get_poller),
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
    symbol: str | None = Query(None),
    tag: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    status: str = Query("all"),
    mode: str | None = Query(None),
):
    # R3-1（2026-09-10）：未帶 `?mode=` 時跟隨 server 執行模式，與 /orders/queue、
    # /orders/deals、/orders/holdings、/stats 一致（見 web/deps.py::resolve_mode）。
    mode = resolve_mode(mode, service)
    trades = repo.list_trades(
        session,
        user_id=user.id,
        mode=mode,
        symbol=symbol or None,
        tag=tag or None,
        date_from=parse_date(date_from),
        date_to=parse_date(date_to, end=True),
        status=status,
    )
    return templates.TemplateResponse(
        request,
        "journal.html",
        {
            "active": "journal",
            "rows": _rows(trades, _mark_price(poller)),
            "symbols": repo.list_symbols(session, user_id=user.id),
            "all_tags": repo.list_all_tags(session, user_id=user.id),
            "f": {"symbol": symbol or "", "tag": tag or "", "date_from": date_from or "",
                  "date_to": date_to or "", "status": status, "mode": mode},
            # 010 讀入口：按時間（trading_day 新到舊）讀每日復盤，與逐筆交易並存；沿用本頁
            # 既有的 date_from/date_to 篩選字串直接比對 trading_day（皆為 ISO 日期字串）。
            "reviews": review_repo.list_reviews(
                session, user_id=user.id, mode=mode, date_from=date_from or None, date_to=date_to or None
            ),
        },
    )


@router.get("/trades", response_class=HTMLResponse)
async def list_trades_fragment(
    session: Session = Depends(get_session),
    poller: QuotePoller | None = Depends(get_poller),
    user: User = Depends(get_current_user),
    symbol: str | None = Query(None),
    tag: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    status: str = Query("all"),
    mode: str = Query("real"),
):
    trades = repo.list_trades(
        session,
        user_id=user.id,
        mode=mode,
        symbol=symbol or None,
        tag=tag or None,
        date_from=parse_date(date_from),
        date_to=parse_date(date_to, end=True),
        status=status,
    )
    return HTMLResponse(render_partial("partials/trade_table.html", rows=_rows(trades, _mark_price(poller))))


def _fmt_dt_input(value) -> str:
    return value.strftime("%Y-%m-%dT%H:%M") if value else ""


def _s(value) -> str:
    return "" if value is None else str(value)


def _form_values(t=None, mode: str = "real") -> dict:
    if t is None:
        return {
            "action": "post", "url": "/trades", "title": "新增交易",
            "symbol": "TXF", "direction": "long", "entry_time": "", "entry_price": "",
            "exit_time": "", "exit_price": "", "stop_loss_price": "", "take_profit_strategy": "",
            "size": "1", "point_value": "200", "fee": "", "pnl": "", "note": "", "tags": "",
            "mode": mode,
        }
    return {
        "action": "put", "url": f"/trades/{t.id}", "title": "編輯交易",
        "symbol": t.symbol, "direction": t.direction,
        "entry_time": _fmt_dt_input(t.entry_time), "entry_price": _s(t.entry_price),
        "exit_time": _fmt_dt_input(t.exit_time), "exit_price": _s(t.exit_price),
        "stop_loss_price": _s(t.stop_loss_price), "take_profit_strategy": _s(t.take_profit_strategy),
        "size": _s(t.size), "point_value": _s(t.point_value), "fee": _s(t.fee),
        "pnl": _s(t.pnl) if t.pnl_is_manual else "",
        "note": _s(t.note), "tags": ",".join(split_tags(t.tags)),
        "mode": t.mode,
    }


@router.get("/trades/new", response_class=HTMLResponse)
async def new_trade_form(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    mode: str = Query("real"),
):
    return templates.TemplateResponse(
        request,
        "partials/trade_form.html",
        {"v": _form_values(None, mode), "all_tags": repo.list_all_tags(session, user_id=user.id)},
    )


@router.get("/trades/{trade_id}/edit", response_class=HTMLResponse)
async def edit_trade_form(
    trade_id: int,
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    trade = repo.get_trade(session, trade_id, user_id=user.id)
    if trade is None:
        return HTMLResponse("找不到交易", status_code=404)
    return templates.TemplateResponse(
        request,
        "partials/trade_form.html",
        {"v": _form_values(trade), "all_tags": repo.list_all_tags(session, user_id=user.id)},
    )


@router.post("/trades", response_class=HTMLResponse)
async def create_trade_route(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    try:
        data = TradeCreate(**await _clean_form(request))
    except (ValidationError, ValueError) as exc:
        return _form_error(str(exc))
    repo.create_trade(session, data, user_id=user.id)
    return _trigger_response(close_modal=True)


@router.put("/trades/{trade_id}", response_class=HTMLResponse)
async def update_trade_route(
    trade_id: int,
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    try:
        data = TradeUpdate(**await _clean_form(request))
        updated = repo.update_trade(session, trade_id, data, user_id=user.id)
    except (ValidationError, ValueError) as exc:
        return _form_error(str(exc))
    if updated is None:
        return HTMLResponse("找不到交易", status_code=404)
    return _trigger_response(close_modal=True)


@router.delete("/trades/{trade_id}", response_class=HTMLResponse)
async def delete_trade_route(
    trade_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    deleted = repo.delete_trade(session, trade_id, user_id=user.id)
    if not deleted:
        raise HTTPException(status_code=404, detail="trade not found")
    return _trigger_response(close_modal=False)
