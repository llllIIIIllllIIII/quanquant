"""Performance stats page, JSON data, and CSV/Excel export."""
from fastapi import APIRouter, Depends, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, Response
from sqlmodel import Session

from quanquant.db.models import User
from quanquant.journal import repository as repo
from quanquant.stats.export import to_csv_bytes, to_xlsx_bytes
from quanquant.stats.metrics import compute_stats
from quanquant.web.deps import get_current_user, get_session, parse_date
from quanquant.web.templating import templates

router = APIRouter()

_XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _filtered(session: Session, user_id: int, symbol, tag, date_from, date_to):
    closed = repo.list_for_stats(
        session,
        user_id=user_id,
        symbol=symbol or None,
        tag=tag or None,
        date_from=parse_date(date_from),
        date_to=parse_date(date_to, end=True),
    )
    detail = repo.list_trades(
        session,
        user_id=user_id,
        symbol=symbol or None,
        tag=tag or None,
        date_from=parse_date(date_from),
        date_to=parse_date(date_to, end=True),
        status="all",
    )
    return detail, closed


@router.get("/stats", response_class=HTMLResponse)
async def stats_page(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    symbol: str | None = Query(None),
    tag: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
):
    _, closed = _filtered(session, user.id, symbol, tag, date_from, date_to)
    result = compute_stats(closed)
    return templates.TemplateResponse(
        request,
        "stats.html",
        {
            "active": "stats",
            "stats": result,
            "symbols": repo.list_symbols(session, user_id=user.id),
            "all_tags": repo.list_all_tags(session, user_id=user.id),
            "f": {"symbol": symbol or "", "tag": tag or "", "date_from": date_from or "",
                  "date_to": date_to or ""},
        },
    )


@router.get("/stats/data")
async def stats_data(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    symbol: str | None = Query(None),
    tag: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
):
    _, closed = _filtered(session, user.id, symbol, tag, date_from, date_to)
    return JSONResponse(jsonable_encoder(compute_stats(closed)))


@router.get("/stats/export.csv")
async def export_csv(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    symbol: str | None = Query(None),
    tag: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
):
    detail, _ = _filtered(session, user.id, symbol, tag, date_from, date_to)
    return Response(
        content=to_csv_bytes(detail),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=quanquant_trades.csv"},
    )


@router.get("/stats/export.xlsx")
async def export_xlsx(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    symbol: str | None = Query(None),
    tag: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
):
    detail, closed = _filtered(session, user.id, symbol, tag, date_from, date_to)
    return Response(
        content=to_xlsx_bytes(detail, compute_stats(closed)),
        media_type=_XLSX_MIME,
        headers={"Content-Disposition": "attachment; filename=quanquant_stats.xlsx"},
    )
