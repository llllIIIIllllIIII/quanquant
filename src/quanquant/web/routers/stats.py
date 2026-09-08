"""交易績效頁（008：合併原「績效統計」＋已平倉逐筆）、JSON 資料、CSV/Excel 匯出、
每日復盤寫入口（010）。

`/stats/data` 刻意維持舊行為（entry_time 為界、不分來源、無「預設今日」）——它未被任何
前端頁面消費，只有 `tests/test_isolation.py` 直接打；008 的新篩選語意（trading_day 區間、
來源規則、結果篩選、分頁）只套用在頁面本身與其匯出端點，避免動到一支無關的既有 API
（見 `_filtered` vs `_period_filtered`的分工）。
"""
import math
from datetime import date as date_cls
from datetime import timedelta

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from sqlmodel import Session

from quanquant.db.models import User
from quanquant.journal import repository as repo
from quanquant.journal import review_repository as reviews
from quanquant.journal.schemas import split_tags
from quanquant.candles.market_calendar import is_trading_day
from quanquant.journal.trading_day import today_trading_day
from quanquant.stats.export import to_csv_bytes, to_xlsx_bytes
from quanquant.stats.metrics import compute_stats
from quanquant.stats.streaks import max_losing_streak_for_trades
from quanquant.web.deps import get_current_user, get_session, parse_date
from quanquant.web.templating import templates

router = APIRouter()

_XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
PAGE_SIZE = 50


def _filtered(session: Session, user_id: int, mode, symbol, tag, date_from, date_to):
    """舊行為（entry_time 範圍、不分來源）——只供 `/stats/data` 用，見檔頭註解。"""
    closed = repo.list_for_stats(
        session,
        user_id=user_id,
        mode=mode,
        symbol=symbol or None,
        tag=tag or None,
        date_from=parse_date(date_from),
        date_to=parse_date(date_to, end=True),
    )
    detail = repo.list_trades(
        session,
        user_id=user_id,
        mode=mode,
        symbol=symbol or None,
        tag=tag or None,
        date_from=parse_date(date_from),
        date_to=parse_date(date_to, end=True),
        status="all",
    )
    return detail, closed


def _resolve_range(date_from: str | None, date_to: str | None) -> tuple[date_cls, date_cls]:
    """008：把篩選列的日期字串解析成 [from, to]——都沒給時預設「今日」（trading_day）。

    終審 LOW-6（2026-09-04）：格式錯誤的日期字串（如 `?date_from=garbage`）原本會讓
    `date.fromisoformat` 拋 `ValueError` 一路炸到 500——這不是本次新引入的問題，但既然
    改到這支函式，順手接起來回 400（而不是讓使用者看到 unhandled exception）。
    """
    if not date_from and not date_to:
        today = today_trading_day()
        return today, today
    try:
        frm = date_cls.fromisoformat(date_from) if date_from else date_cls.fromisoformat(date_to)  # type: ignore[arg-type]
        to = date_cls.fromisoformat(date_to) if date_to else date_cls.fromisoformat(date_from)  # type: ignore[arg-type]
    except ValueError:
        raise HTTPException(status_code=400, detail="date_from/date_to 格式錯誤（需為 YYYY-MM-DD）") from None
    if frm > to:
        frm, to = to, frm
    return frm, to


def _quick_ranges(today: date_cls) -> dict[str, tuple[date_cls, date_cls]]:
    week_start = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)
    return {"today": (today, today), "week": (week_start, today), "month": (month_start, today)}


def _which_quick(frm: date_cls, to: date_cls, ranges: dict) -> str:
    for key, (a, b) in ranges.items():
        if (frm, to) == (a, b):
            return key
    return "custom"


def _period_filtered(
    session: Session, user_id: int, mode: str, symbol, tag, date_from: date_cls, date_to: date_cls,
    result, include_manual: bool,
) -> list:
    return repo.list_closed_for_period(
        session, user_id=user_id, mode=mode, date_from=date_from, date_to=date_to,
        symbol=symbol or None, tag=tag or None, result=result or None, include_manual=include_manual,
    )


def _daily_objective_snapshot(session: Session, *, user_id: int, mode: str, trading_day: date_cls) -> dict:
    """010 客觀數據：一律用「整個交易日、只計 shioaji 來源」的正準範圍算——不受頁面當下
    的商品／標籤／結果篩選影響（那些是瀏覽用的縮小範圍，不該混進『當天真正發生了什麼』
    這個永久快照），也不受「含手動補記」勾選的瞬時狀態影響，確保同一天無論何時預覽/
    儲存都得到一致的數字（見 010 驗收條件「快照語意」與本次交付報告的取捨說明）。

    終審 MEDIUM-2（2026-09-04）：`trade_count` 是已平倉 round-trip 筆數（trading_day
    範圍，夜盤跨日），`order_max_orders_per_day` 是**委託次數**上限、且其計數基準
    （`brepo.count_orders_today`／`trading_day_for`）是**日曆日**、不是 trading_day——
    兩個數字口徑完全不同，不可塞進同一個「N 筆（日配額 M）」括號誤導使用者。改為只顯示
    已平倉筆數；`daily_quota` 固定回 `None`（畫面顯示「—」，不隱瞞「這裡本來想顯示但語意
    對不上」的事實，也保留欄位供未來真的要接「今日委託數／配額」時使用）。"""
    closed = repo.list_closed_for_period(
        session, user_id=user_id, mode=mode, date_from=trading_day, date_to=trading_day,
    )
    ordered_asc = list(reversed(closed))
    m = compute_stats(closed).overall
    return {
        "pnl": m.total_pnl,
        "trade_count": m.count,
        # 口徑對不上（已平倉筆數＝trading_day／round-trip；委託配額＝日曆日／委託次數），
        # 刻意不再顯示成同一個數字，見上方 docstring。
        "daily_quota": None,
        "win_rate": m.win_rate,
        "max_losing_streak": max_losing_streak_for_trades(ordered_asc),
        # None＝查無資料源（KillSwitchState 純 in-memory、未落 DB，見 broker/risk.py 開頭
        # 註解，2026-09-02 查證確認）；畫面顯示「—」，不可顯示假 0。
        "kill_switch_count": None,
    }


@router.get("/stats", response_class=HTMLResponse)
async def stats_page(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    mode: str = Query("real"),
    symbol: str | None = Query(None),
    tag: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    result: str | None = Query(None),
    include_manual: bool = Query(False),
    page: int = Query(1, ge=1),
):
    frm, to = _resolve_range(date_from, date_to)
    closed = _period_filtered(session, user.id, mode, symbol, tag, frm, to, result, include_manual)
    stats_result = compute_stats(closed)

    total = len(closed)
    total_pages = max(1, math.ceil(total / PAGE_SIZE))
    page = min(max(page, 1), total_pages)
    page_slice = closed[(page - 1) * PAGE_SIZE: page * PAGE_SIZE]
    closed_rows = [{"t": t, "tags": split_tags(t.tags)} for t in page_slice]

    today = today_trading_day()
    ranges = _quick_ranges(today)
    quick = _which_quick(frm, to, ranges)
    is_single_day = frm == to

    # 終審 MEDIUM-R1：復盤區塊只在「單日且為交易日」時提供——自訂區間可以選到過去的
    # 週末/假日（frm==to 成立），但那不是交易日，寫入端也會拒絕（見 save_review_route），
    # 畫面就不該給入口，否則產生孤兒 DailyReview。
    show_review = is_single_day and is_trading_day(frm)
    review = None
    review_snapshot = None
    if show_review:
        review = reviews.get_review(session, user_id=user.id, mode=mode, trading_day=frm.isoformat())
        if review is not None:
            review_snapshot = {
                "pnl": review.snapshot_pnl, "trade_count": review.snapshot_trade_count,
                "daily_quota": review.snapshot_daily_quota, "win_rate": review.snapshot_win_rate,
                "max_losing_streak": review.snapshot_max_losing_streak,
                "kill_switch_count": review.snapshot_kill_switch_count,
            }
        else:
            review_snapshot = _daily_objective_snapshot(session, user_id=user.id, mode=mode, trading_day=frm)

    return templates.TemplateResponse(
        request,
        "stats.html",
        {
            "active": "stats",
            "stats": stats_result,
            "closed_rows": closed_rows,
            "symbols": repo.list_symbols(session, user_id=user.id),
            "all_tags": repo.list_all_tags(session, user_id=user.id),
            "f": {
                "symbol": symbol or "", "tag": tag or "", "date_from": frm.isoformat(),
                "date_to": to.isoformat(), "mode": mode, "result": result or "",
                "include_manual": include_manual,
            },
            "quick": quick,
            "ranges": {k: (a.isoformat(), b.isoformat()) for k, (a, b) in ranges.items()},
            "expanded": not is_single_day,
            "is_single_day": is_single_day,
            "show_review": show_review,
            "page": page,
            "total_pages": total_pages,
            "total": total,
            "page_size": PAGE_SIZE,
            "review": review,
            "review_snapshot": review_snapshot,
            "review_trading_day": frm.isoformat(),
        },
    )


@router.get("/stats/data")
async def stats_data(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    mode: str = Query("real"),
    symbol: str | None = Query(None),
    tag: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
):
    _, closed = _filtered(session, user.id, mode, symbol, tag, date_from, date_to)
    return JSONResponse(jsonable_encoder(compute_stats(closed)))


@router.get("/stats/export.csv")
async def export_csv(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    mode: str = Query("real"),
    symbol: str | None = Query(None),
    tag: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    result: str | None = Query(None),
    include_manual: bool = Query(False),
):
    frm, to = _resolve_range(date_from, date_to)
    closed = _period_filtered(session, user.id, mode, symbol, tag, frm, to, result, include_manual)
    return Response(
        content=to_csv_bytes(closed),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=quanquant_trades.csv"},
    )


@router.get("/stats/export.xlsx")
async def export_xlsx(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    mode: str = Query("real"),
    symbol: str | None = Query(None),
    tag: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    result: str | None = Query(None),
    include_manual: bool = Query(False),
):
    frm, to = _resolve_range(date_from, date_to)
    closed = _period_filtered(session, user.id, mode, symbol, tag, frm, to, result, include_manual)
    return Response(
        content=to_xlsx_bytes(closed, compute_stats(closed)),
        media_type=_XLSX_MIME,
        headers={"Content-Disposition": "attachment; filename=quanquant_stats.xlsx"},
    )


_MAX_NOTE_LEN = 2000


@router.post("/stats/review")
def save_review_route(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    mode: str = Form("real"),
    trading_day: str = Form(...),
    discipline_note: str = Form(""),
    emotion_note: str = Form(""),
    tomorrow_focus: str = Form(""),
    symbol: str = Form(""),
    tag: str = Form(""),
    result: str = Form(""),
    include_manual: str = Form(""),
):
    """010 寫入口：只在「今日復盤」區塊出現時才會被送出（頁面只在 date_from==date_to
    時渲染這個表單），這裡的 `trading_day` 就是那個單一交易日；快照只在首次儲存時凍結
    （見 journal/review_repository.py::save_review）。

    終審 LOW-4（2026-09-04）：改成同步 `def`——本路由整支只做同步 ORM 寫入（沒有任何
    `await`），原本掛 `async def` 卻做阻塞 DB 呼叫會壓住事件迴圈（同專案「同步 DB 壓 loop
    凍住 SSE/K 線」那個已知陷阱的同型態問題），FastAPI 對 sync def 路由自動丟進
    threadpool 執行，不佔用 event loop。"""
    if mode not in ("real", "sim"):
        raise HTTPException(status_code=400, detail="mode 只能是 real 或 sim")
    try:
        day = date_cls.fromisoformat(trading_day)
    except ValueError:
        raise HTTPException(status_code=400, detail="trading_day 格式錯誤") from None

    # 終審 MEDIUM-3：①三個主觀欄位長度上限、②不得替未來日期寫復盤（trading_day 只能是
    # 今天或更早——「今天」本身以 mode 無關的 today_trading_day() 判定，交易日曆本來就不
    # 分 sim/real）。
    for field_name, value in (
        ("discipline_note", discipline_note), ("emotion_note", emotion_note), ("tomorrow_focus", tomorrow_focus),
    ):
        if len(value) > _MAX_NOTE_LEN:
            raise HTTPException(status_code=400, detail=f"{field_name} 超過 {_MAX_NOTE_LEN} 字上限")
    if day > today_trading_day():
        raise HTTPException(status_code=400, detail="trading_day 不能是未來日期")
    # 終審 MEDIUM-R1：過去的非交易日（週末/假日）也拒絕——與 trading_day_of 的收斂語意
    # 對稱，否則寫進去的是永遠不會被任何「今日」視圖叫出的孤兒列。
    if not is_trading_day(day):
        raise HTTPException(status_code=400, detail="trading_day 必須是交易日")

    snapshot = _daily_objective_snapshot(session, user_id=user.id, mode=mode, trading_day=day)
    reviews.save_review(
        session,
        user_id=user.id,
        mode=mode,
        trading_day=trading_day,
        discipline_note=discipline_note.strip() or None,
        emotion_note=emotion_note.strip() or None,
        tomorrow_focus=tomorrow_focus.strip() or None,
        snapshot=snapshot,
    )

    qs = f"mode={mode}&date_from={trading_day}&date_to={trading_day}"
    if symbol:
        qs += f"&symbol={symbol}"
    if tag:
        qs += f"&tag={tag}"
    if result:
        qs += f"&result={result}"
    if include_manual:
        qs += "&include_manual=1"
    return RedirectResponse(f"/stats?{qs}", status_code=303)
