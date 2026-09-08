"""Admin-only user management. Plain forms + 303 redirects (no HTMX — this page
is rare-use; keep it dead simple)."""
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session

from quanquant.auth import service
from quanquant.broker import repository as brepo
from quanquant.db.models import User
from quanquant.web.deps import get_session, require_admin
from quanquant.web.templating import templates

_CST = timezone(timedelta(hours=8))


def _fmt_cst(ms: int | None) -> str | None:
    """epoch-ms UTC → 台灣本地 'YYYY-MM-DD HH:MM'（固定 +08:00，台灣無 DST）。"""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=_CST).strftime("%Y-%m-%d %H:%M")

router = APIRouter(prefix="/admin", dependencies=[Depends(require_admin)])


def _user_or_404(session: Session, user_id: int) -> User:
    user = session.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="user not found")
    return user


@router.get("/users", response_class=HTMLResponse)
def users_page(request: Request, session: Session = Depends(get_session), error: str | None = None):
    return templates.TemplateResponse(
        request,
        "admin_users.html",
        {"active": "admin", "users": service.list_users(session), "error": error},
    )


@router.post("/users")
def create_user(
    request: Request,
    session: Session = Depends(get_session),
    username: str = Form(...),
    display_name: str = Form(""),
    password: str = Form(...),
    role: str = Form("user"),
):
    try:
        service.create_user(
            session, username.strip(), password,
            display_name=display_name.strip() or None, role=role,
        )
    except ValueError as exc:
        return users_page(request, session, error=str(exc))
    return RedirectResponse("/admin/users", status_code=303)


@router.post("/users/{user_id}/reset-password")
def reset_password(
    user_id: int, session: Session = Depends(get_session), password: str = Form(...)
):
    service.reset_password(session, _user_or_404(session, user_id), password)
    return RedirectResponse("/admin/users", status_code=303)


@router.post("/users/{user_id}/toggle-active")
def toggle_active(user_id: int, session: Session = Depends(get_session)):
    user = _user_or_404(session, user_id)
    service.set_active(session, user, not user.is_active)
    return RedirectResponse("/admin/users", status_code=303)


@router.post("/users/{user_id}/role")
def change_role(user_id: int, session: Session = Depends(get_session), role: str = Form(...)):
    service.set_role(session, _user_or_404(session, user_id), role)
    return RedirectResponse("/admin/users", status_code=303)


# ---- 冷靜期（self-lockout）管理（2026-08-22，D4；2026-09-02 R2-2/D-2 改唯讀）----
# 使用者自我禁制期間，僅本人可在啟動後 5 分鐘反悔窗內自行取消（見
# web/routers/orders.py::cancel_cooldown）；逾時後任何人（含 admin）都無法提前解除，
# 只能等到期——單人部署下「admin 可提前解除」等於自己的後門，防衝動交易的效果歸零。
# 這頁只列出目前 active 的冷靜期供查看，不再提供解除操作（見 lift_cooling_off）。


@router.get("/cooling-off", response_class=HTMLResponse)
def cooling_off_page(request: Request, session: Session = Depends(get_session)):
    now_ms = brepo.now_epoch_ms()
    items = []
    for cd in brepo.list_active_cooldowns(session, now_ms=now_ms):
        u = session.get(User, cd.user_id)
        items.append({
            "user_id": cd.user_id,
            "username": u.username if u is not None else f"#{cd.user_id}",
            "display_name": (u.display_name if u is not None else None),
            "until_text": _fmt_cst(cd.until_ts),
            "created_text": _fmt_cst(cd.created_ts),
        })
    return templates.TemplateResponse(
        request, "admin_cooling_off.html", {"active": "admin", "items": items},
    )


@router.post("/cooling-off/{user_id}/lift")
def lift_cooling_off(user_id: int, admin: User = Depends(require_admin)):
    """R2-2（D-2，2026-09-02）：admin 提前解除已停用——反悔窗僅本人可在啟動後 5 分鐘內
    自行取消（見 web/routers/orders.py::cancel_cooldown），逾時後任何人（含 admin）都
    無法提前解除，只能等到期。這個端點保留位置回應明確的 403，不整個移除（避免舊書籤/
    文件連結悄悄變 404，含糊帶過權限已收回的事實）；server 端強制，不是只藏掉頁面上的
    按鈕（admin_cooling_off.html 已改唯讀、不再有解除表單）。"""
    raise HTTPException(
        status_code=403,
        detail="冷靜期提前解除已停用：僅本人可在啟動後 5 分鐘內自行取消，逾時後任何人都無法提前解除",
    )
