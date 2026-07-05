"""Admin-only user management. Plain forms + 303 redirects (no HTMX — this page
is rare-use; keep it dead simple)."""
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session

from quanquant.auth import service
from quanquant.db.models import User
from quanquant.web.deps import get_session, require_admin
from quanquant.web.templating import templates

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
