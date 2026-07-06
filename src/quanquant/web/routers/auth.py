"""Login/logout. The login page is standalone (not base.html) — it must render
for anonymous users."""
from fastapi import APIRouter, Depends, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session

from quanquant.auth import service
from quanquant.auth.tokens import MAX_AGE_SECONDS, SESSION_COOKIE, sign_session
from quanquant.db.models import User
from quanquant.web.deps import get_current_user, get_session
from quanquant.web.templating import templates

router = APIRouter()


def set_session_cookie(response: Response, request: Request, user: User) -> None:
    # Behind Caddy the app sees plain HTTP; trust X-Forwarded-Proto for `secure`.
    secure = request.headers.get("x-forwarded-proto", request.url.scheme) == "https"
    response.set_cookie(
        SESSION_COOKIE,
        sign_session(user.id or 0, user.token_version),
        max_age=MAX_AGE_SECONDS,
        httponly=True,
        samesite="lax",
        secure=secure,
    )


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(request, "login.html", {"error": None})


@router.post("/login")
def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    session: Session = Depends(get_session),
):
    user = service.authenticate(session, username.strip(), password)
    if user is None:
        return templates.TemplateResponse(
            request, "login.html", {"error": "帳號或密碼錯誤（連續失敗會暫時鎖定）"}
        )
    response = RedirectResponse("/", status_code=303)
    set_session_cookie(response, request, user)
    return response


@router.post("/logout")
def logout():
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(SESSION_COOKIE)
    return response


@router.get("/account", response_class=HTMLResponse)
def account_page(request: Request, user: User = Depends(get_current_user)):
    return templates.TemplateResponse(
        request, "account.html",
        {"active": "account", "error": None,
         "color_scheme": user.chart_color_scheme or "green_up"},
    )


@router.post("/account/color-scheme")
def change_color_scheme(
    request: Request,
    scheme: str = Form(...),
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    if not service.set_color_scheme(session, user, scheme):
        return templates.TemplateResponse(
            request, "account.html",
            {"active": "account", "error": "配色設定無效",
             "color_scheme": user.chart_color_scheme or "green_up"},
        )
    return RedirectResponse("/account", status_code=303)


@router.post("/account/password")
def change_password(
    request: Request,
    old_password: str = Form(...),
    new_password: str = Form(...),
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    if not service.change_password(session, user, old_password, new_password):
        return templates.TemplateResponse(
            request, "account.html", {"active": "account", "error": "舊密碼錯誤"}
        )
    # token_version was bumped — re-issue THIS device's cookie; other devices log out
    response = RedirectResponse("/", status_code=303)
    set_session_cookie(response, request, user)
    return response
