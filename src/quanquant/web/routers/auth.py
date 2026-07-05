"""Login/logout. The login page is standalone (not base.html) — it must render
for anonymous users."""
from fastapi import APIRouter, Depends, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session

from quanquant.auth import service
from quanquant.auth.tokens import MAX_AGE_SECONDS, SESSION_COOKIE, sign_session
from quanquant.db.models import User
from quanquant.web.deps import get_session
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
