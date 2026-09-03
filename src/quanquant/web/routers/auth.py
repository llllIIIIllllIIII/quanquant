"""Login/logout. The login page is standalone (not base.html) — it must render
for anonymous users."""
from fastapi import APIRouter, Depends, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session

from quanquant.auth import agent_tokens as agent_token_service
from quanquant.auth import service
from quanquant.auth.tokens import MAX_AGE_SECONDS, SESSION_COOKIE, sign_session
from quanquant.db.models import User
from quanquant.web.deps import get_current_user, get_session
from quanquant.web.routers.orders import get_order_risk_guard
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


def _account_context(session: Session, user: User, *, error: str | None, risk_guard) -> dict:
    """R2-4：Agent Token 產生/重置整塊 UI 從下單頁搬到帳戶設定頁——行為與授權不變
    （仍是 owner-only，見 orders.py::issue_agent_token 的 `risk_guard.assert_owner`）；
    這裡只是把畫面渲染需要的 `is_owner`/`token_row`/`raw_token` 湊齊，供三個 account
    路由（GET /account 與兩個表單的錯誤重繪路徑）共用，避免各自漏塞欄位讓卡片在錯誤
    重繪時憑空消失。`raw_token` 一律 None——明文只在 `POST /orders/agent-token` 簽發
    當下的回應顯示一次，這裡（一般頁面渲染）永遠不帶明文。"""
    is_owner = risk_guard is not None and risk_guard.is_owner(user.id)
    token_row = agent_token_service.get_active_token(session, user_id=user.id) if is_owner else None
    return {
        "active": "account", "error": error, "color_scheme": user.chart_color_scheme or "green_up",
        "is_owner": is_owner, "token_row": token_row, "raw_token": None,
        "skip_sim_confirm": bool(user.skip_sim_confirm),  # 007：帳戶頁可改回
    }


@router.get("/account", response_class=HTMLResponse)
def account_page(
    request: Request, session: Session = Depends(get_session), user: User = Depends(get_current_user),
    risk_guard=Depends(get_order_risk_guard),
):
    return templates.TemplateResponse(
        request, "account.html", _account_context(session, user, error=None, risk_guard=risk_guard),
    )


@router.post("/account/color-scheme")
def change_color_scheme(
    request: Request,
    scheme: str = Form(...),
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    risk_guard=Depends(get_order_risk_guard),
):
    if not service.set_color_scheme(session, user, scheme):
        return templates.TemplateResponse(
            request, "account.html",
            _account_context(session, user, error="配色設定無效", risk_guard=risk_guard),
        )
    return RedirectResponse("/account", status_code=303)


@router.post("/account/sim-confirm")
def change_sim_confirm(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    skip_sim_confirm: str | None = Form(None),
):
    """007：帳戶頁可改回 sim 下單確認視窗偏好——checkbox 未勾選時瀏覽器根本不會送出這個
    欄位（`Form(None)`），送出即代表勾選，故「有沒有這個欄位」本身就是 skip 與否的答案，
    不需要另外檢查值內容。"""
    service.set_skip_sim_confirm(session, user, skip_sim_confirm is not None)
    return RedirectResponse("/account", status_code=303)


@router.post("/account/password")
def change_password(
    request: Request,
    old_password: str = Form(...),
    new_password: str = Form(...),
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    risk_guard=Depends(get_order_risk_guard),
):
    if not service.change_password(session, user, old_password, new_password):
        return templates.TemplateResponse(
            request, "account.html",
            _account_context(session, user, error="舊密碼錯誤", risk_guard=risk_guard),
        )
    # token_version was bumped — re-issue THIS device's cookie; other devices log out
    response = RedirectResponse("/", status_code=303)
    set_session_cookie(response, request, user)
    return response
