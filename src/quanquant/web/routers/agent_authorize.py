"""`/agent/authorize`：使用者在正式站手動輸入 device-code flow 的 `user_code` 來核准/
拒絕一次 agent 連線請求（M1 收尾，spec §4.1 step2）。純文字＋表單，無 JS——每次 GET/POST
都回一整頁（`{% extends "base.html" %}`），不走 HTMX 局部 swap。

owner-only：借用下單子系統既有的 `RiskGuard`（`orders.get_order_risk_guard`）判斷「誰是
owner」，比照 `orders.py` 對 `POST /orders/agent-token` 的資格檢查寫法。這個頁面本身不
依賴下單子系統是否已連線——若 `risk_guard` 未接線（子系統停用／本機測試環境），就沒有
owner 名單可比對，見 `_require_owner` 的處理。

CSRF：核准/拒絕（以及查詢）POST 一律要求 `csrf.verify_csrf_token` 過關（double-submit
cookie，見 `web/csrf.py` 模組說明）——session cookie 是 SameSite=Lax，頂層導覽仍會帶上，
不足以防 CSRF。核准用 conditional UPDATE（`device_flow.approve_device_code`），此時不
簽發任何 token（token 簽發是 device-code 輪詢端點的事，見 Task 4）。"""
from fastapi import APIRouter, Depends, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse
from sqlmodel import Session

from quanquant.auth.device_flow import approve_device_code, deny_device_code, find_pending_by_user_code
from quanquant.broker.base import AuthorizationError
from quanquant.db.models import AgentDeviceCode, User
from quanquant.web.csrf import issue_csrf_token, verify_csrf_token
from quanquant.web.deps import get_current_user, get_session
from quanquant.web.routers.orders import get_order_risk_guard
from quanquant.web.templating import templates

router = APIRouter()

_ALREADY_PROCESSED = "已處理，請對方重新開始授權流程"


def _require_owner(risk_guard, user_id: int) -> None:
    """owner-only 檢查；risk_guard 未接線時視為無限制（沒有 owner 名單可比對），比照
    `orders.py` 對『下單子系統停用不擋』的既有精神——這個頁面唯一的硬性前提是已登入
    （由 `Depends(get_current_user)` 把關），owner 名單只有在子系統真的連線時才有意義。"""
    if risk_guard is None:
        return
    try:
        risk_guard.assert_owner(user_id)
    except AuthorizationError:
        raise HTTPException(status_code=403, detail="not owner")


def _page_response(
    request: Request, *, error: str | None = None, confirm: dict | None = None,
    result: str | None = None, status_code: int = 200,
) -> HTMLResponse:
    """統一組裝 `agent_authorize.html` 全頁回應，並在裡頭發一顆新的 CSRF cookie／隱藏欄位
    值。做法：先在一個丟棄用的 `Response()` 上呼叫 `issue_csrf_token` 拿到 token，再把它
    唯一的 Set-Cookie header 轉貼到已經渲染好內容的 `TemplateResponse` 上——`set_cookie`
    只是附加一個 header，跟 body 何時渲染好無關，這個順序不會有 content-length 對不上的
    風險（比直接改 `response.body` 事後拼字串安全，那樣需要手動重算 content-length，
    在 Caddy 反代環境下若漏改容易造成回應被截斷）。"""
    cookie_carrier = Response()
    csrf_token = issue_csrf_token(cookie_carrier, request)
    response = templates.TemplateResponse(
        request, "agent_authorize.html",
        {"active": "agent_authorize", "error": error, "confirm": confirm, "result": result,
         "csrf_token": csrf_token},
        status_code=status_code,
    )
    for name, value in cookie_carrier.raw_headers:
        if name == b"set-cookie":
            response.raw_headers.append((name, value))
    return response


@router.get("/agent/authorize", response_class=HTMLResponse)
def authorize_page(
    request: Request,
    user: User = Depends(get_current_user),
    risk_guard=Depends(get_order_risk_guard),
):
    _require_owner(risk_guard, user.id)
    return _page_response(request)


@router.post("/agent/authorize", response_class=HTMLResponse)
async def authorize_lookup(
    request: Request,
    user_code: str = Form(...),
    csrf_token: str | None = Form(None),
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    risk_guard=Depends(get_order_risk_guard),
):
    if not verify_csrf_token(request, csrf_token):
        raise HTTPException(status_code=403, detail="csrf token 無效")
    _require_owner(risk_guard, user.id)
    # 手動輸碼容錯：碼本身只用大寫英數（見 generate_user_code 的 alphabet），使用者若
    # 輸入小寫或帶前後空白，正規化後再查——這是本任務「手動輸碼」的核心情境，不正規化
    # 幾乎每次都會誤判成「找不到」。
    row = find_pending_by_user_code(session, user_code=user_code.strip().upper())
    if row is None:
        return _page_response(request, error="找不到此代碼或已過期")
    confirm = {"user_code": row.user_code, "created_at": row.created_at, "device_code_id": row.id}
    return _page_response(request, confirm=confirm)


@router.post("/agent/authorize/decide", response_class=HTMLResponse)
async def authorize_decide(
    request: Request,
    device_code_id: int = Form(...),
    decision: str = Form(...),
    csrf_token: str | None = Form(None),
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    risk_guard=Depends(get_order_risk_guard),
):
    if not verify_csrf_token(request, csrf_token):
        raise HTTPException(status_code=403, detail="csrf token 無效")
    _require_owner(risk_guard, user.id)
    if decision not in ("approve", "deny"):
        raise HTTPException(status_code=400, detail="invalid decision")

    row = session.get(AgentDeviceCode, device_code_id)
    if row is None:
        return _page_response(request, result=_ALREADY_PROCESSED)

    if decision == "approve":
        ok = approve_device_code(session, user_code=row.user_code, user_id=user.id) is not None
    else:
        ok = deny_device_code(session, user_code=row.user_code)
    if not ok:
        return _page_response(request, result=_ALREADY_PROCESSED)
    return _page_response(request, result="已核准" if decision == "approve" else "已拒絕")
