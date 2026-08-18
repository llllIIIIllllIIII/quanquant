"""agent 本機 GUI 安全邊界（Task 8，spec §6.2 全語意）：bootstrap exchange（一次性
secret →session cookie）＋除 `/bootstrap` 外所有本機 API/頁面共用的
`require_gui_session` 依賴（session cookie＋exact Host＋same-origin Origin/
Sec-Fetch-Site）＋全站安全標頭 middleware。

bootstrap exchange 語意：GUI 啟動時只在本機開一個帶 `bootstrap_secret` 的一次性 URL
（`bootstrap_url()`），使用者的瀏覽器打開它、`consume_bootstrap()` 驗證通過後立刻
`bootstrap_consumed = True`（再打第二次一律 404）並種下 `session_token`——之後所有頁面/
API 一律靠這個 session cookie＋Host/Origin 檢查放行，不再依賴 secret。`secret` 不符與
`secret` 已消費過回應同為 404（不透露是哪一種，避免時序/存在性洩漏側錄）。
"""
import hmac
import secrets

from fastapi import FastAPI, HTTPException, Request, Response

GUI_SESSION_COOKIE = "qq_agent_gui_session"


class GuiSecurityState:
    """一個 agent GUI 程序的生命週期內單例，掛在 app.state.gui_security。"""

    def __init__(self, *, port: int) -> None:
        self.port = port
        self.bootstrap_secret = secrets.token_urlsafe(32)
        self.bootstrap_consumed = False
        self.session_token: str | None = None


def bootstrap_url(state: GuiSecurityState) -> str:
    """自動開瀏覽器要打開的一次性 URL：http://127.0.0.1:<port>/bootstrap?secret=..."""
    return f"http://127.0.0.1:{state.port}/bootstrap?secret={state.bootstrap_secret}"


def consume_bootstrap(state: GuiSecurityState, response: Response, *, secret: str) -> None:
    """驗證後立即失效、種 session cookie。secret 不符或已消費過 → 一律 404（不透露是
    『不符』還是『已用過』，避免時序/存在性洩漏）。"""
    if state.bootstrap_consumed or not hmac.compare_digest(secret, state.bootstrap_secret):
        raise HTTPException(status_code=404)
    state.bootstrap_consumed = True
    state.session_token = secrets.token_urlsafe(32)
    response.set_cookie(GUI_SESSION_COOKIE, state.session_token, httponly=True,
                         samesite="strict", secure=False)  # 127.0.0.1 loopback，無 TLS


def require_gui_session(request: Request) -> None:
    """FastAPI dependency，掛在除 /bootstrap 外的所有本機 API/頁面：驗 session cookie＋
    exact Host: 127.0.0.1:<port>＋same-origin Origin/Sec-Fetch-Site。任一失敗 → 403。"""
    state: GuiSecurityState = request.app.state.gui_security
    cookie = request.cookies.get(GUI_SESSION_COOKIE)
    if state.session_token is None or cookie is None or not hmac.compare_digest(cookie, state.session_token):
        raise HTTPException(status_code=403)
    expected_host = f"127.0.0.1:{state.port}"
    if request.headers.get("host") != expected_host:
        raise HTTPException(status_code=403)
    origin = request.headers.get("origin")
    # `Origin: null`（字串）是不透明來源的合法值：本專案 Task 8 的 `Referrer-Policy:
    # no-referrer` 安全標頭會讓瀏覽器對同源 form POST 送出 `Origin: null`，若在此誤擋
    # 會讓精靈第一步 `POST /setup/step1/start` 一律 403。同源性改由下方 `Sec-Fetch-Site`
    # （跨站攻擊必得 `cross-site`）與不可猜的 session cookie 把關，故放行 "null"。
    if origin is not None and origin not in ("null", f"http://{expected_host}"):
        raise HTTPException(status_code=403)
    fetch_site = request.headers.get("sec-fetch-site")
    if fetch_site is not None and fetch_site not in ("same-origin", "none"):
        raise HTTPException(status_code=403)


def install_security_headers(app: FastAPI) -> None:
    """middleware：所有回應加 Referrer-Policy: no-referrer、Cache-Control: no-store、
    Content-Security-Policy: default-src 'self'、X-Frame-Options/frame-ancestors: 'none'。"""

    @app.middleware("http")
    async def _headers(request: Request, call_next):
        response = await call_next(request)
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Cache-Control"] = "no-store"
        response.headers["Content-Security-Policy"] = "default-src 'self'; frame-ancestors 'none'"
        response.headers["X-Frame-Options"] = "DENY"
        return response
