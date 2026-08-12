"""CSRF 防護（`/agent/authorize` 核准/拒絕 POST 專用，Task 6）。

double-submit cookie 實作 spec §4.1 的「synchronizer token」意圖——本站無 server-side
session 儲存（session cookie 是 itsdangerous 簽名的無狀態 payload，見 auth/tokens.py），
沒有地方存傳統 synchronizer token 該存哪。double-submit 版本安全性質等價：cookie 值必須
與表單隱藏欄位值相等——跨站偽造請求即使 cookie 自動隨請求帶上，攻擊者讀不到 HttpOnly
cookie 內容，無法在偽造表單裡填出相符的隱藏欄位值，等同傳統 synchronizer token 的防偽造
效果。session cookie 本身的 SameSite=Lax 不足恃（頂層導覽仍會帶 cookie），故另立這個
SameSite=Strict 的獨立 cookie。"""
import hmac
import secrets

from fastapi import Request, Response

CSRF_COOKIE = "qq_csrf_authorize"
_MAX_AGE_SECONDS = 600


def issue_csrf_token(response: Response, request: Request) -> str:
    """產生新 token，寫入 HttpOnly/SameSite=Strict/max_age=600s cookie，回傳同值供模板
    塞進隱藏欄位。

    偏離 brief 逐字介面（原簽名只有 `response` 一個參數）：secure 旗標比照
    `auth/routers.py:set_session_cookie` 需要 `request` 才能讀 `x-forwarded-proto`
    （站台在 Caddy 反代後面，app 端看到的是明碼 HTTP，必須信任這個 header 才知道原始
    請求是否 HTTPS）。若硬寫死 `secure=True`，測試環境（TestClient 用 http://testserver）
    的 cookie jar 不會把這顆 cookie 帶回下一次請求，`verify_csrf_token` 會恆假；寫死
    `secure=False` 則正式站會漏放 Secure 旗標。兩者都不可接受，故改為雙參數簽名。"""
    token = secrets.token_urlsafe(32)
    secure = request.headers.get("x-forwarded-proto", request.url.scheme) == "https"
    response.set_cookie(
        CSRF_COOKIE, token, max_age=_MAX_AGE_SECONDS, httponly=True,
        samesite="strict", secure=secure,
    )
    return token


def verify_csrf_token(request: Request, submitted: str | None) -> bool:
    """cookie 值與 submitted 用 `hmac.compare_digest` 比對；任一缺失回 False。"""
    cookie = request.cookies.get(CSRF_COOKIE)
    if not cookie or not submitted:
        return False
    return hmac.compare_digest(cookie, submitted)
