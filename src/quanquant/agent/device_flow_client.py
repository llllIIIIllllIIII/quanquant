"""Device-code flow（spec §4）agent 端 client（Task 9）：本機 GUI 精靈用來跟正式站
server 談判 device code 授權的 HTTP client。

安全鐵律（逐字落實，勿回退）：
- **verification_path 只做 exact-match**：`initiate()` 拿到 server 回應後，只用
  `==` 跟內建常數 `AGENT_AUTHORIZE_PATH` 比對，不符即 raise——絕不對回傳值呼叫任何
  `urljoin`/`urlsplit`/`urlparse` 做 URL resolve。理由：若對一個像
  `//evil.example/agent/authorize` 這種 network-path reference 值做 resolve，
  瀏覽器／某些 URL 函式庫可能把它解讀成同 scheme 換 host 的絕對網址，等於讓伺服器端
  （或中間人）指定使用者最終要被導去哪個網域完成授權——exact-match 完全不給這個
  攻擊面可乘之機。
- **`approval_url` 只用內建常數＋呼叫端提供的 `site_origin` 純字串拼接**：
  `f"{site_origin}{AGENT_AUTHORIZE_PATH}"`，絕不使用 server 回傳的 `verification_path`
  值本身去組這個網址（即使它剛通過 exact-match）。
- **單一 in-flight 輪詢**：`DeviceFlowClient` 是單一 device flow 嘗試的擁有者，靠一個
  循序 `while True: sleep → 一次 POST` 的 `asyncio` 迴圈達成——不使用任何可能被呼叫端
  重疊呼叫的 API（例如同時 await 兩個 `poll_until_done()`），天然不會有兩個輪詢請求
  同時飛在外面。
"""
import asyncio
import hashlib
import secrets

import httpx

AGENT_AUTHORIZE_PATH = "/agent/authorize"  # 內建常數；server 回傳值僅供 exact-match 核對，
                                            # 絕不採信其值本身去組任何網址。


class VerificationPathMismatchError(RuntimeError):
    """`initiate()` 收到的 `verification_path` 與內建常數 `AGENT_AUTHORIZE_PATH` 不完全
    相符——可能是被竄改、指向非預期網域的 network-path reference，或協定版本不相容。
    拒絕繼續（不組出任何 approval_url），呼叫端應顯示錯誤並中止本輪授權。"""


class DeviceFlowExpiredError(RuntimeError):
    """device code 已過期（使用者逾時未完成核准，見 `AGENT_DEVICE_CODE_TTL_SECONDS`）。"""


class DeviceFlowDeniedError(RuntimeError):
    """使用者在核准頁按了「拒絕」。"""


class DeviceFlowProtocolError(RuntimeError):
    """收到未知/不應出現的 `state` 值——防禦性分支，理論上不會發生（伺服器端 state
    集合封閉，見 `auth/device_flow.py::poll_device_token`）。"""


class DeviceFlowClient:
    """單一 device flow 嘗試的擁有者。同一實例任一時刻只允許一個 in-flight 輪詢——
    設計上用一個循序 asyncio loop（await sleep(interval) → await 一次 POST，逾時才重送）
    達成，不使用可能重疊呼叫的 API，天然滿足『只允許一個 in-flight』。

    公開屬性（`user_code`/`approval_url`/`status`）供 GUI 精靈（本 task 的
    `setup_routes.py`）與 Task 10 的 `/status` 狀態頁讀取顯示進度，不需要另外包一層
    DTO——`poll_until_done()` 執行期間，呼叫端可以隨時讀取這些屬性得到「目前這一輪」的
    最新狀態（`status` 在每次收到伺服器回應後更新，即使該次呼叫尚未觸發任何狀態轉移，
    如 pending 期間也會即時反映）。
    """

    def __init__(self, *, site_origin: str, http_client: httpx.AsyncClient) -> None:
        self._site_origin = site_origin
        self._http = http_client
        self._code_verifier: str | None = None
        self._device_code: str | None = None
        self._interval: int = 5
        self._expires_at: int | None = None  # 目前存 server 回傳的 expires_in（秒數，非
                                               # 換算後的絕對時間戳——本 task 未用到到期
                                               # 判斷，欄位名沿用 brief 命名，留給 Task 10
                                               # 若要顯示倒數計時時再自行換算）。

        self.user_code: str | None = None
        self.approval_url: str | None = None
        self.status: str = "not_started"  # Task 10 消費：not_started/pending/slow_down/
                                           # approved/expired/denied/consumed

    async def initiate(self) -> dict:
        """產生 code_verifier（記憶體，`token_urlsafe(32)`）＋`code_challenge=sha256 hex`，
        `POST /api/agent/device-code`。驗證回應的 `verification_path ==
        AGENT_AUTHORIZE_PATH`（exact-match，不符 → raise `VerificationPathMismatchError`，
        絕不對回傳值做任何 URL resolve）。回傳 server 回應 dict（含
        device_code/user_code/interval/expires_in）。`approval_url` 由 `site_origin` ＋
        內建常數自行拼接：`f'{site_origin}{AGENT_AUTHORIZE_PATH}'`。"""
        self._code_verifier = secrets.token_urlsafe(32)
        challenge = hashlib.sha256(self._code_verifier.encode()).hexdigest()
        resp = await self._http.post("/api/agent/device-code", json={"code_challenge": challenge})
        resp.raise_for_status()
        body = resp.json()
        if body.get("verification_path") != AGENT_AUTHORIZE_PATH:
            raise VerificationPathMismatchError(
                f"verification_path 不符：收到 {body.get('verification_path')!r}，"
                f"預期 {AGENT_AUTHORIZE_PATH!r}——拒絕繼續，不組出 approval_url。"
            )

        self._device_code = body["device_code"]
        self.user_code = body["user_code"]
        self._interval = body["interval"]
        self._expires_at = body["expires_in"]
        self.approval_url = f"{self._site_origin}{AGENT_AUTHORIZE_PATH}"
        self.status = "pending"
        return body

    async def poll_until_done(self) -> dict:
        """迴圈：`sleep(當下 interval)` → `POST` 一次（client timeout=`interval+5s`）→
        依 state 分派：pending/slow_down → 更新 interval 繼續迴圈；approved → 回傳；
        expired/denied → raise 對應例外；consumed → 呼叫 `initiate()` 自動重開新一輪
        （沿用同一 `DeviceFlowClient` 實例，重設 device_code/user_code/verifier），繼續
        迴圈；invalid → raise `DeviceFlowProtocolError`（不應發生，防禦性）。"""
        while True:
            await asyncio.sleep(self._interval)
            resp = await self._http.post(
                "/api/agent/device-token",
                json={"device_code": self._device_code, "code_verifier": self._code_verifier},
                timeout=self._interval + 5,
            )
            body = {"state": "invalid"} if resp.status_code == 404 else resp.json()
            state = body.get("state")
            self.status = state
            if state in ("pending", "slow_down"):
                self._interval = body.get("interval", self._interval)
                continue
            if state == "approved":
                return body
            if state == "expired":
                raise DeviceFlowExpiredError()
            if state == "denied":
                raise DeviceFlowDeniedError()
            if state == "consumed":
                await self.initiate()
                continue
            raise DeviceFlowProtocolError(f"未知 state: {state!r}")
