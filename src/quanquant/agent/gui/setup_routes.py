"""agent GUI `/setup` 三步精靈（Task 9，spec §5.1）：①裝置授權（device flow）②永豐
API 憑證 ③確認啟動。掛在 `coordinator.build_app()`（`app.include_router(router)`），
除本模組外不單獨對外暴露。

**輪詢機制刻意不用 HTMX**：spec 原文設想用 `hx-trigger="load, every 2s"` 打局部更新，
但 Task 8 的 `install_security_headers` 已經把 `Content-Security-Policy` 釘死成
`default-src 'self'`（本機安全邊界的既定鐵律，見 `gui/security.py`）——htmx.org 需要
從 CDN 載入（`web/templates/base.html` 走 `https://unpkg.com/htmx.org`），在這個 CSP 下
會被瀏覽器擋掉，若沒有把整個函式庫改成本機供應（本 task 檔案清單未列，且會讓「純三個
模板檔」的範圍膨脹成額外資產管線）就硬用 hx-trigger，等於精靈在真瀏覽器裡會安靜卡死在
「等待中」，比不做還糟。

**步驟①輪詢：同源背景 JS，不整頁刷新**（UX 改善，取代初版的
`<meta http-equiv="refresh" content="2;url=/setup/poll-status">`——後者每 2 秒整頁重新
載入，會把游標焦點/捲動位置都重置，體驗很干擾）。同樣受 CSP `default-src 'self'` 約束
（inline `<script>` 會被擋），但同源外部 JS 檔與同源 `fetch()` 皆允許，故改為：
`setup_step1.html` 引入同源 `<script src="/setup/step1-poll.js">`（純文字常數
`_STEP1_POLL_JS`，由 `GET /setup/step1-poll.js` 供應，見下方）；該腳本每 ~2 秒
`fetch('/setup/poll-status.json')` 一次，依回應更新頁面上 `#qq-poll-status` 的文字，
只在核准完成（`state="ready"`）時才做一次 `window.location` 導頁。舊的
`GET /setup/poll-status`（回整頁 HTML／303）保留不刪，只是不再被頁面使用——見其
docstring。

**單一 in-flight 輪詢**（承接 `DeviceFlowClient` 自己的保證）：本模組另外用
`app.state.device_flow_task is not None` 判斷「這一輪精靈的 device flow 是否已經在跑」
——`_ensure_device_flow_started()` 是唯一的啟動點，冪等（已存在就直接返回），確保
`POST /setup/step1/start` 無論被呼叫幾次，同一輪精靈期間全域只會有一個背景 task 在跑
`initiate()`+`poll_until_done()`。

**mutation 一律走 POST**（reviewer Important fix）：`GET /setup`／`GET /setup/poll-status`
純渲染目前 `app.state`，絕不觸發 `_ensure_device_flow_started()`（會對外發 HTTP POST
到 `/api/agent/device-code`，是貨真價實的 mutation）——首次造訪 `/setup` 只顯示步驟①
的「開始授權」按鈕（`setup_step1.html` 的 `started` 為 false 分支），使用者按下才真的
`POST /setup/step1/start` 啟動 device flow。這是本 task 第一版唯一被 reviewer 打回的
Important 缺口，已修正。

**秘密處理**（spec §5.1，鐵律）：
- `POST /setup/step2` 用 `await request.form()` 手動解析（不用 Pydantic model／
  `Form(...)`）——FastAPI 對 Pydantic 驗證失敗的預設 422 handler 會把不合法的 `input`
  值原樣塞回 JSON body（`RequestValidationError` 的 `ctx`/`input`），若用
  `secret_key: str = Form(...)` 這種宣告方式，任何驗證失敗都會把使用者剛打的秘密憑證
  回顯在錯誤回應裡；手動解析＋自組固定文案的 `HTTPException`/`TemplateResponse` 完全
  避開這個內建行為。
- 任何 `log.*(...)` 呼叫只記結構性資訊（步驟名稱、成功/失敗、例外類別名、
  `username`/`profile_id`——這些是 server 端已公開回傳的帳號識別，非秘密），秘密欄位
  本身（`api_key`/`secret_key`/`token`）與可能夾帶輸入值的原始例外訊息一律不落 log，
  見 `tests/test_agent_setup_wizard.py::test_wizard_flow_logs_do_not_leak_secrets`。
- 永豐憑證與 device flow 拿到的 token 全程只存 `app.state`（記憶體），本 task 不落地。
  「記住裝置授權」「記住永豐 API 憑證」兩個 opt-in checkbox 目前只記錄旗標——實際寫入
  keyring 是 Task 11 落地後才接線（見 `POST /setup/step3/launch` 內的 TODO 註解）。

**Task 12（profile registry）／Task 11（keyring）尚未完成**：`POST /setup/step3/launch`
理論上該呼叫 `profile_registry.upsert_profile()` 寫入 registry、依 opt-in 呼叫
`keyring_store` 存憑證——但這兩個模組是本 task 之後才進 SDD 序列（Task 11/12），還不
存在。這裡先用既有（Inc0 已完成、非本次新增）的 `AgentRunner`/`ChildHandle`/
`WebsocketsTransport`/`DurableBuffer` 把 agent 實際接上跑，buffer 路徑沿用既有 CLI
（`agent/main.py`）的預設路徑；registry/keyring 寫入的呼叫點留白為 TODO 註解，供
Task 11/12 落地後直接補上，不預先發明它們的介面。
"""
import asyncio
import logging
from pathlib import Path

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from quanquant.agent import keyring_store, profile_registry
from quanquant.agent.device_flow_client import (
    DeviceFlowClient,
    DeviceFlowDeniedError,
    DeviceFlowExpiredError,
    DeviceFlowGaveUpError,
    DeviceFlowProtocolError,
    VerificationPathMismatchError,
)
from quanquant.agent.gui.security import require_gui_session
from quanquant.agent.gui.startup_flow import (
    GuiStartupDecision,
    check_legacy_buffer_conflict,
    reconcile_profile_after_approval,
    resolve_gui_startup,
)

log = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_gui_session)])

_TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

_DEFAULT_SYMBOL = "TXF"  # Inc0 既有唯一支援商品，見 `agent/main.py` 預設值。

# 步驟①錯誤文案對照表：key 是背景 task 存進 app.state.device_flow_error 的例外類別名
# （見 _ensure_device_flow_started）。DeviceFlowGaveUpError 用 reviewer 指定的文案；其餘
# 沿用泛用但仍具體的說明，不逐字回顯例外訊息（避免任何潛在的輸入值/內部細節外洩）。
_ERROR_MESSAGES = {
    "VerificationPathMismatchError": "授權驗證失敗（伺服器回應的驗證路徑不符），請重新開始授權。",
    "DeviceFlowExpiredError": "授權逾時未完成，請重新開始授權。",
    "DeviceFlowDeniedError": "已在核准頁拒絕本次授權，請重新開始授權。",
    "DeviceFlowProtocolError": "與伺服器溝通發生未預期錯誤，請重新開始授權。",
    "DeviceFlowGaveUpError": "授權多次交付失敗，請按「開始授權」重試或檢查伺服器狀態。",
    "UnexpectedError": "發生未預期錯誤，請重新開始授權。",
}


# ---------------------------------------------------------------------------
# Task 13：核准後的 profile 收斂點＋首次 launch 的舊 buffer 防呆
# ---------------------------------------------------------------------------

def _finalize_approved_profile(
    app_state, *, site_origin: str, expected_profile: "profile_registry.ProfileEntry | None",
    approved: dict,
) -> "profile_registry.ProfileEntry":
    """device flow 核准完成（approved dict 含 profile_id/username/token/token_expires_at）
    後的唯一收斂點：一律呼叫 reconcile_profile_after_approval 決定要沿用哪個 profile
    （expected_profile 是精靈啟動當下的預期——0 筆分支/miss 分支為 None，1 筆分支/
    --profile 命中為 resolve_gui_startup 回傳的 decision.profile）。回傳值存進
    app_state.gui_current_profile，後續 step2/step3 一律讀這個欄位取得 buffer_path，
    不得再各自讀 approved["profile_id"] 另外組路徑。"""
    entry, _is_new = reconcile_profile_after_approval(
        site_origin=site_origin, expected_profile=expected_profile,
        approved_profile_id=approved["profile_id"], username=approved["username"],
    )
    app_state.gui_current_profile = entry
    return entry


def _guard_legacy_buffer_before_first_launch(*, site_origin: str, profile_id: str) -> str | None:
    """回傳非 None＝擋下 launch（HTTP 409，顯示這段文案，不建立 profile、不啟動 runner）；
    None＝放行。只在『這個 (site_origin, profile_id) 在 registry 裡還不存在』時才檢查——
    舊 buffer 衝突只對『這台機器第一次從 headless 轉 GUI』有意義。"""
    if profile_registry.find_profile(site_origin=site_origin, profile_id=profile_id) is not None:
        return None
    return check_legacy_buffer_conflict()


# ---------------------------------------------------------------------------
# device flow 背景 task
# ---------------------------------------------------------------------------

def _device_flow_http_client(app) -> httpx.AsyncClient:
    """Lazy-init、快取在 `app.state.agent_http_client`——測試可預先塞入一個
    `MockTransport` backed 的 client 取代真網路（見 `tests/conftest.py` 的
    `gui_client` fixture）。"""
    client = getattr(app.state, "agent_http_client", None)
    if client is None:
        client = httpx.AsyncClient(base_url=app.state.site_origin)
        app.state.agent_http_client = client
    return client


def _ensure_device_flow_started(app) -> None:
    """本輪精靈的唯一啟動點，冪等：`app.state.device_flow_task` 已存在就直接返回。
    背景 task 本身吞下所有可預期的終態例外（expired/denied/verification 不符/未知
    state），改記在 `app.state.device_flow_error`（例外類別名，非原始訊息）供
    `GET /setup`／`GET /setup/poll-status` 顯示；非預期例外同樣被攔下歸類為
    `UnexpectedError`，不讓背景 task 的例外無人接住變成 unhandled task exception。"""
    if getattr(app.state, "device_flow_task", None) is not None:
        return

    http_client = _device_flow_http_client(app)
    client = DeviceFlowClient(site_origin=app.state.site_origin, http_client=http_client)
    app.state.device_flow_client = client
    app.state.device_flow_result = None
    app.state.device_flow_error = None

    async def _run() -> None:
        try:
            await client.initiate()
            result = await client.poll_until_done()
            # Task 13 Step 3b：核准回應到手的當下、寫入 app.state.device_flow_result 之前，
            # 唯一收斂點——不得讓 step2/step3 handler 各自 upsert_profile／另組 buffer 路徑。
            decision = getattr(app.state, "gui_startup_decision", None)
            expected_profile = decision.profile if decision is not None else None
            entry = _finalize_approved_profile(
                app.state, site_origin=app.state.site_origin,
                expected_profile=expected_profile, approved=result,
            )
            profile_hint = getattr(app.state, "profile", None)
            if profile_hint is not None and profile_hint != entry.profile_id:
                # spec §5.3：--profile 捷徑命中的帳號與核准頁實際登入的帳號不同——常見於
                # 使用者換了帳號登入核准頁。不擋，正常完成精靈，只在步驟③額外提醒使用者
                # 桌面捷徑已經跟不上了。
                app.state.gui_shortcut_mismatch_notice = (
                    "捷徑指向的帳號已變更，請重新產生捷徑或修改 --profile"
                )
            app.state.device_flow_result = result
            log.info("setup: device flow 已核准（帳號：%s）", result.get("username"))
        except (VerificationPathMismatchError, DeviceFlowExpiredError,
                DeviceFlowDeniedError, DeviceFlowProtocolError, DeviceFlowGaveUpError) as exc:
            app.state.device_flow_error = type(exc).__name__
            log.warning("setup: device flow 未完成（%s）", type(exc).__name__)
        except Exception:
            app.state.device_flow_error = "UnexpectedError"
            log.exception("setup: device flow 背景 task 發生未預期例外")

    app.state.device_flow_task = asyncio.create_task(_run())


# ---------------------------------------------------------------------------
# 各步驟渲染
# ---------------------------------------------------------------------------

def _render_step1(request: Request, *, status_code: int = 200) -> HTMLResponse:
    client: DeviceFlowClient | None = getattr(request.app.state, "device_flow_client", None)
    error_code = getattr(request.app.state, "device_flow_error", None)
    decision: GuiStartupDecision | None = getattr(request.app.state, "gui_startup_decision", None)
    context = {
        "started": client is not None,
        "user_code": client.user_code if client is not None else None,
        "approval_url": client.approval_url if client is not None else None,
        "error": _ERROR_MESSAGES.get(error_code, error_code) if error_code else None,
        "notice": decision.notice if decision is not None else None,
    }
    return templates.TemplateResponse(request, "setup_step1.html", context, status_code=status_code)


def _render_step2(request: Request, *, error: str | None = None, status_code: int = 200) -> HTMLResponse:
    return templates.TemplateResponse(
        request, "setup_step2.html", {"error": error}, status_code=status_code,
    )


def _render_step3(request: Request, *, launch_error: str | None = None, status_code: int = 200) -> HTMLResponse:
    approved = getattr(request.app.state, "device_flow_result", None) or {}
    context = {
        "site_origin": getattr(request.app.state, "site_origin", ""),
        "mode": "sim",
        "symbol": _DEFAULT_SYMBOL,
        "username": approved.get("username", ""),
        "launch_error": launch_error,
        "shortcut_notice": getattr(request.app.state, "gui_shortcut_mismatch_notice", None),
    }
    return templates.TemplateResponse(request, "setup_step3.html", context, status_code=status_code)


# ---------------------------------------------------------------------------
# 路由
# ---------------------------------------------------------------------------

@router.get("/setup", response_class=HTMLResponse)
async def setup_page(request: Request) -> HTMLResponse:
    """依 `app.state` 目前進度顯示對應步驟：尚未拿到核准結果 → 步驟①（純渲染，不觸發
    任何 mutation——見 module docstring「mutation 一律走 POST」；尚未啟動 device flow
    時 `setup_step1.html` 會顯示「開始授權」按鈕，由使用者按下才真的
    `POST /setup/step1/start`）；已核准但永豐憑證未收 → 步驟②；兩者皆備 → 步驟③。

    Task 13：`resolve_gui_startup()` 決定 `start_step=2`（既有 profile 的 token 仍有效，
    只缺永豐憑證）時，`app.state.gui_startup_decision.profile` 帶著這個既有 profile——
    這裡直接用 keyring 裡現成的 token 建出等價的 `device_flow_result`，不必重跑一次裝置
    授權，讓上面既有的『依進度顯示步驟』邏輯自然落在步驟②。"""
    app_state = request.app.state
    decision: GuiStartupDecision | None = getattr(app_state, "gui_startup_decision", None)
    if (decision is not None and decision.kind == "setup" and decision.start_step >= 2
            and decision.profile is not None
            and getattr(app_state, "device_flow_result", None) is None):
        token = keyring_store.load_token(
            site_origin=app_state.site_origin, profile_id=decision.profile.profile_id,
        )
        if token is not None:
            app_state.device_flow_result = {
                "token": token["token"], "profile_id": decision.profile.profile_id,
                "username": token["username"], "token_expires_at": token["expires_at"],
            }
            app_state.gui_current_profile = decision.profile
    if getattr(app_state, "device_flow_result", None) is None:
        return _render_step1(request)
    if getattr(app_state, "broker_credentials", None) is None:
        return _render_step2(request)
    return _render_step3(request)


@router.get("/profiles", response_class=HTMLResponse)
async def profiles_page(request: Request) -> HTMLResponse:
    """spec §6.3：多筆 profile 且無 --profile 命中時的落地頁——列出所有已知帳號，各自
    一顆「使用此帳號」（POST /profiles/select）；固定一個「新增帳號」連到 /setup（不帶
    任何 profile 相關參數，等同全新精靈）。"""
    entries = profile_registry.list_profiles(site_origin=request.app.state.site_origin)
    return templates.TemplateResponse(request, "profile_select.html", {"profiles": entries})


@router.post("/profiles/select")
async def profiles_select(request: Request) -> Response:
    """使用者在 /profiles 選了一個帳號：以該 profile_id 重呼 resolve_gui_startup，依回傳
    的 decision.kind 導向對應頁面——kind="direct" 比照 coordinator 既有 direct 分支邏輯
    （見 coordinator.launch_direct，兩處共用同一段實作，不重複）。"""
    form = await request.form()
    profile_id = str(form.get("profile_id") or "")
    app_state = request.app.state
    decision = resolve_gui_startup(
        site_origin=app_state.site_origin, profile_hint=profile_id, reset=False,
    )
    app_state.gui_startup_decision = decision

    if decision.kind == "direct":
        from quanquant.agent.gui.coordinator import launch_direct
        target_path, notice = await launch_direct(
            request.app, profile=decision.profile, site_origin=app_state.site_origin,
        )
        if notice is not None:
            app_state.gui_startup_decision = GuiStartupDecision(
                kind="setup", start_step=1, profile=decision.profile, notice=notice,
            )
    else:
        target_path = "/setup"

    response = Response(status_code=303)
    response.headers["location"] = target_path
    return response


@router.post("/setup/step1/start", response_class=HTMLResponse)
async def setup_step1_start(request: Request) -> HTMLResponse:
    """唯一的 mutation 啟動點（見 module docstring）：使用者在步驟①按下「開始授權」
    （首次或錯誤後重試皆同一顆按鈕）時呼叫。若上一輪已經以錯誤終結
    （`device_flow_error` 非 None），先清掉舊 task/client/error 讓
    `_ensure_device_flow_started` 真的重開一輪；否則（例如尚未啟動的首次呼叫）維持
    冪等、不重複啟動。"""
    app_state = request.app.state
    if getattr(app_state, "device_flow_error", None) is not None:
        app_state.device_flow_task = None
        app_state.device_flow_client = None
        app_state.device_flow_error = None
    _ensure_device_flow_started(request.app)
    return _render_step1(request)


@router.get("/setup/poll-status")
async def setup_poll_status(request: Request) -> Response:
    """步驟①頁面每 2 秒的自動刷新目的地（`setup_step1.html` 的 `<meta http-equiv=
    "refresh">`）：已核准 → 303 導回 `/setup`（`setup_page` 會依此時的 `app.state`
    改渲染步驟②）；尚未核准 → 重新渲染步驟①局部（含目前 user_code／錯誤狀態）。

    UX 改善（背景 JS 輪詢取代整頁刷新）之後，`setup_step1.html` 已改用
    `GET /setup/poll-status.json`（見下方）當作輪詢目的地，本路由不再被頁面呼叫；
    保留不刪是因為 `test_poll_status_redirects_to_setup_once_approved` 這個既有回歸測試
    還在斷言它的行為，且它是無害的純渲染 GET，留著不影響新行為。"""
    if getattr(request.app.state, "device_flow_result", None) is not None:
        response = Response(status_code=303)
        response.headers["location"] = "/setup"
        return response
    return _render_step1(request)


# 步驟①背景輪詢用的同源 JS——不可 inline（CSP `default-src 'self'` 擋 inline script，
# 見 module docstring），也不可引 CDN，故手寫成一支由 `GET /setup/step1-poll.js` 供應的
# 純文字檔。邏輯：每 ~2 秒 fetch 一次 `poll-status.json`，依 state 更新頁面上
# `#qq-poll-status` 的文字（不整頁 reload）；state=ready 才做一次 `window.location`
# 導頁；state=error 顯示訊息並停止輪詢；網路層 fetch 失敗（暫時性）不中止輪詢，留給
# 下一輪重試，避免使用者被偶發網路問題卡在錯誤畫面。
_STEP1_POLL_JS = """(function () {
  "use strict";
  var statusEl = document.getElementById("qq-poll-status");
  var timer = null;

  function stop() {
    if (timer !== null) {
      clearInterval(timer);
      timer = null;
    }
  }

  function poll() {
    fetch("/setup/poll-status.json", { credentials: "same-origin" })
      .then(function (resp) {
        if (!resp.ok) {
          throw new Error("HTTP " + resp.status);
        }
        return resp.json();
      })
      .then(function (data) {
        if (data.state === "ready") {
          stop();
          window.location = data.next || "/setup";
          return;
        }
        if (data.state === "error") {
          stop();
          if (statusEl) {
            statusEl.textContent = data.message || "發生未預期錯誤，請重新開始授權。";
          }
          return;
        }
        if (statusEl) {
          statusEl.textContent = "等待核准中……";
        }
      })
      .catch(function () {
        // 暫時性網路錯誤：不中止輪詢，留給下一輪重試。
      });
  }

  poll();
  timer = setInterval(poll, 2000);
})();
"""


@router.get("/setup/poll-status.json")
async def setup_poll_status_json(request: Request) -> JSONResponse:
    """`step1-poll.js` 背景輪詢的 JSON 版本：純讀 `app.state`、不觸發任何 mutation
    （比照 `GET /setup`／`GET /setup/poll-status` 的既有鐵律）。狀態對應沿用既有 device
    flow 狀態，不自創新語意：
    - `device_flow_result` 非 None（已核准）→ `"ready"`，`next` 固定為 `/setup`
      （`setup_page` 會依此時的 `app.state` 自動渲染下一步，不在這裡重複判斷邏輯）。
    - `device_flow_error` 非 None（`DeviceFlowGaveUpError`／expired／denied／協定錯誤／
      未預期例外，見 `_ensure_device_flow_started`）→ `"error"`，`message` 用既有
      `_ERROR_MESSAGES` 對照表（與步驟①錯誤頁同一份文案，不重複定義）。
    - 兩者皆無 → `"waiting"`（仍在等待使用者於核准頁完成授權）。"""
    app_state = request.app.state
    if getattr(app_state, "device_flow_result", None) is not None:
        return JSONResponse({"state": "ready", "next": "/setup", "message": None})
    error_code = getattr(app_state, "device_flow_error", None)
    if error_code is not None:
        return JSONResponse({
            "state": "error", "next": None,
            "message": _ERROR_MESSAGES.get(error_code, error_code),
        })
    return JSONResponse({"state": "waiting", "next": None, "message": None})


@router.get("/setup/step1-poll.js")
async def setup_step1_poll_js() -> Response:
    """同源供應 `_STEP1_POLL_JS`（見上方常數的設計理由）。`require_gui_session` 依賴掛在
    整個 router 上自動套用；同源 `<script src="/setup/step1-poll.js">` 請求會帶
    session cookie，Origin/Sec-Fetch-Site 檢查對同源 subresource 請求同樣放行
    （見 `security.require_gui_session`），故不需要額外處理。"""
    return Response(content=_STEP1_POLL_JS, media_type="application/javascript")


@router.post("/setup/step2")
async def setup_step2(request: Request) -> Response:
    """收永豐憑證進記憶體。手動解析表單（見 module docstring）：422 時只回固定文案，
    不論成功或失敗都不把 `api_key`/`secret_key` 原樣塞回任何回應或 log。"""
    form = await request.form()
    api_key = str(form.get("api_key") or "")
    secret_key = str(form.get("secret_key") or "")
    remember_device = form.get("remember_device") is not None
    remember_broker = form.get("remember_broker") is not None

    if not api_key or not secret_key:
        log.info("setup: step2 表單驗證失敗（欄位缺漏，不記錄欄位值）")
        return _render_step2(request, error="請輸入完整的 API Key 與 Secret Key", status_code=422)

    request.app.state.broker_credentials = {"api_key": api_key, "secret_key": secret_key}
    request.app.state.remember_device_auth = remember_device
    request.app.state.remember_broker_credentials = remember_broker
    log.info("setup: step2 完成（永豐憑證已收，僅存於記憶體，未落地）")

    response = Response(status_code=303)
    response.headers["location"] = "/setup"
    return response


def _derive_ws_url(site_origin: str) -> str:
    """`http(s)://host[:port]` → `ws(s)://host[:port]/ws/agent`（`agent_ws.py` 掛載的
    固定路徑）。本轉換只是 scheme 替換＋固定 path 後綴，不是 Task 14 的
    `canonicalize_site`（那是驗證/正規化 `--site` 輸入本身的規則，本 task 直接消費
    CLI 層已經算好的 `site_origin`，不重算——見本 task Interfaces）。"""
    if site_origin.startswith("https://"):
        return "wss://" + site_origin[len("https://"):] + "/ws/agent"
    if site_origin.startswith("http://"):
        return "ws://" + site_origin[len("http://"):] + "/ws/agent"
    raise ValueError(f"未知的 site_origin scheme：{site_origin!r}")


@router.post("/setup/step3/launch")
async def setup_step3_launch(request: Request) -> Response:
    """精靈最後一步：用 device flow 拿到的 token＋永豐憑證組出真正的
    `AgentRunner`/`ChildHandle`/`WebsocketsTransport`，交給 coordinator 的
    `attach_runner()` 啟動（沿用既有 Inc0 machinery，非本 task 新增）。

    Task 13：`app_state.gui_current_profile`（唯一由 `_finalize_approved_profile` 寫入，
    見其 docstring）是這裡唯一的 profile 來源——不得再各自讀 `approved["profile_id"]`
    另組 buffer 路徑。啟動前先跑 `_guard_legacy_buffer_before_first_launch`：只在這是這台
    機器第一次建立這個 `(site_origin, profile_id)` 時才擋（見其 docstring），擋下就完全
    不建 registry／不寫 keyring／不建構 runner。

    Task 14 補入項（spec §5.3）：建構 `AgentRunner` 前，對解析後的 buffer 路徑取得
    `profile_registry.InstanceLock`——同 profile 已有另一個 agent 程序在跑時
    `acquire()` 拋 `AgentAlreadyRunningError`，這裡轉譯成主題化錯誤頁（沿用既有的
    `_render_step3(launch_error=...)`），不繼續 upsert registry／不建構 runner。成功
    acquire 後存 `app_state.instance_lock`；runner 建構/掛載失敗時連帶釋放（否則使用者
    照錯誤頁指示重試會被自己剛才那次失敗卡死），GUI 關閉時由 `shutdown_runner()`
    收尾釋放。"""
    app_state = request.app.state
    approved = getattr(app_state, "device_flow_result", None)
    broker = getattr(app_state, "broker_credentials", None)
    profile = getattr(app_state, "gui_current_profile", None)
    if approved is None or broker is None or profile is None:
        raise HTTPException(status_code=409, detail="尚未完成前面步驟，無法啟動")

    site_origin = app_state.site_origin

    conflict = _guard_legacy_buffer_before_first_launch(
        site_origin=site_origin, profile_id=profile.profile_id,
    )
    if conflict is not None:
        return HTMLResponse(conflict, status_code=409)

    # Task 14 補入項：同 profile 單實例 process lock——擋在 upsert_profile()／建構 runner
    # 之前，衝突時完全不動 registry、不建 runner。
    instance_lock = profile_registry.InstanceLock(profile.buffer_path)
    try:
        instance_lock.acquire()
    except profile_registry.AgentAlreadyRunningError:
        return _render_step3(request, launch_error="此帳號的 Agent 已在執行中",
                              status_code=409)

    profile_registry.upsert_profile(
        site_origin=site_origin, profile_id=profile.profile_id,
        username=profile.username, buffer_path=profile.buffer_path,
    )

    from quanquant.agent.buffer import DurableBuffer
    from quanquant.agent.gui.coordinator import attach_runner
    from quanquant.agent.runner import AgentRunner, ChildHandle
    from quanquant.agent.ws_client import WebsocketsTransport

    # reviewer Minor fix：建構/掛載失敗（例如 site_origin 格式異常、AgentRunner 建構期
    # 拋例外）不該變成未接住的 500 裸例外——包 try/except 改走已存在但先前從未真的被填的
    # `_render_step3(launch_error=...)` 主題化錯誤頁，使用者留在步驟③可以再按一次「啟動」。
    try:
        ws_url = _derive_ws_url(site_origin)
        runner = AgentRunner(
            transport=WebsocketsTransport(ws_url, token=approved["token"]),
            buffer=DurableBuffer(profile.buffer_path),
            child=ChildHandle(credentials=broker, symbol=_DEFAULT_SYMBOL, mode="sim",
                               buffer_path=profile.buffer_path),
            mode="sim",
        )
        attach_runner(request.app, runner)
        app_state.instance_lock = instance_lock
    except Exception:
        instance_lock.release()   # 不洩漏：使用者照錯誤頁指示重試不該被自己這次失敗卡死
        log.exception("setup: step3 啟動 agent runner 失敗")
        return _render_step3(request, launch_error="啟動失敗，請確認伺服器位址與憑證正確後重試。",
                              status_code=500)

    # opt-in：依 step2 收的 remember_device_auth／remember_broker_credentials 兩個旗標，
    # 分別把這一輪憑證寫入系統 keyring——寫入失敗只記警告、不影響本次已經啟動的 runner
    # （下次啟動時 resolve_gui_startup 會因為 keyring 沒這筆而重新走精靈，不會裝作記住了）。
    if getattr(app_state, "remember_device_auth", False):
        result = keyring_store.save_token(
            site_origin=site_origin, profile_id=profile.profile_id, token=approved["token"],
            expires_at=approved["token_expires_at"], username=profile.username,
        )
        if not result.ok:
            log.warning("setup: 記住裝置授權寫入 keyring 失敗，不影響本次啟動（%s）", result.error)
    if getattr(app_state, "remember_broker_credentials", False):
        result = keyring_store.save_broker_credentials(
            site_origin=site_origin, profile_id=profile.profile_id,
            api_key=broker["api_key"], secret_key=broker["secret_key"],
        )
        if not result.ok:
            log.warning("setup: 記住永豐 API 憑證寫入 keyring 失敗，不影響本次啟動（%s）", result.error)

    log.info("setup: 精靈完成，agent runner 已啟動（帳號：%s）", profile.username)

    response = Response(status_code=303)
    response.headers["location"] = "/status"
    return response
