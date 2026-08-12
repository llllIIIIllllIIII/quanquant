"""agent GUI `/setup` 三步精靈（Task 9，spec §5.1）：①裝置授權（device flow）②永豐
API 憑證 ③確認啟動。掛在 `coordinator.build_app()`（`app.include_router(router)`），
除本模組外不單獨對外暴露。

**輪詢機制刻意不用 HTMX**：spec 原文設想用 `hx-trigger="load, every 2s"` 打局部更新，
但 Task 8 的 `install_security_headers` 已經把 `Content-Security-Policy` 釘死成
`default-src 'self'`（本機安全邊界的既定鐵律，見 `gui/security.py`）——htmx.org 需要
從 CDN 載入（`web/templates/base.html` 走 `https://unpkg.com/htmx.org`），在這個 CSP 下
會被瀏覽器擋掉，若沒有把整個函式庫改成本機供應（本 task 檔案清單未列，且會讓「純三個
模板檔」的範圍膨脹成額外資產管線）就硬用 hx-trigger，等於精靈在真瀏覽器裡會安靜卡死在
「等待中」，比不做還糟。改用 `<meta http-equiv="refresh" content="2;url=...">`
（`setup_step1.html`）純 HTML 達成等價的『每 2 秒自動刷新』效果，不需要任何 script、
不觸碰 CSP，仍完全滿足 `GET /setup/poll-status` 這個介面本身（brief 明確要求的路由）。

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
import os
from pathlib import Path

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from quanquant.agent.device_flow_client import (
    DeviceFlowClient,
    DeviceFlowDeniedError,
    DeviceFlowExpiredError,
    DeviceFlowGaveUpError,
    DeviceFlowProtocolError,
    VerificationPathMismatchError,
)
from quanquant.agent.gui.security import require_gui_session

log = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_gui_session)])

_TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

_LEGACY_BUFFER_PATH = os.path.expanduser("~/.quanquant-agent/outbox.db")  # 與
    # `agent/main.py` 的 `--buffer` 預設值一致；per-profile 路徑待 Task 12
    # `profile_registry.buffer_path_for()` 落地後改用。
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
    context = {
        "started": client is not None,
        "user_code": client.user_code if client is not None else None,
        "approval_url": client.approval_url if client is not None else None,
        "error": _ERROR_MESSAGES.get(error_code, error_code) if error_code else None,
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
    `POST /setup/step1/start`）；已核准但永豐憑證未收 → 步驟②；兩者皆備 → 步驟③。"""
    app_state = request.app.state
    if getattr(app_state, "device_flow_result", None) is None:
        return _render_step1(request)
    if getattr(app_state, "broker_credentials", None) is None:
        return _render_step2(request)
    return _render_step3(request)


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
    改渲染步驟②）；尚未核准 → 重新渲染步驟①局部（含目前 user_code／錯誤狀態）。"""
    if getattr(request.app.state, "device_flow_result", None) is not None:
        response = Response(status_code=303)
        response.headers["location"] = "/setup"
        return response
    return _render_step1(request)


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
    `attach_runner()` 啟動（沿用既有 Inc0 machinery，非本 task 新增）。"""
    app_state = request.app.state
    approved = getattr(app_state, "device_flow_result", None)
    broker = getattr(app_state, "broker_credentials", None)
    if approved is None or broker is None:
        raise HTTPException(status_code=409, detail="尚未完成前面步驟，無法啟動")

    # TODO(Task 11 keyring 落地後補上)：依 app_state.remember_device_auth／
    # remember_broker_credentials 兩個 opt-in 旗標，分別呼叫
    # keyring_store.save_token()/save_broker_credentials() 把這一輪憑證寫入系統
    # keyring；目前這兩個旗標只停留在 app_state，尚未有任何持久化效果。
    # TODO(Task 12 profile registry 落地後補上)：呼叫
    # profile_registry.upsert_profile(site_origin=..., profile_id=approved["profile_id"],
    # username=approved["username"], buffer_path=...) 寫入
    # ~/.quanquant-agent/profiles.json，並改用 profile_registry.buffer_path_for() 算出
    # per-profile 的 buffer 路徑（目前先沿用下面的 legacy 單一路徑）。

    from quanquant.agent.buffer import DurableBuffer
    from quanquant.agent.gui.coordinator import attach_runner
    from quanquant.agent.runner import AgentRunner, ChildHandle
    from quanquant.agent.ws_client import WebsocketsTransport

    # reviewer Minor fix：建構/掛載失敗（例如 site_origin 格式異常、AgentRunner 建構期
    # 拋例外）不該變成未接住的 500 裸例外——包 try/except 改走已存在但先前從未真的被填的
    # `_render_step3(launch_error=...)` 主題化錯誤頁，使用者留在步驟③可以再按一次「啟動」。
    try:
        ws_url = _derive_ws_url(app_state.site_origin)
        runner = AgentRunner(
            transport=WebsocketsTransport(ws_url, token=approved["token"]),
            buffer=DurableBuffer(_LEGACY_BUFFER_PATH),
            child=ChildHandle(credentials=broker, symbol=_DEFAULT_SYMBOL, mode="sim",
                               buffer_path=_LEGACY_BUFFER_PATH),
            mode="sim",
        )
        attach_runner(request.app, runner)
    except Exception:
        log.exception("setup: step3 啟動 agent runner 失敗")
        return _render_step3(request, launch_error="啟動失敗，請確認伺服器位址與憑證正確後重試。",
                              status_code=500)

    log.info("setup: 精靈完成，agent runner 已啟動（帳號：%s）", approved.get("username"))

    response = Response(status_code=303)
    response.headers["location"] = "/status"
    return response
