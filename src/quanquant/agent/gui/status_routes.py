"""agent GUI `/status` 儀表板（Task 10，spec §6.4）：連線後的狀態頁——連線 badge／
mode=sim 標示／buffer pending 筆數／latched 大紅 fail-stop 警示／token 到期倒數／
「重新授權」「清除已存憑證／刪除 profile」「停止 Agent」按鈕。掛在
`coordinator.build_app()`（`app.include_router(router)`），除本模組外不單獨對外暴露。

**輪詢機制沿用 Task 9 的 meta-refresh 模式**（`setup_step1.html` 的既定作法）：CSP 已釘死
`default-src 'self'`（`gui/security.py`），htmx.org 需要 CDN、會被擋——這裡同樣用
`<meta http-equiv="refresh" content="1;url=/status">` 達成『每 1 秒自動更新』（spec §6.4：
本機、單一使用者，1 秒輪詢無負載疑慮），不需要任何 script。

**mutation 全 POST**：整個 router 掛 `require_gui_session`（router-wide dependency，含
`GET /status` 本身——比照 `setup_routes.router` 既定作法），所有會改動狀態的動作
（`/status/stop`／`/status/reauth`／`/status/clear-credential`／`/status/delete-profile`）
一律 `POST`，`GET /status` 純渲染，不觸發任何 mutation。

**終審收口：接上 Task 11（keyring）／Task 12（profile registry）**：「清除已存憑證」
「刪除 profile」「重新授權」三個 handler 原本一律回報「憑證儲存模組尚未完成」的樁接
文案（初版 Task 10 交付時 Task 11/12 確實還不存在）——Task 11/12 完成並測試過後，這裡
補上真正串接，`templates/status.html` 對應的 `disabled` 按鈕與假文案一併移除。

- **清除已存憑證**（`clear_credential`）：`_current_profile()` 定位目前 profile 後直接呼叫
  `keyring_store.clear_secret()`，只清這一筆（token 或永豐憑證），registry entry 不動。
- **刪除 profile**（`delete_profile`）：buffer pending 防呆維持原樣（唯一在 Task 11/12
  落地前就已完整的部分）；buffer 淨空後改為真的呼叫 `keyring_store.clear_profile()`
  （這個 profile 的 token／永豐憑證兩筆都刪）＋`profile_registry.remove_profile()`——
  keyring 清除失敗就整個拒絕（不繼續刪 registry entry，避免 registry 已清但 keyring
  殘留兩者不同步的中間態）。
- **重新授權**（`reauth`）：依 spec §5.4 分流。「未勾記住」分支本來就不動 Task 11/12
  （從不預先取 token），維持原樣。「已勾記住」分支的難點是：`resolve_gui_startup()`
  只在 token **已過期**（`_is_expired`）才會把使用者導回精靈重建授權——`/status` 顯示
  「重新授權」按鈕的時機（`token_days_left < 3`）token 通常還沒真的過期，若只是引導
  使用者「停止後重開」，`resolve_gui_startup` 會判定舊 token 仍有效直接走 `direct`
  重連，完全不會觸發新一輪裝置授權，倒數形同虛設。因此「已勾記住」必須**立即**跑一輪
  獨立的 device flow（`_start_reauth_flow`），核准後直接 `keyring_store.
  rotate_token_secret()` 寫入新 token——**不**透過 `setup_routes.py` 的精靈重跑一遍：
  `/status` 可見即代表 `agent_runner` 已掛上、對應 profile 的
  `profile_registry.InstanceLock` 正被目前這個 runner 持有，若照精靈整套跑完，
  `setup_step3_launch()` 一定會在重新 `acquire()` 這把自己已經持有的鎖時撞見
  `AgentAlreadyRunningError`、卡死在『此帳號的 Agent 已在執行中』，永遠到不了那支
  `keyring_store.save_token()`——等於重新授權在唯一可觸達的情境下必然失敗。改成獨立跑
  一輪 `DeviceFlowClient`（`app.state.reauth_*`，刻意與精靈的 `device_flow_*` 分開命名，
  不共用/不干擾精靈狀態），完全不碰 `AgentRunner`／`InstanceLock`／registry，只在核准後
  寫 keyring 這一筆——真正連線套用仍如 spec 所述，需要「停止 Agent」後重新啟動；核准帳號
  與目前 profile 不符時（使用者在核准頁登入了別的帳號）拒絕寫入，不會把新 token 錯寫進
  別的 profile_id。

**停止 Agent 走 coordinator 既有關閉序列**：`shutdown_runner()`（Task 8）已經是
『stop runner → 關 transport → cancel 背景 task → 驗證 child 終止』的完整、冪等序列
（見 `coordinator.py` module docstring）——這裡直接呼叫它，不重新發明一份簡化版，換取
與 OS signal 關閉路徑完全一致的行為；驗死失敗（回傳 `False`）時如實顯示錯誤，不謊稱已
停止。應使用者要求的『延遲一小段，讓 HTTP response 先送出』，才在背景 task 裡把
`uvicorn.Server.should_exit` 設 `True`（讓這次 POST 的回應先送達瀏覽器，使用者才看得到
停止結果）。

**秘密處理**：本頁不顯示任何 token／API 秘密明文，只顯示到期時間／帳號等非秘密識別資訊；
`log.*` 呼叫比照 `setup_routes.py` 只記結構性資訊。
"""
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from quanquant.agent import keyring_store, profile_registry
from quanquant.agent.gui.security import require_gui_session

log = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_gui_session)])

_TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

_CONNECTION_LABELS = {
    "connecting": "連線中",
    "connected": "已連線",
    "reconnecting": "重新連線中",
    "offline": "離線",
    "rejected": "已被拒絕",
}

_FAILSTOP_GUIDANCE = (
    "系統偵測到本機資料寫入異常，Agent 已自動停止一切下單／改單／刪單操作（唯讀查詢仍"
    "可運作），避免委託遺失或重複執行。請先確認本機磁碟空間與權限正常，排除底層儲存"
    "問題後，按下方「停止 Agent」再重新啟動 Agent 程序——程序啟動時會自動重新偵測是否"
    "已恢復；session 進行中不會自動解除，需要重啟才能清除這個警示。"
)

# reauth 背景 device flow 的終態例外 → 使用者可讀文案（見 module docstring「重新授權」段）。
_REAUTH_ERROR_MESSAGES = {
    "VerificationPathMismatchError": "授權驗證失敗（伺服器回應的驗證路徑不符），請重新按「重新授權」。",
    "DeviceFlowExpiredError": "授權逾時未完成，請重新按「重新授權」。",
    "DeviceFlowDeniedError": "已在核准頁拒絕本次授權，請重新按「重新授權」。",
    "DeviceFlowProtocolError": "與伺服器溝通發生未預期錯誤，請重新按「重新授權」。",
    "DeviceFlowGaveUpError": "授權多次交付失敗，請重新按「重新授權」重試或檢查伺服器狀態。",
    "ProfileMismatch": "核准頁登入的帳號與目前使用的帳號不同，已拒絕寫入本機憑證庫；請用同一個"
                        "帳號完成核准後再試一次。",
    "UnexpectedError": "發生未預期錯誤，請重新按「重新授權」。",
}

# 停止 Agent 後，延遲多久才把 uvicorn.Server.should_exit 設 True——讓這次 POST 的
# response 有機會先送達瀏覽器，使用者才看得到停止結果，而不是連線直接被砍斷。
_SHUTDOWN_DELAY_SECONDS = 0.5


def _connection_label(state: str) -> str:
    return _CONNECTION_LABELS.get(state, state)


def _current_profile(app_state) -> "profile_registry.ProfileEntry | None":
    """定位『目前這個 runner 對應的 profile』，供 clear_credential／delete_profile／
    reauth 取得 site_origin+profile_id。兩個入口路徑各自寫入不同欄位（見
    `setup_routes.py`／`coordinator.py`）：精靈完成（`setup_step3_launch`）寫
    `gui_current_profile`；direct 快速連線（`coordinator.launch_direct`）不寫這個欄位，
    但 `resolve_gui_startup()` 決策當下已經把同一個 `ProfileEntry` 存進
    `gui_startup_decision.profile`——這裡依序 fallback，兩條路徑都能定位到同一份資料，
    不需要另外改動 `coordinator.py`。"""
    profile = getattr(app_state, "gui_current_profile", None)
    if profile is not None:
        return profile
    decision = getattr(app_state, "gui_startup_decision", None)
    if decision is not None:
        return decision.profile
    return None


def _reauth_status_context(app_state) -> dict:
    client = getattr(app_state, "reauth_client", None)
    task = getattr(app_state, "reauth_task", None)
    keyring_error = getattr(app_state, "reauth_keyring_error", None)
    error_code = getattr(app_state, "reauth_error", None)
    if keyring_error:
        error_message = keyring_error
    elif error_code:
        error_message = _REAUTH_ERROR_MESSAGES.get(error_code, error_code)
    else:
        error_message = None
    return {
        "reauth_in_progress": task is not None and not task.done(),
        "reauth_user_code": client.user_code if client is not None else None,
        "reauth_approval_url": client.approval_url if client is not None else None,
        "reauth_success": getattr(app_state, "reauth_success", False),
        "reauth_error_message": error_message,
    }


def _start_reauth_flow(app) -> None:
    """spec §5.4『已勾記住裝置授權』分支：立即獨立跑一輪 device flow，核准後直接
    `keyring_store.rotate_token_secret()` 寫入新 token——不透過 `setup_routes.py` 整套
    精靈重跑（理由見 module docstring「重新授權」段：InstanceLock 已被目前這個 runner
    持有，走精靈必卡在 `setup_step3_launch` 的 `AgentAlreadyRunningError`）。冪等：已有
    一輪未結束就直接返回，比照精靈 `_ensure_device_flow_started()` 的既有慣例。"""
    existing_task = getattr(app.state, "reauth_task", None)
    if existing_task is not None and not existing_task.done():
        return

    from quanquant.agent.device_flow_client import (
        DeviceFlowClient,
        DeviceFlowDeniedError,
        DeviceFlowExpiredError,
        DeviceFlowGaveUpError,
        DeviceFlowProtocolError,
        VerificationPathMismatchError,
    )
    from quanquant.agent.gui.setup_routes import _device_flow_http_client

    http_client = _device_flow_http_client(app)
    client = DeviceFlowClient(site_origin=app.state.site_origin, http_client=http_client)
    app.state.reauth_client = client
    app.state.reauth_error = None
    app.state.reauth_success = False
    app.state.reauth_keyring_error = None

    async def _run() -> None:
        try:
            await client.initiate()
            result = await client.poll_until_done()
            profile = _current_profile(app.state)
            if profile is None or result["profile_id"] != profile.profile_id:
                app.state.reauth_error = "ProfileMismatch"
                log.warning("reauth: 核准的帳號與目前 profile 不符，拒絕寫入 keyring")
                return
            kr_result = keyring_store.rotate_token_secret(
                site_origin=app.state.site_origin, profile_id=profile.profile_id,
                new_token=result["token"], expires_at=result["token_expires_at"],
                username=result["username"],
            )
            if not kr_result.ok:
                app.state.reauth_keyring_error = kr_result.error
                log.warning("reauth: keyring 寫入失敗（%s）", kr_result.error)
            else:
                app.state.reauth_success = True
                log.info("reauth: 已核准並寫入本機憑證庫（帳號：%s）", result.get("username"))
        except (VerificationPathMismatchError, DeviceFlowExpiredError,
                DeviceFlowDeniedError, DeviceFlowProtocolError, DeviceFlowGaveUpError) as exc:
            app.state.reauth_error = type(exc).__name__
            log.warning("reauth: device flow 未完成（%s）", type(exc).__name__)
        except Exception:
            app.state.reauth_error = "UnexpectedError"
            log.exception("reauth: 背景 task 發生未預期例外")

    app.state.reauth_task = asyncio.create_task(_run())


def _token_expiry_context(request: Request) -> dict:
    """裝置授權 token 到期資訊——來自 Task 9 精靈存在 `app.state.device_flow_result`
    的 `token_expires_at`（AgentSnapshot 本身不帶這個欄位，見 Task 7 定義）。解析失敗
    （格式異常）一律視為「沒有可顯示的到期資訊」，不讓整頁渲染因此炸掉。"""
    result = getattr(request.app.state, "device_flow_result", None) or {}
    expires_at = result.get("token_expires_at")
    days_left = None
    expiring_soon = False
    if expires_at:
        try:
            deadline = datetime.fromisoformat(expires_at)
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            days_left = (deadline - datetime.now(timezone.utc)).days
            expiring_soon = days_left < 3
        except ValueError:
            expires_at = None
    return {
        "token_expires_at": expires_at,
        "token_days_left": days_left,
        "token_expiring_soon": expiring_soon,
    }


async def _render_status(request: Request, *, banner: str | None = None,
                          banner_error: bool = False, status_code: int = 200,
                          stopped: bool = False) -> HTMLResponse:
    """`stopped=True`（只有 `/status/stop` 成功時傳）：模板據此**省略** 1 秒
    meta-refresh——見 `stop_agent` docstring reviewer Important fix 2，避免使用者被自動
    導去一個即將關閉的 server。"""
    runner = request.app.state.agent_runner
    snap = await runner.snapshot()
    context = {
        "snap": snap,
        "connection_label": _connection_label(snap.connection),
        "failstop_guidance": _FAILSTOP_GUIDANCE,
        "banner": banner,
        "banner_error": banner_error,
        "stopped": stopped,
        "remember_device_auth": getattr(request.app.state, "remember_device_auth", False),
        **_token_expiry_context(request),
        **_reauth_status_context(request.app.state),
    }
    return templates.TemplateResponse(request, "status.html", context, status_code=status_code)


@router.get("/status", response_class=HTMLResponse)
async def status_page(request: Request) -> HTMLResponse:
    """輪詢 1 秒（本機、無負載疑慮，spec §6.4）：純渲染 `runner.snapshot()`，不做任何
    mutation。"""
    return await _render_status(request)


@router.post("/status/stop", response_class=HTMLResponse)
async def stop_agent(request: Request) -> HTMLResponse:
    """呼叫 `shutdown_runner()`（Task 8 既有完整關閉序列）；成功才排程
    `uvicorn.Server.should_exit = True`（延遲，讓這次 response 先送出）。驗死失敗
    （回傳 `False`）如實顯示錯誤，不排程關閉、不謊稱已停止。

    延遲 import（比照 `setup_routes.py` 對 `attach_runner` 的既有做法）：`coordinator.py`
    在模組層級 `include_router` 這個檔案的 `router`，若這裡在模組層級反向 import
    `coordinator.shutdown_runner` 會形成循環 import（`coordinator` 匯入到一半、
    `shutdown_runner` 尚未定義就被回頭 import）——延到函式呼叫時才 import，兩個模組互不
    卡在對方初始化過程中。

    reviewer Important fix 1：`asyncio.create_task(_exit_soon())` 的回傳值必須存住
    （`request.app.state.shutdown_task`）——全 codebase 其他 `create_task` 呼叫皆存引用
    （`runner.py` 的 session task 列表／`coordinator.attach_runner` 的
    `app.state.runner_task`／`setup_routes._ensure_device_flow_started` 的
    `app.state.device_flow_task`），這裡原本是唯一例外：沒有外部引用的 task 有被
    CPython 文件明載的 GC 風險（尚未在 uvloop 下實測重現，但沒理由自己開這個先例）。
    存進 `app.state` 也讓測試能直接 `await` 它，驗證 `should_exit` 真的被翻成 True。"""
    from quanquant.agent.gui.coordinator import shutdown_runner

    clean = await shutdown_runner(request.app)
    if not clean:
        log.error("停止 Agent：child 子程序驗死失敗，GUI 保持存活等待人工處理")
        return await _render_status(
            request,
            banner="子程序無法確認終止，可能仍有殘留程序在跑，請人工檢查後再關閉這個"
                   "視窗——尚未真正停止，GUI 不會自動關閉。",
            banner_error=True, status_code=500,
        )

    server = getattr(request.app.state, "uvicorn_server", None)
    if server is not None:
        async def _exit_soon() -> None:
            await asyncio.sleep(_SHUTDOWN_DELAY_SECONDS)
            server.should_exit = True
        request.app.state.shutdown_task = asyncio.create_task(_exit_soon())
    log.info("停止 Agent：已確認終止，GUI 即將關閉")
    # reviewer Important fix 2：這個成功頁**不能**沿用 status.html 預設的 1 秒
    # meta-refresh——refresh 間隔（1s）比 `_SHUTDOWN_DELAY_SECONDS`（0.5s）還長，會導航到
    # 一個已經 should_exit=True、隨時可能真的關閉的 server，使用者實際看到的是連線錯誤
    # 而非「已停止」訊息，違背這裡想達成的『讓使用者看得到停止結果』。改渲染無
    # meta-refresh 的終態頁，明確告知可以關閉分頁。
    return await _render_status(
        request, banner="Agent 已停止，可以關閉這個分頁。", stopped=True,
    )


@router.post("/status/reauth", response_class=HTMLResponse)
async def reauth(request: Request) -> HTMLResponse:
    """依 opt-in 分流（spec §5.4）。「未勾」分支不預先取 token，顯示重啟指引（不受本次
    改動影響）；「已勾記住」分支立即啟動獨立的 device flow（`_start_reauth_flow`，見其
    docstring 與 module docstring「重新授權」段），核准後才真的把新 token 寫進 keyring；
    真正套用仍需「停止 Agent」後重新啟動（spec D6：第一版不做熱換）。"""
    form = await request.form()
    remember = form.get("remember_device") is not None
    request.app.state.remember_device_auth = remember
    if not remember:
        return await _render_status(
            request,
            banner="請先「停止 Agent」後重新啟動——啟動時會重新引導你完成裝置授權。",
        )
    profile = _current_profile(request.app.state)
    if profile is None:
        return await _render_status(
            request, banner="無法識別目前使用的帳號，請先「停止 Agent」後重新啟動再試一次。",
            banner_error=True, status_code=409,
        )
    _start_reauth_flow(request.app)
    return await _render_status(request)


@router.post("/status/clear-credential", response_class=HTMLResponse)
async def clear_credential(request: Request, which: str) -> HTMLResponse:
    """`which` ∈ {"token","broker"}：`_current_profile()` 定位目前 profile 後呼叫
    `keyring_store.clear_secret()`，只刪這一筆，registry entry 不動（spec §5.2 分項清除
    語意）。"""
    if which not in ("token", "broker"):
        return await _render_status(
            request, banner="未知的憑證類型。", banner_error=True, status_code=400,
        )
    profile = _current_profile(request.app.state)
    if profile is None:
        return await _render_status(
            request, banner="無法識別目前使用的帳號，請先「停止 Agent」後重新啟動再試一次。",
            banner_error=True, status_code=409,
        )
    which_label = "登入 token" if which == "token" else "永豐 API 憑證"
    result = keyring_store.clear_secret(
        site_origin=request.app.state.site_origin, profile_id=profile.profile_id, which=which,
    )
    if not result.ok:
        log.warning("清除已存憑證失敗（which=%s）：%s", which, result.error)
        return await _render_status(
            request, banner=f"清除已存{which_label}失敗：{result.error}",
            banner_error=True, status_code=500,
        )
    log.info("清除已存憑證成功（which=%s，profile_id=%s）", which, profile.profile_id)
    return await _render_status(
        request, banner=f"已清除本機儲存的{which_label}，重新啟動 Agent 後生效。",
    )


@router.post("/status/delete-profile", response_class=HTMLResponse)
async def delete_profile(request: Request) -> HTMLResponse:
    """buffer 有未送資料（`unsent_count() > 0`）→ 拒絕並警示（409），避免刪掉還有未送達
    資料的 profile。buffer 淨空時，`_current_profile()` 定位目前 profile 後依序
    `keyring_store.clear_profile()`（token／永豐憑證兩筆）＋`profile_registry.
    remove_profile()`——keyring 清除失敗就整個拒絕、不繼續刪 registry entry，避免
    「registry 已清、keyring 殘留」的不同步中間態。"""
    # `request.app.state.agent_runner` 在這裡的假設與 `GET /status`（`_render_status` 直接
    # 呼叫 `runner.snapshot()`）相同：只有精靈完成、`attach_runner()` 已掛上真正的
    # `AgentRunner` 之後才會導到這個頁面／可以按這個按鈕。`_buffer` 是 `AgentRunner.__init__`
    # 的必要參數（非 optional），跨模組直接讀取內部屬性——AgentRunner 目前沒有公開的
    # buffer property（YAGNI，本 task 前沒有任何呼叫端需要），不為此新增。
    runner = request.app.state.agent_runner
    pending = await asyncio.to_thread(runner._buffer.unsent_count)
    if pending > 0:
        log.warning("刪除 profile 被拒絕：暫存箱仍有 %d 筆未送出", pending)
        return await _render_status(
            request,
            banner=f"暫存箱（buffer）尚有 {pending} 筆資料尚未送出，拒絕刪除 profile"
                   "（避免資料遺失）。請等待送出完成，或確認可捨棄後先手動清空暫存箱再重試。",
            banner_error=True, status_code=409,
        )
    profile = _current_profile(request.app.state)
    if profile is None:
        return await _render_status(
            request, banner="無法識別目前使用的帳號，請先「停止 Agent」後重新啟動再試一次。",
            banner_error=True, status_code=409,
        )
    site_origin = request.app.state.site_origin
    keyring_result = keyring_store.clear_profile(site_origin=site_origin, profile_id=profile.profile_id)
    if not keyring_result.ok:
        log.warning("刪除 profile：清除本機憑證失敗（profile_id=%s）：%s",
                    profile.profile_id, keyring_result.error)
        return await _render_status(
            request,
            banner=f"刪除 profile 失敗（清除已存憑證時發生錯誤，registry 未變動）：{keyring_result.error}",
            banner_error=True, status_code=500,
        )
    profile_registry.remove_profile(site_origin=site_origin, profile_id=profile.profile_id)
    log.info("刪除 profile 成功（profile_id=%s）", profile.profile_id)
    return await _render_status(
        request,
        banner="已刪除 profile 與已存憑證。目前這個 Agent 仍在執行中，請按「停止 Agent」結束本次連線。",
    )
