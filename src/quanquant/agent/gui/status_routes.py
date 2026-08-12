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

**Task 11（keyring）／Task 12（profile registry）尚未落地**：「重新授權」「清除已存
token／永豐憑證」在 `status.html` 先以**禁用態**呈現（`disabled` 按鈕＋說明文案），對應
的 POST handler 仍存在（符合本 task 的路由介面），但目前一律回報「憑證儲存模組尚未完成」
的樁接文案，不嘗試呼叫尚不存在的 `keyring_store`/`profile_registry` 模組——真正串接留給
Task 11/12 落地後補上（沿用 `setup_routes.py` 對這兩個模組的既有 TODO 慣例）。`刪除
profile` 的 buffer pending 檢查（spec 核心防呆：避免刪掉還有未送資料的 profile）是本
task 唯一可以現在就完整落地的部分，因為它只依賴既有的 `DurableBuffer.unsent_count()`。

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

_KEYRING_STUB_NOTICE = "憑證儲存模組（Task 11）尚未完成，這個功能暫時停用。"

# 停止 Agent 後，延遲多久才把 uvicorn.Server.should_exit 設 True——讓這次 POST 的
# response 有機會先送達瀏覽器，使用者才看得到停止結果，而不是連線直接被砍斷。
_SHUTDOWN_DELAY_SECONDS = 0.5


def _connection_label(state: str) -> str:
    return _CONNECTION_LABELS.get(state, state)


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
                          banner_error: bool = False, status_code: int = 200) -> HTMLResponse:
    runner = request.app.state.agent_runner
    snap = await runner.snapshot()
    context = {
        "snap": snap,
        "connection_label": _connection_label(snap.connection),
        "failstop_guidance": _FAILSTOP_GUIDANCE,
        "banner": banner,
        "banner_error": banner_error,
        "remember_device_auth": getattr(request.app.state, "remember_device_auth", False),
        **_token_expiry_context(request),
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
    卡在對方初始化過程中。"""
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
        asyncio.create_task(_exit_soon())
    log.info("停止 Agent：已確認終止，GUI 即將關閉")
    return await _render_status(request, banner="Agent 已停止，這個視窗即將關閉。")


@router.post("/status/reauth", response_class=HTMLResponse)
async def reauth(request: Request) -> HTMLResponse:
    """依 opt-in 分流（spec §5.4）。Task 11 的 `rotate_token_secret` 尚未落地——「記住裝置
    授權」分支目前無法真正完成憑證輪替，一律顯示樁接文案；「未勾」分支不受 Task 11 影響
    （本來就不預先取 token），照 brief 顯示指引文案。"""
    form = await request.form()
    remember = form.get("remember_device") is not None
    request.app.state.remember_device_auth = remember
    if not remember:
        return await _render_status(
            request,
            banner="請先「停止 Agent」後重新啟動——啟動時會重新引導你完成裝置授權。",
        )
    return await _render_status(request, banner=_KEYRING_STUB_NOTICE, banner_error=True)


@router.post("/status/clear-credential", response_class=HTMLResponse)
async def clear_credential(request: Request, which: str) -> HTMLResponse:
    """`which` ∈ {"token","broker"}——對應 Task 11 清除函式尚未落地，目前一律樁接
    （registry entry 保留，spec §5.2 分項清除的語意留給 Task 11 完成後補上）。"""
    if which not in ("token", "broker"):
        return await _render_status(
            request, banner="未知的憑證類型。", banner_error=True, status_code=400,
        )
    return await _render_status(request, banner=_KEYRING_STUB_NOTICE, banner_error=True)


@router.post("/status/delete-profile", response_class=HTMLResponse)
async def delete_profile(request: Request) -> HTMLResponse:
    """buffer 有未送資料（`unsent_count() > 0`）→ 拒絕並警示（409），這是唯一現在就能
    完整落地的防呆——避免刪掉還有未送達資料的 profile。buffer 淨空時，真正的 registry
    entry／keyring 清除（Task 11 `clear_profile` ＋ Task 12 `remove_profile`）尚未落地，
    同樣先樁接。"""
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
    return await _render_status(
        request,
        banner="profile registry（Task 12）與憑證清除（Task 11）尚未完成，刪除 profile "
               "功能暫時停用；暫存箱已確認淨空，之後這兩個模組完成後即可安全刪除。",
        banner_error=True,
    )
