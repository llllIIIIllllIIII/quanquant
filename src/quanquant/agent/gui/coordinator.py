"""agent GUI coordinator（Task 8）：本機 HTTP GUI 的生命週期骨架。

`run_gui()` 是唯一入口：①預綁 127.0.0.1:0 拿實際 port（`_bind_ephemeral_socket`）②建
`GuiSecurityState` + FastAPI app（`build_app`：掛 `install_security_headers`
middleware、`/bootstrap` 一次性交換路由；Task 9 的 `/setup`、Task 10 的 `/status` 各自
在自己的 task 內對這個 app 呼叫 `include_router`）③`uvicorn.Server.serve(
sockets=[sock])`，`access_log=False`（GUI 完全不記 access log，見 spec §6.1）④
`webbrowser.open(bootstrap_url(...))` ⑤精靈完成、憑證齊備後（Task 9）呼叫
`attach_runner()` 才建構 ChildHandle/WebsocketsTransport/AgentRunner、啟動
`run_forever()` 背景 task ⑥runner fatal 時 GUI 保持存活顯示錯誤——`attach_runner()`
內部 try/except 收攏，不 raise 出協調器。

關閉序列（`shutdown_runner()`）是『停止 Agent』（Task 10 `/status/stop`）與 OS signal
共用的唯一路徑：兩者最終都會讓 uvicorn 的 `should_exit` 變 True——OS signal 靠
`uvicorn.Server.serve()` 內建的 `capture_signals()`；Task 10 的按鈕靠自己排程設定。
`should_exit` 變 True 後 `Server.shutdown()` 會照 ASGI lifespan 協定觸發 `_lifespan()`
的 shutdown 段，這裡統一呼叫 `shutdown_runner()`：stop runner → 關 WS → 驗證 child
終止。Task 10 的 `/status/stop` handler 另外會在設定 `should_exit=True` 之前『自己先
await 一次 shutdown_runner()』，好把終止結果（成功/失敗）寫進 HTTP response——
`shutdown_runner()` 全程冪等（`runner.stop()`／`transport.close()`／
`child.terminate()` 重複呼叫皆為安全 no-op，見各自 docstring），兩條路徑各自呼叫互不
干擾、也不會重複真的殺兩次子程序。
"""
import asyncio
import logging
import socket
import webbrowser
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Response, status

from quanquant.agent.gui.security import (
    GuiSecurityState,
    bootstrap_url,
    consume_bootstrap,
    install_security_headers,
)

log = logging.getLogger(__name__)

_SETUP_PATH = "/setup"   # Task 9 掛的精靈首頁；本 task 階段該路由尚不存在，redirect
                          # 目的地先寫死常數，Task 9 落地後即可直接命中，不需回頭改這裡。


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """ASGI lifespan：啟動段無事可做（`run_gui()` 在呼叫 `server.serve()` 之前已完成
    所有啟動步驟）；shutdown 段是 OS signal 觸發關閉時實際執行清理的地方，見 module
    docstring。"""
    yield
    await shutdown_runner(app)


def build_app(state: GuiSecurityState) -> FastAPI:
    """組出本機 GUI 的 FastAPI app：安全標頭 middleware、`/bootstrap` 一次性交換路由，
    關掉 OpenAPI/docs（本機管控介面不需要對外揭露 schema，減少不必要的攻擊面）。
    `app.state.gui_security` 是 `require_gui_session` 的讀取點；`app.state.
    agent_runner`/`runner_task` 預留 None，供 `attach_runner()`（Task 9 精靈完成後）
    填入。"""
    app = FastAPI(lifespan=_lifespan, openapi_url=None, docs_url=None, redoc_url=None)
    install_security_headers(app)

    @app.get("/bootstrap")
    async def bootstrap(secret: str, response: Response) -> Response:
        # 必須是 async def：sync def 的路由函式 FastAPI 會丟進 thread-pool 執行——真
        # OS 執行緒併發下，consume_bootstrap() 的 check-then-set（bootstrap_consumed
        # 判斷＋寫入）沒有鎖保護，兩個併發的首請求（瀏覽器 prefetch/雙擊/防毒掃連結）
        # 可能都通過檢查、各自發出不同 session_token，只有最後寫入的存活，另一個拿到
        # 的 cookie 永久失效且 secret 已消費——需要重啟整個 GUI 程序才能復原。改
        # async def 後這個路由整段（含 consume_bootstrap，內部無任何 await）在單執行緒
        # event loop 上原子執行，不會有其他 request 插入到 check 與 set 之間，不需要
        # 額外的鎖。
        consume_bootstrap(state, response, secret=secret)
        # 303 導到乾淨 URL（不帶 secret）。刻意沿用同一個 response 物件（而非另外
        # `return RedirectResponse(...)`）——FastAPI 對路由函式回傳「另一個」Response
        # 實例時會整份取代掉注入的 response（含它身上 consume_bootstrap 剛
        # set_cookie() 種上去的 Set-Cookie 標頭），不會合併；沿用同一物件才能確保
        # session cookie 真的隨這個 303 一起送出。
        response.status_code = status.HTTP_303_SEE_OTHER
        response.headers["location"] = _SETUP_PATH
        return response

    app.state.gui_security = state
    app.state.agent_runner = None
    app.state.runner_task = None
    return app


def _bind_ephemeral_socket() -> socket.socket:
    """預綁 127.0.0.1:0 拿實際 port——只 bind 不 listen：uvicorn 收到現成 sockets 時，
    底層 `asyncio.loop.create_server` 的 `Server._start_serving()` 會自己補上
    `sock.listen(backlog)`（見 cpython `asyncio.base_events.Server._start_serving`），
    這裡先 bind 只是為了在啟動 uvicorn 之前就能讀出核給的實際 port，好組
    `bootstrap_url()`。"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    return sock


def attach_runner(app: FastAPI, runner) -> asyncio.Task:
    """Task 9 精靈完成、憑證齊備後呼叫：掛上建好的 AgentRunner、啟動 `run_forever()`
    背景 task。runner fatal（`FatalAgentError` 等）在這裡收攏——只記 log，不 raise 出
    協調器，GUI 保持存活（Task 10 `/status` 頁面從 `runner.snapshot()` 顯示錯誤狀態），
    符合 module docstring ⑥。"""

    async def _run() -> None:
        try:
            await runner.run_forever()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception(
                "agent runner 發生致命錯誤，GUI 保持存活（狀態頁顯示錯誤，需人工介入）"
            )

    app.state.agent_runner = runner
    app.state.runner_task = asyncio.create_task(_run())
    return app.state.runner_task


async def shutdown_runner(app: FastAPI) -> bool:
    """『停止 Agent』（Task 10）與 OS signal（經 ASGI lifespan shutdown，見
    `_lifespan()`）共用的關閉序列：stop runner → 關 WS → 驗證 child 終止。回傳
    True＝確認乾淨終止（或本來就沒有掛 runner，尚在精靈階段）；False＝child 驗死
    失敗，呼叫端不得謊稱已停止（比照 `ChildHandle.terminate()` 自己的三態回傳契約）。

    直接 cancel 背景 task，而非只呼叫 `runner.stop()` 乾等它自然收斂：
    `AgentRunner.run_forever()` 只在每輪 `run_once()` 結束後才檢查 `_stopping`（見
    runner.py `run_forever` docstring）——若當下正處於重連 backoff 的
    `asyncio.sleep()`，光呼叫 `stop()` 最壞要等到那一輪 sleep 醒來才會被看到，對 GUI
    的『停止 Agent』互動來說等不起。這裡改為：先關閉 transport 逼停目前 session（若
    正在跑，觸發 `_receive_loop` 例外，讓 `run_once()` 提早結束），再無條件 cancel
    背景 task 保證收斂。未送出的 durable buffer 資料本來就會在下次啟動時自動補送
    （見 runner.py module docstring「零丟單設計」），cancel 不影響正確性、不會丟單。
    """
    runner = getattr(app.state, "agent_runner", None)
    if runner is None:
        return True
    runner.stop()
    transport = getattr(runner, "_transport", None)
    if transport is not None:
        try:
            await transport.close()
        except Exception:
            log.exception("關閉 agent WS transport 時發生例外，繼續走完關閉序列")
    task = getattr(app.state, "runner_task", None)
    if task is not None and not task.done():
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    child = getattr(runner, "_child", None)
    if child is None:
        return True
    died = await asyncio.to_thread(child.terminate)
    return died is not False


async def run_gui(*, site_origin: str, profile: str | None, reset: bool) -> None:
    """GUI 協調器主入口，見 module docstring。`site_origin`/`profile`/`reset` 目前只
    存進 `app.state`——Task 9（device flow 需要 `site_origin`）、Task 12/13（profile
    選擇／`--reset` 決策樹）落地後才會真的讀取，本 task 只負責原樣傳遞、不預先發明
    用法（YAGNI）。"""
    sock = _bind_ephemeral_socket()
    port = sock.getsockname()[1]
    state = GuiSecurityState(port=port)
    app = build_app(state)
    app.state.site_origin = site_origin
    app.state.profile = profile
    app.state.reset = reset

    config = uvicorn.Config(app, access_log=False)
    server = uvicorn.Server(config)
    app.state.uvicorn_server = server   # Task 10『停止 Agent』排程 should_exit=True 用

    webbrowser.open(bootstrap_url(state))
    log.info("agent GUI 已啟動：http://127.0.0.1:%d/bootstrap", port)
    await server.serve(sockets=[sock])
