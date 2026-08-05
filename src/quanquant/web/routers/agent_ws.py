"""本機 broker agent 的 WebSocket 端點（上行回報/ack、下行指令的傳輸層）。

鐵律：本 receive 迴圈絕不取得 supervisor.lock、絕不 inline await 長工作
（login 觸發的 reconcile 一律 create_task）——否則 receive 迴圈等 reconcile、
reconcile 等 cmd_ack、cmd_ack 需要 receive 迴圈 → 死鎖。
"""
import asyncio
import logging
import secrets as _secrets

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import ValidationError

from quanquant.broker.agent_protocol import (
    DownReportAck, UpCmdAck, UpHealth, UpLogin, UpReport, parse_uplink,
)
from quanquant.broker.inbox_worker import commit_raw_callback
from quanquant.config import get_settings

log = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/ws/agent")
async def agent_ws(websocket: WebSocket) -> None:
    settings = get_settings()
    state = websocket.app.state
    channel = getattr(state, "agent_channel", None)
    token = websocket.headers.get("x-agent-token", "")
    await websocket.accept()
    if (channel is None or not settings.agent_ws_token
            or not _secrets.compare_digest(token, settings.agent_ws_token)):
        await websocket.close(code=1008)
        return
    # 連線洩漏防呆：這三個 app.state 屬性務必在 channel.attach 之前讀完——缺任一個
    # （wiring 未完成）就直接關閉連線並 return，channel 才不會卡在 attached 態卻永遠等不到
    # 對應的 finally 清理（之後真正的 agent 連線會被誤判成「已有連線」而被踢掉，永久連不上）。
    try:
        order_state = state.order_session_state
        adapter = state.order_service
        session_factory = state.order_session_factory
    except AttributeError:
        log.error(
            "agent WS wiring 不完整（缺 order_session_state/order_service/"
            "order_session_factory），拒絕連線"
        )
        await websocket.close(code=1011)
        return
    hub = getattr(state, "order_events", None)
    if channel.connected:
        channel.detach()   # 新連線取代殘留半開連線（agent 重啟）
    channel.attach(websocket.send_json)
    try:
        while True:
            data = await websocket.receive_json()
            try:
                msg = parse_uplink(data)
            except ValidationError:
                log.warning("agent 上行訊息格式不符，忽略：%s", str(data)[:200])
                continue
            if isinstance(msg, UpLogin):
                channel.mark_logged_in(msg.account)
                adapter.account = msg.account
                order_state.mark_ready()
                if hub is not None:
                    hub.publish()
                asyncio.create_task(_reconcile_after_login(adapter))
            elif isinstance(msg, UpReport):
                await asyncio.to_thread(
                    commit_raw_callback, session_factory,
                    kind=msg.kind, broker="shioaji", payload=msg.payload,
                )
                await websocket.send_json(DownReportAck(event_id=msg.event_id).model_dump())
            elif isinstance(msg, UpCmdAck):
                channel.resolve_ack(msg)
            elif isinstance(msg, UpHealth):
                channel.note_heartbeat()
    except WebSocketDisconnect:
        pass
    except Exception:
        # 觀測用（不改變既有中斷語意）：非 WebSocketDisconnect 的例外原本就會讓迴圈往上炸、
        # 帶著 finally 清理連線，這裡只加一行 log 讓「炸在哪、為什麼」不再無聲無息。
        log.exception("agent WS 處理上行訊息失敗")
        raise
    finally:
        channel.detach()
        order_state.mark_disabled("agent 離線")
        if hub is not None:
            hub.publish()
        log.warning("agent WS 連線中斷，下單暫停（等待 agent 重連）")


async def _reconcile_after_login(adapter) -> None:
    try:
        await adapter.reconcile()
    except Exception:
        log.exception("agent 登入後 reconcile 失敗（best-effort，不影響連線）")
