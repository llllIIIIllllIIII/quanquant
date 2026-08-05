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
    if channel.connected:
        channel.detach()   # 新連線取代殘留半開連線（agent 重啟）
    channel.attach(websocket.send_json)
    order_state = state.order_session_state
    hub = getattr(state, "order_events", None)
    adapter = state.order_service
    session_factory = state.order_session_factory
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
