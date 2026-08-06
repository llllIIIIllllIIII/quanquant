"""本機 broker agent 的 WebSocket 端點（上行回報/ack、下行指令的傳輸層）。

鐵律：本 receive 迴圈絕不取得 supervisor.lock、絕不 inline await 長工作
（login 觸發的 reconcile 一律 create_task）——否則 receive 迴圈等 reconcile、
reconcile 等 cmd_ack、cmd_ack 需要 receive 迴圈 → 死鎖。

D2：連線驗證改為 per-user DB opaque token（`auth/agent_tokens.py`），取代 Inc0 全站共用的
`AGENT_WS_TOKEN` 靜態密鑰。握手拿到的 `user_id` 本 task 先只當作 owner 授權判定用（單一
`agent_channel`／單 slot 架構不變，Task 7 才會把它接上 per-user registry）。
"""
import asyncio
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import ValidationError
from sqlalchemy import func
from sqlmodel import select

from quanquant.auth.agent_tokens import validate_token
from quanquant.broker.agent_protocol import (
    DownReportAck, UpCmdAck, UpHealth, UpLogin, UpReport, parse_uplink,
)
from quanquant.broker.inbox_worker import commit_raw_callback
from quanquant.db.models import RawInbox, User

log = logging.getLogger(__name__)
router = APIRouter()


def _authenticate(session_factory, risk_guard, raw_token: str) -> int | None:
    """同步 DB 工作（呼叫端須用 `asyncio.to_thread` 包起來，receive 迴圈鐵律：絕不 inline
    await 長工作——這裡是握手階段、尚未進入 receive 迴圈，但仍離開 event loop 以免擋住其他
    連線）：`x-agent-token` → `validate_token`（sha256 查表，過期/撤銷回 None）→ 載 User
    驗 `is_active` → `RiskGuard.is_owner` 白名單。任一步失敗回 None（呼叫端一律 close(1008)，
    不區分是 token 無效／帳號停用／非 owner——避免對外洩漏驗證失敗在哪一步）。"""
    with session_factory() as session:
        token_row = validate_token(session, raw=raw_token)
        if token_row is None:
            return None
        user = session.get(User, token_row.user_id)
        if user is None or not user.is_active:
            return None
        if risk_guard is None or not risk_guard.is_owner(user.id):
            return None
        return user.id


@router.websocket("/ws/agent")
async def agent_ws(websocket: WebSocket) -> None:
    state = websocket.app.state
    channel = getattr(state, "agent_channel", None)
    raw_token = websocket.headers.get("x-agent-token", "")
    await websocket.accept()
    if channel is None:
        await websocket.close(code=1008)
        return
    # 連線洩漏防呆：這幾個 app.state 屬性務必在 channel.attach 之前讀完——缺任一個
    # （wiring 未完成）就直接關閉連線並 return，channel 才不會卡在 attached 態卻永遠等不到
    # 對應的 finally 清理（之後真正的 agent 連線會被誤判成「已有連線」而被踢掉，永久連不上）。
    # order_risk_guard 也在這裡一併讀出——token 驗證的 owner 判定需要它，缺席同樣視為
    # wiring 不完整（1011），而非驗證失敗（1008）。
    try:
        order_state = state.order_session_state
        adapter = state.order_service
        session_factory = state.order_session_factory
        risk_guard = state.order_risk_guard
    except AttributeError:
        log.error(
            "agent WS wiring 不完整（缺 order_session_state/order_service/"
            "order_session_factory/order_risk_guard），拒絕連線"
        )
        await websocket.close(code=1011)
        return
    agent_user_id = await asyncio.to_thread(_authenticate, session_factory, risk_guard, raw_token)
    if agent_user_id is None:
        await websocket.close(code=1008)
        return
    log.info("agent WS 通過驗證，user_id=%s（Task 7 前仍沿用單一共享 channel）", agent_user_id)
    hub = getattr(state, "order_events", None)
    if channel.connected:
        channel.detach()   # 新連線取代殘留半開連線（agent 重啟；無條件，不帶 generation）
    my_generation = channel.attach(websocket.send_json)
    try:
        while True:
            data = await websocket.receive_json()
            if channel.generation != my_generation:
                # 這條連線已被更新的連線取代（generation 已前進）——舊連線收到的訊息一律
                # 靜默忽略，不再處理／不再改動 channel 狀態，讓迴圈自然落到 finally 退場。
                log.info("agent WS 舊連線（generation=%s）已被新連線取代，忽略後續訊息並退場",
                         my_generation)
                break
            try:
                msg = parse_uplink(data)
            except ValidationError:
                log.warning("agent 上行訊息格式不符，忽略：%s", str(data)[:200])
                continue
            if isinstance(msg, UpLogin):
                # codex round3 fix1/fix3：count 查詢＋決策＋mark_logged_in＋adapter.account
                # 設定整段包在 inbox_lock 內——與 UpReport 分支的 commit 序列化（消 TOCTOU：
                # 舊連線的 report commit 飛行中時，這裡的 count 查詢會等它 commit 完才跑，
                # 不會看到「尚未落地」的 0 而誤放行換帳號）。
                async with channel.inbox_lock:
                    if adapter.account and msg.account != adapter.account:
                        # codex round2 fix2：agent 端的 tripwire（buffer.assert_account）只擋
                        # 得住「尚未送出」的回報列；server 已 commit RawInbox 並 ack、worker
                        # 尚未處理的列不受保護——這個窗口換帳號登入，worker 之後映射
                        # order_report 用的是 mutable adapter.account（已被新帳號覆蓋），舊
                        # 帳號的回報會被錯配進新帳號的部位/委託。查詢未處理列數，有殘留就
                        # 整個拒絕這次 login；沒有殘留才放行（帳號覆蓋屬正常換帳號重啟）。
                        unprocessed = await asyncio.to_thread(
                            _count_unprocessed_raw_inbox, session_factory
                        )
                        if unprocessed > 0:
                            # codex round3 fix1（HIGH）：只 continue 會留下半開連線——agent
                            # 端的 pump 不等 login 確認就送 report，若 UpReport 分支沒檔會被
                            # 拒帳號的回報照樣 commit+ack。直接關閉連線消滅半開態；agent 端
                            # 會 backoff 重連，帳號不符會一直停在離線（UI 可見）。
                            log.warning(
                                "拒絕帳號切換登入，關閉連線：尚有 %d 筆未處理回報"
                                "（原帳號 %s、新帳號 %s）",
                                unprocessed, adapter.account, msg.account,
                            )
                            await websocket.close(code=1008)
                            break
                    channel.mark_logged_in(msg.account)
                    adapter.account = msg.account
                order_state.mark_ready()
                if hub is not None:
                    hub.publish()
                asyncio.create_task(_reconcile_after_login(adapter))
            elif isinstance(msg, UpReport):
                # codex round3 fix2（HIGH）：未登入（或已被更新連線取代）一律不 commit、不
                # ack——堵住「login 被拒/尚未確認時，agent 端提早送出的 report 仍被落地」的
                # 跨帳號錯配缺口。agent 端該列維持 unacked，之後正常登入才會補送。
                if not channel.logged_in or channel.generation != my_generation:
                    log.warning(
                        "忽略未登入連線的回報（event_id=%s），不 commit 也不 ack", msg.event_id
                    )
                    continue
                async with channel.inbox_lock:
                    # Inc1 D5：蓋章 user_id=連線認證身分、account/mode=envelope 帶的值（agent
                    # 端在落 outbox 當下就蓋章，換帳號/重連/重送都不改變歸屬，見 UpReport 定義）。
                    # scope 驗證（R1-5：account 是否屬於這個 user 的 binding）與 dead-letter
                    # 分級全部在 commit_raw_callback → stage_scoped_raw_inbox 內完成，違規列仍
                    # 照常落地＋commit-then-ack（I1/I4），不在這裡另外攔截。
                    await asyncio.to_thread(
                        commit_raw_callback, session_factory,
                        kind=msg.kind, broker="shioaji", payload=msg.payload,
                        user_id=agent_user_id, account=msg.account, mode=msg.mode,
                        ops_alerter=getattr(state, "ops_alerter", None),
                    )
                    await websocket.send_json(
                        DownReportAck(event_id=msg.event_id).model_dump()
                    )
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
        # 只有這條連線仍是目前這一代（沒被更新的連線取代）時，detach 才真的生效——
        # 也只有真的生效才標 offline/publish，避免舊連線的遲到 finally 誤傷新連線。
        was_current = channel.generation == my_generation
        channel.detach(my_generation)
        if was_current:
            order_state.mark_disabled("agent 離線")
            if hub is not None:
                hub.publish()
            log.warning("agent WS 連線中斷，下單暫停（等待 agent 重連）")


async def _reconcile_after_login(adapter) -> None:
    try:
        await adapter.reconcile()
    except Exception:
        log.exception("agent 登入後 reconcile 失敗（best-effort，不影響連線）")


def _count_unprocessed_raw_inbox(session_factory) -> int:
    """同步 DB 查詢（呼叫端須用 asyncio.to_thread 包起來，receive 迴圈鐵律：絕不 inline
    await 長工作）：尚未處理的 RawInbox 列數——換帳號登入前的安全檢查（codex round2
    fix2）。codex round4 修正：不再排除 quarantine==True 的列——quarantined 列仍是「未
    處理」，run_agent_watchdog 的 _retry_quarantined 之後會自動解除隔離讓 worker 重新
    處理；若那時帳號已切到新帳號，舊帳號的 order_report 會用新帳號的 mutable
    adapter.account 映射，造成延遲跨帳號錯配。換帳號 guard 必須擋下所有
    processed==False 列，不論 quarantine 與否。"""
    with session_factory() as session:
        return session.exec(
            select(func.count()).where(
                RawInbox.processed == False,  # noqa: E712 - SQLAlchemy 表達式需字面 == 比較
            )
        ).one()
