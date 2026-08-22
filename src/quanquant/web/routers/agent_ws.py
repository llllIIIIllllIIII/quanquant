"""本機 broker agent 的 WebSocket 端點（上行回報/ack、下行指令的傳輸層）。

鐵律：本 receive 迴圈絕不取得 supervisor.lock、絕不 inline await 長工作
（login 觸發的 reconcile 一律 create_task）——否則 receive 迴圈等 reconcile、
reconcile 等 cmd_ack、cmd_ack 需要 receive 迴圈 → 死鎖。

D2：連線驗證改為 per-user DB opaque token（`auth/agent_tokens.py`），取代 Inc0 全站共用的
`AGENT_WS_TOKEN` 靜態密鑰。

Task 6：UpLogin 分支改為 D10/R1-2/R1-8 的四步接受順序（見 `_check_uplogin`）——先綁先贏的
帳號綁定、歷史 Order ownership 雙查、per-user 換帳號 guard、他帳號未 resolved 曝險指令 guard。

Task 7（D1）：握手拿到的 `user_id` 用來向 `app.state.agent_registry` 換這個 user 專屬的
`UserAgentSlot`——`channel`/`adapter`/`order_state` 三者全部改從 slot 讀（不再是單一全域
`app.state.agent_channel`/`order_service`/`order_session_state`），Inc0 的 generation
detach/attach 機制沿用不變、只是變成 per-slot（同 user 第二條連線踢舊、不同 user 互不影響，
各自的 `channel.inbox_lock` 也是各自的，不會彼此排隊）。`order_session_factory`/
`order_risk_guard` 仍是全站唯一（token 驗證的前置依賴，發生在還不知道 user_id 之前）。
"""
import asyncio
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import ValidationError

from quanquant.auth.agent_tokens import validate_token
from quanquant.broker import repository as brepo
from quanquant.broker.agent_commands import apply_command_ack, prepare_replay
from quanquant.broker.agent_protocol import (
    DownReportAck, UpCmdAck, UpCommandRejected, UpHealth, UpLogin, UpQueryResult, UpReport,
    parse_uplink,
)
from quanquant.broker.inbox_worker import commit_raw_callback
from quanquant.db.models import User

log = logging.getLogger(__name__)
router = APIRouter()

# Task 13（Task 12 審查必修）：UpHealth 的 `detail` 是 agent 端自己機器上的原始例外字串
# （可能夾帶英文例外類名、agent 本機檔案路徑），不是「已知秘密值」——`redaction.py` 的
# redact_secrets 只抹已知秘密子字串，不足以擋這種任意內容。orders 頁 badge 是一般 owner
# 使用者看得到的 UI，不是維運限定的 /healthz，因此**不原樣顯示**，一律換成固定的繁體
# 通用訊息；原始 detail 只寫進 log／OpsAlerter（維運頻道）供除錯。
_AGENT_FAILSTOP_USER_MESSAGE = "agent 儲存故障，交易已停止"


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


def _connection_blocked(state, session_factory, user_id: int) -> bool:
    """WS 連線 gate（同步 DB 工作，呼叫端須 to_thread）：手動斷線（in-memory
    `agent_connection_gate`，D9）或冷靜期（DB `active_cooldown`，持久化、admin-only 解除）
    任一命中即封鎖。gate 缺席（in-process/wiring 未完成）視為未封鎖，不影響連線。"""
    gate = getattr(state, "agent_connection_gate", None)
    if gate is not None and gate.is_blocked(user_id):
        return True
    with session_factory() as session:
        return brepo.active_cooldown(
            session, user_id=user_id, now_ms=brepo.now_epoch_ms()
        ) is not None


@router.websocket("/ws/agent")
async def agent_ws(websocket: WebSocket) -> None:
    state = websocket.app.state
    raw_token = websocket.headers.get("x-agent-token", "")
    await websocket.accept()
    # 連線洩漏防呆：這幾個 app.state 屬性務必在 channel.attach 之前讀完——缺任一個
    # （wiring 未完成）就直接關閉連線並 return，channel 才不會卡在 attached 態卻永遠等不到
    # 對應的 finally 清理（之後真正的 agent 連線會被誤判成「已有連線」而被踢掉，永久連不上）。
    # order_risk_guard 也在這裡一併讀出——token 驗證的 owner 判定需要它，缺席同樣視為
    # wiring 不完整（1011），而非驗證失敗（1008）。Task 7：per-user 的 channel/adapter/
    # order_state 要等驗證拿到 user_id 之後才能從 agent_registry 查到對應 slot，不屬於這裡
    # 的前置檢查範圍——這裡只檢查「握手本身能不能跑」的三個全站依賴。
    try:
        registry = state.agent_registry
        session_factory = state.order_session_factory
        risk_guard = state.order_risk_guard
    except AttributeError:
        log.error(
            "agent WS wiring 不完整（缺 agent_registry/order_session_factory/"
            "order_risk_guard），拒絕連線"
        )
        await websocket.close(code=1011)
        return
    agent_user_id = await asyncio.to_thread(_authenticate, session_factory, risk_guard, raw_token)
    if agent_user_id is None:
        await websocket.close(code=1008)
        return
    # 連線 gate（D9/D11）：手動斷線（in-memory gate）或冷靜期（DB active_cooldown）中的 user，
    # 拒絕（重）連——擋 agent 端自動重連，直到使用者在下單頁按「允許 Agent 重連」、或冷靜期
    # 到期/admin 解除。1008 與驗證失敗同碼（不對外洩漏被拒的精確原因；下單頁本就顯示斷線/
    # 冷靜期狀態讓使用者知道為何）。
    if await asyncio.to_thread(_connection_blocked, state, session_factory, agent_user_id):
        log.info("agent WS：user_id=%s 目前被封鎖連線（手動斷線/冷靜期），拒絕", agent_user_id)
        await websocket.close(code=1008)
        return
    slot = registry.get(agent_user_id)
    if slot is None:
        # 防禦性：D1 eager 建置後，通過驗證（含 is_owner 白名單）的 user 理論上必定有 slot——
        # 查無代表 owner 名單與 registry 建置時不同步（如設定改了但沒重啟），視為 wiring 不
        # 一致，1011（不是 1008：token 本身是有效的，問題不在驗證）。
        log.error(
            "agent WS：user_id=%s 通過驗證但 registry 查無對應 slot（wiring 不一致），拒絕連線",
            agent_user_id,
        )
        await websocket.close(code=1011)
        return
    channel = slot.channel
    adapter = slot.adapter
    order_state = slot.session_state
    log.info("agent WS 通過驗證並取得 slot，user_id=%s", agent_user_id)
    hub = getattr(state, "order_events", None)
    if channel.connected:
        channel.detach()   # 新連線取代殘留半開連線（agent 重啟；無條件，不帶 generation）
    # 連線 gate（D9/D11）：注入 closer 讓 route 端可主動踢線；`closer=websocket.close`。
    my_generation = channel.attach(websocket.send_json, closer=websocket.close)
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
                # Task 6（D10/R1-2/R1-8）UpLogin guard v2：四步接受順序全部包在 inbox_lock
                # 內、與 UpReport 分支的 commit 序列化（承襲 codex round3 fix1/fix3 的理由——
                # 消 TOCTOU：舊連線的 report commit 飛行中時，這裡的檢查會等它 commit 完才
                # 跑，不會看到「尚未落地」的殘留而誤放行）。全過才 mark_logged_in／設定
                # adapter.account；任一步被拒，`_check_uplogin` 內已 rollback 掉可能的
                # binding 寫入，直接關閉連線（不留半開態——同 codex round3 fix1，agent 端會
                # backoff 重連，帳號不符會一直停在離線，UI 可見）。
                async with channel.inbox_lock:
                    reject_reason = await asyncio.to_thread(
                        _check_uplogin, session_factory, user_id=agent_user_id, account=msg.account
                    )
                    if reject_reason is not None:
                        log.warning(
                            "拒絕登入，關閉連線（user_id=%s，帳號=%s）：%s",
                            agent_user_id, msg.account, reject_reason,
                        )
                        await websocket.close(code=1008)
                        break
                    # Inc1 D9/G2⑤/⑥（Task 12）：UpLogin 宣告本 session 的 health_epoch 基準
                    # （直接覆寫，非取 max——見 AgentChannel.mark_logged_in docstring／R5-1）；
                    # 廢除「登入即 ready」——slot 進 pending_health，`session_state.ready`
                    # 維持目前值（多半是斷線時留下的 not-ready）直到收到本連線一則有效
                    # UpHealth(ok)（見下方 UpHealth 分支）才轉 ready。
                    channel.mark_logged_in(msg.account, health_epoch=msg.health_epoch)
                    adapter.account = msg.account
                    # Task 10（D4 重連補送，限同 scope）：四步 guard 全過、mark_logged_in
                    # 後，立刻查詢＋直送這個 user 在**這次登入綁定帳號**下尚未 transport ack
                    # 也尚未 resolved 的指令（agent 端 ledger 去重收斂，見 Task 9）——他帳號
                    # 的指令永不下行（`prepare_replay` 的 account 過濾）。用這條連線的
                    # `websocket.send_json` 直接送（fire-and-resend，不經
                    # `AgentChannel.request` 等待 ack），仍在 `inbox_lock` 內完成，確保補送
                    # 一定發生在下面 `_reconcile_after_login` 排程之前。
                    replay_cmds = await asyncio.to_thread(
                        prepare_replay, session_factory, user_id=agent_user_id, account=msg.account,
                    )
                    for down in replay_cmds:
                        await websocket.send_json(down)
                # D9⑥：不再在這裡 mark_ready()——slot 停在目前狀態（pending_health）等這條
                # 連線收到有效 UpHealth(ok) 才轉 ready；仍 publish 一次讓 UI 感知「已登入、
                # 等待健康確認」這個中繼狀態的變化（帳號/連線本身已經變了）。
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
                    #
                    # 事件喚醒：commit 成功後透過這個 slot 自己 adapter 的
                    # `raw_committed_hook`（`web/app.py::_start_agent_channel_subsystem` 接到
                    # 這個 slot 的 `RawInboxWorker.request_wake`）喚醒正確的 worker，取代純
                    # idle_interval 逾時輪詢。`getattr` 防禦性讀取——測試替身/未接線的 adapter
                    # 可能沒有這個屬性，一律當 None（no-op），不因此讓 report 落地路徑報錯。
                    await asyncio.to_thread(
                        commit_raw_callback, session_factory,
                        kind=msg.kind, broker="shioaji", payload=msg.payload,
                        user_id=agent_user_id, account=msg.account, mode=msg.mode,
                        ops_alerter=getattr(state, "ops_alerter", None),
                        on_committed=getattr(adapter, "raw_committed_hook", None),
                    )
                    await websocket.send_json(
                        DownReportAck(event_id=msg.event_id).model_dump()
                    )
            elif isinstance(msg, UpCmdAck):
                # Task 8（D4/G1）：applier 是唯一效果套用入口，且必須在 `channel.resolve_ack`
                # 之前完整 commit——route 端 `AgentChannel.request()` 的 future 才不會在效果
                # 落地前就把控制權還給呼叫端（brief 規則 7：route 讀已落庫結果渲染回應）。
                # 與 UpReport 分支共用 `inbox_lock`：`has_unresolved_risky_commands_other_
                # account`（UpLogin guard）查的正是這張表的 resolved_at，序列化在同一顆鎖
                # 內避免與換帳號判定之間出現 TOCTOU（同 UpReport 分支既有理由）。
                async with channel.inbox_lock:
                    outcome = await asyncio.to_thread(
                        apply_command_ack, session_factory, cmd_id=msg.cmd_id,
                        user_id=agent_user_id, ack=msg,
                    )
                    if outcome.found and not outcome.user_mismatch:
                        # C1（HIGH，codex 終審）：applier commit 完成後立刻回 DownReportAck——
                        # agent 端 `runner.py::_receive_loop` 只在收到這則才 `buffer.mark_sent()`
                        # （見 `_pump`），缺這步會讓 agent 的 outbox 對這筆 cmd_ack 永久重送
                        # （逾時→重送→server 再 no-op→再逾時……無限循環）。owned＋found（含
                        # transport CAS 輸掉的重複重送，`outcome.transport_won=False`，代表
                        # agent 端上一次送出的同一筆還沒被 mark_sent、仍在重試佇列）都要 ack；
                        # user mismatch／查無 ledger 維持不 ack（同 UpReport 分支既有的
                        # commit-then-ack 順序：commit 失敗會讓例外往上拋、這裡連
                        # send_json 都不會跑到，天然滿足「commit 失敗不得 ack」）。
                        await websocket.send_json(
                            DownReportAck(event_id=msg.event_id).model_dump()
                        )
                if outcome.user_mismatch:
                    # R1-5：cmd_id 存在但屬於別的 user——拒絕＋告警，完全不觸碰、不 ack。
                    log.warning(
                        "agent WS：cmd_ack user 不符（cmd_id=%s，連線 user_id=%s），忽略",
                        msg.cmd_id, agent_user_id,
                    )
                    ops = getattr(state, "ops_alerter", None)
                    if ops is not None:
                        ops.emit(
                            "agent_cmd_ack_user_mismatch", "cmd_ack user 不符",
                            detail=f"cmd_id={msg.cmd_id} user_id={agent_user_id}", severity="warn",
                        )
                    continue
                if not outcome.found:
                    # 查無此 cmd_id——可能是舊連線 generation 的殘留重送、或 client 端 bug。
                    # 無效果可套，`channel.resolve_ack` 對沒有對應 pending future 的 cmd_id
                    # 本就是安全 no-op（見 AgentChannel.resolve_ack），僅記 log 供觀測。
                    log.warning("agent WS：查無 cmd_id=%s 的 agent_commands 列，忽略", msg.cmd_id)
                channel.resolve_ack(msg)
            elif isinstance(msg, UpQueryResult):
                # Task 11（D7 R1-7）：volatile——只 resolve 這個 slot 的 pending future
                # （reconcile 快照／query_qty），不進 outbox 補送機制、不回 DownReportAck。
                channel.resolve_query_result(msg)
            elif isinstance(msg, UpCommandRejected):
                # Inc1 D9/G2②/R2-5（Task 12）：failstop latch 期間 agent 拒絕的 mutating
                # 指令——合成一筆 error_kind="failstop" 的 UpCmdAck 餵給既有兩維 CAS
                # applier（唯一效果套用入口，同 UpCmdAck 分支），落 acked_error＋依轉移表
                # failed/release delta（`_EXPLICIT_REJECT_KINDS` 已納入 failstop）。與
                # UpCmdAck 分支共用 `inbox_lock`（同樣寫 `resolved_at`，序列化理由同該分支）。
                synth_ack = UpCmdAck(cmd_id=msg.cmd_id, event_id=0, ok=False,
                                     error_kind=msg.error_kind,
                                     message="agent failstop latch 生效，拒絕執行")
                async with channel.inbox_lock:
                    outcome = await asyncio.to_thread(
                        apply_command_ack, session_factory, cmd_id=msg.cmd_id,
                        user_id=agent_user_id, ack=synth_ack,
                    )
                if outcome.user_mismatch:
                    log.warning(
                        "agent WS：cmd_rejected user 不符（cmd_id=%s，連線 user_id=%s），忽略",
                        msg.cmd_id, agent_user_id,
                    )
                    ops = getattr(state, "ops_alerter", None)
                    if ops is not None:
                        ops.emit(
                            "agent_cmd_ack_user_mismatch", "cmd_ack user 不符",
                            detail=f"cmd_id={msg.cmd_id} user_id={agent_user_id}", severity="warn",
                        )
                    continue
                if not outcome.found:
                    log.warning("agent WS：查無 cmd_id=%s 的 agent_commands 列，忽略", msg.cmd_id)
                channel.resolve_ack(synth_ack)
            elif isinstance(msg, UpHealth):
                # Inc1 D9/G2③/⑤/⑥（Task 12）：epoch 單調性（見 AgentChannel.note_health）；
                # 只有這條連線仍是目前這一代（generation 未被取代）時，才據此改變
                # `session_state`——舊連線的遲到健康訊息不該影響已被新連線取代的 slot 狀態
                # （同 UpReport/UpLogin 分支既有 generation fencing 原則）。
                was_failstop = channel.failstop
                accepted = channel.note_health(status=msg.status, health_epoch=msg.health_epoch)
                if accepted and channel.generation == my_generation:
                    if msg.status == "ok":
                        order_state.mark_ready()
                    else:
                        if msg.detail:
                            log.warning(
                                "agent failstop detail user_id=%s: %s", agent_user_id, msg.detail
                            )
                        order_state.mark_unhealthy(_AGENT_FAILSTOP_USER_MESSAGE)
                    if hub is not None:
                        hub.publish()
                    ops = getattr(state, "ops_alerter", None)
                    if ops is not None and was_failstop != channel.failstop:
                        # G2：連線/斷線不告警，只在健康語意「真的」轉換（進入/解除 failstop）
                        # 時才報，不是每則 heartbeat 都送。
                        ops.failstop(user_id=agent_user_id, enabled=channel.failstop,
                                     detail=msg.detail or "")
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
            log.warning("agent WS 連線中斷，user_id=%s 下單暫停（等待該使用者 agent 重連）",
                       agent_user_id)


async def _reconcile_after_login(adapter) -> None:
    try:
        await adapter.reconcile()
    except Exception:
        log.exception("agent 登入後 reconcile 失敗（best-effort，不影響連線）")


def _check_uplogin(session_factory, *, user_id: int, account: str) -> str | None:
    """同步 DB 工作（呼叫端須用 asyncio.to_thread 包起來，receive 迴圈鐵律：絕不 inline
    await 長工作）：Task 6 UpLogin 四步接受順序（spec D10/R1-2/R1-8）：
      ①`bind_account` 先綁先贏——帳號已綁定別的 user → 拒登。
      ②`account_owned_by_other_user_in_orders` 歷史 Order ownership 雙查（R1-8：backfill
        理論上已讓①攔下這種情況，這裡是不信任新表的第二道防線）。
      ③`count_unprocessed_for_login`（S2 per-user 化，取代舊版全域
        `_count_unprocessed_raw_inbox`）：這個 user 名下還有未處理、且 account 不同於這次
        登入帳號（或未蓋章 NULL）的 RawInbox 殘留 → 拒登（codex round2 fix2 的原始理由：
        worker 晚一步映射會用到已被新帳號覆蓋的 mutable adapter.account，造成跨帳號錯配；
        round4 修正沿用：quarantined 但未 processed 的列仍要擋，因為 watchdog 之後會解除
        隔離重新處理）。
      ④`has_unresolved_risky_commands_other_account`（R1-2）：這個 user 在別的帳號還有未
        resolved 的曝險指令（place/update；cancel 不算曝險，不擋）→ 拒登，要求先用原帳號
        連線收斂。

    四步與①的寫入包在同一個 session/交易內——全過才 `session.commit()`（binding 才真正
    落地）；任一步失敗立即 `session.rollback()` 並回傳中文拒絕原因（呼叫端據此 log 並
    close(1008)）——即使①這次剛好是新綁定成功、後面步驟才擋下，也不會留下錯誤的綁定
    殘影。回傳 `None` 代表全部通過。
    """
    with session_factory() as session:
        if not brepo.bind_account(session, broker="shioaji", account=account, user_id=user_id):
            session.rollback()
            return "此帳號已綁定其他使用者"
        if brepo.account_owned_by_other_user_in_orders(
            session, broker="shioaji", account=account, user_id=user_id
        ):
            session.rollback()
            return "此帳號歷史委託屬於其他使用者，請聯絡管理員"
        if brepo.count_unprocessed_for_login(session, user_id=user_id, account=account) > 0:
            session.rollback()
            return "尚有未處理的其他帳號回報殘留，請稍候或先用原帳號連線收斂"
        if brepo.has_unresolved_risky_commands_other_account(
            session, user_id=user_id, account=account
        ):
            session.rollback()
            return "原帳號尚有未收斂的下單/改單指令，請先用原帳號連線收斂"
        session.commit()
        return None
