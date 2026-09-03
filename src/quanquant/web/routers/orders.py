"""下單面板 + 委託列表 + 部位（HTMX-first，比照 trades.py 樣板）。

mode 一律取 app.state.order_service.mode（server-side），本檔不接受表單覆寫執行 mode；
`mode` query 參數只用來過濾「委託列表」顯示範圍（純讀取，不影響任何寫入路徑）。

round3 #1（BLOCKER 修正，Task 9 核心）：`PUT /orders/{id}` 的兩階段確認 token 簽發，
route 必須先以複合 scope 讀出既有 Order，把「未修改的欄位」用既有值、只覆蓋要改的欄位，
組成完整 payload，再用與 `ShioajiAdapter.update`（broker/shioaji_adapter.py）**完全相同**
的合併演算法算 canonical hash——否則 route 簽的 token 與 adapter 驗證時重算的 hash
不會一致，real 改單的兩階段確認永遠鎖死（見 `_update_confirm_dialog`）。

round3 #7：client_order_id 由 `orders_page` 首次渲染時生成一次，寫進 hidden input；
同一次表單渲染內的 HTTP retry 沿用同一個值（`_order_request_from_form` 只在表單完全
沒帶這個欄位時才 fallback 生新的）。

round3 #9：委託列表除了取消鈕，另提供「改單」鈕開 modal（`GET /orders/{id}/edit`）
帶現有 qty/price 預填，送出走 `PUT /orders/{id}`，real 一樣走兩步確認。

round3 #6：`positions` 非 owner 一律 403（不吞成 200 空表）；cancel/update 的所有權驗證
交給 `OrderService`（adapter 內部已用 (user_id,broker,mode,broker_order_id) 複合 scope
驗證），本檔只負責把 `AuthorizationError` 映射成 HTTP 403。
"""
import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from sqlmodel import Session
from sse_starlette.sse import EventSourceResponse

from quanquant.auth import agent_tokens as agent_token_service
from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, OrderError, RiskError
from quanquant.broker.position_tracker import point_value
from quanquant.broker.redaction import redact_secrets
from quanquant.broker.types import OrderRequest, Position, canonical_payload_hash
from quanquant.config import get_settings
from quanquant.db.models import User
from quanquant.journal.pnl import unrealized_pnl
from quanquant.poller import QuotePoller
from quanquant.web.deps import get_agent_slot, get_current_user, get_order_service, get_poller, get_session
from quanquant.web.templating import render_partial, templates

router = APIRouter()


def _safe_str(exc: Exception, service) -> str:
    """F8：`OrderError`/`RiskError` 訊息可能源自 adapter 未 redact 的路徑（防禦性，即使
    adapter 內部已對已知分支 redact，這裡是回顯給瀏覽器前的最後一道防線）。`service` 就是
    `ShioajiAdapter` instance，經 `secrets_to_redact` property 取得同一份秘密清單。"""
    return redact_secrets(str(exc), secrets=getattr(service, "secrets_to_redact", []))


def get_order_risk_guard(request: Request):
    return getattr(request.app.state, "order_risk_guard", None)


def _mode(raw: str | None) -> str:
    return raw if raw in ("sim", "real") else "sim"


def _parse_kill_switch_enabled(raw) -> bool:
    """HTMX 隱藏欄位 `enabled` 送 "true"/"false"（也容忍 1/on/yes）；其餘一律視為 False。"""
    return str(raw or "").strip().lower() in ("true", "1", "on", "yes")


def _parse_kill_switch_scope(raw) -> str | None:
    """D3：兩層 kill switch 的 `scope` 隱藏欄位——只接受 'self'/'global'，其餘（含缺席）
    一律回 None，呼叫端映射成 400（不像 `enabled` 那樣寬鬆容錯：翻錯層級的風控開關後果
    比表單格式錯誤嚴重，寧可拒絕也不要用預設值猜測使用者的意圖）。"""
    value = str(raw or "").strip().lower()
    return value if value in ("self", "global") else None


_DISABLED_KILL_SWITCH_VIEW = {"global_on": False, "self_on": False, "blocked": False, "global_actor": None}


def _count_open_orders_best_effort(session: Session, service) -> int:
    """kill switch 告警用的未成交掛單數：best-effort，計數失敗一律回 0，絕不擋住切換。"""
    try:
        mode = getattr(service, "mode", None) if service is not None else None
        return brepo.count_open_orders(session, mode=mode)
    except Exception:  # noqa: BLE001 — 計數只供告警參考，任何失敗都不得反噬切換
        return 0


# ---- 007：下單反饋橫幅（banner-stack）——HTTP 同步回應這半（P0-4/P0-5/P0-6）----
#
# 事件來源有兩種，互補不重疊：
#   1. 這裡：place/cancel/update 的「使用者剛剛按下送出/取消/改單」這個動作本身的立即
#      成敗——同步失敗（RiskError 不需確認／OrderError，如超過風控額度）永遠不會經過
#      RawInboxWorker，SSE 的 order-report 事件不會發生，只能靠 HTTP 回應本身帶banner。
#   2. static/banners.js 監聽的 SSE `deal`/`order-report`（見 broker/order_events.py，
#      批次 B-1 已交付）——券商端非同步狀態變化（成交/委託狀態轉換）。
# 兩者共用同一套視覺元件（.banner-stack／.order-banner），backend 只需要送純文字摘要
# （title/detail），不需要結構化欄位——JS 端不必為兩種事件來源各寫一套渲染邏輯。
_BANNER_FAIL_TITLES = {"place": "委託失敗", "cancel": "取消失敗", "update": "改單失敗"}


def _banner_event(*, kind: str, ok: bool, title: str, detail: str = "") -> dict:
    return {"kind": kind, "ok": ok, "title": title, "detail": detail}


def _hx_trigger_header(events: dict) -> str:
    """HTTP header 值只能是 latin-1（ASCII 子集）——`ensure_ascii=True`（預設）把中文
    文案跳脫成 `\\uXXXX`，htmx 收到後照 JSON 語意解回原字串，畫面顯示不受影響。"""
    return json.dumps(events)


def _clear_form_error_oob() -> str:
    """P0-4：成功時舊的錯誤 banner（`.form-error-slot`）沒有被清掉——htmx OOB swap 支援
    任意 CSS selector 當目標（`innerHTML:<selector>`），送一段空內容就能清空既有殘留，
    不需要知道裡面現在裝了什麼。"""
    return '<div hx-swap-oob="innerHTML:.form-error-slot"></div>'


def _form_error(message: str, *, banner_kind: str | None = None) -> HTMLResponse:
    """`banner_kind` 為 None（預設）：純表單欄位驗證錯誤（打錯價格之類），只走既有
    `.form-error-slot`、不進橫幅——見 007 規格「不要動到的部分」。帶 `banner_kind`
    （'place'/'cancel'/'update'）：post-submission 失敗，額外用 HX-Trigger 觸發橫幅
    （P0-4/P0-5/P0-6），與 `.form-error-slot` 的訊息並存（不是二選一）。"""
    html = render_partial("partials/form_error.html", message=message)
    headers = {"HX-Retarget": ".form-error-slot", "HX-Reswap": "innerHTML"}
    if banner_kind is not None:
        banner = _banner_event(kind=banner_kind, ok=False, title=_BANNER_FAIL_TITLES[banner_kind], detail=message)
        headers["HX-Trigger"] = _hx_trigger_header({"order-banner": banner})
    return HTMLResponse(html, status_code=200, headers=headers)


def _orders_trigger(*, close_modal: bool = False, banner: dict | None = None) -> HTMLResponse:
    events: dict = {"refreshorders": True}
    if close_modal:
        events["closeordermodal"] = True
    if banner is not None:
        events["order-banner"] = banner
    # P0-4：成功時一併清掉舊的錯誤 banner（見 _clear_form_error_oob）。
    return HTMLResponse(_clear_form_error_oob(), headers={"HX-Trigger": _hx_trigger_header(events)})


def _place_success(req: OrderRequest) -> HTMLResponse:
    """下單成功：body 帶一個 out-of-band swap，把下單面板的 client_order_id hidden input
    換成全新 UUID——client_order_id 由 orders_page 首渲染時生成一次（round3 #7，同一張表單的
    HTTP retry 沿用同鍵才能冪等去重），但成功送出後若不換鍵，下一筆（尤其反向/不同 payload）
    會沿用同一顆鍵、被 repository 冪等防護擋成「同鍵不同 payload」。只在**成功**路徑換鍵，
    失敗/需確認時不換（保留 retry 冪等）。同時觸發 refreshorders 刷新委託/部位列表。

    007（P0-4/P0-6）：額外帶一個「委託送出成功」橫幅（HTTP 層的立即回饋，內容含商品/方向/
    口數/價格）＋清掉舊的錯誤 banner——這是券商 ack（SSE order-report）之外**唯一**保證
    使用者按下送出後馬上看得到結果的路徑；SSE 事件隨後仍會依券商實際狀態（已委託/成交…）
    再跳一條，兩者互補不衝突（見本檔 `_orders_trigger` 上方說明）。"""
    price_text = "市價" if req.price_type == "MKT" else str(req.price)
    detail = f"{req.symbol} {'買' if req.action == 'Buy' else '賣'} {req.qty} 口 @ {price_text}"
    banner = _banner_event(kind="place", ok=True, title="委託送出成功", detail=detail)
    html = (
        render_partial("partials/client_order_id_input.html", client_order_id=str(uuid.uuid4()))
        + _clear_form_error_oob()
    )
    events = {"refreshorders": True, "order-banner": banner}
    return HTMLResponse(html, headers={"HX-Trigger": _hx_trigger_header(events)})


def _parse_order_price(raw: str | None, *, price_type: str | None) -> Decimal:
    """bug 2：MKT（市價單）不需要價格——`Decimal(form.get("price"))` 對 MKT 沒有特判，
    空字串/缺欄位（MKT 的 price 欄位停用時瀏覽器不會送出這個欄位）一律 `decimal.
    ConversionSyntax`/`TypeError`。MKT 或空值一律視為 0；OrderRequest.__post_init__ 已改成
    price_type 感知（只有 LMT 才要求 price>0），這裡不需要重複判斷、也不吞掉 LMT 真正的
    格式錯誤（非空但不合法的字串仍讓 Decimal() 自然拋錯，由呼叫端既有的 except 顯示錯誤）。"""
    stripped = (raw or "").strip()
    if price_type == "MKT" or not stripped:
        return Decimal("0")
    return Decimal(stripped)


def _parse_optional_update_price(raw: str | None) -> Decimal | None:
    """bug 1（simtrade 實測回歸）：改單表單的 price 欄位留白／整個缺席（`form.get()` 回
    `None`）一律視為「沿用既有值」（回 `None`，交給 `ShioajiAdapter.update` 的合併規則採用
    `order.price`），不得裸呼叫 `Decimal(form.get("price"))`——`Decimal(None)` 會炸
    `TypeError: conversion from NoneType to Decimal is not supported`。同 `_parse_order_price`
    一致：`(raw or "").strip()` 把 None 與空字串統一處理，兩者都不會走到裸的
    `Decimal(raw)` 呼叫；非空但格式不合法的字串仍讓 `Decimal()` 自然拋錯，交由呼叫端既有
    的 except 顯示表單錯誤。"""
    stripped = (raw or "").strip()
    return Decimal(stripped) if stripped else None


def _order_request_from_form(form, *, user_id: int) -> OrderRequest:
    """round3 #7：client_order_id 只在表單完全沒帶這個欄位時才 fallback 生新的
    （正常流程一律沿用 orders_page 首次渲染時寫進 hidden input 的那個值）。"""
    client_order_id = (form.get("client_order_id") or "").strip() or str(uuid.uuid4())
    price_type = form.get("price_type")
    return OrderRequest(
        client_order_id=client_order_id,
        symbol=form.get("symbol"), action=form.get("action"), qty=int(form.get("qty")),
        price=_parse_order_price(form.get("price"), price_type=price_type), price_type=price_type,
        order_type=form.get("order_type"), octype=form.get("octype"), user_id=user_id,
    )


def _place_confirm_dialog(session: Session, risk_guard, *, actor_user_id: int, req: OrderRequest,
                          account: str, mode: str) -> HTMLResponse:
    """簽發 place 的兩階段確認 token；hash 算法與 ShioajiAdapter.place 內部完全相同
    （同一組 req 欄位 + service.account/service.mode），故 token 一定驗得過。"""
    request_hash = canonical_payload_hash(
        symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
        price_type=req.price_type, order_type=req.order_type, octype=req.octype,
        account=account, mode=mode,
    )
    token = risk_guard.issue_confirm_token(session, actor_user_id=actor_user_id, payload_hash=request_hash)
    html = render_partial(
        "partials/confirm_dialog.html", kind="place", token=token,
        client_order_id=req.client_order_id, symbol=req.symbol, side=req.action, qty=req.qty,
        price=req.price, price_type=req.price_type, order_type=req.order_type, octype=req.octype,
        broker_order_id=None,
    )
    return HTMLResponse(
        html, status_code=200, headers={"HX-Retarget": ".confirm-slot", "HX-Reswap": "innerHTML"}
    )


def _find_order_for_service(session: Session, service, broker_order_id: str):
    broker = getattr(service, "broker", "shioaji")
    return brepo.find_order_by_broker_id(
        session, broker=broker, account=service.account, mode=service.mode, broker_order_id=broker_order_id,
    )


def _update_confirm_dialog(session: Session, risk_guard, service, *, actor_user_id: int,
                           broker_order_id: str, price, qty) -> HTMLResponse:
    """round3 #1 的關鍵修正點：先讀既有 Order，未提供的欄位（price/qty 任一為 None）沿用
    既有值，組成「這次改單後真正會送給券商的完整內容」，用與 ShioajiAdapter.update 完全
    相同的合併規則算 canonical hash——這樣簽出的 token，adapter 在第二次呼叫時重新用同一套
    規則（同一顆既有 Order，此刻仍未變動，因為第一次因缺 token 被拒時整筆交易已 rollback）
    重算出的 hash 才會相符，兩階段確認 round-trip 才會成功（不再鎖死）。

    不重覆驗證所有權：能走到這裡代表 service.update() 已經在更早的 AuthorizationError
    分支放行過（adapter 的 check_update/owner 驗證發生在 RiskError 之前），此處只是為了
    算 hash 才重新查一次同一筆 Order。
    """
    order = _find_order_for_service(session, service, broker_order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="找不到委託")
    new_price = price if price is not None else order.price
    new_qty = qty if qty is not None else order.qty
    request_hash = canonical_payload_hash(
        symbol=order.symbol, action=order.action, qty=new_qty, price=new_price,
        price_type=order.price_type, order_type=order.order_type, octype=order.octype,
        account=service.account, mode=service.mode,
    )
    token = risk_guard.issue_confirm_token(session, actor_user_id=actor_user_id, payload_hash=request_hash)
    html = render_partial(
        "partials/confirm_dialog.html", kind="update", token=token, client_order_id=None,
        symbol=order.symbol, side=order.action, qty=new_qty, price=new_price,
        price_type=order.price_type, order_type=order.order_type, octype=order.octype,
        broker_order_id=broker_order_id,
    )
    return HTMLResponse(html, status_code=200)


def _edit_form_error(session: Session, service, broker_order_id: str, message: str, *,
                     price=None, qty=None, banner_kind: str | None = None) -> HTMLResponse:
    """改單表單驗證/送出失敗：重新渲染同一個改單表單並帶上錯誤訊息，讓使用者原地修正重試
    （不像 `_form_error` 那樣用 HX-Retarget 打到頁面共用的 `.form-error-slot`——那個共用
    slot 在改單 modal 開著時仍可能被下單面板本身佔用，容易兩邊訊息互相打架）。

    `banner_kind`（007，P0-6）：帶值時額外用 HX-Trigger 觸發橫幅——與 `_form_error` 的
    `banner_kind` 同慣例，只在 post-submission 失敗（service.update() 拋出的
    RiskError/OrderError）才傳，表單解析錯誤（呼叫端另一條路徑）不傳、不進橫幅。"""
    order = _find_order_for_service(session, service, broker_order_id)
    if order is None:
        return _form_error(message, banner_kind=banner_kind)
    html = render_partial(
        "partials/order_edit_form.html", order=order,
        qty=qty if qty is not None else order.qty,
        price=price if price is not None else order.price,
        error=message,
    )
    headers = {}
    if banner_kind is not None:
        banner = _banner_event(kind=banner_kind, ok=False, title=_BANNER_FAIL_TITLES[banner_kind], detail=message)
        headers["HX-Trigger"] = _hx_trigger_header({"order-banner": banner})
    return HTMLResponse(html, status_code=200, headers=headers)


@router.get("/orders", response_class=HTMLResponse)
def orders_page(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
    risk_guard=Depends(get_order_risk_guard),
):
    """006：下單頁精簡化——只留報價列／表單／精簡條／agent 連線狀態／風控橫幅。委託表、
    部位表、mode 檢視分頁、改單 modal、Agent Token 產生區塊全部搬到「交易」大類的其他頁面
    （見 orders_queue_page/orders_deals_page/orders_holdings_page）或帳戶設定頁（R2-4，
    見 web/routers/auth.py::account_page）。本頁不再接受 `mode` query（下單表單一律送給
    server-side 決定的執行 mode，本來就不吃表單覆寫；委託列表的 mode 檢視已隨表格搬到
    /orders/queue，該頁自己接 `mode` 參數）。

    kill_switch／cooldown 仍需在這裡算出：R2-5 用來決定表單倉別是否鎖定「平倉」，
    以及風控橫幅是否顯示——這兩者都不需要 owner/admin 身份（一般登入者也看得到自己的
    急停/冷靜期狀態），故不再像搬家前那樣額外算 is_owner/is_admin/token_row/agent_blocked
    （那些只服務已移出本頁的 Agent Token／連線控制區塊）。

    終審必修 LOW-8：改回同步 `def`（FastAPI 會自動丟 threadpool 執行，離開 event
    loop）——本頁現在對「所有登入使用者」（不再只有 owner）都無條件呼叫
    `brepo.active_cooldown` 這個同步 DB 讀，`async def` 會讓這個同步呼叫直接卡在事件
    迴圈上，與專案既有慣例（`/orders/list`／`/orders/positions`／`/api/candles` 一律
    sync def 跑 threadpool，見本檔 `orders_list` 上方註解）牴觸。函式本體內沒有任何
    `await`，改成 sync 是無痛轉換。"""
    kill_switch = risk_guard.kill_switch_view(user.id) if risk_guard is not None else _DISABLED_KILL_SWITCH_VIEW
    now_ms = brepo.now_epoch_ms()
    cooldown = brepo.active_cooldown(session, user_id=user.id, now_ms=now_ms)
    # 009：exec_mode 反映 server 執行 mode（`app.state.order_service.mode`），與任何
    # `?mode=` 檢視參數無關；子系統停用（service is None）時 None，模板一律當非 sim
    # 處理（不顯示「模擬單」徽章——沒有東西在跑，顯示模擬單反而誤導）。
    exec_mode = service.mode if service is not None else None
    # 007（sim 確認視窗偏好）：sim 且使用者未勾選「不再顯示」時，送出前預設要跳確認視窗；
    # real 的兩階段確認是後端強制（見 broker/risk.py needs_confirm），完全不受這個偏好
    # 影響，這裡的判斷只管 sim 這一支。
    # F5（強制二次確認，尚未實作）預留：上線後這裡要在 `not user.skip_sim_confirm` 之前
    # 短路——`or risk_guard.force_confirm(user.id)` 之類——讓 F5 生效時無視使用者的
    # 「不再顯示」偏好，一律要求確認。
    sim_confirm_required = exec_mode == "sim" and not user.skip_sim_confirm
    return templates.TemplateResponse(request, "orders.html", {
        "active": "orders",
        "client_order_id": str(uuid.uuid4()), "service_available": service is not None,
        "symbols": ["TXF"], "kill_switch": kill_switch,
        "cooldown": cooldown, "cooldown_until_text": _fmt_cst(cooldown.until_ts) if cooldown else None,
        "exec_mode": exec_mode, "sim_confirm_required": sim_confirm_required,
    })


# ---- 006：「交易」大類的其餘三頁——委託／成交／未平倉（承接原下單頁被搬出的區塊） ----

@router.get("/orders/queue", response_class=HTMLResponse)
async def orders_queue_page(
    request: Request, user: User = Depends(get_current_user),
    service=Depends(get_order_service), mode: str | None = Query(None),
):
    """006：「委託」頁——承接原下單頁委託表的全部既有功能（mode 檢視分頁、改單 modal、
    取消、SSE 刷新）；改單/取消端點與 `/orders/list` partial 完全不變，本頁只是換了個
    殼。P0-1（mode 分頁選中態）隨 R2-10 共用 macro 根治。"""
    resolved_mode = _mode(mode or (service.mode if service is not None else None))
    return templates.TemplateResponse(request, "orders_queue.html", {
        "active": "orders_queue", "mode": resolved_mode,
    })


@router.get("/orders/deals", response_class=HTMLResponse)
async def orders_deals_page(
    request: Request, user: User = Depends(get_current_user),
    service=Depends(get_order_service), mode: str | None = Query(None),
):
    """006：「成交」頁——逐筆成交（時間/商品/方向/口數/價格/手續費），資料源既有 `Deal`
    表，零 schema 變更。"""
    resolved_mode = _mode(mode or (service.mode if service is not None else None))
    return templates.TemplateResponse(request, "orders_deals.html", {
        "active": "orders_deals", "mode": resolved_mode,
    })


@router.get("/orders/deals-list", response_class=HTMLResponse)
def orders_deals_list(
    session: Session = Depends(get_session), user: User = Depends(get_current_user),
    mode: str = Query("sim"),
):
    deals = brepo.list_deals(session, user_id=user.id, mode=_mode(mode))
    return HTMLResponse(render_partial("partials/deal_table.html", deals=deals))


@router.get("/orders/holdings", response_class=HTMLResponse)
async def orders_holdings_page(
    request: Request, user: User = Depends(get_current_user),
    service=Depends(get_order_service), mode: str | None = Query(None),
):
    """006：「未平倉」頁——承接原下單頁部位表＋浮動損益欄（見 `orders_positions`/
    `_positions_with_pnl`）。"""
    resolved_mode = _mode(mode or (service.mode if service is not None else None))
    return templates.TemplateResponse(request, "orders_holdings.html", {
        "active": "orders_holdings", "mode": resolved_mode,
    })


@router.post("/orders/agent-token", response_class=HTMLResponse)
async def issue_agent_token(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    risk_guard=Depends(get_order_risk_guard),
):
    """owner-only：簽發／rotation agent WS token（D2）。明文只在這次回應顯示一次——DB
    只存 hash，離開這個回應後無法再取得明文，只能重新產生（rotation，舊枚立即作廢）。
    子系統停用（risk_guard 為 None）時優雅回一個停用片段（比照 kill switch），不 500——
    按鈕正常情況下只在 is_owner 時才會渲染，這裡是防呆（如子系統在使用者開著頁面時被關）。"""
    if risk_guard is None:
        return HTMLResponse(render_partial(
            "partials/agent_token_control.html", token_row=None, raw_token=None, disabled=True,
        ))
    try:
        risk_guard.assert_owner(user.id)  # 非 owner → AuthorizationError → 403（比照 kill switch）
    except AuthorizationError:
        raise HTTPException(status_code=403, detail="not owner")
    ttl_days = get_settings().agent_token_ttl_days
    raw = agent_token_service.issue_token(session, user_id=user.id, ttl_days=ttl_days)
    token_row = agent_token_service.get_active_token(session, user_id=user.id)
    return HTMLResponse(render_partial(
        "partials/agent_token_control.html", token_row=token_row, raw_token=raw,
    ))


@router.post("/orders/kill-switch", response_class=HTMLResponse)
async def toggle_kill_switch(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
    risk_guard=Depends(get_order_risk_guard),
):
    """owner-only kill switch runtime 開關（runtime 即時生效，見 RiskGuard.set_kill_switch）。
    D3 拍板形：兩層——`scope='self'` 只翻 actor 自己的個人急停（本端點結構上沒有目標 user
    參數，server 端天然無法替他人翻閘）；`scope='global'` 翻全站總閘（沿用 Tier0 語意，
    任一 owner 可翻，火警拉桿原則）。翻 ON 只擋新單、不自動撤既有掛單（自動撤單危險，留給
    人工/T0.4）；改以告警列出當下未成交掛單數，提醒人工決定。子系統停用（risk_guard 為
    None）時優雅回一個停用片段，不 500。

    R2-1（D-1，2026-09-02）：`scope='self'` 的 403 gate 從「role=='admin'」改為「本人
    （owner）即可」——本端點結構上翻不到別人，把自我煞車真正交到使用者手上。
    `scope='global'` 會擋所有人下單，維持 admin-only 不變。兩種 scope 最終都仍要求
    actor 是 owner（`assert_owner`），這一層授權沒有被拿掉。"""
    if risk_guard is None:
        return HTMLResponse(
            render_partial(
                "partials/kill_switch_control.html", kill_switch=_DISABLED_KILL_SWITCH_VIEW, disabled=True
            )
        )
    form = await request.form()
    enabled = _parse_kill_switch_enabled(form.get("enabled"))
    scope = _parse_kill_switch_scope(form.get("scope"))
    if scope is None:
        raise HTTPException(status_code=400, detail="invalid scope（僅接受 self/global）")
    # scope=global 會擋所有人下單，維持 admin-only（2026-08-27 定案，R2-1 未改變這層）；
    # scope=self 只影響 actor 自己，2026-09-02 起開放帳號本人（owner）操作，不再要求 admin。
    if scope == "global" and user.role != "admin":
        raise HTTPException(status_code=403, detail="admin only")
    try:
        risk_guard.assert_owner(user.id)
    except AuthorizationError:
        raise HTTPException(status_code=403, detail="not owner")
    risk_guard.set_kill_switch(enabled, scope=scope, actor_user_id=user.id)  # runtime 即時生效
    open_count = _count_open_orders_best_effort(session, service)
    ops = getattr(request.app.state, "ops_alerter", None)
    if ops is not None:  # 告警本身絕不能反噬切換
        ops.kill_switch(enabled=enabled, actor_user_id=user.id, scope=scope, open_order_count=open_count)
    # 終審 HIGH #1：這個回應片段直接 outerHTML 換掉 #kill-switch-control，一定要重新算
    # self_readonly/global_readonly（同 risk_page 的邏輯），不能讓模板的
    # `global_readonly|default(false)` 落回 False——否則非 admin owner 翻完 scope=self
    # 後，畫面會多長出一顆「啟動全站緊急停止」表單（server 端仍 403，但違反 R2-1「全站對
    # 非 admin 唯讀」的 UI 契約，等於改壞了自己剛做的授權分流）。
    return HTMLResponse(
        render_partial(
            "partials/kill_switch_control.html",
            kill_switch=risk_guard.kill_switch_view(user.id), disabled=False,
            self_readonly=False, global_readonly=(user.role != "admin"),
        )
    )


# ---- 冷靜期（self-lockout）＋手動斷開 Agent（2026-08-22，D9/D11）----
_MAX_COOLDOWN_DAYS = 90
_COOLDOWN_CST = timezone(timedelta(hours=8))
# R2-2（D-2，2026-09-02）：啟動後 5 分鐘反悔窗——本人可自行取消；逾時後任何人（含 admin）
# 都無法提前解除，只能等到期（見 broker/repository.py::lift_cooldown_by_owner）。
_COOLDOWN_REVOKE_WINDOW_MS = 5 * 60_000
# R2-3（2026-09-02）：冷靜期快捷時長——伺服器端以「現在＋時長」換算到期時間（見
# set_cooldown 的 duration_preset 分支），不靠前端 JS 算數。
_COOLDOWN_PRESETS_MS = {"1h": 3_600_000, "1d": 86_400_000, "1w": 604_800_000}


def _fmt_cst(ms: int | None) -> str | None:
    """epoch-ms UTC → 台灣本地 'YYYY-MM-DD HH:MM' 顯示字串（固定 +08:00，台灣無 DST）。"""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=_COOLDOWN_CST).strftime("%Y-%m-%d %H:%M")


def _fmt_cst_sec(ms: int | None) -> str | None:
    """同 `_fmt_cst`，多帶秒——R2-2 反悔窗只有 5 分鐘，分鐘級精度會讓使用者以為畫面卡住
    不動，秒級才看得出視窗真的在倒數。"""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=_COOLDOWN_CST).strftime("%Y-%m-%d %H:%M:%S")


def _cooldown_cancel_window(cooldown) -> tuple[bool, str | None]:
    """R2-2（D-2）：5 分鐘反悔窗——啟動後 `_COOLDOWN_REVOKE_WINDOW_MS` 內本人可取消，
    逾時後任何人都不行。以 DB `created_ts`（真 UTC epoch-ms）計算，process 重啟不影響
    （不是 in-memory 計時）。回傳 (是否仍在窗內, 視窗到期時刻的顯示字串或 None)。"""
    if cooldown is None:
        return False, None
    now_ms = brepo.now_epoch_ms()
    deadline_ms = cooldown.created_ts + _COOLDOWN_REVOKE_WINDOW_MS
    can_cancel = now_ms < deadline_ms
    return can_cancel, (_fmt_cst_sec(deadline_ms) if can_cancel else None)


def _parse_cooldown_until(raw) -> int | None:
    """到期時間輸入以固定 +08:00 解析成真 UTC epoch-ms（與 repository.now_epoch_ms 同框可比）。
    flatpickr 送 'YYYY-MM-DD HH:MM'、fallback 的 datetime-local 送 'YYYY-MM-DDTHH:MM'，
    fromisoformat 兩者皆收；格式不符回 None。"""
    if not raw or not isinstance(raw, str):
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_COOLDOWN_CST)
    return int(dt.timestamp() * 1000)


def _connection_control(*, blocked: bool, disabled: bool = False) -> HTMLResponse:
    return HTMLResponse(render_partial(
        "partials/agent_connection_control.html", blocked=blocked, disabled=disabled,
    ))


def _cooldown_control(*, cooldown, error: str | None = None, disabled: bool = False) -> HTMLResponse:
    can_cancel, cancel_deadline_text = _cooldown_cancel_window(cooldown)
    return HTMLResponse(render_partial(
        "partials/cooldown_control.html",
        cooldown=cooldown, until_text=_fmt_cst(cooldown.until_ts) if cooldown else None,
        error=error, disabled=disabled,
        can_cancel=can_cancel, cancel_deadline_text=cancel_deadline_text,
    ))


@router.post("/orders/agent-disconnect", response_class=HTMLResponse)
async def agent_disconnect(
    request: Request,
    user: User = Depends(get_current_user),
    risk_guard=Depends(get_order_risk_guard),
    slot=Depends(get_agent_slot),
):
    """owner-only（D9）：手動斷開 agent——封鎖重連（in-memory gate）＋主動關閉現有 WS。比
    kill switch 更強（kill switch 只擋新單、連線仍在）。自助恢復見 /orders/agent-reconnect。
    子系統停用（risk_guard 為 None）優雅回停用片段，不 500。"""
    if risk_guard is None:
        return _connection_control(blocked=False, disabled=True)
    # 手動斷開/重連 Agent 收歸 admin-only（2026-08-27）：斷開會切斷連線＋封鎖重連，屬高權限
    # 控制，非 admin 測試者只用冷靜期自保。server 端強制，非只靠 UI 隱藏。
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="admin only")
    gate = getattr(request.app.state, "agent_connection_gate", None)
    if gate is not None:
        gate.block(user.id)
    if slot is not None:
        await slot.channel.force_close()  # 即時踢現有連線；離線標記由 agent_ws finally 接手
    return _connection_control(blocked=True)


@router.post("/orders/agent-reconnect", response_class=HTMLResponse)
async def agent_reconnect(
    request: Request,
    user: User = Depends(get_current_user),
    risk_guard=Depends(get_order_risk_guard),
):
    """owner-only（D9）：解除手動斷線封鎖——agent 端 supervisor 會自動重連（不需重開 App）。
    冷靜期的封鎖走 DB、不受此影響（仍 admin-only 解除）。"""
    if risk_guard is None:
        return _connection_control(blocked=False, disabled=True)
    if user.role != "admin":  # admin-only（2026-08-27，同 agent-disconnect）
        raise HTTPException(status_code=403, detail="admin only")
    gate = getattr(request.app.state, "agent_connection_gate", None)
    if gate is not None:
        gate.allow(user.id)
    return _connection_control(blocked=False)


@router.post("/orders/cooldown", response_class=HTMLResponse)
async def set_cooldown(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    risk_guard=Depends(get_order_risk_guard),
    slot=Depends(get_agent_slot),
):
    """owner-only（D5/D9）：建立冷靜期（self-lockout）——期間只能平倉、agent 一併斷線。
    R2-2（D-2，2026-09-02）：啟動後 5 分鐘反悔窗內本人可自行取消（見 `cancel_cooldown`）；
    逾時後任何人（含 admin）都無法提前解除，只能等到期——admin 提前解除的舊路徑已停用
    （見 web/routers/admin.py::lift_cooling_off）。R2-3：`duration_preset`
    （'1h'/'1d'/'1w'）快捷以「現在＋時長」換算到期時間，優先於 `until` 自訂欄位；未帶或
    值不合法時 fallback 走原本的 `until` 文字解析（flatpickr／datetime-local）。兩條路徑
    算出的到期時間都要通過同一套 until>now 且 ≤90 天檢查。已在冷靜期則拒絕（擋自我縮短/
    重設）。子系統停用優雅回停用片段。"""
    if risk_guard is None:
        return _cooldown_control(cooldown=None, disabled=True)
    try:
        risk_guard.assert_owner(user.id)
    except AuthorizationError:
        raise HTTPException(status_code=403, detail="not owner")
    now_ms = brepo.now_epoch_ms()
    existing = brepo.active_cooldown(session, user_id=user.id, now_ms=now_ms)
    if existing is not None:
        return _cooldown_control(
            cooldown=existing,
            error="你已在冷靜期中，無法變更或重新設定；如仍在啟動後 5 分鐘反悔窗內，"
                  "可用下方按鈕自行取消。",
        )
    form = await request.form()
    preset = str(form.get("duration_preset") or "").strip()
    if preset:
        preset_ms = _COOLDOWN_PRESETS_MS.get(preset)
        if preset_ms is None:
            return _cooldown_control(cooldown=None, error="不支援的快捷時長")
        until_ms = now_ms + preset_ms
    else:
        until_ms = _parse_cooldown_until(form.get("until"))
        if until_ms is None:
            return _cooldown_control(cooldown=None, error="請選擇有效的到期日期與時間")
    if until_ms <= now_ms:
        return _cooldown_control(cooldown=None, error="到期時間必須晚於現在")
    if until_ms > now_ms + _MAX_COOLDOWN_DAYS * 86_400_000:
        return _cooldown_control(cooldown=None, error=f"冷靜期最長 {_MAX_COOLDOWN_DAYS} 天")
    created = brepo.create_cooldown(session, user_id=user.id, until_ms=until_ms, now_ms=now_ms)
    session.commit()
    if created is None:  # 競態：另一請求先建立成功
        active = brepo.active_cooldown(session, user_id=user.id, now_ms=now_ms)
        return _cooldown_control(cooldown=active, error="你已在冷靜期中，無法變更")
    if slot is not None:
        await slot.channel.force_close()  # D3：進入冷靜期一併斷線（DB cooldown 擋後續重連）
    return _cooldown_control(cooldown=created)


@router.post("/orders/cooldown/cancel", response_class=HTMLResponse)
async def cancel_cooldown(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    risk_guard=Depends(get_order_risk_guard),
):
    """owner-only（R2-2／D-2，2026-09-02）：啟動後 5 分鐘反悔窗內本人可自行取消（化解
    誤觸）；逾時後任何人（含 admin）都無法提前解除，只能等到期——時間窗以 DB
    `created_ts`（真 UTC epoch-ms）判斷、寫進 SQL WHERE（見
    `broker.repository.lift_cooldown_by_owner`），process 重啟不影響，也不是只靠這裡少
    判斷一次就放行。admin 提前解除的舊路徑已停用（見 web/routers/admin.py）。"""
    if risk_guard is None:
        return _cooldown_control(cooldown=None, disabled=True)
    try:
        risk_guard.assert_owner(user.id)
    except AuthorizationError:
        raise HTTPException(status_code=403, detail="not owner")
    now_ms = brepo.now_epoch_ms()
    lifted = brepo.lift_cooldown_by_owner(
        session, user_id=user.id, now_ms=now_ms, window_ms=_COOLDOWN_REVOKE_WINDOW_MS,
    )
    if lifted:
        session.commit()
        return _cooldown_control(cooldown=None)
    existing = brepo.active_cooldown(session, user_id=user.id, now_ms=now_ms)
    if existing is None:
        return _cooldown_control(cooldown=None)
    return _cooldown_control(
        cooldown=existing, error="已超過 5 分鐘反悔窗，無法取消，只能等到期。",
    )


# ---- 005：獨立的「風險控管」頁 ----
@router.get("/risk", response_class=HTMLResponse)
async def risk_page(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    risk_guard=Depends(get_order_risk_guard),
):
    """緊急停止下單（原 kill switch，更名）——R2-1（D-1，2026-09-02）：`scope=self`（我的
    緊急停止）開放帳號本人操作，`scope=global`（全站）維持 admin-only。＋交易冷靜期
    （owner 皆可自我禁制，5 分鐘反悔窗見 R2-2）＋Agent 連線控制（admin-only，授權不變）
    集中一頁。本頁只是呈現層的搬家——各控制的實際切換仍走既有 POST 端點與既有授權判定
    （assert_owner／admin-only／R2-1 的 scope 分流），不在這裡重新決策。kill_switch_
    control.html 用 `self_readonly`/`global_readonly` 兩個獨立旗標分別控制兩層是否顯示
    切換表單。"""
    is_admin = user.role == "admin"
    is_owner = risk_guard is not None and risk_guard.is_owner(user.id)
    kill_switch = risk_guard.kill_switch_view(user.id) if risk_guard is not None else _DISABLED_KILL_SWITCH_VIEW
    now_ms = brepo.now_epoch_ms()
    cooldown = brepo.active_cooldown(session, user_id=user.id, now_ms=now_ms) if is_owner else None
    cooldown_can_cancel, cooldown_cancel_deadline_text = _cooldown_cancel_window(cooldown)
    gate = getattr(request.app.state, "agent_connection_gate", None)
    agent_blocked = bool(is_admin and gate is not None and gate.is_blocked(user.id))
    return templates.TemplateResponse(request, "risk.html", {
        "active": "risk", "is_admin": is_admin, "is_owner": is_owner, "kill_switch": kill_switch,
        "cooldown": cooldown, "cooldown_until_text": _fmt_cst(cooldown.until_ts) if cooldown else None,
        "cooldown_can_cancel": cooldown_can_cancel,
        "cooldown_cancel_deadline_text": cooldown_cancel_deadline_text,
        "agent_blocked": agent_blocked,
    })


# 委託/部位是每 2s 輪詢的唯讀端點，一律用同步 `def`（比照 /api/candles 慣例）跑 threadpool、
# 完全離開 event loop——否則這兩個每 2s 的同步 DB 讀會壓在單一 event loop 上，與餵 K 線的
# 報價 SSE/tick fan-out 搶 loop，造成下單延遲與 K 線凍住（見診斷）。positions 讀 DB 快照，
# 且**不搶 supervisor 序列化鎖**（positions_snapshot），不與每 1s 的 RawInboxWorker/place 競爭。
@router.get("/orders/list", response_class=HTMLResponse)
def orders_list(
    session: Session = Depends(get_session), user: User = Depends(get_current_user),
    mode: str = Query("sim"), service=Depends(get_order_service),
):
    orders = brepo.list_orders(session, user_id=user.id, mode=_mode(mode))
    live_mode = service.mode if service is not None else None
    return HTMLResponse(render_partial("partials/order_table.html", orders=orders, live_mode=live_mode))


def _positions_with_pnl(
    positions: list[Position], mark_price: Decimal | None
) -> list[tuple[Position, Decimal | None]]:
    """006：未平倉頁補浮動損益欄——重用 `unrealized_pnl`，不另寫數學；`mark_price` 為
    None（無報價）時每筆一律 None（不可用陳舊或缺值報價捏造損益），模板照既有 `.pnl`
    慣例把 None 顯示成「—」。"""
    if mark_price is None:
        return [(p, None) for p in positions]
    return [
        (p, unrealized_pnl(p.direction, p.avg_price, mark_price, p.qty, point_value(p.symbol)))
        for p in positions
    ]


@router.get("/orders/positions", response_class=HTMLResponse)
def orders_positions(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
    poller: QuotePoller | None = Depends(get_poller),
    risk_guard=Depends(get_order_risk_guard),
    mode: str | None = Query(None),
):
    """部位表（006 搬到獨立的「未平倉」頁）＋浮動損益欄。`mode` 省略或等於目前執行 mode
    時，走原本 `service.positions_snapshot()` 路徑——owner 檢查、行為與既有測試涵蓋的
    語意完全不變。R2-10：未平倉頁也套 mode 分頁，顯式指定為非目前執行 mode 時（如目前
    real 在跑、想看 sim 過去留下的未平倉部位），改直接查 BrokerPosition（純讀，比照
    `/orders/list` 對 `mode` 的既有慣例——mode 只過濾顯示範圍，不影響任何寫入路徑），
    owner 檢查改走 `risk_guard.assert_owner`（存在時；risk_guard 為 None 代表下單子系統
    停用，此時任一分支都回空清單，不需要驗證）。"""
    if service is None:
        positions: list[Position] = []
    elif mode is None or _mode(mode) == service.mode:
        try:
            positions = service.positions_snapshot(actor_user_id=user.id)
        except AuthorizationError:
            raise HTTPException(status_code=403, detail="not owner")
    else:
        if risk_guard is not None:
            try:
                risk_guard.assert_owner(user.id)
            except AuthorizationError:
                raise HTTPException(status_code=403, detail="not owner")
        rows = brepo.list_open_positions(
            session, user_id=user.id, broker=getattr(service, "broker", "shioaji"),
            account=service.account, mode=_mode(mode), symbol=service.symbol,
        )
        positions = [
            Position(symbol=r.symbol, direction=r.direction,
                     qty=brepo.remaining_qty(r), avg_price=brepo.avg_entry_price(r))
            for r in rows
        ]
    mark_price = poller.last.price if poller and poller.last else None
    rows = _positions_with_pnl(positions, mark_price)
    return HTMLResponse(render_partial("partials/position_table.html", rows=rows))


def _real_margin_provider(service) -> Decimal | None:
    """006/F4：real 模式可動用保證金——介面留好的縫，待 Shioaji margin API 查證（欄位/
    快取頻率，見 docs 的「查證 1」）後在此接上。目前尚未實作，一律回 None；呼叫端
    （`_available_margin`）與模板一律把 None 顯示成中性占位「—」，不可另外顯示 0 或
    捏造數字。"""
    return None


def _available_margin(service) -> Decimal | None:
    """006：精簡條「可動用保證金」是帳戶層級（不分商品）。sim 模式在虛擬保證金功能
    （批次 4，D1-D5）上線前固定回 None——顯示中性占位，不可顯示錯誤數字；real 模式走
    `_real_margin_provider` 這個介面縫，目前也回 None（同上）。子系統停用
    （service is None）比照 sim，回 None。"""
    if service is None or service.mode == "sim":
        return None
    return _real_margin_provider(service)


@router.get("/orders/position-strip", response_class=HTMLResponse)
def orders_position_strip(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
    poller: QuotePoller | None = Depends(get_poller),
):
    """006：下單頁「未平倉/浮動損益/可動用保證金」精簡條——symbol-scoped，跟著商品選擇器走。
    比照 002 quote 的 symbol-scoping 邊界（`dashboard._resolve_snap`）：`symbol` 與
    `service.symbol`（這個 adapter 追蹤的商品）不同時一律視為「未持有」，不可誤植別檔的
    部位；未持有時模板把部位/浮損兩段整段不 render（partials/position_strip.html），不是
    顯示 0。浮動損益重用 `journal/pnl.py::unrealized_pnl`，不另寫數學；point_value 重用
    `broker/position_tracker.py::point_value`，與自動部位結算共用同一份點值表。"""
    pos = None
    if service is not None and symbol and symbol == service.symbol:
        try:
            positions = [p for p in service.positions_snapshot(actor_user_id=user.id) if p.symbol == symbol]
        except AuthorizationError:
            raise HTTPException(status_code=403, detail="not owner")
        pos = positions[0] if positions else None  # 正常流程同一 symbol 只會有一個 open 方向
    mark_price = poller.last.price if poller and poller.last else None
    unrealized = None
    if pos is not None and mark_price is not None:
        unrealized = unrealized_pnl(pos.direction, pos.avg_price, mark_price, pos.qty, point_value(pos.symbol))
    margin = _available_margin(service)
    return HTMLResponse(render_partial(
        "partials/position_strip.html", pos=pos, unrealized=unrealized, margin=margin,
    ))


@router.get("/orders/stream")
async def orders_stream(request: Request, user: User = Depends(get_current_user)):
    """SSE：委託/成交/部位有變動時推一個 `orders-changed` 事件，取代下單頁每 2s 盲輪詢。
    主要發布者是 RawInboxWorker 的非同步成交落地（見 broker/inbox_worker.py）；動作當下的
    刷新仍由 place/cancel/update 回應的 `refreshorders` HX-Trigger 負責。hub 未接線
    （下單子系統停用或測試無 lifespan）時回空 stream、不 500。ping 不帶 per-user 資料，
    瀏覽器收到後各自重抓 user-scoped 的委託/部位（本就以 user_id 過濾 + 驗所有權），無跨用戶洩漏。

    007（批次 B-1）：這條連線同時訂閱 user-scoped 帶內容事件——`hub.subscribe(user.id)`
    讓這個連線既收得到既有廣播 ping，也收得到只發給這個 user 的 `deal`/`order-report`
    事件（見 broker/order_events.py）。同一顆 queue 兩種 item 並存：無資料 ping 是裸字串
    "1"（沿用既有 `orders-changed` 事件名/data，行為完全不變）；scoped 事件是
    `{"event": <str>, "payload": <dict>}`，依 `item["event"]` 分派成對應的 SSE 事件名，
    payload 序列化用 `default=str`（Decimal 等非原生 JSON 型別安全轉字串，不因為某個
    欄位型別意外炸掉整條串流）。"""
    hub = getattr(request.app.state, "order_events", None)
    if hub is None:
        return EventSourceResponse(iter(()))
    queue = hub.subscribe(user.id)

    async def event_generator():
        try:
            while True:
                item = await queue.get()
                if isinstance(item, dict):
                    yield {"event": item["event"], "data": json.dumps(item["payload"], default=str)}
                else:
                    yield {"event": "orders-changed", "data": item}  # 既有無資料 ping（恆為 "1"）
        finally:
            hub.unsubscribe(queue)

    return EventSourceResponse(event_generator())


@router.get("/orders/agent-status", response_class=HTMLResponse)
def orders_agent_status(
    request: Request, user: User = Depends(get_current_user), slot=Depends(get_agent_slot),
    risk_guard=Depends(get_order_risk_guard),
):
    """Task 9：agent 通道連線狀態 badge（僅 order_channel=="agent" 時顯示；inprocess 通道
    沒有「agent 連線」這個概念，partial 直接回空字串）。SSE `orders-changed`/`refreshorders`
    觸發時 orders.html 的 #agent-status-box 會重打這支端點刷新（見 orders.html）。

    Task 7（D9）：per-user 化——`agent_registry` 已 wiring 時改讀**自己**這個 slot 的
    `session_state`（每個 user 只看得到自己的 agent 連線狀態，不是全站共用一份）；registry
    不存在（in-process 模式／agent 模式尚未成功 wiring）時 fallback 回舊的全站
    `order_session_state`，與 Task 7 之前完全零改動的既有路徑（見 `get_agent_slot`）。

    終審必修 LOW-7：`is_owner` 一併算出——R2-4「未連線時提示到帳戶頁設定」的連結只對
    owner 有意義（帳戶頁的 Agent Token 卡片本身就是 owner-only，見
    web/routers/auth.py::_account_context／account.html）；非 owner 看到這個連結會是
    死路（過去點了也看不到任何卡片）。"""
    if getattr(request.app.state, "agent_registry", None) is not None:
        state = slot.session_state if slot is not None else None
    else:
        state = getattr(request.app.state, "order_session_state", None)
    is_owner = risk_guard is not None and risk_guard.is_owner(user.id)
    return HTMLResponse(render_partial(
        "partials/agent_status.html",
        channel=get_settings().order_channel,
        ready=bool(state and state.ready),
        reason=(state.last_error if state else None),
        is_owner=is_owner,
    ))


@router.post("/orders", response_class=HTMLResponse)
async def place_order(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
    risk_guard=Depends(get_order_risk_guard),
):
    if service is None:
        return _form_error("下單子系統目前未啟用")
    form = await request.form()
    try:
        req = _order_request_from_form(form, user_id=user.id)
    except (ValueError, TypeError, InvalidOperation) as exc:
        return _form_error(str(exc))

    confirm_token = form.get("confirm_token") or None
    try:
        await service.place(req, actor_user_id=user.id, confirm_token=confirm_token)
    except AuthorizationError:
        raise HTTPException(status_code=403, detail="not owner")
    except RiskError as exc:
        if exc.needs_confirm and risk_guard is not None:
            return _place_confirm_dialog(
                session, risk_guard, actor_user_id=user.id, req=req,
                account=getattr(service, "account", ""), mode=service.mode,
            )
        # 007（P0-4）：post-submission 失敗（如超過風控額度）——帶 banner_kind 觸發橫幅。
        return _form_error(_safe_str(exc, service), banner_kind="place")
    except OrderError as exc:
        return _form_error(_safe_str(exc, service), banner_kind="place")
    return _place_success(req)


@router.get("/orders/{broker_order_id}/edit", response_class=HTMLResponse)
async def edit_order_form(
    broker_order_id: str,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
):
    if service is None:
        return HTMLResponse('<div class="error-banner">下單子系統目前未啟用</div>')
    order = _find_order_for_service(session, service, broker_order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="找不到委託")
    if order.user_id != user.id:
        raise HTTPException(status_code=403, detail="not owner")
    html = render_partial(
        "partials/order_edit_form.html", order=order, qty=order.qty, price=order.price, error=None,
    )
    return HTMLResponse(html)


@router.delete("/orders/{broker_order_id}", response_class=HTMLResponse)
async def cancel_order(
    broker_order_id: str, user: User = Depends(get_current_user), service=Depends(get_order_service),
):
    if service is None:
        raise HTTPException(status_code=404, detail="order subsystem disabled")
    try:
        await service.cancel(broker_order_id, actor_user_id=user.id)
    except AuthorizationError:
        raise HTTPException(status_code=403, detail="not owner")
    except OrderError as exc:
        # 007（P0-5）：取消失敗過去完全看不到，現在額外帶 banner_kind 觸發橫幅。
        return _form_error(_safe_str(exc, service), banner_kind="cancel")
    # 007（P0-6）：取消成功過去沒有任何回饋，補一條橫幅。
    banner = _banner_event(kind="cancel", ok=True, title="取消成功", detail=f"委託 {broker_order_id}")
    return _orders_trigger(banner=banner)


@router.put("/orders/{broker_order_id}", response_class=HTMLResponse)
async def update_order(
    broker_order_id: str,
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
    risk_guard=Depends(get_order_risk_guard),
):
    if service is None:
        raise HTTPException(status_code=404, detail="order subsystem disabled")
    form = await request.form()
    try:
        # 同步 _parse_order_price 的空白/None 安全處理（bug 1/2）：留白或欄位整個缺席
        # （disabled 欄位不送出時 form.get() 回 None）＝沿用既有值（None，見下方
        # service.update 的合併規則），不強制歸零、也不裸呼叫 Decimal(None)——price_type
        # 不可經改單變更，MKT 委託的既有 price 本來就已經是 0（下單當下由
        # _parse_order_price 定的），這裡留白/缺席直接沿用既有值即可，不需要重新判斷
        # price_type。見 _parse_optional_update_price docstring。
        raw_qty = (form.get("qty") or "").strip()
        price = _parse_optional_update_price(form.get("price"))
        qty = int(raw_qty) if raw_qty else None
    except (ValueError, InvalidOperation) as exc:
        return _edit_form_error(session, service, broker_order_id, str(exc))

    confirm_token = form.get("confirm_token") or None
    try:
        await service.update(broker_order_id, actor_user_id=user.id, price=price, qty=qty,
                             confirm_token=confirm_token)
    except AuthorizationError:
        raise HTTPException(status_code=403, detail="not owner")
    except RiskError as exc:
        if exc.needs_confirm and risk_guard is not None:
            return _update_confirm_dialog(
                session, risk_guard, service, actor_user_id=user.id,
                broker_order_id=broker_order_id, price=price, qty=qty,
            )
        # 007（P0-6）：post-submission 失敗——帶 banner_kind 觸發橫幅。
        return _edit_form_error(session, service, broker_order_id, _safe_str(exc, service), price=price, qty=qty,
                                banner_kind="update")
    except OrderError as exc:
        return _edit_form_error(session, service, broker_order_id, _safe_str(exc, service), price=price, qty=qty,
                                banner_kind="update")
    # 007（P0-6）：改單成功過去沒有任何回饋，補一條橫幅。
    banner = _banner_event(kind="update", ok=True, title="改單成功", detail=f"委託 {broker_order_id}")
    return _orders_trigger(close_modal=True, banner=banner)
