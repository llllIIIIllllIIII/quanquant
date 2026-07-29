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
import uuid
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from sqlmodel import Session
from sse_starlette.sse import EventSourceResponse

from quanquant.broker import repository as brepo
from quanquant.broker.base import AuthorizationError, OrderError, RiskError
from quanquant.broker.redaction import redact_secrets
from quanquant.broker.types import OrderRequest, canonical_payload_hash
from quanquant.db.models import User
from quanquant.web.deps import get_current_user, get_session
from quanquant.web.templating import render_partial, templates

router = APIRouter()


def _safe_str(exc: Exception, service) -> str:
    """F8：`OrderError`/`RiskError` 訊息可能源自 adapter 未 redact 的路徑（防禦性，即使
    adapter 內部已對已知分支 redact，這裡是回顯給瀏覽器前的最後一道防線）。`service` 就是
    `ShioajiAdapter` instance，經 `secrets_to_redact` property 取得同一份秘密清單。"""
    return redact_secrets(str(exc), secrets=getattr(service, "secrets_to_redact", []))


def get_order_service(request: Request):
    return getattr(request.app.state, "order_service", None)


def get_order_risk_guard(request: Request):
    return getattr(request.app.state, "order_risk_guard", None)


def _mode(raw: str | None) -> str:
    return raw if raw in ("sim", "real") else "sim"


def _parse_kill_switch_enabled(raw) -> bool:
    """HTMX 隱藏欄位 `enabled` 送 "true"/"false"（也容忍 1/on/yes）；其餘一律視為 False。"""
    return str(raw or "").strip().lower() in ("true", "1", "on", "yes")


def _count_open_orders_best_effort(session: Session, service) -> int:
    """kill switch 告警用的未成交掛單數：best-effort，計數失敗一律回 0，絕不擋住切換。"""
    try:
        mode = getattr(service, "mode", None) if service is not None else None
        return brepo.count_open_orders(session, mode=mode)
    except Exception:  # noqa: BLE001 — 計數只供告警參考，任何失敗都不得反噬切換
        return 0


def _form_error(message: str) -> HTMLResponse:
    html = render_partial("partials/form_error.html", message=message)
    return HTMLResponse(
        html, status_code=200, headers={"HX-Retarget": ".form-error-slot", "HX-Reswap": "innerHTML"}
    )


def _orders_trigger(*, close_modal: bool = False) -> HTMLResponse:
    events = "closeordermodal, refreshorders" if close_modal else "refreshorders"
    return HTMLResponse("", headers={"HX-Trigger": events})


def _place_success() -> HTMLResponse:
    """下單成功：body 帶一個 out-of-band swap，把下單面板的 client_order_id hidden input
    換成全新 UUID——client_order_id 由 orders_page 首渲染時生成一次（round3 #7，同一張表單的
    HTTP retry 沿用同鍵才能冪等去重），但成功送出後若不換鍵，下一筆（尤其反向/不同 payload）
    會沿用同一顆鍵、被 repository 冪等防護擋成「同鍵不同 payload」。只在**成功**路徑換鍵，
    失敗/需確認時不換（保留 retry 冪等）。同時觸發 refreshorders 刷新委託/部位列表。"""
    html = render_partial("partials/client_order_id_input.html", client_order_id=str(uuid.uuid4()))
    return HTMLResponse(html, headers={"HX-Trigger": "refreshorders"})


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
                     price=None, qty=None) -> HTMLResponse:
    """改單表單驗證/送出失敗：重新渲染同一個改單表單並帶上錯誤訊息，讓使用者原地修正重試
    （不像 `_form_error` 那樣用 HX-Retarget 打到頁面共用的 `.form-error-slot`——那個共用
    slot 在改單 modal 開著時仍可能被下單面板本身佔用，容易兩邊訊息互相打架）。"""
    order = _find_order_for_service(session, service, broker_order_id)
    if order is None:
        return _form_error(message)
    html = render_partial(
        "partials/order_edit_form.html", order=order,
        qty=qty if qty is not None else order.qty,
        price=price if price is not None else order.price,
        error=message,
    )
    return HTMLResponse(html, status_code=200)


@router.get("/orders", response_class=HTMLResponse)
async def orders_page(
    request: Request,
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
    risk_guard=Depends(get_order_risk_guard),
    mode: str | None = Query(None),
):
    resolved_mode = _mode(mode or (service.mode if service is not None else None))
    # kill switch 控制只給 owner 看（server 端切換仍一律經 assert_owner，非只靠前端隱藏）；
    # risk_guard 可能為 None（下單子系統停用）——此時無 owner、也不顯示控制。
    is_owner = risk_guard is not None and risk_guard.is_owner(user.id)
    kill_switch = risk_guard.kill_switch if risk_guard is not None else False
    return templates.TemplateResponse(request, "orders.html", {
        "active": "orders", "mode": resolved_mode,
        "client_order_id": str(uuid.uuid4()), "service_available": service is not None,
        "symbols": ["TXF"], "is_owner": is_owner, "kill_switch": kill_switch,
    })


@router.post("/orders/kill-switch", response_class=HTMLResponse)
async def toggle_kill_switch(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    service=Depends(get_order_service),
    risk_guard=Depends(get_order_risk_guard),
):
    """owner-only 全域 kill switch runtime 開關（runtime 即時生效，見 RiskGuard.set_kill_switch）。
    翻 ON 只擋新單、不自動撤既有掛單（自動撤單危險，留給人工/T0.4）；改以告警列出當下未成交
    掛單數，提醒人工決定。子系統停用（risk_guard 為 None）時優雅回一個停用片段，不 500。"""
    if risk_guard is None:
        return HTMLResponse(
            render_partial("partials/kill_switch_control.html", kill_switch=False, disabled=True)
        )
    try:
        risk_guard.assert_owner(user.id)  # 非 owner → AuthorizationError → 403（比照 positions L232-233）
    except AuthorizationError:
        raise HTTPException(status_code=403, detail="not owner")
    form = await request.form()
    enabled = _parse_kill_switch_enabled(form.get("enabled"))
    risk_guard.set_kill_switch(enabled)  # runtime 即時生效
    open_count = _count_open_orders_best_effort(session, service)
    ops = getattr(request.app.state, "ops_alerter", None)
    if ops is not None:  # 告警本身絕不能反噬切換
        ops.kill_switch(enabled=enabled, actor_user_id=user.id, open_order_count=open_count)
    return HTMLResponse(
        render_partial("partials/kill_switch_control.html", kill_switch=enabled, disabled=False)
    )


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


@router.get("/orders/positions", response_class=HTMLResponse)
def orders_positions(user: User = Depends(get_current_user), service=Depends(get_order_service)):
    if service is None:
        return HTMLResponse(render_partial("partials/position_table.html", positions=[]))
    try:
        positions = service.positions_snapshot(actor_user_id=user.id)
    except AuthorizationError:
        raise HTTPException(status_code=403, detail="not owner")
    return HTMLResponse(render_partial("partials/position_table.html", positions=positions))


@router.get("/orders/stream")
async def orders_stream(request: Request, user: User = Depends(get_current_user)):
    """SSE：委託/成交/部位有變動時推一個 `orders-changed` 事件，取代下單頁每 2s 盲輪詢。
    主要發布者是 RawInboxWorker 的非同步成交落地（見 broker/inbox_worker.py）；動作當下的
    刷新仍由 place/cancel/update 回應的 `refreshorders` HX-Trigger 負責。hub 未接線
    （下單子系統停用或測試無 lifespan）時回空 stream、不 500。ping 不帶 per-user 資料，
    瀏覽器收到後各自重抓 user-scoped 的委託/部位（本就以 user_id 過濾 + 驗所有權），無跨用戶洩漏。"""
    hub = getattr(request.app.state, "order_events", None)
    if hub is None:
        return EventSourceResponse(iter(()))
    queue = hub.subscribe()

    async def event_generator():
        try:
            while True:
                await queue.get()
                yield {"event": "orders-changed", "data": "1"}
        finally:
            hub.unsubscribe(queue)

    return EventSourceResponse(event_generator())


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
        return _form_error(_safe_str(exc, service))
    except OrderError as exc:
        return _form_error(_safe_str(exc, service))
    return _place_success()


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
        return _form_error(_safe_str(exc, service))
    return _orders_trigger()


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
        return _edit_form_error(session, service, broker_order_id, _safe_str(exc, service), price=price, qty=qty)
    except OrderError as exc:
        return _edit_form_error(session, service, broker_order_id, _safe_str(exc, service), price=price, qty=qty)
    return _orders_trigger(close_modal=True)
