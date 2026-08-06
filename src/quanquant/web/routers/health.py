"""Unauthenticated health check (GCP uptime check hits this without credentials)."""
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from quanquant.broker.session_state import OrderSessionState
from quanquant.poller import QuotePoller
from quanquant.web.deps import get_order_session_state, get_poller

router = APIRouter()


@router.get("/healthz")
async def healthz(
    poller: QuotePoller | None = Depends(get_poller),
    order_state: OrderSessionState | None = Depends(get_order_session_state),
):
    age: float | None = None
    if poller is not None and poller.last is not None:
        age = (datetime.now(timezone.utc) - poller.last.fetched_at).total_seconds()

    # T0.3：/healthz 健康碼。只有「下單子系統真故障」（connect 失敗/設定錯，disabled=False
    # 且未 ready）才回 503；「刻意停用」（disabled=True，缺金鑰的本機/唯讀部署）與 ready 皆算
    # 健康（200）。feed 停滯不在這裡翻 503（那由 OpsAlerter 盤中告警負責；此站無 failover，
    # 對短暫 feed 停滯翻 503 會連帶把看盤 UI 一起拉掉），只在 body 附 last_quote_age_s。
    order_section = None
    unhealthy = False
    if order_state is not None:
        if getattr(order_state, "disabled", False):
            sub_status = "disabled"
        elif order_state.ready:
            sub_status = "ready"
        else:
            sub_status = "unhealthy"
            unhealthy = True
        order_section = {
            "status": sub_status,
            "ready": order_state.ready,
            "last_error": order_state.last_error,
            "reconnect_attempts": order_state.reconnect_attempts,
        }
    return JSONResponse(
        {
            "status": "unhealthy" if unhealthy else "ok",
            "last_quote_age_s": age,
            "order_subsystem": order_section,
        },
        status_code=503 if unhealthy else 200,
    )
