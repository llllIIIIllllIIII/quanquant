"""Market Pulse runtime controls: web toggle for Telegram notifications.

Audio is client-side (static/pulse.js); Telegram is a server-side push, so its
on/off lives here — applied live on the running PulseEngine and persisted (via
pulse.prefs) so it survives restarts/deploys.
"""
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import Session

from quanquant.config import get_settings
from quanquant.db.models import User
from quanquant.pulse import prefs
from quanquant.pulse.engine import PulseEngine
from quanquant.web.deps import get_pulse, get_session, require_admin

router = APIRouter()


class TelegramToggle(BaseModel):
    enabled: bool


@router.get("/api/pulse/state")
def pulse_state(pulse: PulseEngine | None = Depends(get_pulse)) -> dict:
    return {"telegramEnabled": bool(pulse.telegram_enabled) if pulse else False}


@router.put("/api/pulse/telegram")
def set_pulse_telegram(
    body: TelegramToggle,
    session: Session = Depends(get_session),
    pulse: PulseEngine | None = Depends(get_pulse),
    _: User = Depends(require_admin),
) -> dict:
    prefs.save_telegram_enabled(session, get_settings().symbol, body.enabled)
    if pulse is not None:
        pulse.telegram_enabled = body.enabled  # apply live
    return {"telegramEnabled": body.enabled}
