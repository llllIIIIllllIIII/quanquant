"""Persistence for Market Pulse runtime prefs (Telegram on/off).

Reuses the generic ChartState key-value store (symbol, kind="pulse", JSON
payload) so the web toggle survives restarts/deploys — no new table/migration.
"""
import json

from sqlmodel import Session, select

from quanquant.db.models import ChartState, _utcnow

_KIND = "pulse"


def load_telegram_enabled(db: Session, symbol: str) -> bool | None:
    """Persisted Telegram-enabled flag, or None if never set (use config default)."""
    row = db.exec(
        select(ChartState).where(ChartState.symbol == symbol, ChartState.kind == _KIND)
    ).first()
    if row is None:
        return None
    try:
        return bool(json.loads(row.payload).get("telegramEnabled"))
    except (ValueError, TypeError):
        return None


def save_telegram_enabled(db: Session, symbol: str, enabled: bool) -> None:
    """Persist the Telegram-enabled flag (select-or-create upsert, like ChartState)."""
    row = db.exec(
        select(ChartState).where(ChartState.symbol == symbol, ChartState.kind == _KIND)
    ).first()
    payload = json.dumps({"telegramEnabled": bool(enabled)})
    if row is None:
        db.add(ChartState(symbol=symbol, kind=_KIND, payload=payload))
    else:
        row.payload = payload
        row.updated_at = _utcnow()
    db.commit()
