"""Unauthenticated health check (GCP uptime check hits this without credentials)."""
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from quanquant.poller import QuotePoller
from quanquant.web.deps import get_poller

router = APIRouter()


@router.get("/healthz")
async def healthz(poller: QuotePoller | None = Depends(get_poller)):
    age: float | None = None
    if poller is not None and poller.last is not None:
        age = (datetime.now(timezone.utc) - poller.last.fetched_at).total_seconds()
    return JSONResponse({"status": "ok", "last_quote_age_s": age})
