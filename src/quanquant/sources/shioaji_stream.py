"""Shioaji (永豐金) tick streaming producer.

Push-based live source: logs in on a worker thread, subscribes the front-month
TXF futures contract, and forwards every tick into the shared QuotePoller bus via
`loop.call_soon_threadsafe(hub.publish, ...)`. Downstream consumers (candle
builder, /quote SSE, alert engine) are unchanged — they still see QuoteEvents.

`shioaji` is imported lazily inside `_connect` so that merely importing this
module (e.g. in unit tests, or when source != "shioaji") never pulls the ~38MB
native client. Read-only market data needs only `api.login(key, secret)` — no CA
certificate (CA is order-only).

Robustness: the MIS fallback poll (see web/app.py) keeps the bus alive whenever
this stream is silent, so a login failure or API-shape mismatch degrades to 5s
polling rather than taking the site down. A watchdog forces a reconnect if ticks
stop arriving while the market is open.
"""
import asyncio
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from quanquant.candles.market_calendar import is_trading_session
from quanquant.models import FuturesSnapshot
from quanquant.poller import QuoteEvent, QuotePoller

log = logging.getLogger(__name__)

_WATCHDOG_INTERVAL = 30.0       # how often to check for a dead stream
_STALE_RECONNECT_SECONDS = 45.0  # no ticks this long while open → force reconnect


def _dec(value: object, fallback: str = "0") -> Decimal:
    try:
        return Decimal(str(value)) if value not in (None, "") else Decimal(fallback)
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(fallback)


class ShioajiStreamer:
    """Streams TXF ticks from Shioaji into a QuotePoller hub, with auto-reconnect."""

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        symbol: str,
        hub: QuotePoller,
        loop: asyncio.AbstractEventLoop,
        *,
        reconnect_delay: float = 10.0,
    ) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._symbol = symbol
        self._hub = hub
        self._loop = loop
        self._reconnect_delay = reconnect_delay
        self._api = None
        self._contract_code: str | None = None
        # Set on the solace callback thread; read by the asyncio watchdog. A plain
        # float assignment is atomic enough for a liveness timestamp.
        self._last_tick_monotonic: float | None = None

    async def run(self) -> None:
        """Connect + stream forever, reconnecting on failure. Cancellation logs out."""
        try:
            while True:
                try:
                    await asyncio.to_thread(self._connect)
                    self._last_tick_monotonic = time.monotonic()  # grace baseline
                    log.info("Shioaji stream connected: %s", self._contract_code)
                    await self._watchdog()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.warning(
                        "Shioaji stream error (%s); reconnecting in %.0fs",
                        exc, self._reconnect_delay,
                    )
                    await self._safe_logout()
                    await asyncio.sleep(self._reconnect_delay)
        finally:
            await self._safe_logout()

    async def _watchdog(self) -> None:
        """Idle while ticks flow; raise to trigger reconnect if the stream goes dead."""
        while True:
            await asyncio.sleep(_WATCHDOG_INTERVAL)
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            market_open = is_trading_session(now_ms) is not None
            silent_for = self._seconds_since_tick()
            if market_open and silent_for > _STALE_RECONNECT_SECONDS:
                raise RuntimeError(
                    f"no ticks for {silent_for:.0f}s while market open"
                )

    def _seconds_since_tick(self) -> float:
        if self._last_tick_monotonic is None:
            return float("inf")
        return time.monotonic() - self._last_tick_monotonic

    # --- worker-thread side (blocking shioaji calls) ---

    def _connect(self) -> None:
        """Blocking: login, resolve front contract, subscribe, register callback."""
        import shioaji as sj  # lazy: keep the native client off the import path

        api = sj.Shioaji()
        api.login(self._api_key, self._secret_key, fetch_contract=True)
        self._api = api

        contract = self._front_contract(api)
        self._contract_code = contract.code

        @api.on_tick_fop_v1()
        def _quote_callback(_exchange, tick):  # runs on solace thread
            self._on_tick(tick)

        api.quote.subscribe(
            contract,
            quote_type=sj.constant.QuoteType.Tick,
            version=sj.constant.QuoteVersion.v1,
        )

    def _front_contract(self, api):
        """Nearest non-expired concrete-month TXF contract (not continuous R1/R2)."""
        category = getattr(api.Contracts.Futures, self._symbol)
        today = datetime.now(timezone.utc).astimezone().strftime("%Y/%m/%d")
        months = [
            c for c in category
            if "R" not in c.code[len(self._symbol):]  # drop TXFR1/TXFR2 continuous
        ]
        if not months:
            raise RuntimeError(f"no month contracts for {self._symbol}")
        active = [c for c in months if (c.delivery_date or "") >= today]
        return min(active or months, key=lambda c: c.delivery_date or "9999/99/99")

    def _on_tick(self, tick) -> None:
        """Solace-thread callback: tick → QuoteEvent → publish on the event loop."""
        if getattr(tick, "simtrade", 0):
            return  # ignore simulated/test ticks — not a real market price
        self._last_tick_monotonic = time.monotonic()
        try:
            event = self._to_event(tick)
        except Exception as exc:  # never let a bad tick kill the callback
            log.debug("dropping malformed tick: %s", exc)
            return
        self._loop.call_soon_threadsafe(self._hub.publish, event)

    def _to_event(self, tick) -> QuoteEvent:
        snap = FuturesSnapshot(
            symbol=self._symbol,
            price=_dec(tick.close),
            change=_dec(getattr(tick, "price_chg", 0)),
            change_pct=float(getattr(tick, "pct_chg", 0) or 0),
            volume=int(getattr(tick, "total_volume", 0) or 0),  # cumulative (builder diffs it)
            open_price=_dec(getattr(tick, "open", 0)),
            high_price=_dec(getattr(tick, "high", 0)),
            low_price=_dec(getattr(tick, "low", 0)),
            fetched_at=datetime.now(timezone.utc),  # match TAIFEX path: bucket by wall clock
            data_date="",  # skip the day-replay staleness net (Shioaji doesn't replay)
            contract_month=self._contract_code or "",
        )
        return QuoteEvent(snapshot=snap, error=None, at=snap.fetched_at)

    async def _safe_logout(self) -> None:
        api, self._api = self._api, None
        if api is None:
            return
        try:
            await asyncio.to_thread(api.logout)
        except Exception:
            pass
