"""Live 1m candle construction from the snapshot stream.

Stateful but DB-free: feed FuturesSnapshots in, get Candle rows to upsert out.
Every snapshot yields an upsert of the in-progress bar (crash-safe; trivial
write load under SQLite WAL at one row per 5 seconds).

Volume: TAIFEX reports a cumulative session volume (CTotalVolume). Per-bucket
volume is the diff between consecutive snapshots; when the cumulative value
drops, a new session started and the raw value is the new baseline.

Trading-day gating uses `market_calendar.is_trading_session` (not bare time-of-
day) so phantom bars are never built on weekends/holidays, plus a day-session
data_date staleness net for ad-hoc closures the calendar doesn't know about.
"""
from datetime import datetime

from quanquant.candles.bucketing import bucket_start_ms, day_session_date
from quanquant.candles.market_calendar import is_trading_session
from quanquant.db.models import Candle, _utcnow
from quanquant.market_hours import CST
from quanquant.models import FuturesSnapshot


class CandleBuilder:
    def __init__(self, symbol: str) -> None:
        self._symbol = symbol
        self._prev_cum_vol: int | None = None
        self._prev_vol_contract: str | None = None  # identity that set _prev_cum_vol
        self._cur: Candle | None = None

    def on_snapshot(self, snap: FuturesSnapshot) -> list[Candle]:
        """Rows to upsert for this snapshot (0, 1, or 2 — old final + new bar)."""
        ts_ms = int(snap.fetched_at.timestamp() * 1000)
        session = is_trading_session(ts_ms)
        if session is None:  # closed / weekend / holiday — reset, emit nothing
            self._prev_cum_vol = None
            self._cur = None
            return []

        # Staleness net (day session only): on an unlisted closure the API replays
        # the last session with a stale CDate. Night data_date is ambiguous, so
        # day-only; rebuild passes data_date="" and is skipped.
        if session == "day" and self._is_stale_day_quote(snap.data_date, ts_ms):
            self._prev_cum_vol = None
            self._cur = None
            return []

        # 資料級新鮮度閘門（日/夜盤皆適用）：非新成交 → 凍結 in-progress bar，不出棒。
        # 不重置 _cur / _prev_cum_vol，讓下一筆 fresh 能接續同一根並正確累加量差。
        if not snap.is_fresh:
            return []

        bucket = bucket_start_ms(ts_ms, "1m")
        assert bucket is not None  # session is open

        dv = self._volume_delta(snap.volume, snap.contract_month)

        if self._cur is not None and self._cur.ts == bucket:
            cur = self._cur
            cur.high = max(cur.high, snap.price)
            cur.low = min(cur.low, snap.price)
            cur.close = snap.price
            cur.volume += dv
            cur.updated_at = _utcnow()
            return [cur]

        new_bar = Candle(
            symbol=self._symbol,
            timeframe="1m",
            ts=bucket,
            open=snap.price,
            high=snap.price,
            low=snap.price,
            close=snap.price,
            volume=dv,
            source="live",
            session=session,
            trading_date=day_session_date(ts_ms),
        )
        rows = [self._cur, new_bar] if self._cur is not None else [new_bar]
        self._cur = new_bar
        return rows

    @staticmethod
    def _is_stale_day_quote(data_date: str, ts_ms: int) -> bool:
        """True when a day-session quote's CDate is clearly older than today (CST)."""
        digits = "".join(ch for ch in data_date if ch.isdigit())
        if len(digits) < 8:
            return False  # empty / unknown format → don't block
        today = datetime.fromtimestamp(ts_ms / 1000, tz=CST).strftime("%Y%m%d")
        return digits[:8] < today

    def _volume_delta(self, cum_vol: int, contract: str) -> int:
        prev = self._prev_cum_vol
        prev_contract = self._prev_vol_contract
        self._prev_cum_vol = cum_vol
        self._prev_vol_contract = contract
        if prev is None or contract != prev_contract:
            # Baseline unknown, OR a different source/contract set it. Cumulative
            # counters aren't comparable across identities — the Shioaji stream
            # ("TXFG6") and the MIS fallback ("TXFG6-M"/"-F") keep separate session
            # totals, and a real contract roll starts a fresh count. Diffing across
            # them produced a phantom spike (e.g. a 40k 1m bar at a source switch),
            # so reset the baseline and emit no volume for this boundary tick.
            return 0
        if cum_vol >= prev:
            return cum_vol - prev
        return cum_vol  # cumulative dropped → new session restarted counting
