"""Live 1m candle construction from the snapshot stream.

Stateful but DB-free: feed FuturesSnapshots in, get Candle rows to upsert out.
Every snapshot yields an upsert of the in-progress bar (crash-safe; trivial
write load under SQLite WAL at one row per 5 seconds).

Volume: TAIFEX reports a cumulative session volume (CTotalVolume). Per-bucket
volume is the diff between consecutive snapshots; when the cumulative value
drops, a new session started and the raw value is the new baseline.
"""
from quanquant.candles.bucketing import bucket_start_ms, day_session_date, session_of_ms
from quanquant.db.models import Candle, _utcnow
from quanquant.models import FuturesSnapshot


class CandleBuilder:
    def __init__(self, symbol: str) -> None:
        self._symbol = symbol
        self._prev_cum_vol: int | None = None
        self._cur: Candle | None = None

    def on_snapshot(self, snap: FuturesSnapshot) -> list[Candle]:
        """Rows to upsert for this snapshot (0, 1, or 2 — old final + new bar)."""
        ts_ms = int(snap.fetched_at.timestamp() * 1000)
        session = session_of_ms(ts_ms)
        if session is None:  # market closed — reset volume baseline, emit nothing
            self._prev_cum_vol = None
            self._cur = None
            return []

        bucket = bucket_start_ms(ts_ms, "1m")
        assert bucket is not None  # session is open

        dv = self._volume_delta(snap.volume)

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

    def _volume_delta(self, cum_vol: int) -> int:
        prev = self._prev_cum_vol
        self._prev_cum_vol = cum_vol
        if prev is None:
            return 0  # first tick after start: baseline unknown
        if cum_vol >= prev:
            return cum_vol - prev
        return cum_vol  # cumulative dropped → new session restarted counting
