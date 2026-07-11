"""Central freshness judgment for the snapshot stream.

單一有狀態 tracker 掛在 pub/sub 發布邊界（QuotePoller.publish），讓每個下游
（CandleBuilder、報價 SSE、alert）對同一筆快照看到一致的 is_fresh。"Fresh" =
這筆反映了「新成交」；休市/假日重播會凍結累積量與最後成交時間，兩者皆不前進即
判為 stale。

兩個獨立訊號 OR（分層聯集）：真實成交必使累積量前進，但若來源凍結某一路訊號，
用另一路救援；兩路皆凍 → stale。
"""
from dataclasses import replace
from datetime import datetime

from quanquant.models import FuturesSnapshot


class FreshnessTracker:
    def __init__(self) -> None:
        self._last_cum_vol: int | None = None
        self._last_contract: str | None = None
        self._last_trade_time: datetime | None = None
        self._last_advance_at: datetime | None = None

    @property
    def last_advance_at(self) -> datetime | None:
        """最近一筆判為 fresh 的快照牆鐘（UTC）。"""
        return self._last_advance_at

    def evaluate(self, snap: FuturesSnapshot) -> FuturesSnapshot:
        """回傳把 is_fresh 依量/成交時間前進填好的 snapshot。"""
        # 尊重來源已標記的 not-fresh（結算價 fallback）——tracker 只降級不升級。
        fresh = snap.is_fresh and self._is_fresh(snap)
        if fresh:
            self._last_advance_at = snap.fetched_at
        self._last_cum_vol = snap.volume
        self._last_contract = snap.contract_month
        if snap.trade_time is not None:
            self._last_trade_time = snap.trade_time
        return replace(snap, is_fresh=fresh)

    def _is_fresh(self, snap: FuturesSnapshot) -> bool:
        prev_vol = self._last_cum_vol
        # 新身分，或累積量下降（新盤重數）→ 以本筆為基準；有量才算 fresh。
        if (
            prev_vol is None
            or snap.contract_month != self._last_contract
            or snap.volume < prev_vol
        ):
            return snap.volume > 0
        volume_advanced = snap.volume > prev_vol
        time_advanced = (
            snap.trade_time is not None
            and self._last_trade_time is not None
            and snap.trade_time > self._last_trade_time
        )
        return volume_advanced or time_advanced
