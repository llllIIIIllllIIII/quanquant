from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class FuturesSnapshot:
    symbol: str
    price: Decimal
    change: Decimal
    change_pct: float
    volume: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    fetched_at: datetime       # UTC
    data_date: str             # trading date from API, e.g. "2026-06-04"
    contract_month: str        # nearest active contract, e.g. "202607"
    trade_time: datetime | None = None  # 來源最後成交時間（CST），解析失敗為 None
    is_fresh: bool = True       # 本筆是否為新成交（FreshnessTracker 於 publish 注入）
