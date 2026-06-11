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
