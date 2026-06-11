"""Historical candle providers (backfill)."""
from quanquant.history.base import HistoryProvider
from quanquant.history.finmind_daily import FinMindDailyProvider
from quanquant.history.finmind_tick import FinMindTickProvider

_PROVIDERS: dict[str, type[HistoryProvider]] = {
    "finmind": FinMindDailyProvider,
    "finmind-tick": FinMindTickProvider,
}


def make_history_provider(name: str) -> HistoryProvider:
    try:
        factory = _PROVIDERS[name]
    except KeyError:
        valid = ", ".join(sorted(_PROVIDERS))
        raise ValueError(f"Unknown history provider {name!r}. Valid: {valid}") from None
    return factory()
