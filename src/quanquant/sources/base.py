from abc import ABC, abstractmethod
from quanquant.models import FuturesSnapshot


class DataSource(ABC):
    """Abstract interface for all price data sources."""

    @abstractmethod
    async def fetch_snapshot(self, symbol: str) -> FuturesSnapshot:
        """Fetch the latest price snapshot for a futures contract."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Release any persistent connections."""
        ...

    async def __aenter__(self) -> "DataSource":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()
