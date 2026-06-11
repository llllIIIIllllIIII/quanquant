import asyncio
import sys

from quanquant.config import get_settings
from quanquant.display import format_snapshot
from quanquant.market_hours import get_session
from quanquant.poller import QuotePoller
from quanquant.sources.registry import make_source


async def watch_loop() -> None:
    settings = get_settings()
    print(f"QuanQuant — watching {settings.symbol}  (Ctrl+C to stop)\n")

    source = make_source(settings.source)
    poller = QuotePoller(source, settings.symbol, settings.poll_interval_seconds)
    queue = poller.subscribe()

    async with source:
        task = asyncio.create_task(poller.run())
        try:
            while True:
                event = await queue.get()
                if event.snapshot is not None:
                    print(format_snapshot(event.snapshot, get_session()), end="", flush=True)
                else:
                    print(f"\r[ERROR] {event.error}                    ", end="", flush=True)
        finally:
            task.cancel()
            poller.unsubscribe(queue)


def main() -> None:
    try:
        asyncio.run(watch_loop())
    except KeyboardInterrupt:
        print("\nBye.")
        sys.exit(0)
