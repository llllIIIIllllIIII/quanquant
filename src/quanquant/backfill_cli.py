"""Backfill CLI: `quanquant-backfill daily` / `quanquant-backfill rebuild-1m`."""
import argparse
import asyncio
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from sqlmodel import Session, select

from quanquant.candles.builder import CandleBuilder
from quanquant.candles.repo import upsert_candles
from quanquant.config import get_settings
from quanquant.db.engine import get_engine, init_db
from quanquant.db.models import Candle, Quote
from quanquant.history import make_history_provider
from quanquant.models import FuturesSnapshot


def _to_rows(symbol: str, timeframe: str, candles) -> list[Candle]:
    return [
        Candle(
            symbol=symbol,
            timeframe=timeframe,
            ts=c.ts,
            open=c.open, high=c.high, low=c.low, close=c.close,
            volume=c.volume,
            source=c.source,
            session=c.session,
            trading_date=c.trading_date,
        )
        for c in candles
    ]


async def run_daily(symbol: str, start: date, end: date, provider_name: str) -> int:
    async with make_history_provider(provider_name) as provider:
        candles = await provider.fetch_candles(symbol, start, end)

    rows = _to_rows(symbol, provider.timeframe, candles)
    with Session(get_engine()) as session:
        upsert_candles(session, rows)
    return len(rows)


async def run_minute(symbol: str, start: date, end: date, provider_name: str) -> int:
    """Day-by-day tick→1m backfill: upserts per day (resumable, shows progress)."""
    total = 0
    async with make_history_provider(provider_name) as provider:
        day = start
        while day <= end:
            candles = await provider.fetch_candles(symbol, day, day)
            if candles:
                rows = _to_rows(symbol, provider.timeframe, candles)
                with Session(get_engine()) as session:
                    upsert_candles(session, rows)
                total += len(rows)
                print(f"  {day}: {len(rows)} bars (total {total})", flush=True)
            day += timedelta(days=1)
    return total


def run_rebuild_1m(symbol: str, days: int | None) -> int:
    """Replay stored raw quotes through CandleBuilder to (re)generate 1m candles."""
    builder = CandleBuilder(symbol)
    total = 0
    with Session(get_engine()) as session:
        stmt = (
            select(Quote)
            .where(Quote.symbol == symbol)
            .order_by(Quote.fetched_at)  # type: ignore[arg-type]
        )
        if days is not None:
            cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=days)
            stmt = stmt.where(Quote.fetched_at >= cutoff)
        pending: dict[tuple, Candle] = {}
        for quote in session.exec(stmt):
            snap = FuturesSnapshot(
                symbol=quote.symbol,
                price=quote.price,
                change=Decimal(0),
                change_pct=0.0,
                volume=quote.volume,
                open_price=quote.price,
                high_price=quote.price,
                low_price=quote.price,
                # quotes store naive UTC — attach tz before any .timestamp() math
                fetched_at=quote.fetched_at.replace(tzinfo=timezone.utc),
                data_date="",
                contract_month="",
            )
            for row in builder.on_snapshot(snap):
                row.source = "rebuild"
                pending[(row.symbol, row.timeframe, row.ts)] = row
        rows = list(pending.values())
        upsert_candles(session, rows)
        total = len(rows)
    return total


def main() -> None:
    parser = argparse.ArgumentParser(prog="quanquant-backfill", description="Candle backfill")
    sub = parser.add_subparsers(dest="command", required=True)

    p_daily = sub.add_parser("daily", help="backfill daily candles from an external provider")
    p_daily.add_argument("--symbol", default=None, help="default: settings.symbol")
    p_daily.add_argument("--start", default="2015-01-01", help="YYYY-MM-DD")
    p_daily.add_argument("--end", default=None, help="YYYY-MM-DD (default: today)")
    p_daily.add_argument("--provider", default="finmind")

    p_minute = sub.add_parser(
        "minute", help="backfill 1m candles from tick data (FinMind sponsor tier)"
    )
    p_minute.add_argument("--symbol", default=None, help="default: settings.symbol")
    p_minute.add_argument("--start", default=None, help="YYYY-MM-DD (default: 90 days ago)")
    p_minute.add_argument("--end", default=None, help="YYYY-MM-DD (default: today)")
    p_minute.add_argument("--provider", default="finmind-tick")

    p_rebuild = sub.add_parser("rebuild-1m", help="rebuild 1m candles from stored raw quotes")
    p_rebuild.add_argument("--symbol", default=None)
    p_rebuild.add_argument("--days", type=int, default=None, help="only last N days of quotes")

    args = parser.parse_args()
    init_db()
    settings = get_settings()
    symbol = args.symbol or settings.symbol

    try:
        if args.command == "daily":
            start = date.fromisoformat(args.start)
            end = date.fromisoformat(args.end) if args.end else date.today()
            count = asyncio.run(run_daily(symbol, start, end, args.provider))
            print(f"Upserted {count} daily candles for {symbol} ({start} → {end}).")
        elif args.command == "minute":
            start = (
                date.fromisoformat(args.start)
                if args.start
                else date.today() - timedelta(days=90)
            )
            end = date.fromisoformat(args.end) if args.end else date.today()
            count = asyncio.run(run_minute(symbol, start, end, args.provider))
            print(f"Upserted {count} 1m candles for {symbol} ({start} → {end}).")
        else:
            count = run_rebuild_1m(symbol, args.days)
            print(f"Rebuilt {count} 1m candles for {symbol} from raw quotes.")
    except (ValueError, OSError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
