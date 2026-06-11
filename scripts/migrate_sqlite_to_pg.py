"""One-time SQLite -> Postgres data migration.

Copies candles/quotes/trades/chart_states through SQLAlchemy Core using the
table objects' own types (DecimalText str<->Decimal round-trips exactly, so
values land byte-identical), resets the serial sequences of id-keyed tables,
then verifies row counts and aggregates — non-zero exit on any mismatch.

Usage (Postgres reachable via a local port publish, see docker-compose.override.yml):
    uv run python scripts/migrate_sqlite_to_pg.py \
        --pg "postgresql+psycopg://quanquant:<password>@127.0.0.1:5432/quanquant"
"""
from __future__ import annotations

import argparse
import sys

from sqlalchemy import create_engine, func, select, text
from sqlmodel import SQLModel

from quanquant.db import models  # noqa: F401  (registers tables on metadata)

TABLES = ["candles", "quotes", "trades", "chart_states"]
SERIAL_ID_TABLES = ["quotes", "trades", "chart_states"]
CHUNK = 1_000


def copy_table(src_conn, dst_conn, table, skip: bool) -> int:
    if skip:
        print(f"  {table.name}: skipped")
        return 0
    rows = [dict(m) for m in src_conn.execute(select(table)).mappings()]
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i : i + CHUNK]
        if chunk:
            dst_conn.execute(table.insert(), chunk)
    print(f"  {table.name}: copied {len(rows)} rows")
    return len(rows)


def verify(src_conn, dst_conn) -> list[str]:
    errors: list[str] = []
    meta = SQLModel.metadata

    for name in TABLES:
        table = meta.tables[name]
        n_src = src_conn.execute(select(func.count()).select_from(table)).scalar_one()
        n_dst = dst_conn.execute(select(func.count()).select_from(table)).scalar_one()
        status = "OK" if n_src == n_dst else "MISMATCH"
        print(f"  count {name}: sqlite={n_src} pg={n_dst} [{status}]")
        if n_src != n_dst:
            errors.append(f"{name}: count {n_src} != {n_dst}")

    candles = meta.tables["candles"]
    agg = select(
        candles.c.timeframe,
        func.count(),
        func.min(candles.c.ts),
        func.max(candles.c.ts),
        func.sum(candles.c.volume),
    ).group_by(candles.c.timeframe)
    src_agg = {r[0]: tuple(r[1:]) for r in src_conn.execute(agg)}
    dst_agg = {r[0]: tuple(r[1:]) for r in dst_conn.execute(agg)}
    for tf in sorted(src_agg | dst_agg):
        s, d = src_agg.get(tf), dst_agg.get(tf)
        status = "OK" if s == d else "MISMATCH"
        print(f"  candles[{tf}]: sqlite={s} pg={d} [{status}]")
        if s != d:
            errors.append(f"candles[{tf}]: {s} != {d}")

    states = meta.tables["chart_states"]
    q = select(states.c.symbol, states.c.kind, states.c.payload).order_by(
        states.c.symbol, states.c.kind
    )
    if list(src_conn.execute(q)) != list(dst_conn.execute(q)):
        errors.append("chart_states: payload mismatch")
        print("  chart_states payloads: MISMATCH")
    else:
        print("  chart_states payloads: OK")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite", default="sqlite:///./quanquant.db")
    parser.add_argument("--pg", required=True, help="postgresql+psycopg://... target URL")
    parser.add_argument("--skip-quotes", action="store_true", help="omit raw 5s quotes")
    parser.add_argument(
        "--truncate", action="store_true", help="empty target tables first (re-run safety)"
    )
    args = parser.parse_args()

    src_engine = create_engine(args.sqlite)
    dst_engine = create_engine(args.pg)

    print("Creating target schema (SQLModel.metadata.create_all)...")
    SQLModel.metadata.create_all(dst_engine)

    meta = SQLModel.metadata
    with src_engine.connect() as src_conn, dst_engine.begin() as dst_conn:
        if args.truncate:
            dst_conn.execute(text(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE"))
            print("Target tables truncated.")

        print("Copying tables...")
        for name in TABLES:
            copy_table(src_conn, dst_conn, meta.tables[name], skip=(name == "quotes" and args.skip_quotes))

        # Serial sequences still start at 1 after explicit-id inserts; without
        # setval the first new row on the target hits a duplicate key.
        for name in SERIAL_ID_TABLES:
            dst_conn.execute(
                text(
                    f"SELECT setval(pg_get_serial_sequence('{name}','id'),"
                    f" COALESCE((SELECT max(id) FROM {name}), 0) + 1, false)"
                )
            )
        print("Serial sequences reset.")

    with src_engine.connect() as src_conn, dst_engine.connect() as dst_conn:
        print("Verifying...")
        errors = verify(src_conn, dst_conn)

    if errors:
        print(f"\nMIGRATION FAILED — {len(errors)} mismatch(es):")
        for e in errors:
            print(f"  - {e}")
        return 1
    print("\nMigration verified OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
