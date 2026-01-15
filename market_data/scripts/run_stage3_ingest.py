from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from typing import Tuple

from market_data.config.settings import load_settings
from market_data.ingest.binance_fetcher import BinanceFetcher
from market_data.ingest.normalizer import (
    normalize_funding_rates,
    normalize_liquidations,
    normalize_open_interest,
)
from market_data.storage.clickhouse_client import ClickHouseClient


def _period_for_timeframe(tf_minutes: int) -> Tuple[int, str]:
    mapping = {
        5: "5m",
        15: "15m",
        30: "30m",
        60: "1h",
        120: "2h",
        240: "4h",
        360: "6h",
        720: "12h",
        1440: "1d",
    }
    if tf_minutes in mapping:
        return tf_minutes, mapping[tf_minutes]
    # default to 5m buckets for OI history when unsupported
    return 5, "5m"


async def main_async() -> None:
    parser = argparse.ArgumentParser(description="Stage 3 ingest: funding/OI/liquidations")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--timeframe-min", type=int, default=15)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    settings = load_settings()
    ch = ClickHouseClient(
        host=settings.clickhouse.host,
        port=settings.clickhouse.port,
        username=settings.clickhouse.username,
        password=settings.clickhouse.password,
        database=settings.clickhouse.database,
        secure=settings.clickhouse.secure,
    )
    fetcher = BinanceFetcher(settings)

    start_dt = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    # Funding rate history
    funding_raw = await fetcher.fetch_funding_rates(args.symbol, start_ms, end_ms)
    funding_rows = normalize_funding_rates(
        funding_raw, symbol=args.symbol, exchange=settings.exchange, timeframe_min=args.timeframe_min
    )
    ch.insert_funding_rates(funding_rows)

    # Open interest history
    tf_min, period = _period_for_timeframe(args.timeframe_min)
    oi_raw = await fetcher.fetch_open_interest_hist(args.symbol, period, start_ms, end_ms)
    oi_rows = normalize_open_interest(
        oi_raw, symbol=args.symbol, exchange=settings.exchange, timeframe_min=tf_min
    )
    ch.insert_open_interest(oi_rows)

    # Liquidations (aggregated)
    liq_raw = await fetcher.fetch_liquidations(args.symbol, start_ms, end_ms)
    liq_rows = normalize_liquidations(
        liq_raw, symbol=args.symbol, exchange=settings.exchange, timeframe_min=args.timeframe_min
    )
    ch.insert_liquidations(liq_rows)


if __name__ == "__main__":
    asyncio.run(main_async())


