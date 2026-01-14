import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Sequence, Tuple

from market_data.aggregate.aggregator import aggregate_from_1m
from market_data.common.models import Candle
from market_data.common.timeframes import tf_to_minutes
from market_data.config.settings import load_settings
from market_data.features.features import compute_features_inplace
from market_data.ingest.binance_fetcher import BinanceFetcher
from market_data.ingest.normalizer import normalize_binance_klines
from market_data.storage.clickhouse_client import ClickHouseClient
from market_data.validate.validators import validate_all_1m
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("run_mvp")


def _compute_missing_ranges_seconds(
    ch: ClickHouseClient,
    *,
    symbol: str,
    timeframe_minutes: int,
    ts_from: datetime,
    ts_to: datetime,
) -> Tuple[List[Tuple[int, int]], int]:
    """
    Return list of missing contiguous (start_sec, end_sec) ranges at 1m granularity and total missing count.
    """
    if ts_from.tzinfo is None:
        ts_from = ts_from.replace(tzinfo=timezone.utc)
    if ts_to.tzinfo is None:
        ts_to = ts_to.replace(tzinfo=timezone.utc)
    step_sec = timeframe_minutes * 60
    existing_seconds = ch.select_existing_timestamps(
        symbol=symbol,
        timeframe=timeframe_minutes,
        ts_from=ts_from,
        ts_to=ts_to,
    )
    start_sec = int(ts_from.timestamp())
    end_sec = int(ts_to.timestamp())
    # Build missing seconds without storing all expected seconds at once
    missing_ranges: List[Tuple[int, int]] = []
    total_missing = 0
    current_range_start: int = -1
    prev_missing: int = -1
    cur = start_sec
    while cur <= end_sec:
        if cur not in existing_seconds:
            if current_range_start == -1:
                current_range_start = cur
            elif prev_missing != -1 and cur != prev_missing + step_sec:
                # Gap in missing; close previous range
                missing_ranges.append((current_range_start, prev_missing))
                current_range_start = cur
            prev_missing = cur
            total_missing += 1
        cur += step_sec
    if current_range_start != -1:
        missing_ranges.append((current_range_start, prev_missing))
    return missing_ranges, total_missing


async def fetch_and_normalize_1m(settings, ch: ClickHouseClient) -> List[Candle]:
    fetcher = BinanceFetcher(settings)
    start_ms = int(settings.start_date_utc.timestamp() * 1000)
    end_ms = int(settings.end_date_utc.timestamp() * 1000)
    # Compute missing 1m ranges from ClickHouse to avoid re-downloading existing data
    missing_ranges, total_missing = _compute_missing_ranges_seconds(
        ch,
        symbol=settings.symbol,
        timeframe_minutes=tf_to_minutes("1m"),
        ts_from=settings.start_date_utc,
        ts_to=settings.end_date_utc,
    )
    if total_missing == 0:
        logger.info("No missing 1m candles to fetch; skipping download")
        return []
    all_candles: List[Candle] = []
    with tqdm(total=total_missing, desc=f"Fetching {settings.symbol} 1m", unit="candle") as pbar:
        for start_sec, end_sec in missing_ranges:
            range_start_ms = start_sec * 1000
            range_end_ms = end_sec * 1000
            async for page in fetcher.fetch_klines_paginated(
                symbol=settings.symbol,
                interval="1m",
                start_ms=range_start_ms,
                end_ms=range_end_ms,
            ):
                candles = normalize_binance_klines(page, settings.symbol, settings.exchange, "1m")
                all_candles.extend(candles)
                pbar.update(len(candles))
    logger.info("Fetched %d missing raw 1m candles", len(all_candles))
    return all_candles


def run_pipeline() -> None:
    settings = load_settings()
    ch = ClickHouseClient(
        host=settings.clickhouse.host,
        port=settings.clickhouse.port,
        username=settings.clickhouse.username,
        password=settings.clickhouse.password,
        database=settings.clickhouse.database,
        secure=settings.clickhouse.secure,
    )
    with open("market_data/storage/ddl.sql", "r", encoding="utf-8") as f:
        ddl_sql = f.read()
    ch.create_tables(ddl_sql)

    candles_1m: List[Candle] = asyncio.run(fetch_and_normalize_1m(settings, ch))
    candles_1m = validate_all_1m(candles_1m)
    compute_features_inplace(candles_1m, settings.feature_thresholds)
    inserted = ch.insert_candles_idempotent(candles_1m)
    logger.info("Inserted %d 1m candles", inserted)

    for tf in settings.aggregate_timeframes:
        tf_min = tf_to_minutes(tf)
        agg = aggregate_from_1m(candles_1m, tf_min)
        compute_features_inplace(agg, settings.feature_thresholds)
        ins = ch.insert_candles_idempotent(agg)
        logger.info("Inserted %d %s candles", ins, tf)


if __name__ == "__main__":
    run_pipeline()



