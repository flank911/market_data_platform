import asyncio
import logging
from typing import List

from market_data.aggregate.aggregator import aggregate_from_1m
from market_data.common.models import Candle
from market_data.common.timeframes import tf_to_minutes
from market_data.config.settings import load_settings
from market_data.features.features import compute_features_inplace
from market_data.ingest.binance_fetcher import BinanceFetcher
from market_data.ingest.normalizer import normalize_binance_klines
from market_data.storage.clickhouse_client import ClickHouseClient
from market_data.validate.validators import validate_all_1m

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("run_mvp")


async def fetch_and_normalize_1m(settings) -> List[Candle]:
    fetcher = BinanceFetcher(settings)
    start_ms = int(settings.start_date_utc.timestamp() * 1000)
    end_ms = int(settings.end_date_utc.timestamp() * 1000)
    all_candles: List[Candle] = []
    async for page in fetcher.fetch_klines_paginated(
        symbol=settings.symbol,
        interval="1m",
        start_ms=start_ms,
        end_ms=end_ms,
    ):
        candles = normalize_binance_klines(page, settings.symbol, settings.exchange, "1m")
        all_candles.extend(candles)
    logger.info("Fetched %d raw 1m candles", len(all_candles))
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

    candles_1m: List[Candle] = asyncio.run(fetch_and_normalize_1m(settings))
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


