from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Dict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from market_data.config.settings import load_settings
from market_data.datasets import DatasetBuilder
from market_data.storage.clickhouse_client import ClickHouseClient


def _write_parquet(df: pd.DataFrame, path: str) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression="zstd", coerce_timestamps="us", use_deprecated_int96_timestamps=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export dataset to Parquet")
    parser.add_argument("--dataset-id", required=True, help="Dataset ID")
    parser.add_argument("--format", default="parquet", choices=["parquet"], help="Export format")
    parser.add_argument("--out-dir", default="data/exports", help="Output base directory")
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

    meta_row = ch.get_dataset(args.dataset_id)
    if not meta_row:
        raise SystemExit(f"Dataset not found: {args.dataset_id}")
    feature_set: Dict[str, object] = json.loads(meta_row["feature_set"])
    date_from: datetime = meta_row["date_from"]
    date_to: datetime = meta_row["date_to"]
    timeframe_min: int = int(meta_row["timeframe"])
    symbol: str = str(meta_row["symbol"])

    builder = DatasetBuilder(settings, ch)
    # Rebuild deterministically without republishing
    _, df_candles, df_features = builder.build(
        symbol=symbol,
        timeframe=f"{timeframe_min}m",
        date_from=date_from,
        date_to=date_to,
        feature_set=feature_set,
        publish=False,
    )

    ds_dir = os.path.join(args.out_dir, args.dataset_id)
    os.makedirs(ds_dir, exist_ok=True)
    # Write Parquet
    _write_parquet(df_candles, os.path.join(ds_dir, "candles.parquet"))
    _write_parquet(df_features, os.path.join(ds_dir, "features.parquet"))
    # Write metadata
    metadata = {
        "dataset_id": args.dataset_id,
        "symbol": symbol,
        "timeframe": timeframe_min,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "feature_set": feature_set,
        "checksum": meta_row["checksum"],
        "created_at": meta_row["created_at"].isoformat(),
        "format": args.format,
        "contract": "dataset is immutable; features versioned; derived from ClickHouse OHLCV",
    }
    with open(os.path.join(ds_dir, "metadata.yaml"), "w", encoding="utf-8") as f:
        yaml.safe_dump(metadata, f, sort_keys=True)

    print(f"Exported dataset to: {ds_dir}")


if __name__ == "__main__":
    main()


