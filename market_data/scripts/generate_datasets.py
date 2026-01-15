from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import pandas as pd
import yaml

from market_data.config.settings import load_settings
from market_data.datasets import DatasetBuilder
from market_data.export.export_dataset import _write_parquet
from market_data.storage.clickhouse_client import ClickHouseClient


def _year_windows(start: datetime, end: datetime) -> List[Tuple[datetime, datetime]]:
    windows: List[Tuple[datetime, datetime]] = []
    year = start.year
    while True:
        ws = datetime(year, 1, 1, tzinfo=timezone.utc)
        we = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        if we <= start:
            year += 1
            continue
        if ws >= end:
            break
        windows.append((max(ws, start), min(we, end)))
        year += 1
    return windows


def _write_subset(
    *,
    base_dir: str,
    dataset_id: str,
    tag: str,
    df_candles: pd.DataFrame,
    df_features: pd.DataFrame,
    metadata: Dict[str, object],
) -> None:
    out_dir = os.path.join(base_dir, f"{dataset_id}_{tag}")
    os.makedirs(out_dir, exist_ok=True)
    _write_parquet(df_candles, os.path.join(out_dir, "candles.parquet"))
    _write_parquet(df_features, os.path.join(out_dir, "features.parquet"))
    with open(os.path.join(out_dir, "metadata.yaml"), "w", encoding="utf-8") as f:
        yaml.safe_dump(metadata, f, sort_keys=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate research datasets (Stage 3)")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--timeframe", required=True, help="e.g. 15m")
    parser.add_argument("--strategy", default="research_v1")
    parser.add_argument("--start", required=False, help="YYYY-MM-DD")
    parser.add_argument("--end", required=False, help="YYYY-MM-DD")
    parser.add_argument("--out-dir", default="data/exports")
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
    builder = DatasetBuilder(settings, ch)

    start = settings.start_date_utc if not args.start else datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = settings.end_date_utc if not args.end else datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)

    if args.strategy != "research_v1":
        raise SystemExit(f"Unsupported strategy: {args.strategy}")

    feature_set = {
        "atr": "v1",
        "volatility": {"version": "v1", "window": 30},
        "trend": {"version": "v1", "short_span": 12, "long_span": 26},
        "sessions": "v1",
        "volatility_regime": "v1",
        "trend_regime": "v1",
        "cross_market": {"version": "v1", "window": 20},
        "regime_ml": {
            "version": "v1",
            "type": "hdbscan",
            "features": ["volatility_roll_30_v1", "funding_dev_roll_20_v1", "oi_delta_v1"],
            "params": {"min_cluster_size": 50, "min_samples": 10},
        },
    }

    for ws, we in _year_windows(start, end):
        meta, df_candles, df_features = builder.build(
            symbol=args.symbol,
            timeframe=args.timeframe,
            date_from=ws,
            date_to=we,
            feature_set=feature_set,
            publish=True,
        )
        base_dir = os.path.join(args.out_dir, meta.dataset_id)
        os.makedirs(base_dir, exist_ok=True)
        _write_parquet(df_candles, os.path.join(base_dir, "candles.parquet"))
        _write_parquet(df_features, os.path.join(base_dir, "features.parquet"))
        with open(os.path.join(base_dir, "metadata.yaml"), "w", encoding="utf-8") as f:
            yaml.safe_dump(
                {
                    "dataset_id": meta.dataset_id,
                    "symbol": meta.symbol,
                    "timeframe": meta.timeframe,
                    "date_from": meta.date_from.isoformat(),
                    "date_to": meta.date_to.isoformat(),
                    "feature_set": feature_set,
                    "checksum": meta.checksum,
                    "created_at": meta.created_at.isoformat(),
                    "strategy": args.strategy,
                },
                f,
                sort_keys=True,
            )

        # Derived splits (bull/bear) from trend_regime
        if "trend_regime_v1" in df_features.columns:
            bull_mask = df_features["trend_regime_v1"] == 2
            bear_mask = df_features["trend_regime_v1"] == 0
            for tag, mask in (("bull", bull_mask), ("bear", bear_mask)):
                _write_subset(
                    base_dir=args.out_dir,
                    dataset_id=meta.dataset_id,
                    tag=tag,
                    df_candles=df_candles[mask].reset_index(drop=True),
                    df_features=df_features[mask].reset_index(drop=True),
                    metadata={
                        "parent_dataset_id": meta.dataset_id,
                        "split": tag,
                        "row_count": int(mask.sum()),
                    },
                )

        # Regime-specific subsets
        if "regime_ml_v1" in df_features.columns:
            for label in sorted(df_features["regime_ml_v1"].unique()):
                mask = df_features["regime_ml_v1"] == label
                _write_subset(
                    base_dir=args.out_dir,
                    dataset_id=meta.dataset_id,
                    tag=f"regime_{int(label)}",
                    df_candles=df_candles[mask].reset_index(drop=True),
                    df_features=df_features[mask].reset_index(drop=True),
                    metadata={
                        "parent_dataset_id": meta.dataset_id,
                        "regime_label": int(label),
                        "row_count": int(mask.sum()),
                    },
                )


if __name__ == "__main__":
    main()


