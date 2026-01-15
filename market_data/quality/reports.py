from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Tuple

import pandas as pd


def _expected_rows_count(ts_from: datetime, ts_to: datetime, timeframe_min: int) -> int:
    if ts_from.tzinfo is None:
        ts_from = ts_from.replace(tzinfo=timezone.utc)
    if ts_to.tzinfo is None:
        ts_to = ts_to.replace(tzinfo=timezone.utc)
    delta_min = int((ts_to - ts_from).total_seconds() // 60)
    return max(0, delta_min // timeframe_min + 1)


def compute_completeness(df_candles: pd.DataFrame, ts_from: datetime, ts_to: datetime, timeframe_min: int) -> Tuple[int, float]:
    expected = _expected_rows_count(ts_from, ts_to, timeframe_min)
    actual = int(df_candles.shape[0])
    missing = max(0, expected - actual)
    missing_pct = (missing / expected * 100.0) if expected > 0 else 0.0
    return missing, missing_pct


def compute_gaps(df_candles: pd.DataFrame, timeframe_min: int) -> int:
    if df_candles.empty:
        return 0
    ts_sorted = df_candles["ts"].sort_values().reset_index(drop=True)
    deltas = ts_sorted.diff().dropna().dt.total_seconds().div(60).astype(int)
    gaps = deltas.sub(timeframe_min).clip(lower=0)
    return int(gaps.sum())


def feature_nan_outliers(df_features: pd.DataFrame) -> Dict[str, int]:
    return {col: int(df_features[col].isna().sum()) for col in df_features.columns}


def quality_report(
    df_candles: pd.DataFrame,
    df_features: pd.DataFrame,
    *,
    ts_from: datetime,
    ts_to: datetime,
    timeframe_min: int,
) -> Dict[str, object]:
    missing_count, missing_pct = compute_completeness(df_candles, ts_from, ts_to, timeframe_min)
    gaps = compute_gaps(df_candles, timeframe_min)
    nan_counts = feature_nan_outliers(df_features)
    total_rows = int(df_features.shape[0]) if not df_features.empty else 0
    nan_pct = {k: (v / total_rows * 100.0 if total_rows > 0 else 0.0) for k, v in nan_counts.items()}
    return {
        "missing_count": missing_count,
        "missing_pct": missing_pct,
        "gaps": gaps,
        "nan_counts": nan_counts,
        "nan_pct": nan_pct,
        "rows": total_rows,
    }



