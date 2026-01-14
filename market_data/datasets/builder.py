from __future__ import annotations

import hashlib
import io
import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from market_data.common.timeframes import tf_to_minutes
from market_data.config.settings import AppSettings
from market_data.datasets.models import DatasetMeta
from market_data.features.atr import compute_atr_v1
from market_data.features.regimes import compute_trend_regime_v1, compute_volatility_regime_v1
from market_data.features.sessions import compute_session_code_v1
from market_data.features.trend import compute_trend_strength_ema_diff_v1
from market_data.features.volatility import compute_rolling_volatility_v1
from market_data.quality.reports import quality_report
from market_data.storage.clickhouse_client import ClickHouseClient


class DatasetBuilder:
    def __init__(self, settings: AppSettings, ch: ClickHouseClient) -> None:
        self.settings = settings
        self.ch = ch

    def _build_dataset_id(
        self,
        *,
        symbol: str,
        timeframe: int,
        date_from: datetime,
        date_to: datetime,
        feature_set: Dict[str, object],
        builder_version: int = 1,
    ) -> str:
        tf_str = f"{timeframe}m"
        df_str = date_from.strftime("%Y%m%d")
        dt_str = date_to.strftime("%Y%m%d")
        return f"ds_{symbol.lower()}_{tf_str}_{df_str}_{dt_str}_v{builder_version}"

    def _candles_df(
        self, *, symbol: str, timeframe: int, date_from: datetime, date_to: datetime
    ) -> pd.DataFrame:
        rows = self.ch.select_ohlcv(symbol=symbol, timeframe=timeframe, ts_from=date_from, ts_to=date_to)
        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        if not df.empty:
            df["ts"] = pd.to_datetime(df["ts"], utc=True)
        return df

    def _features_df(
        self,
        df_candles: pd.DataFrame,
        *,
        feature_set: Dict[str, object],
    ) -> pd.DataFrame:
        feats = pd.DataFrame(index=df_candles.index.copy())
        # ATR v1
        if feature_set.get("atr") == "v1":
            feats["atr_14_v1"] = compute_atr_v1(df_candles, window=14)
        # Rolling volatility
        vol_cfg = feature_set.get("volatility")
        if vol_cfg == "v1" or (isinstance(vol_cfg, dict) and vol_cfg.get("version") == "v1"):
            window = 30
            if isinstance(vol_cfg, dict):
                window = int(vol_cfg.get("window", window))
            feats[f"volatility_roll_{window}_v1"] = compute_rolling_volatility_v1(df_candles, window=window)
        # Trend strength (EMA diff)
        trend_cfg = feature_set.get("trend")
        if trend_cfg == "v1" or (isinstance(trend_cfg, dict) and trend_cfg.get("version") == "v1"):
            s = 12
            l = 26
            if isinstance(trend_cfg, dict):
                s = int(trend_cfg.get("short_span", s))
                l = int(trend_cfg.get("long_span", l))
            feats[f"trend_strength_ema_{s}_{l}_v1"] = compute_trend_strength_ema_diff_v1(df_candles, short_span=s, long_span=l)
        # Sessions
        if feature_set.get("sessions") == "v1":
            feats["session_code_v1"] = compute_session_code_v1(df_candles)
        return feats

    def _regimes_df(
        self,
        df_candles: pd.DataFrame,
        df_features: pd.DataFrame,
        *,
        feature_set: Dict[str, object],
    ) -> pd.DataFrame:
        reg = pd.DataFrame(index=df_candles.index.copy())
        # Volatility regime from ATR%
        if feature_set.get("volatility_regime") == "v1":
            # Find any atr_*_v1 column
            atr_cols = [c for c in df_features.columns if c.startswith("atr_") and c.endswith("_v1")]
            if not atr_cols:
                raise ValueError("volatility_regime_v1 requires ATR feature present")
            atr_col = atr_cols[0]
            reg["volatility_regime_v1"] = compute_volatility_regime_v1(
                pd.concat([df_candles[["close"]], df_features[[atr_col]]], axis=1),
                atr_col=atr_col,
                low_pct=self.settings.feature_thresholds.low_pct,
                mid_pct=self.settings.feature_thresholds.mid_pct,
            )
        # Trend regime from trend strength
        if feature_set.get("trend_regime") == "v1":
            ts_cols = [c for c in df_features.columns if c.startswith("trend_strength_") and c.endswith("_v1")]
            if not ts_cols:
                raise ValueError("trend_regime_v1 requires trend strength feature present")
            reg["trend_regime_v1"] = compute_trend_regime_v1(df_features, trend_strength_col=ts_cols[0], thr=0.0)
        return reg

    @staticmethod
    def _sha256_of_parquet(df: pd.DataFrame) -> str:
        table = pa.Table.from_pandas(df, preserve_index=False)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="zstd", coerce_timestamps="us", use_deprecated_int96_timestamps=False)
        return hashlib.sha256(buf.getvalue()).hexdigest()

    def build(
        self,
        *,
        symbol: str,
        timeframe: str,
        date_from: datetime,
        date_to: datetime,
        feature_set: Dict[str, object],
        publish: bool = True,
    ) -> Tuple[DatasetMeta, pd.DataFrame, pd.DataFrame]:
        if date_from.tzinfo is None:
            date_from = date_from.replace(tzinfo=timezone.utc)
        if date_to.tzinfo is None:
            date_to = date_to.replace(tzinfo=timezone.utc)
        timeframe_min = tf_to_minutes(timeframe)
        df_candles = self._candles_df(symbol=symbol, timeframe=timeframe_min, date_from=date_from, date_to=date_to)
        df_features = self._features_df(df_candles, feature_set=feature_set)
        df_regimes = self._regimes_df(df_candles, df_features, feature_set=feature_set)
        # Join and align on index
        df_features = pd.concat([df_features, df_regimes], axis=1)
        # Quality gate
        report = quality_report(
            df_candles,
            df_features,
            ts_from=date_from,
            ts_to=date_to,
            timeframe_min=timeframe_min,
        )
        if report["missing_pct"] > self.settings.quality_thresholds.max_gap_pct:
            raise RuntimeError(f"Quality gate failed: gaps {report['missing_pct']:.2f}% > {self.settings.quality_thresholds.max_gap_pct:.2f}%")
        nan_pct_max = max(report["nan_pct"].values()) if report["nan_pct"] else 0.0
        if nan_pct_max > self.settings.quality_thresholds.max_nan_feature_pct:
            raise RuntimeError(
                f"Quality gate failed: NaN features {nan_pct_max:.2f}% > {self.settings.quality_thresholds.max_nan_feature_pct:.2f}%"
            )
        # Checksum over content
        ch1 = self._sha256_of_parquet(df_candles)
        ch2 = self._sha256_of_parquet(df_features)
        checksum = hashlib.sha256(f"{ch1}:{ch2}".encode("utf-8")).hexdigest()
        # Build dataset_id
        ds_id = self._build_dataset_id(
            symbol=symbol,
            timeframe=timeframe_min,
            date_from=date_from,
            date_to=date_to,
            feature_set=feature_set,
            builder_version=1,
        )
        created_at = datetime.utcnow().replace(tzinfo=timezone.utc)
        meta = DatasetMeta(
            dataset_id=ds_id,
            symbol=symbol,
            timeframe=timeframe_min,
            date_from=date_from,
            date_to=date_to,
            feature_set=feature_set,
            checksum=checksum,
            created_at=created_at,
        )
        if publish:
            self.ch.insert_dataset_metadata(
                dataset_id=meta.dataset_id,
                symbol=meta.symbol,
                timeframe=meta.timeframe,
                date_from=meta.date_from,
                date_to=meta.date_to,
                feature_set_serialized=json.dumps(feature_set, sort_keys=True),
                checksum=meta.checksum,
                created_at=meta.created_at,
            )
        return meta, df_candles.reset_index(drop=True), df_features.reset_index(drop=True)


