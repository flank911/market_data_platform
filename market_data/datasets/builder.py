from __future__ import annotations

import hashlib
import io
import json
import os
from datetime import datetime, timezone
from typing import Dict, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from market_data.common.timeframes import tf_to_minutes
from market_data.config.settings import AppSettings
from market_data.datasets.models import DatasetMeta
from market_data.features.atr import compute_atr_v1
from market_data.features.cross_market import (
    compute_funding_deviation_v1,
    compute_liquidation_density_v1,
    compute_oi_delta_v1,
    compute_oi_volume_ratio_v1,
    compute_price_oi_divergence_v1,
)
from market_data.features.regimes import compute_trend_regime_v1, compute_volatility_regime_v1
from market_data.features.regimes import train_regime_model_v1
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

    @staticmethod
    def _infer_builder_version(feature_set: Dict[str, object]) -> int:
        if feature_set.get("cross_market") or feature_set.get("regime_ml"):
            return 3
        return 1

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
        df_signals: pd.DataFrame,
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
        # Cross-market features (requires signals)
        cross_cfg = feature_set.get("cross_market")
        if cross_cfg == "v1" or (isinstance(cross_cfg, dict) and cross_cfg.get("version") == "v1"):
            window = 20
            if isinstance(cross_cfg, dict):
                window = int(cross_cfg.get("window", window))
            df_join = pd.concat([df_candles, df_signals], axis=1)
            funding_dev = compute_funding_deviation_v1(df_join, window=window)
            feats[funding_dev.name] = funding_dev
            feats["oi_delta_v1"] = compute_oi_delta_v1(df_join)
            feats["oi_volume_ratio_v1"] = compute_oi_volume_ratio_v1(df_join)
            liq_density = compute_liquidation_density_v1(df_join, window=window)
            feats[liq_density.name] = liq_density
            feats["price_oi_divergence_v1"] = compute_price_oi_divergence_v1(df_join)
        return feats

    def _regimes_df(
        self,
        df_candles: pd.DataFrame,
        df_features: pd.DataFrame,
        df_signals: pd.DataFrame,
        *,
        feature_set: Dict[str, object],
        dataset_id: str,
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
        # ML regime detection (offline)
        ml_cfg = feature_set.get("regime_ml")
        if ml_cfg:
            if not isinstance(ml_cfg, dict):
                raise ValueError("regime_ml must be a dict with type/features/version")
            if ml_cfg.get("version") != "v1":
                raise ValueError("regime_ml supports only v1")
            model_type = str(ml_cfg.get("type", "hdbscan"))
            features = list(ml_cfg.get("features", []))
            if not features:
                raise ValueError("regime_ml requires non-empty features list")
            df_join = pd.concat([df_features, df_signals, df_candles[["close"]]], axis=1)
            labels, artifact = train_regime_model_v1(
                df_join,
                dataset_id=dataset_id,
                model_type=model_type,
                features=features,
                params=dict(ml_cfg.get("params", {})),
            )
            reg["regime_ml_v1"] = labels
            self._persist_regime_model(artifact)
        return reg

    def _signals_df(
        self,
        *,
        symbol: str,
        timeframe: int,
        date_from: datetime,
        date_to: datetime,
        df_candles: pd.DataFrame,
    ) -> pd.DataFrame:
        if df_candles.empty:
            return pd.DataFrame(index=df_candles.index.copy())
        idx = pd.to_datetime(df_candles["ts"], utc=True)
        out = pd.DataFrame(index=idx)

        funding_rows = self.ch.select_funding_rates(
            symbol=symbol, timeframe=timeframe, ts_from=date_from, ts_to=date_to
        )
        if funding_rows:
            s = pd.Series(
                {pd.to_datetime(r[0], utc=True): float(r[1]) for r in funding_rows},
                dtype="float64",
            )
            out["funding_rate"] = s.reindex(out.index).ffill().fillna(0.0)
        else:
            out["funding_rate"] = 0.0

        oi_rows = self.ch.select_open_interest(symbol=symbol, timeframe=timeframe, ts_from=date_from, ts_to=date_to)
        if oi_rows:
            s = pd.Series(
                {pd.to_datetime(r[0], utc=True): float(r[1]) for r in oi_rows},
                dtype="float64",
            )
            out["open_interest"] = s.reindex(out.index).ffill().fillna(0.0)
        else:
            out["open_interest"] = 0.0

        liq_rows = self.ch.select_liquidations(symbol=symbol, timeframe=timeframe, ts_from=date_from, ts_to=date_to)
        if liq_rows:
            liq_map = {pd.to_datetime(r[0], utc=True): (float(r[1]), float(r[2]), int(r[3])) for r in liq_rows}
            liq_df = pd.DataFrame.from_dict(
                liq_map, orient="index", columns=["liquidation_qty", "liquidation_notional", "liquidation_count"]
            )
            liq_df = liq_df.reindex(out.index).fillna(0.0)
            out = pd.concat([out, liq_df], axis=1)
        else:
            out["liquidation_qty"] = 0.0
            out["liquidation_notional"] = 0.0
            out["liquidation_count"] = 0
        return out.reset_index(drop=True)

    def _persist_regime_model(self, artifact) -> None:
        model_dir = "data/models"
        os.makedirs(model_dir, exist_ok=True)
        path = os.path.join(model_dir, f"{artifact.model_id}.json")
        payload = {
            "model_id": artifact.model_id,
            "dataset_id": artifact.dataset_id,
            "model_type": artifact.model_type,
            "features": artifact.features,
            "params": artifact.params,
            "checksum": artifact.checksum,
            "created_at": artifact.created_at.isoformat(),
            "label_counts": artifact.label_counts,
            "payload": artifact.payload,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, sort_keys=True)
        self.ch.insert_regime_model(
            model_id=artifact.model_id,
            dataset_id=artifact.dataset_id,
            model_type=artifact.model_type,
            features_serialized=json.dumps(artifact.features, sort_keys=True),
            params_serialized=json.dumps(artifact.params, sort_keys=True),
            checksum=artifact.checksum,
            created_at=artifact.created_at,
        )

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
        df_signals = self._signals_df(
            symbol=symbol,
            timeframe=timeframe_min,
            date_from=date_from,
            date_to=date_to,
            df_candles=df_candles,
        )
        df_features = self._features_df(df_candles, feature_set=feature_set, df_signals=df_signals)
        ds_id = self._build_dataset_id(
            symbol=symbol,
            timeframe=timeframe_min,
            date_from=date_from,
            date_to=date_to,
            feature_set=feature_set,
            builder_version=self._infer_builder_version(feature_set),
        )
        df_regimes = self._regimes_df(
            df_candles,
            df_features,
            df_signals,
            feature_set=feature_set,
            dataset_id=ds_id,
        )
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
            # lineage: raw tables + feature/regime versions
            self.ch.insert_dataset_lineage(
                dataset_id=meta.dataset_id,
                source_type="table",
                source_id="market_candles",
                created_at=meta.created_at,
            )
            self.ch.insert_dataset_lineage(
                dataset_id=meta.dataset_id,
                source_type="table",
                source_id="funding_rates",
                created_at=meta.created_at,
            )
            self.ch.insert_dataset_lineage(
                dataset_id=meta.dataset_id,
                source_type="table",
                source_id="open_interest",
                created_at=meta.created_at,
            )
            self.ch.insert_dataset_lineage(
                dataset_id=meta.dataset_id,
                source_type="table",
                source_id="liquidations",
                created_at=meta.created_at,
            )
            for k, v in sorted(feature_set.items()):
                self.ch.insert_dataset_lineage(
                    dataset_id=meta.dataset_id,
                    source_type="feature",
                    source_id=f"{k}:{v}",
                    created_at=meta.created_at,
                )
        return meta, df_candles.reset_index(drop=True), df_features.reset_index(drop=True)


