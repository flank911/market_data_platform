from __future__ import annotations

import pandas as pd


def compute_funding_deviation_v1(df: pd.DataFrame, *, window: int = 20) -> pd.Series:
    s = df["funding_rate"].astype(float)
    dev = s - s.rolling(window=window, min_periods=1).mean()
    return dev.rename(f"funding_dev_roll_{window}_v1")


def compute_oi_delta_v1(df: pd.DataFrame) -> pd.Series:
    s = df["open_interest"].astype(float)
    return s.diff().fillna(0.0).rename("oi_delta_v1")


def compute_oi_volume_ratio_v1(df: pd.DataFrame) -> pd.Series:
    oi = df["open_interest"].astype(float)
    vol = df["volume"].astype(float).replace(0.0, pd.NA)
    ratio = (oi / vol).fillna(0.0)
    return ratio.rename("oi_volume_ratio_v1")


def compute_liquidation_density_v1(df: pd.DataFrame, *, window: int = 20) -> pd.Series:
    liq = df["liquidation_notional"].astype(float)
    vol = df["volume"].astype(float).replace(0.0, pd.NA)
    liq_roll = liq.rolling(window=window, min_periods=1).sum()
    vol_roll = vol.rolling(window=window, min_periods=1).sum()
    density = (liq_roll / vol_roll).fillna(0.0)
    return density.rename(f"liquidation_density_roll_{window}_v1")


def compute_price_oi_divergence_v1(df: pd.DataFrame) -> pd.Series:
    close = df["close"].astype(float).replace(0.0, pd.NA)
    oi = df["open_interest"].astype(float).replace(0.0, pd.NA)
    ret = close.pct_change().fillna(0.0)
    oi_ret = oi.pct_change().fillna(0.0)
    div = (ret - oi_ret).fillna(0.0)
    return div.rename("price_oi_divergence_v1")


