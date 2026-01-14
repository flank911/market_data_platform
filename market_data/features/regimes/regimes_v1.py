from __future__ import annotations

import pandas as pd


def compute_volatility_regime_v1(
    df: pd.DataFrame, *, atr_col: str, low_pct: float, mid_pct: float
) -> pd.Series:
    """
    Volatility regimes from ATR% thresholds (0=low,1=mid,2=high)
    Returns Series named 'volatility_regime_v1'
    """
    close = df["close"].astype(float)
    atr = df[atr_col].astype(float)
    atr_pct = (atr / close.replace(0.0, pd.NA)) * 100.0
    out = pd.Series(0, index=df.index, dtype="int64")
    out = out.mask(atr_pct >= low_pct, 1)
    out = out.mask(atr_pct >= mid_pct, 2)
    return out.rename("volatility_regime_v1")


def compute_trend_regime_v1(df: pd.DataFrame, *, trend_strength_col: str, thr: float = 0.0) -> pd.Series:
    """
    Trend regimes from trend strength (0=bear,1=neutral,2=bull)
    Returns Series named 'trend_regime_v1'
    """
    s = df[trend_strength_col].astype(float)
    out = pd.Series(1, index=df.index, dtype="int64")
    out = out.mask(s < -abs(thr), 0)
    out = out.mask(s > abs(thr), 2)
    return out.rename("trend_regime_v1")


