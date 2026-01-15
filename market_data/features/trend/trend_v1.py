from __future__ import annotations

import pandas as pd


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False, min_periods=span).mean()


def compute_trend_strength_ema_diff_v1(
    df: pd.DataFrame, short_span: int = 12, long_span: int = 26
) -> pd.Series:
    """
    Trend strength via normalized EMA difference: (EMA_s - EMA_l) / EMA_l
    Returns Series named 'trend_strength_ema_{short}_{long}_v1'
    """
    close = df["close"].astype(float)
    ema_s = _ema(close, short_span)
    ema_l = _ema(close, long_span)
    strength = (ema_s - ema_l) / ema_l
    return strength.rename(f"trend_strength_ema_{short_span}_{long_span}_v1")



