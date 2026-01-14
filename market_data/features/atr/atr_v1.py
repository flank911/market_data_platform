from __future__ import annotations

import numpy as np
import pandas as pd


def compute_atr_v1(df: pd.DataFrame, window: int = 14) -> pd.Series:
    """
    Deterministic ATR implementation (Wilder) with simple rolling mean of TR.
    Expects columns: high, low, close
    Returns a Series named 'atr_{window}_v1'
    """
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(window=window, min_periods=window).mean()
    out = atr.rename(f"atr_{window}_v1")
    return out


