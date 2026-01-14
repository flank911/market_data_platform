from __future__ import annotations

import numpy as np
import pandas as pd


def compute_rolling_volatility_v1(df: pd.DataFrame, window: int = 30) -> pd.Series:
    """
    Rolling volatility on log returns with configurable window.
    Returns Series named 'volatility_roll_{window}_v1'
    """
    close = df["close"].astype(float)
    log_ret = np.log(close).diff()
    vol = log_ret.rolling(window=window, min_periods=window).std()
    return vol.rename(f"volatility_roll_{window}_v1")


