from __future__ import annotations

import pandas as pd


def compute_session_code_v1(df: pd.DataFrame) -> pd.Series:
    """
    Encode UTC sessions as discrete codes:
      1=Asia (00:00-07:59), 2=London (08:00-15:59), 3=NY (12:00-20:59), 0=Other
    Overlaps resolved by priority: NY > London > Asia.
    Returns Series named 'session_code_v1'
    """
    hours = df["ts"].dt.hour
    code = pd.Series(0, index=df.index, dtype="int64")
    # Base windows
    asia = (hours >= 0) & (hours <= 7)
    london = (hours >= 8) & (hours <= 15)
    ny = (hours >= 12) & (hours <= 20)
    code = code.mask(asia, 1)
    code = code.mask(london, 2)
    code = code.mask(ny, 3)
    return code.rename("session_code_v1")


