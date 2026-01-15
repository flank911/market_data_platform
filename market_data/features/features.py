from typing import List

from market_data.common.models import Candle
from market_data.config.settings import FeatureThresholds


def compute_returns_pct(candles: List[Candle]) -> None:
    prev_close = None
    for c in candles:
        if prev_close is not None and prev_close != 0.0:
            pass
        prev_close = c.close


def compute_atr_14(candles: List[Candle]) -> None:
    prev_close = None
    tr_window: List[float] = []
    for c in candles:
        if prev_close is None:
            tr = c.high - c.low
        else:
            tr = max(
                c.high - c.low,
                abs(c.high - prev_close),
                abs(c.low - prev_close),
            )
        tr_window.append(tr)
        if len(tr_window) > 14:
            tr_window.pop(0)
        c.atr = sum(tr_window) / 14.0 if len(tr_window) == 14 else 0.0
        prev_close = c.close


def compute_volatility_regime(candles: List[Candle], thresholds: FeatureThresholds) -> None:
    for c in candles:
        if c.close <= 0:
            c.volatility_regime = 0
            continue
        atr_pct = (c.atr / c.close) * 100.0
        if atr_pct < thresholds.low_pct:
            c.volatility_regime = 0
        elif atr_pct < thresholds.mid_pct:
            c.volatility_regime = 1
        else:
            c.volatility_regime = 2


def compute_features_inplace(candles: List[Candle], thresholds: FeatureThresholds) -> None:
    compute_returns_pct(candles)
    compute_atr_14(candles)
    compute_volatility_regime(candles, thresholds)




