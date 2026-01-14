import logging
from datetime import timedelta
from typing import List, Tuple

from market_data.common.models import Candle

logger = logging.getLogger(__name__)


def validate_price_bounds(candles: List[Candle]) -> List[Candle]:
    valid: List[Candle] = []
    dropped = 0
    for c in candles:
        if c.high < c.open or c.high < c.close or c.low > c.open or c.low > c.close:
            dropped += 1
            continue
        if c.low > c.high:
            dropped += 1
            continue
        valid.append(c)
    if dropped:
        logger.warning("Dropped %d invalid candles due to price bounds", dropped)
    return valid


def detect_gaps_1m(candles: List[Candle]) -> Tuple[List[Candle], int]:
    if not candles:
        return candles, 0
    gaps = 0
    candles.sort(key=lambda c: c.ts)
    expected = candles[0].ts
    one_min = timedelta(minutes=1)
    for c in candles:
        if c.ts != expected:
            gaps += int((c.ts - expected).total_seconds() // 60)
        expected = c.ts + one_min
    if gaps:
        logger.warning("Detected %d missing 1m candles (gaps)", gaps)
    return candles, gaps


def validate_all_1m(candles: List[Candle]) -> List[Candle]:
    bounded = validate_price_bounds(candles)
    sorted_candles, _ = detect_gaps_1m(bounded)
    return sorted_candles


