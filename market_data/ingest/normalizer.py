from datetime import datetime, timezone
from typing import Iterable, List

from market_data.common.models import Candle
from market_data.common.timeframes import tf_to_minutes


def normalize_binance_klines(
    raw_klines: Iterable[list],
    symbol: str,
    exchange: str,
    timeframe_str: str,
) -> List[Candle]:
    tf_minutes = tf_to_minutes(timeframe_str)
    candles: List[Candle] = []
    for arr in raw_klines:
        open_ms = int(arr[0])
        open_time = datetime.fromtimestamp(open_ms / 1000.0, tz=timezone.utc)
        open_price = float(arr[1])
        high = float(arr[2])
        low = float(arr[3])
        close = float(arr[4])
        volume = float(arr[5])
        candles.append(
            Candle(
                symbol=symbol,
                exchange=exchange,
                timeframe=tf_minutes,
                ts=open_time,
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=volume,
            )
        )
    candles.sort(key=lambda c: c.ts)
    return candles


