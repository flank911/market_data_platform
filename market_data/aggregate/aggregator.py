from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Iterable, List

from market_data.common.models import Candle


def floor_to_bucket(ts: datetime, minutes: int) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    total_minutes = (ts.hour * 60 + ts.minute)
    floored_minutes = total_minutes - (total_minutes % minutes)
    return ts.replace(
        hour=floored_minutes // 60,
        minute=floored_minutes % 60,
        second=0,
        microsecond=0,
    )


def aggregate_from_1m(candles_1m: Iterable[Candle], target_minutes: int) -> List[Candle]:
    buckets: Dict[datetime, List[Candle]] = defaultdict(list)
    for c in candles_1m:
        b = floor_to_bucket(c.ts, target_minutes)
        buckets[b].append(c)
    result: List[Candle] = []
    for bts, arr in sorted(buckets.items(), key=lambda kv: kv[0]):
        arr.sort(key=lambda x: x.ts)
        open_price = arr[0].open
        close_price = arr[-1].close
        high = max(x.high for x in arr)
        low = min(x.low for x in arr)
        volume = sum(x.volume for x in arr)
        first = arr[0]
        result.append(
            Candle(
                symbol=first.symbol,
                exchange=first.exchange,
                timeframe=target_minutes,
                ts=bts,
                open=open_price,
                high=high,
                low=low,
                close=close_price,
                volume=volume,
            )
        )
    return result



