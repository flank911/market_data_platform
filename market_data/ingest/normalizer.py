from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Tuple

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


def _floor_to_bucket(ts: datetime, timeframe_min: int) -> datetime:
    delta = timedelta(minutes=timeframe_min)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    seconds = int((ts - epoch).total_seconds())
    bucket = (seconds // int(delta.total_seconds())) * int(delta.total_seconds())
    return epoch + timedelta(seconds=bucket)


def normalize_funding_rates(
    raw: Iterable[Dict[str, object]],
    *,
    symbol: str,
    exchange: str,
    timeframe_min: int,
) -> List[Tuple]:
    rows: List[Tuple] = []
    for r in raw:
        ts = datetime.fromtimestamp(int(r["fundingTime"]) / 1000.0, tz=timezone.utc)
        rows.append((symbol, exchange, timeframe_min, ts, float(r["fundingRate"])))
    rows.sort(key=lambda r: r[3])
    return rows


def normalize_open_interest(
    raw: Iterable[Dict[str, object]],
    *,
    symbol: str,
    exchange: str,
    timeframe_min: int,
) -> List[Tuple]:
    rows: List[Tuple] = []
    for r in raw:
        ts = datetime.fromtimestamp(int(r["timestamp"]) / 1000.0, tz=timezone.utc)
        rows.append((symbol, exchange, timeframe_min, ts, float(r["sumOpenInterest"])))
    rows.sort(key=lambda r: r[3])
    return rows


def normalize_liquidations(
    raw: Iterable[Dict[str, object]],
    *,
    symbol: str,
    exchange: str,
    timeframe_min: int,
) -> List[Tuple]:
    buckets: Dict[datetime, Dict[str, float]] = {}
    for r in raw:
        ts = datetime.fromtimestamp(int(r["time"]) / 1000.0, tz=timezone.utc)
        bucket = _floor_to_bucket(ts, timeframe_min)
        if bucket not in buckets:
            buckets[bucket] = {"qty": 0.0, "notional": 0.0, "count": 0.0}
        buckets[bucket]["qty"] += float(r.get("qty", 0.0))
        buckets[bucket]["notional"] += float(r.get("notional", 0.0))
        buckets[bucket]["count"] += 1.0
    rows: List[Tuple] = []
    for bucket_ts, agg in buckets.items():
        rows.append(
            (
                symbol,
                exchange,
                timeframe_min,
                bucket_ts,
                float(agg["qty"]),
                float(agg["notional"]),
                int(agg["count"]),
            )
        )
    rows.sort(key=lambda r: r[3])
    return rows



