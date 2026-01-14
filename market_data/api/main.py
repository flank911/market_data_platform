import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from market_data.common.timeframes import tf_to_minutes
from market_data.config.settings import load_settings
from market_data.storage.clickhouse_client import ClickHouseClient

logger = logging.getLogger(__name__)

app = FastAPI(title="Market Data API", version="0.1.0")
settings = load_settings()
ch = ClickHouseClient(
    host=settings.clickhouse.host,
    port=settings.clickhouse.port,
    username=settings.clickhouse.username,
    password=settings.clickhouse.password,
    database=settings.clickhouse.database,
    secure=settings.clickhouse.secure,
)


class CandleOut(BaseModel):
    symbol: str
    exchange: str
    timeframe: int
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    atr: Optional[float] = None
    volatility_regime: Optional[int] = None


@app.get("/candles", response_model=List[CandleOut])
def get_candles(
    symbol: str = Query(..., description="Symbol (e.g., BTCUSDT)"),
    timeframe: str = Query(..., description="Timeframe (e.g., 1m,5m,15m,1h,4h,1d)"),
    from_ts: datetime = Query(..., alias="from", description="Start time (UTC ISO)"),
    to_ts: datetime = Query(..., alias="to", description="End time (UTC ISO)"),
    include_features: bool = Query(False, description="Include ATR and regime"),
):
    try:
        tf_minutes = tf_to_minutes(timeframe)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if from_ts.tzinfo is None:
        from_ts = from_ts.replace(tzinfo=timezone.utc)
    if to_ts.tzinfo is None:
        to_ts = to_ts.replace(tzinfo=timezone.utc)
    rows = ch.select_candles(
        symbol=symbol,
        timeframe=tf_minutes,
        ts_from=from_ts,
        ts_to=to_ts,
        include_features=include_features,
    )
    result: List[CandleOut] = []
    for r in rows:
        co = {
            "symbol": r[0],
            "exchange": r[1],
            "timeframe": r[2],
            "ts": r[3],
            "open": r[4],
            "high": r[5],
            "low": r[6],
            "close": r[7],
            "volume": r[8],
        }
        if include_features:
            co["atr"] = r[9]
            co["volatility_regime"] = r[10]
        result.append(CandleOut(**co))
    return result



