import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from market_data.common.timeframes import tf_to_minutes
from market_data.config.settings import load_settings
from market_data.datasets import DatasetBuilder
from market_data.storage.clickhouse_client import ClickHouseClient
from market_data.export.export_dataset import _write_parquet  # reuse writer for deterministic settings
import os
import json
from datetime import timezone as _tz
import yaml

logger = logging.getLogger(__name__)

app = FastAPI(title="Market Data API", version="0.2.0")
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


class DatasetOut(BaseModel):
    dataset_id: str
    symbol: str
    timeframe: int
    date_from: datetime
    date_to: datetime
    feature_set: str
    checksum: str
    created_at: datetime


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


@app.get("/datasets", response_model=List[DatasetOut])
def list_datasets(symbol: Optional[str] = Query(None, description="Filter by symbol"), limit: int = 200):
    rows = ch.list_datasets(symbol=symbol, limit=limit)
    return [
        DatasetOut(
            dataset_id=r["dataset_id"],
            symbol=r["symbol"],
            timeframe=r["timeframe"],
            date_from=r["date_from"],
            date_to=r["date_to"],
            feature_set=r["feature_set"],
            checksum=r["checksum"],
            created_at=r["created_at"],
        )
        for r in rows
    ]


@app.get("/datasets/{dataset_id}", response_model=DatasetOut)
def get_dataset(dataset_id: str):
    r = ch.get_dataset(dataset_id)
    if not r:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return DatasetOut(
        dataset_id=r["dataset_id"],
        symbol=r["symbol"],
        timeframe=r["timeframe"],
        date_from=r["date_from"],
        date_to=r["date_to"],
        feature_set=r["feature_set"],
        checksum=r["checksum"],
        created_at=r["created_at"],
    )


@app.get("/datasets/{dataset_id}/export")
def export_dataset(dataset_id: str, format: str = "parquet"):
    if format != "parquet":
        raise HTTPException(status_code=400, detail="Only parquet format is supported")
    meta = ch.get_dataset(dataset_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Dataset not found")
    feature_set = json.loads(meta["feature_set"])
    builder = DatasetBuilder(settings, ch)
    _, df_candles, df_features = builder.build(
        symbol=meta["symbol"],
        timeframe=f"{int(meta['timeframe'])}m",
        date_from=meta["date_from"].astimezone(_tz.utc),
        date_to=meta["date_to"].astimezone(_tz.utc),
        feature_set=feature_set,
        publish=False,
    )
    out_dir = os.path.join("data", "exports", dataset_id)
    os.makedirs(out_dir, exist_ok=True)
    _write_parquet(df_candles, os.path.join(out_dir, "candles.parquet"))
    _write_parquet(df_features, os.path.join(out_dir, "features.parquet"))
    with open(os.path.join(out_dir, "metadata.yaml"), "w", encoding="utf-8") as f:
        yaml.safe_dump(
            {
                "dataset_id": dataset_id,
                "symbol": meta["symbol"],
                "timeframe": int(meta["timeframe"]),
                "date_from": meta["date_from"].isoformat(),
                "date_to": meta["date_to"].isoformat(),
                "feature_set": feature_set,
                "checksum": meta["checksum"],
                "created_at": meta["created_at"].isoformat(),
                "format": "parquet",
            },
            f,
            sort_keys=True,
        )
    return {"export_path": out_dir}


