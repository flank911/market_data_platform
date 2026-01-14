# Market Data Platform (MVP)

Deterministic, reproducible pipeline for Binance BTCUSDT OHLCV with idempotent storage in ClickHouse and a read-only FastAPI.

## Stage 2 (Research-grade) — Datasets & Versioned Features

- Dataset is an immutable snapshot identified by `dataset_id`
- Features are versioned and never overwritten
- Experiments reference `dataset_id` only
- Multi-symbol support (registry via `SYMBOLS`)
- Deterministic Parquet export with checksum and quality gate

### Dataset contract
Dataset metadata is stored in ClickHouse table `datasets`:

- `dataset_id String` (e.g., `ds_btcusdt_1m_20200101_20220101_v1`)
- `symbol String`
- `timeframe UInt16` (minutes)
- `date_from DateTime` / `date_to DateTime`
- `feature_set String` (JSON with versions/params)
- `checksum String` (content hash of candles+features)
- `created_at DateTime`

Immutable rule: any change produces a new `dataset_id`.

### Build a dataset (Python)
```bash
python -c "from datetime import datetime, timezone as tz; \
from market_data.config.settings import load_settings; \
from market_data.storage.clickhouse_client import ClickHouseClient; \
from market_data.datasets import DatasetBuilder; \
s=load_settings(); \
ch=ClickHouseClient(host=s.clickhouse.host,port=s.clickhouse.port,username=s.clickhouse.username,password=s.clickhouse.password,database=s.clickhouse.database,secure=s.clickhouse.secure); \
b=DatasetBuilder(s, ch); \
meta, df_c, df_f = b.build(symbol='BTCUSDT', timeframe='1m', date_from=datetime(2020,1,1,tzinfo=tz.utc), date_to=datetime(2022,1,1,tzinfo=tz.utc), feature_set={'atr':'v1','volatility':{'version':'v1','window':30},'trend':{'version':'v1','short_span':12,'long_span':26},'sessions':'v1','volatility_regime':'v1','trend_regime':'v1'}); \
print(meta)"
```

### Export to Parquet
```bash
python -m market_data.export.export_dataset --dataset-id ds_btcusdt_1m_20200101_20220101_v1 --format parquet
```
Output directory:
```
data/exports/<dataset_id>/
├── metadata.yaml
├── candles.parquet
└── features.parquet
```

### API (dataset-centric)
- GET `/datasets`
- GET `/datasets/{id}`
- GET `/datasets/{id}/export`

## Architecture
- Fetcher → Normalizer → Validator → Aggregator → Feature Builder → Storage → API
- Scope (hard-limited): exchange `binance`, symbol `BTCUSDT`, base TF `1m`, aggregated TFs `5m,15m,1h,4h,1d`

## Project layout
```
market-data/
├── ingest/          # fetch + normalize
├── validate/        # data validation
├── aggregate/       # timeframe aggregation
├── features/        # ATR, returns, regimes
├── storage/         # ClickHouse client & DDL
├── api/             # FastAPI
├── config/
├── scripts/
├── docker/
└── README.md
```

## Prerequisites
- Docker and docker-compose

## Configuration
Create a `.env` file in the repository root with:

```
# ClickHouse
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DB=default
CLICKHOUSE_SECURE=0

# Data scope
EXCHANGE=binance
SYMBOL=BTCUSDT
BASE_TIMEFRAME=1m
AGG_TIMEFRAMES=5m,15m,1h,4h,1d

# Date range (UTC). For MVP target >=3y of 1m.
START_DATE_UTC=2021-01-01
END_DATE_UTC=2024-12-31

# Binance + backoff
BINANCE_BASE_URL=https://api.binance.com
BACKOFF_INITIAL_S=0.5
BACKOFF_MAX_S=8.0

# Feature thresholds (% ATR of price)
VOL_LOW_ATR_PCT=0.5
VOL_MID_ATR_PCT=1.5

# Quality gate thresholds (%)
QUALITY_MAX_GAP_PCT=0.5
QUALITY_MAX_NAN_FEATURE_PCT=0.5

# Multi-symbol registry (Stage 2)
SYMBOLS=BTCUSDT,ETHUSDT
```

## One-command startup

1) Build and start ClickHouse and API:
```
docker-compose up -d --build clickhouse app
```

2) Run the idempotent data load (can be re-run safely):
```
docker-compose run --rm worker
```

The worker will:
- fetch 1m BTCUSDT from Binance (paginated + retry/backoff)
- normalize to UTC floats and sort
- validate (price bounds + 1m gap detection logging)
- compute features (ATR(14), regimes, returns)
- insert idempotently into ClickHouse
- aggregate into 5m, 15m, 1h, 4h, 1d
- compute features for each TF and insert idempotently

## API
FastAPI served at http://localhost:8000

- GET `/candles`
  - Query params: `symbol`, `timeframe`, `from`, `to`, `include_features`
  - Example:
```
curl "http://localhost:8000/candles?symbol=BTCUSDT&timeframe=1h&from=2022-01-01T00:00:00Z&to=2022-01-03T00:00:00Z&include_features=true"
```

Response is stable JSON, sorted by time. When `include_features=false`, `atr` and `volatility_regime` are omitted.

## Determinism and idempotency
- All timestamps normalized to UTC
- Strict price-bound validation; invalid rows dropped with logging
- Aggregation rules: open=first, close=last, high=max, low=min, volume=sum
- Idempotent inserts: existing keys filtered before insert
- Re-runnable worker: safe to repeat the entire pipeline for the same range

## Notes
- No Pandas used in ingestion path
- Only 1 symbol/1 exchange by design in MVP
- No streaming/exec/strategy logic included

