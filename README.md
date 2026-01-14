# Market Data Platform (MVP)

Deterministic, reproducible pipeline for Binance BTCUSDT OHLCV with idempotent storage in ClickHouse and a read-only FastAPI.

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

