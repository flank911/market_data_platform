CREATE TABLE IF NOT EXISTS market_candles
(
    symbol String,
    exchange String,
    timeframe UInt16,
    ts DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    atr Float64,
    volatility_regime UInt8
)
ENGINE = ReplacingMergeTree
PARTITION BY (symbol, timeframe, toYear(ts))
PRIMARY KEY (symbol, timeframe, ts)
ORDER BY (symbol, timeframe, ts)
SETTINGS index_granularity = 8192;

-- Immutable dataset registry (metadata only)
CREATE TABLE IF NOT EXISTS datasets
(
  dataset_id String,
  symbol String,
  timeframe UInt16,
  date_from DateTime,
  date_to DateTime,
  feature_set String,      -- serialized mapping, e.g. JSON
  checksum String,         -- checksum of exported dataset contents
  created_at DateTime
)
ENGINE = MergeTree
PARTITION BY (symbol, timeframe, toYear(date_from))
PRIMARY KEY (dataset_id)
ORDER BY (dataset_id)
SETTINGS index_granularity = 8192;

-- Funding rates (e.g., Binance Futures)
CREATE TABLE IF NOT EXISTS funding_rates
(
  symbol String,
  exchange String,
  timeframe UInt16,
  ts DateTime,
  funding_rate Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY (symbol, timeframe, toYear(ts))
PRIMARY KEY (symbol, timeframe, ts)
ORDER BY (symbol, timeframe, ts)
SETTINGS index_granularity = 8192;

-- Open interest history
CREATE TABLE IF NOT EXISTS open_interest
(
  symbol String,
  exchange String,
  timeframe UInt16,
  ts DateTime,
  open_interest Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY (symbol, timeframe, toYear(ts))
PRIMARY KEY (symbol, timeframe, ts)
ORDER BY (symbol, timeframe, ts)
SETTINGS index_granularity = 8192;

-- Liquidations aggregated to timeframe buckets
CREATE TABLE IF NOT EXISTS liquidations
(
  symbol String,
  exchange String,
  timeframe UInt16,
  ts DateTime,
  liquidation_qty Float64,
  liquidation_notional Float64,
  liquidation_count UInt32
)
ENGINE = ReplacingMergeTree
PARTITION BY (symbol, timeframe, toYear(ts))
PRIMARY KEY (symbol, timeframe, ts)
ORDER BY (symbol, timeframe, ts)
SETTINGS index_granularity = 8192;

-- Regime model registry (offline artifacts)
CREATE TABLE IF NOT EXISTS regime_models
(
  model_id String,
  dataset_id String,
  model_type String,
  features String,
  params String,
  checksum String,
  created_at DateTime
)
ENGINE = MergeTree
PARTITION BY toYear(created_at)
PRIMARY KEY (model_id)
ORDER BY (model_id)
SETTINGS index_granularity = 8192;

-- Dataset lineage (raw -> features -> regimes)
CREATE TABLE IF NOT EXISTS dataset_lineage
(
  dataset_id String,
  source_type String,
  source_id String,
  created_at DateTime
)
ENGINE = MergeTree
PARTITION BY toYear(created_at)
PRIMARY KEY (dataset_id, source_type, source_id)
ORDER BY (dataset_id, source_type, source_id)
SETTINGS index_granularity = 8192;


