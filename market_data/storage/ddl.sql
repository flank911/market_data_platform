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


