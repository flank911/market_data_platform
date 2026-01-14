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
PRIMARY KEY (symbol, timeframe, ts)
ORDER BY (symbol, timeframe, ts)
SETTINGS index_granularity = 8192;


