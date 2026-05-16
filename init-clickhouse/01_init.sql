-- ============================================================
-- Trading Bot — ClickHouse schema
-- ============================================================

-- Raw + aggregated candles (1m stored by data service;
-- higher TFs written on-demand by DependencyResolver)
CREATE TABLE IF NOT EXISTS candles
(
    timestamp UInt64,
    exchange  LowCardinality(String),
    symbol    LowCardinality(String),
    timeframe LowCardinality(String),
    open      Float64,
    high      Float64,
    low       Float64,
    close     Float64,
    volume    Float64
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(toDateTime(timestamp))
ORDER BY (exchange, symbol, timeframe, timestamp);

-- On-demand indicator cache
-- Keyed by (indicator_type, params_hash, exchange, symbol, timestamp)
-- params_hash links to params_meta for full parameter reconstruction.
CREATE TABLE IF NOT EXISTS indicators
(
    indicator_type LowCardinality(String),
    params_hash    String,
    exchange       LowCardinality(String),
    symbol         LowCardinality(String),
    timestamp      UInt64,
    value          Float64
)
ENGINE = ReplacingMergeTree()
ORDER BY (indicator_type, params_hash, exchange, symbol, timestamp);

-- Bidirectional hash ↔ params registry
-- Populated whenever a new IndicatorParams or StrategyParams is first used.
CREATE TABLE IF NOT EXISTS params_meta
(
    params_hash  String,
    params_type  LowCardinality(String),  -- 'indicator' | 'strategy'
    params_json  String,
    created_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree()
ORDER BY (params_hash);

-- On-demand strategy signal cache
-- Separate hash per strategy configuration version — old signals remain
-- untouched when parameters change (new hash = new slot).
CREATE TABLE IF NOT EXISTS strategy_signals
(
    strategy_type  LowCardinality(String),
    params_hash    String,
    exchange       LowCardinality(String),
    symbol         LowCardinality(String),
    timestamp      UInt64,
    signal_type    LowCardinality(String),  -- 'BUY' | 'SELL'
    confidence     Float64,
    price          Float64,
    metadata       String DEFAULT '{}'       -- JSON for extra signal data
)
ENGINE = MergeTree()
ORDER BY (strategy_type, params_hash, exchange, symbol, timestamp);
