-- =============================================================================
-- Trading Bot — Initial Schema
-- TimescaleDB + PostgreSQL 15
--
-- All timestamp columns store Unix epoch seconds (BIGINT).
-- Time-series tables are converted to hypertables partitioned by timestamp.
-- Chunk intervals are calibrated to expected data volume:
--   candles          — 1 week   (1-min bars × 2 pairs ≈ ~20k rows/week)
--   candles_realtime — 1 day    (rolling window, small table)
--   indicators       — 1 week   (mirrors candles cadence)
--   strategy_signals — 4 weeks  (sparse; one signal per bar at most)
-- =============================================================================

-- Drop legacy tables that are no longer used
DROP TABLE IF EXISTS strategy_indicator_dependencies CASCADE;
DROP TABLE IF EXISTS strategy_dependencies CASCADE;
DROP TABLE IF EXISTS indicator_dependencies CASCADE;
DROP TABLE IF EXISTS dependency_tracking CASCADE;
DROP TABLE IF EXISTS strategies CASCADE;

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ---------------------------------------------------------------------------
-- CANDLES
-- Primary time-series table for closed OHLCV bars (historical + aggregated).
-- Partitioned by timestamp, chunk = 1 week.
-- source_type: 'exchange' | 'aggregated'
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS candles (
    id                  BIGSERIAL       NOT NULL,
    exchange            VARCHAR(50)     NOT NULL,
    symbol              VARCHAR(20)     NOT NULL,
    timeframe           VARCHAR(10)     NOT NULL,
    timestamp           BIGINT          NOT NULL,   -- Unix seconds
    open_price          NUMERIC(20, 8)  NOT NULL,
    high_price          NUMERIC(20, 8)  NOT NULL,
    low_price           NUMERIC(20, 8)  NOT NULL,
    close_price         NUMERIC(20, 8)  NOT NULL,
    volume              NUMERIC(20, 8)  NOT NULL,
    source_type         VARCHAR(20)     NOT NULL DEFAULT 'exchange',
    source_timeframe    VARCHAR(10),
    aggregation_method  VARCHAR(20),
    source_candles_count INTEGER,
    data_completeness   NUMERIC(5, 2),
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, timestamp)
);

SELECT create_hypertable(
    'candles',
    'timestamp',
    chunk_time_interval => 604800,   -- 1 week in seconds
    if_not_exists       => TRUE
);

-- Unique business key — includes source_timeframe to allow aggregated candles
-- alongside base-timeframe candles for the same bar.
CREATE UNIQUE INDEX IF NOT EXISTS uq_candles_business_key
    ON candles (exchange, symbol, timeframe, timestamp, source_timeframe);

CREATE INDEX IF NOT EXISTS idx_candles_lookup
    ON candles (exchange, symbol, timeframe, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_candles_source_type
    ON candles (source_type, source_timeframe);

-- Compress chunks older than 7 days.
ALTER TABLE candles SET (
    timescaledb.compress,
    timescaledb.compress_orderby   = 'timestamp DESC',
    timescaledb.compress_segmentby = 'exchange, symbol, timeframe'
);
SELECT add_compression_policy('candles', BIGINT '604800', if_not_exists => TRUE);

-- ---------------------------------------------------------------------------
-- CANDLES_REALTIME
-- Active / unclosed candle updated in-flight by the WebSocket feed.
-- Smaller chunk interval because only recent data is live.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS candles_realtime (
    id              BIGSERIAL       NOT NULL,
    exchange        VARCHAR(50)     NOT NULL,
    symbol          VARCHAR(20)     NOT NULL,
    timeframe       VARCHAR(10)     NOT NULL,
    timestamp       BIGINT          NOT NULL,   -- Unix seconds, bar open time
    open_price      NUMERIC(20, 8)  NOT NULL,
    high_price      NUMERIC(20, 8)  NOT NULL,
    low_price       NUMERIC(20, 8)  NOT NULL,
    close_price     NUMERIC(20, 8)  NOT NULL,
    volume          NUMERIC(20, 8)  NOT NULL,
    volume_quote    NUMERIC(20, 8),
    trades_count    INTEGER,
    is_closed       BOOLEAN         NOT NULL DEFAULT FALSE,
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    last_update     TIMESTAMPTZ,
    server_time     BIGINT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, timestamp)
);

SELECT create_hypertable(
    'candles_realtime',
    'timestamp',
    chunk_time_interval => 86400,    -- 1 day in seconds
    if_not_exists       => TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_candles_realtime_business_key
    ON candles_realtime (exchange, symbol, timeframe, timestamp);

CREATE INDEX IF NOT EXISTS idx_candles_realtime_active
    ON candles_realtime (exchange, symbol, is_active, timestamp DESC);

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER trg_candles_realtime_updated_at
    BEFORE UPDATE ON candles_realtime
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- ---------------------------------------------------------------------------
-- INDICATORS
-- Computed indicator values (RSI, MA, etc.) keyed to a bar timestamp.
-- No DB-level FK to candles — the candles table is a hypertable and TimescaleDB
-- requires the partition key in all unique constraints; a bare-id FK would
-- violate that. Referential integrity is enforced at the application layer.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS indicators (
    id               BIGSERIAL       NOT NULL,
    connection_name  VARCHAR(100)    NOT NULL,
    indicator_name   VARCHAR(100)    NOT NULL,
    exchange         VARCHAR(50)     NOT NULL,
    symbol           VARCHAR(20)     NOT NULL,
    timeframe        VARCHAR(10)     NOT NULL,
    timestamp        BIGINT          NOT NULL,   -- Unix seconds
    value            NUMERIC(20, 8),
    meta_data        TEXT,                       -- JSON
    source_candle_id BIGINT,                     -- logical reference to candles.id
    created_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, timestamp)
);

SELECT create_hypertable(
    'indicators',
    'timestamp',
    chunk_time_interval => 604800,
    if_not_exists       => TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_indicators_business_key
    ON indicators (indicator_name, exchange, symbol, timeframe, timestamp);

CREATE INDEX IF NOT EXISTS idx_indicators_lookup
    ON indicators (connection_name, indicator_name, timestamp DESC);

ALTER TABLE indicators SET (
    timescaledb.compress,
    timescaledb.compress_orderby   = 'timestamp DESC',
    timescaledb.compress_segmentby = 'connection_name, indicator_name, exchange, symbol, timeframe'
);
SELECT add_compression_policy('indicators', BIGINT '604800', if_not_exists => TRUE);

-- ---------------------------------------------------------------------------
-- STRATEGY_SIGNALS
-- Trading signals emitted by strategies (BUY / SELL / HOLD).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS strategy_signals (
    id               BIGSERIAL       NOT NULL,
    strategy_name    VARCHAR(100)    NOT NULL,
    connection_name  VARCHAR(100)    NOT NULL,
    signal_type      VARCHAR(20)     NOT NULL,   -- BUY | SELL | HOLD
    timestamp        BIGINT          NOT NULL,   -- Unix seconds
    confidence       NUMERIC(5, 4)   NOT NULL,
    price            NUMERIC(20, 8)  NOT NULL,
    indicators_data  TEXT,                       -- JSON snapshot of indicator values
    meta_data        TEXT,                       -- JSON
    created_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, timestamp)
);

SELECT create_hypertable(
    'strategy_signals',
    'timestamp',
    chunk_time_interval => 2419200,  -- 4 weeks in seconds
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_strategy_signals_lookup
    ON strategy_signals (strategy_name, connection_name, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_signals_type
    ON strategy_signals (signal_type, timestamp DESC);

-- ---------------------------------------------------------------------------
-- SERVER_TIME_SYNC
-- Exchange clock offset records for drift compensation.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS server_time_sync (
    id               BIGSERIAL       NOT NULL PRIMARY KEY,
    exchange         VARCHAR(50)     NOT NULL,
    connection_name  VARCHAR(100),
    local_time       BIGINT          NOT NULL,
    server_time      BIGINT          NOT NULL,
    offset_seconds   INTEGER         NOT NULL,
    created_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_time_sync_exchange
    ON server_time_sync (exchange, created_at DESC);
