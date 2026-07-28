-- Nexus Core schema (fresh installs)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 1. Raw Market Data (hypertable)
CREATE TABLE IF NOT EXISTS market_data (
    symbol VARCHAR(20) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    time_bucket TIMESTAMPTZ NOT NULL,
    open DECIMAL(20, 8) NOT NULL,
    high DECIMAL(20, 8) NOT NULL,
    low DECIMAL(20, 8) NOT NULL,
    close DECIMAL(20, 8) NOT NULL,
    volume DECIMAL(20, 4) NOT NULL,
    vwap DECIMAL(20, 8),
    trade_count INTEGER,
    UNIQUE (symbol, exchange, time_bucket)
);
SELECT create_hypertable('market_data', 'time_bucket', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);

-- 2. Feature Cache (Brain v2 feature set)
CREATE TABLE IF NOT EXISTS feature_cache (
    symbol VARCHAR(20) NOT NULL,
    time_bucket TIMESTAMPTZ NOT NULL,
    rsi_14 DECIMAL(12, 6),
    macd_line DECIMAL(14, 6),
    macd_signal DECIMAL(14, 6),
    macd_hist DECIMAL(14, 6),
    bb_upper DECIMAL(20, 8),
    bb_lower DECIMAL(20, 8),
    bb_pct_b DECIMAL(10, 4),
    bb_width DECIMAL(12, 8),
    atr_14 DECIMAL(20, 8),
    atr_pct DECIMAL(12, 8),
    volume_profile_ratio DECIMAL(12, 4),
    vol_z DECIMAL(10, 4),
    ret_1 DECIMAL(12, 8),
    ret_3 DECIMAL(12, 8),
    ret_12 DECIMAL(12, 8),
    adx_14 DECIMAL(10, 4),
    dist_sma50 DECIMAL(12, 4),
    dist_sma200 DECIMAL(12, 4),
    dist_vwap DECIMAL(12, 4),
    hour_sin DECIMAL(8, 6),
    hour_cos DECIMAL(8, 6),
    correlation_spy DECIMAL(6, 4),
    sentiment_score DECIMAL(6, 4),
    forward_return_1h DECIMAL(12, 8),
    regime_label VARCHAR(20) DEFAULT 'unknown',
    PRIMARY KEY (symbol, time_bucket)
);
SELECT create_hypertable('feature_cache', 'time_bucket', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);

-- 3. Fundamental / Macro snapshot
CREATE TABLE IF NOT EXISTS fundamental_snapshot (
    symbol VARCHAR(20) NOT NULL,
    snapshot_date DATE NOT NULL,
    pe_ratio DECIMAL(10, 2),
    debt_to_equity DECIMAL(10, 2),
    eps DECIMAL(10, 2),
    implied_volatility_rank DECIMAL(5, 2),
    vix_close DECIMAL(10, 2),
    dxy_close DECIMAL(10, 2),
    tnx_yield DECIMAL(6, 4),
    PRIMARY KEY (symbol, snapshot_date)
);

-- 4. Indexes
CREATE INDEX IF NOT EXISTS idx_market_data_symbol_time ON market_data (symbol, time_bucket DESC);
CREATE INDEX IF NOT EXISTS idx_feature_cache_symbol_time ON feature_cache (symbol, time_bucket DESC);
