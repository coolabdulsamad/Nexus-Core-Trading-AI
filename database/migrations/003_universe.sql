-- Migration 003: symbol universe + daily selection (PR 2)
-- psql $DATABASE_URL -f database/migrations/003_universe.sql

CREATE TABLE IF NOT EXISTS symbols (
    symbol VARCHAR(20) NOT NULL,
    asset_type VARCHAR(10) NOT NULL DEFAULT 'stock',   -- 'stock' | 'crypto'
    active BOOLEAN NOT NULL DEFAULT TRUE,
    verified_broker BOOLEAN NOT NULL DEFAULT FALSE,    -- confirmed tradable on Alpaca
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT,
    PRIMARY KEY (symbol, asset_type)
);

-- What the AI decided to trade each day, and why
CREATE TABLE IF NOT EXISTS daily_selection (
    selection_date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    asset_type VARCHAR(10) NOT NULL DEFAULT 'stock',
    rank INTEGER NOT NULL,
    score DECIMAL(8, 4) NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (selection_date, symbol)
);

-- Seed with the classic five
INSERT INTO symbols (symbol, asset_type, active, verified_broker, notes) VALUES
    ('AAPL', 'stock', TRUE, FALSE, 'seed'),
    ('TSLA', 'stock', TRUE, FALSE, 'seed'),
    ('MSFT', 'stock', TRUE, FALSE, 'seed'),
    ('GOOGL', 'stock', TRUE, FALSE, 'seed'),
    ('NVDA', 'stock', TRUE, FALSE, 'seed')
ON CONFLICT DO NOTHING;
