-- database/migrations/005_timeframe_1h.sql
-- v3: 1-hour data layer. The 5-min tables stay untouched (history kept).
CREATE TABLE IF NOT EXISTS market_data_1h (LIKE market_data INCLUDING ALL);
CREATE TABLE IF NOT EXISTS feature_cache_1h (LIKE feature_cache INCLUDING ALL);
