-- database/migrations/006_1h_unique_indexes.sql
-- The LIKE-copied 1h tables have no unique constraint on
-- (symbol, time_bucket), so upserts fail with
-- "no unique or exclusion constraint matching the ON CONFLICT
-- specification". Unique indexes fix that (ON CONFLICT works with any
-- matching unique index). IF NOT EXISTS = safe to re-run.
CREATE UNIQUE INDEX IF NOT EXISTS market_data_1h_symbol_bucket_uidx
    ON market_data_1h (symbol, time_bucket);
CREATE UNIQUE INDEX IF NOT EXISTS feature_cache_1h_symbol_bucket_uidx
    ON feature_cache_1h (symbol, time_bucket);
