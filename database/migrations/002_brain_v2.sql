-- Migration 002: Brain v2 feature columns (run once on existing databases)
-- psql $DATABASE_URL -f database/migrations/002_brain_v2.sql

ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS macd_hist DECIMAL(14, 6);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS bb_pct_b DECIMAL(10, 4);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS bb_width DECIMAL(12, 8);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS atr_pct DECIMAL(12, 8);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS vol_z DECIMAL(10, 4);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS ret_1 DECIMAL(12, 8);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS ret_3 DECIMAL(12, 8);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS ret_12 DECIMAL(12, 8);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS adx_14 DECIMAL(10, 4);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS dist_sma50 DECIMAL(12, 4);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS dist_sma200 DECIMAL(12, 4);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS dist_vwap DECIMAL(12, 4);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS hour_sin DECIMAL(8, 6);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS hour_cos DECIMAL(8, 6);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS sentiment_score DECIMAL(6, 4);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS forward_return_1h DECIMAL(12, 8);
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS regime_label VARCHAR(20) DEFAULT 'unknown';
