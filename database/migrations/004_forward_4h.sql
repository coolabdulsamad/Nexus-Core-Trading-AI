-- database/migrations/004_forward_4h.sql
-- v2.2: 4-hour forward-return target.
-- The brain predicts this horizon; 1h forward returns proved too small
-- (~0.1%) relative to round-trip costs (~0.16%).
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS forward_return_4h DOUBLE PRECISION;
