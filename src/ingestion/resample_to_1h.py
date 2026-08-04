#!/usr/bin/env python3
"""
src/ingestion/resample_to_1h.py
Builds the complete 1-hour data layer from the 5-min market_data:

  1. resamples OHLCV to 1h bars (bins anchored at :30 so session bars are
     clean 9:30-10:30, 10:30-11:30, ... for US equities)
  2. computes the full v2 indicator set on the 1h series (proper warm-up,
     daily-reset VWAP, regime labels)
  3. computes forward returns on the 1h series (1 bar = 1h, 4 bars = 4h)
  4. upserts market_data_1h + feature_cache_1h (5-min tables untouched)

v3.1:
- Symbols come from the DB itself (every symbol in market_data), not
  config.symbols - backfill a new symbol, re-run this, done. Crypto
  symbols are skipped (they get native 1h bars from backfill_crypto).
- The per-symbol upsert is exported as upsert_1h_layer() so the crypto
  backfill and the daily pump share ONE implementation.
- Feature upserts are NULL-safe (COALESCE): a short-window refresh can
  never wipe long-warmup features (dist_sma200 etc.) with NULLs.

The 1h tables were LIKE-copied from the originals, so they inherit extra
NOT NULL columns (e.g. market_data.exchange). Those are filled from the
source data when available, else a type-safe default.

Run after migrations 005 + 006 and after backfilling market_data:
    python -m src.ingestion.resample_to_1h
Then:
    python -m src.memory.build_memory
    python -m src.memory.update_qdrant_payloads
"""
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from config.settings import config
from src.ingestion.indicator_calculator import calculate_all_indicators
from src.utils.logger import setup_logger

logger = setup_logger("ResampleTo1h", "logs/ingestion.log")

MD_BASE = ["symbol", "time_bucket", "open", "high", "low", "close", "volume", "vwap"]
PRESERVE = {"symbol", "time_bucket", "sentiment_score"}


def _required_columns(conn, table):
    """NOT NULL columns without a default -> must appear in every INSERT."""
    cur = conn.cursor()
    cur.execute(
        """SELECT column_name, data_type FROM information_schema.columns
           WHERE table_name = %s AND is_nullable = 'NO' AND column_default IS NULL""",
        (table,))
    rows = dict(cur.fetchall())
    cur.close()
    return rows


def _table_columns(conn, table):
    cur = conn.cursor()
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
        (table,))
    cols = [r[0] for r in cur.fetchall()]
    cur.close()
    return cols


def _default_for(dtype):
    if dtype in ('double precision', 'real', 'numeric', 'integer', 'bigint', 'smallint'):
        return 0.0
    if dtype.startswith('timestamp'):
        return pd.Timestamp.utcnow().to_pydatetime()
    if dtype == 'boolean':
        return False
    return ''


def _convert(col, val):
    if val is None or pd.isna(val):
        return None
    if col == 'regime_label':
        return str(val)
    return float(val)


def _resample_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['vwap_num'] = df['vwap'] * df['volume']
    df = df.set_index('timestamp').sort_index()
    out = df.resample('1h', origin='start_day', offset='30min').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
        'volume': 'sum', 'vwap_num': 'sum',
    }).dropna(subset=['close'])
    out['vwap'] = out['vwap_num'] / out['volume'].replace(0, pd.NA)
    return out.drop(columns=['vwap_num']).reset_index()


def upsert_1h_layer(conn, symbol: str, df1: pd.DataFrame):
    """
    Upsert one symbol's 1h OHLCV frame (columns: timestamp, open, high,
    low, close, volume, vwap) into market_data_1h + feature_cache_1h,
    computing forward returns and indicators v2.

    Shared by the 5-min resampler (full history), the Alpaca crypto
    backfill (native 1h bars) and the daily pump (recent window).
    Feature upserts never overwrite an existing value with NULL.
    Returns (n_market_rows, n_feature_rows).
    """
    if df1 is None or df1.empty:
        return 0, 0
    df1 = df1.sort_values('timestamp').reset_index(drop=True)

    # Forward returns on the 1h series (bar-shift; overnight moves count)
    df1['forward_return_1h'] = df1['close'].shift(-1) / df1['close'] - 1
    df1['forward_return_4h'] = df1['close'].shift(-4) / df1['close'] - 1

    fc_cols = set(_table_columns(conn, 'feature_cache_1h'))
    if not fc_cols:
        logger.error("feature_cache_1h missing - run migration 005 first")
        return 0, 0
    md_required = _required_columns(conn, 'market_data_1h')
    fc_required = _required_columns(conn, 'feature_cache_1h')

    # --- market_data_1h ---
    md_extras = {c: _default_for(t) for c, t in md_required.items() if c not in MD_BASE}
    md_cols = MD_BASE + list(md_extras.keys())
    md_sql = (
        f"INSERT INTO market_data_1h ({', '.join(md_cols)}) VALUES %s "
        f"ON CONFLICT (symbol, time_bucket) DO UPDATE SET "
        + ", ".join(f"{c} = EXCLUDED.{c}" for c in md_cols if c not in ('symbol', 'time_bucket'))
    )
    md_records = [
        (symbol, pd.Timestamp(r['timestamp']).to_pydatetime(),
         float(r['open']), float(r['high']), float(r['low']), float(r['close']),
         float(r['volume']), None if pd.isna(r['vwap']) else float(r['vwap']),
         *md_extras.values())
        for _, r in df1.iterrows()
    ]
    cur = conn.cursor()
    execute_values(cur, md_sql, md_records, page_size=1000)
    conn.commit()

    # --- indicators + forward returns -> feature_cache_1h ---
    feat = calculate_all_indicators(
        df1[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy())
    if feat.empty:
        cur.close()
        logger.warning(f"{symbol}: indicator frame empty")
        return len(md_records), 0
    feat = feat.merge(
        df1[['timestamp', 'forward_return_1h', 'forward_return_4h']],
        on='timestamp', how='left')

    update_cols = [c for c in feat.columns
                   if c in fc_cols and c not in PRESERVE and c != 'timestamp']
    fc_extras = {c: _default_for(t) for c, t in fc_required.items()
                 if c not in update_cols and c not in PRESERVE}
    all_cols = ["symbol", "time_bucket"] + update_cols + list(fc_extras.keys())
    fc_sql = (
        f"INSERT INTO feature_cache_1h ({', '.join(all_cols)}) VALUES %s "
        f"ON CONFLICT (symbol, time_bucket) DO UPDATE SET "
        + ", ".join(f"{c} = COALESCE(EXCLUDED.{c}, feature_cache_1h.{c})"
                    for c in update_cols)
    )
    fc_records = [
        (symbol, pd.Timestamp(r['timestamp']).to_pydatetime(),
         *[_convert(c, r[c]) for c in update_cols],
         *fc_extras.values())
        for _, r in feat.iterrows()
    ]
    execute_values(cur, fc_sql, fc_records, page_size=1000)
    conn.commit()
    cur.close()
    return len(md_records), len(fc_records)


def main():
    logger.info("===== 1H DATA LAYER BUILD =====")
    conn = psycopg2.connect(config.database.url)
    fc_cols = set(_table_columns(conn, 'feature_cache_1h'))
    if not fc_cols:
        logger.error("feature_cache_1h missing - run migration 005 first")
        return

    symbols = pd.read_sql(
        "SELECT DISTINCT symbol FROM market_data ORDER BY symbol", conn)['symbol'].tolist()
    if not symbols:
        logger.error("market_data is empty - run backfill_history first")
        conn.close()
        return
    logger.info(f"Symbols in market_data: {symbols}")

    for symbol in symbols:
        if '/' in symbol:
            logger.info(f"{symbol}: crypto keeps native 1h bars (backfill_crypto) - skipped")
            continue
        df5 = pd.read_sql(
            """SELECT time_bucket AS timestamp, open, high, low, close, volume, vwap
               FROM market_data WHERE symbol = %s ORDER BY time_bucket""",
            conn, params=(symbol,))
        if df5.empty:
            logger.warning(f"{symbol}: no 5-min data, skipped")
            continue

        df1 = _resample_ohlcv(df5)
        n_md, n_fc = upsert_1h_layer(conn, symbol, df1)
        logger.info(f"{symbol}: {n_md} 1h bars | {n_fc} feature rows")

    conn.close()
    logger.info("===== 1H BUILD COMPLETE -> build_memory, update_qdrant_payloads =====")


if __name__ == "__main__":
    main()
