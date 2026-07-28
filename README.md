# Nexus Core — Self-Adaptive Trading AI

Case-based-reasoning trading system: every market state is encoded into a
vector and stored in Qdrant. At decision time the brain retrieves the most
similar **past** states and trades on what actually happened next.

**Stack:** TimescaleDB (bars + features) · Qdrant (memory) · Polygon.io
(history) · Alpaca (live paper trading) · FinBERT (news sentiment) · Telegram.

## What's new in v2 (this branch)

**Honesty fixes**
- Look-ahead bias removed: memory search is time-filtered, the current bar
  can never retrieve itself or future states.
- Broker P&L no longer double-counted; SL/TP detected with bar high/low.
- Qdrant point IDs are unique per (symbol, timestamp) — multi-symbol memory
  no longer overwrites itself.
- Short-signal gate fixed (conviction-based, symmetric).
- MD5 "synthetic sentiment" noise channel removed.

**Brain v2**
- 17 scale-free features (momentum, ADX, BB %B, vol z-score, time-of-day…).
- Regime labels (trend_up / trend_down / range) + regime-filtered recall.
- Neighbor-agreement gate: no trade when memory is ambivalent.
- Signal **quality score** (0–1) on every decision.

**Money management**
- Signal-strength sizing: STRONG/MEDIUM/WEAK tiers risk different %.
- Daily profit target: hit it → stop trading, lock open trades to breakeven.
- `sma_cross` exit fixed: buffered (0.25×ATR) + confirmed (2 bars).

**Ops**
- Data-freshness guard (no trading on >10-min-old bars) with lag reporting.
- Telegram: entries/exits/partials/guards + heartbeat capital updates +
  end-of-day analysis report.

## Setup

```bash
pip install -r requirements.txt
# TA-Lib C library: https://github.com/ta-lib/ta-lib-python#installation

cp .env.example .env   # fill in keys: DATABASE_URL, POLYGON_API_KEY,
                       # ALPACA_API_KEY, ALPACA_SECRET_KEY,
                       # TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, NEWS_API_KEY

make start             # TimescaleDB + Qdrant via docker-compose
```

Existing database? Run the migration once:

```bash
psql "$DATABASE_URL" -f database/migrations/002_brain_v2.sql
```

## Rebuild the brain (required after this upgrade)

Old memory used colliding IDs and old features — rebuild:

```bash
python -m src.ingestion.run_pump              # fresh data + features v2
python -m src.memory.build_memory             # scaler+PCA -> Qdrant
python -m src.memory.backfill_forward_returns # realised outcomes
python -m src.memory.update_qdrant_payloads   # outcomes -> memory
```

## Honest backtest

```bash
python -m src.backtester.engine AAPL 2026-03-24 2026-06-24
```

The summary now includes an **exit-reason breakdown** — watch whether
`sma_cross` still loses after the fix.

## Live paper trading

```bash
python -m src.live.live_paper_trader_multi
```

All knobs live in `config/settings.py` (thresholds, tiers, daily target,
SMA-exit buffer, freshness, notifications).
