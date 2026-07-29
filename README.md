# Nexus Core — Self-Adaptive Trading AI

Case-based-reasoning trading system: every market state is encoded into a
vector and stored in Qdrant. At decision time the brain retrieves the most
similar **past** states and trades on what actually happened next.

**Stack:** TimescaleDB (bars + features) · Qdrant (memory) · Polygon.io
(history) · Alpaca (live paper trading) · FinBERT (news sentiment) · Telegram.

## v2 — truth fixes + brain upgrade (merged)

Look-ahead removed · broker double-PnL fixed · unique memory IDs · symmetric
short gate · honest sentiment · 17 scale-free features · regime labels ·
neighbor-agreement gate · signal quality score · signal-strength sizing ·
daily profit target · buffered sma_cross exit · data-freshness guard ·
Telegram EOD reports.

## v3 — universe + crypto (this branch)

- **Symbol universe lives in the DB** (`symbols` table) — no hardcoded lists.
  `SymbolManager.add_symbol()` verifies tradability on Alpaca before activating.
- **Daily top-N selection** (`UNIVERSE_MODE=auto`): the `DailySelector` scores
  every active symbol each morning — 45% brain edge, 20% regime, 15% volatility
  fit, 10% liquidity, 10% news sentiment — and trades only the best N
  (default 5). Scores + reasons persisted in `daily_selection`.
- **Crypto**: BTC/USD, ETH/USD, SOL/USD — 24/7 trading, fractional quantities,
  GTC orders, Alpaca crypto feed, Polygon `X:` backfill.
- Ingestion pump is universe-driven (stocks + crypto).

## Setup

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
# TA-Lib C library: https://github.com/ta-lib/ta-lib-python#installation

cp .env.example .env   # DATABASE_URL, POLYGON_API_KEY, ALPACA_API_KEY,
                       # ALPACA_SECRET_KEY, TELEGRAM_BOT_TOKEN,
                       # TELEGRAM_CHAT_ID, NEWS_API_KEY, UNIVERSE_MODE

make start             # TimescaleDB + Qdrant via docker-compose
```

### Migrations (existing databases)

No local `psql`? Run through Docker:

```bash
docker ps   # find the timescaledb container, e.g. nexus_core-db-1
docker exec -i <container> psql -U postgres -d nexus_core < database/migrations/002_brain_v2.sql
docker exec -i <container> psql -U postgres -d nexus_core < database/migrations/003_universe.sql
```

## Rebuild the brain (required once after v2)

```bash
source venv/bin/activate
python -m src.ingestion.run_pump
python -m src.memory.build_memory
python -m src.memory.backfill_forward_returns
python -m src.memory.update_qdrant_payloads
```

## Manage the universe

```bash
python -m src.universe.symbol_manager           # list all symbols
python -m src.universe.selector                 # run today's top-N selection
```

Add crypto to the universe (one time, in Python or psql):

```sql
INSERT INTO symbols (symbol, asset_type, active) VALUES
  ('BTC/USD','crypto',TRUE), ('ETH/USD','crypto',TRUE), ('SOL/USD','crypto',TRUE)
ON CONFLICT DO NOTHING;
```

## Honest backtest

```bash
python -m src.backtester.engine AAPL 2026-03-24 2026-06-24
```

Includes the exit-reason breakdown (watch `sma_cross`).

## Live paper trading

```bash
python -m src.live.live_paper_trader_multi
```

- `UNIVERSE_MODE=manual` → trades all active symbols in the DB
- `UNIVERSE_MODE=auto`   → trades the daily top-N selection
- Crypto trades 24/7; stocks only during market hours.

All knobs live in `config/settings.py`.
