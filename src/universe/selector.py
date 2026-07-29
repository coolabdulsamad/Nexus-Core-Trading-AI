#!/usr/bin/env python3
"""
src/universe/selector.py
Daily symbol selection: scores every active symbol by how favourable it is
RIGHT NOW, then picks the top N. This is how the system trades the best
opportunities instead of everything at once.

Favorability score (0..1) per symbol:
  45%  current brain edge      - signed quality of the live signal
  20%  regime fit              - trending regimes preferred over chop
  15%  volatility fit          - ATR% inside the tradable sweet spot
  10%  liquidity               - current volume vs its own average
  10%  news sentiment          - real FinBERT (if available)

Run:  python -m src.universe.selector            (select + persist today)
"""
import numpy as np
import pandas as pd
import psycopg2
from datetime import datetime
from config.settings import config
from src.models.meta_learner import MetaLearner
from src.ingestion.indicator_calculator import calculate_all_indicators
from src.universe.symbol_manager import SymbolManager
from src.news_engine.orchestrator import NewsOrchestrator
from src.utils.logger import setup_logger

logger = setup_logger("DailySelector", "logs/universe.log")

# Volatility sweet spot for 5-min trading (ATR as % of price)
ATR_PCT_MIN, ATR_PCT_MAX = 0.0015, 0.030


class DailySelector:
    def __init__(self):
        self.mgr = SymbolManager()
        self.meta = MetaLearner()
        self.news = NewsOrchestrator() if config.USE_REAL_SENTIMENT else None

    # ------------------------------------------------------------------
    def _recent_row(self, symbol: str) -> pd.Series | None:
        """Latest feature row for a symbol from the DB."""
        try:
            conn = psycopg2.connect(config.database.url)
            df = pd.read_sql("""
                SELECT m.time_bucket AS timestamp, m.open, m.high, m.low, m.close, m.volume,
                       f.rsi_14, f.atr_14, f.atr_pct, f.adx_14, f.regime_label,
                       f.volume_profile_ratio, f.sentiment_score
                FROM market_data m
                JOIN feature_cache f ON m.symbol = f.symbol AND m.time_bucket = f.time_bucket
                WHERE m.symbol = %s
                ORDER BY m.time_bucket DESC LIMIT 250
            """, conn, params=(symbol,))
            conn.close()
            if df.empty:
                return None
            df = df.sort_values('timestamp').reset_index(drop=True)
            df['sma_200'] = df['close'].rolling(200).mean()
            row = df.iloc[-1]
            if pd.isna(row['sma_200']):
                return None
            return row
        except Exception as e:
            logger.error(f"{symbol}: recent data failed: {e}")
            return None

    # ------------------------------------------------------------------
    def score_symbol(self, symbol: str, asset_type: str = 'stock') -> dict | None:
        row = self._recent_row(symbol)
        if row is None:
            logger.warning(f"{symbol}: no recent data - skipped")
            return None

        # 1. Brain edge: signed quality (direction x conviction x agreement)
        result = self.meta.get_signal(symbol, row, timestamp=row['timestamp'], mode='live')
        prob = result['confidence']
        signed_edge = (prob - 0.5) * 2 * result.get('quality', 0)      # -1..1
        brain_score = min(1.0, abs(signed_edge) * 2.5)                 # scale to 0..1

        # 2. Regime fit
        regime = row.get('regime_label', 'unknown')
        regime_score = {'trend_up': 1.0, 'trend_down': 1.0,
                        'transition': 0.5, 'range': 0.25}.get(regime, 0.4)

        # 3. Volatility fit
        atr_pct = float(row.get('atr_pct', 0) or 0)
        if ATR_PCT_MIN <= atr_pct <= ATR_PCT_MAX:
            vol_score = 1.0
        elif atr_pct < ATR_PCT_MIN:
            vol_score = max(0.0, atr_pct / ATR_PCT_MIN) * 0.5
        else:
            vol_score = max(0.0, 1 - (atr_pct - ATR_PCT_MAX) / ATR_PCT_MAX)

        # 4. Liquidity
        liq = float(row.get('volume_profile_ratio', 1.0) or 1.0)
        liq_score = min(1.0, liq / 1.5)

        # 5. Sentiment
        sent = 0.0
        if self.news is not None:
            try:
                sent = self.news.get_sentiment_for_symbol(symbol, row['timestamp'])
            except Exception:
                pass
        sent_score = (sent + 1) / 2                                    # -1..1 -> 0..1

        score = (0.45 * brain_score + 0.20 * regime_score + 0.15 * vol_score
                 + 0.10 * liq_score + 0.10 * sent_score)

        reason = (f"brain={brain_score:.2f}(edge {signed_edge:+.2f}) regime={regime} "
                  f"atr%={atr_pct:.4f} liq={liq:.1f}x sent={sent:+.2f}")
        logger.info(f"{symbol}: score={score:.3f} | {reason}")
        return {'symbol': symbol, 'asset_type': asset_type, 'score': round(score, 4),
                'signal': result['signal'], 'quality': result.get('quality', 0),
                'regime': regime, 'reason': reason}

    # ------------------------------------------------------------------
    def select(self, top_n: int = None, persist: bool = True) -> list:
        top_n = top_n or config.TOP_N_SYMBOLS
        candidates = self.mgr.list_symbols(active_only=True)
        if not candidates:
            candidates = [{'symbol': s, 'asset_type': 'stock'} for s in config.symbols]

        scored = []
        for c in candidates:
            if c['asset_type'] == 'crypto' and not config.CRYPTO_ENABLED:
                continue
            r = self.score_symbol(c['symbol'], c['asset_type'])
            if r:
                scored.append(r)

        scored.sort(key=lambda x: -x['score'])
        selected = scored[:top_n]
        today = datetime.utcnow().date()

        if persist and selected:
            try:
                conn = psycopg2.connect(config.database.url)
                cur = conn.cursor()
                for rank, r in enumerate(selected, start=1):
                    cur.execute("""
                        INSERT INTO daily_selection (selection_date, symbol, asset_type, rank, score, reason)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (selection_date, symbol) DO UPDATE SET
                            rank = EXCLUDED.rank, score = EXCLUDED.score, reason = EXCLUDED.reason;
                    """, (today, r['symbol'], r['asset_type'], rank, r['score'], r['reason']))
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                logger.error(f"Persisting selection failed: {e}")

        logger.info(f"Daily selection ({today}): "
                    + ", ".join(f"#{i+1} {r['symbol']}={r['score']:.2f}"
                                for i, r in enumerate(selected)))
        return selected


if __name__ == "__main__":
    sel = DailySelector().select()
    print("\n=== TODAY'S SELECTION ===")
    for i, r in enumerate(sel, 1):
        print(f"#{i} {r['symbol']} ({r['asset_type']}) score={r['score']:.2f} "
              f"signal={r['signal']} q={r['quality']:.2f}\n   {r['reason']}")
