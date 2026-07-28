"""
src/models/meta_learner.py  (Brain v2)
======================================
Case-based reasoning over Qdrant memory. For the current market state it
retrieves the k most similar HISTORICAL states and averages their realised
1-hour forward returns.

Brain v2 upgrades
-----------------
1. NO LOOK-AHEAD: only neighbours with ts <= (now - MEMORY_MIN_AGE_MINUTES)
   are used, so every neighbour's outcome was fully known at decision time.
   The current bar can never retrieve itself or the future.
2. ROBUST WEIGHTING: negative/zero cosine scores are discarded, a similarity
   floor is applied, weights are squared-positive scores (no sign flips, no
   division blow-ups).
3. NEIGHBOUR AGREEMENT: a signal is only valid if a weighted majority of
   neighbours agree on the direction - kills weak/coin-flip signals.
4. REGIME FILTER: optionally only compare against states from the same
   market regime (trend_up / trend_down / range).
5. SIGNAL QUALITY SCORE (0..1): conviction x agreement x similarity.
   Used by RiskGate for signal-strength position sizing.
6. SENTIMENT HONESTY: real FinBERT sentiment is a LIVE-only bias. In
   backtests it is off by default (SENTIMENT_IN_BACKTEST=False); the old
   MD5 "synthetic sentiment" noise channel is gone.
"""
import numpy as np
import pandas as pd
from config.settings import config
from src.memory.vector_encoder import VectorEncoder
from src.memory.qdrant_memory import QdrantMemory
from src.news_engine.orchestrator import NewsOrchestrator
from src.utils.logger import setup_logger

logger = setup_logger("MetaLearner", "logs/meta_learner.log")

BUY_THRESHOLD = config.BUY_THRESHOLD
SELL_THRESHOLD = config.SELL_THRESHOLD
NEIGHBORS = config.MEMORY_NEIGHBORS


class MetaLearner:
    def __init__(self):
        self.encoder = VectorEncoder()
        self.memory = QdrantMemory()
        self.news = NewsOrchestrator() if config.USE_REAL_SENTIMENT else None

    # ------------------------------------------------------------------
    def _sigmoid(self, x):
        return 1 / (1 + np.exp(-x))

    def _quality_score(self, prob, agreement, sim_avg):
        conviction = min(1.0, abs(prob - 0.5) / 0.10)   # full marks at +/-10%
        return round(float(conviction * abs(agreement) * max(0.0, min(1.0, sim_avg))), 4)

    # ------------------------------------------------------------------
    def get_signal(self, symbol: str, current_row: pd.Series, timestamp=None,
                   mode: str = 'live') -> dict:
        """
        mode='live'     -> real-time sentiment bias allowed
        mode='backtest' -> strictly point-in-time; sentiment bias off unless
                           SENTIMENT_IN_BACKTEST is enabled.
        """
        now_ts = int(pd.Timestamp(timestamp).timestamp()) if timestamp is not None \
            else int(pd.Timestamp.now(tz='UTC').timestamp())

        # ---- 1. Sentiment (honest by mode) ----
        if mode == 'live' and self.news is not None:
            sentiment = self.news.get_sentiment_for_symbol(symbol, timestamp)
        elif mode == 'backtest' and config.SENTIMENT_IN_BACKTEST:
            sentiment = float(current_row.get('sentiment_score', 0.0) or 0.0)
        else:
            sentiment = 0.0

        row_copy = current_row.copy()
        row_copy['sentiment_score'] = sentiment

        # ---- 2. Encode state ----
        try:
            current_vector = self.encoder.transform_single(row_copy)
        except Exception as e:
            logger.error(f"Vector encoding failed: {e}")
            return {'symbol': symbol, 'signal': 'HOLD', 'confidence': 0.5,
                    'quality': 0.0, 'reason': 'encoding_error'}

        # ---- 3. Retrieve similar PAST states (look-ahead guard) ----
        before_ts = now_ts - config.MEMORY_MIN_AGE_MINUTES * 60
        regime = row_copy.get('regime_label', 'unknown') if config.REGIME_FILTER_ENABLED else None
        try:
            hits = self.memory.search_similar(
                current_vector, symbol=None, limit=NEIGHBORS,
                before_ts=before_ts, regime=regime,
            )
            # Regime filter can starve the search early in memory life; retry without it
            if len(hits) < 10 and regime is not None:
                hits = self.memory.search_similar(
                    current_vector, symbol=None, limit=NEIGHBORS, before_ts=before_ts,
                )
        except Exception as e:
            logger.error(f"Qdrant search failed: {e}")
            return {'symbol': symbol, 'signal': 'HOLD', 'confidence': 0.5,
                    'quality': 0.0, 'reason': 'memory_error'}

        # ---- 4. Filter neighbours: usable outcomes + similarity floor ----
        usable = [h for h in hits
                  if h.get('forward_return_1h') is not None
                  and h['score'] >= config.MIN_NEIGHBOR_SIMILARITY]

        if len(usable) < 5:
            return self._trend_fallback(symbol, row_copy,
                                        reason=f'thin_memory({len(usable)})')

        # ---- 5. Robust weighting (positive squared scores) ----
        scores = np.array([max(h['score'], 0.0) for h in usable]) ** 2
        rets = np.array([h['forward_return_1h'] for h in usable], dtype=float)
        weights = scores / scores.sum()

        wmean = float(np.sum(rets * weights))
        agreement = float(np.sum(weights * np.sign(rets)))        # -1..1
        sim_avg = float(np.mean([h['score'] for h in usable]))

        prob = float(self._sigmoid(wmean * 100))

        # Live sentiment nudge (bounded, off in backtests by default)
        if mode == 'live':
            prob = float(np.clip(prob + sentiment * config.SENTIMENT_BIAS, 0.0, 1.0))

        # ---- 6. Decision with HOLD zone + agreement gate ----
        direction = 'HOLD'
        if prob > BUY_THRESHOLD:
            direction = 'BUY'
        elif prob < SELL_THRESHOLD:
            direction = 'SELL'

        # Agreement gate: agreement is weighted sign balance on [-1, 1]
        # (0.10 = 55/45 majority). MIN_NEIGHBOR_AGREEMENT=0.55 means we
        # require at least a 55/45 majority, i.e. |agreement| >= 0.10.
        min_balance = 2 * config.MIN_NEIGHBOR_AGREEMENT - 1
        if direction != 'HOLD' and abs(agreement) < min_balance:
            direction = 'HOLD'

        quality = self._quality_score(prob, agreement, sim_avg)

        reason = (f"wmean={wmean:+.5f} agree={agreement:+.2f} sim={sim_avg:.2f} "
                  f"n={len(usable)} sent={sentiment:+.2f} regime={row_copy.get('regime_label', 'n/a')}")
        logger.info(f"{symbol} | {direction} | prob={prob:.4f} q={quality:.3f} | {reason}")

        return {
            'symbol': symbol, 'signal': direction,
            'confidence': round(prob, 4),
            'quality': quality,
            'agreement': round(agreement, 4),
            'weighted_avg_return': round(wmean, 6),
            'similarity_avg': round(sim_avg, 4),
            'neighbors_used': len(usable),
            'sentiment': sentiment,
            'regime': row_copy.get('regime_label', 'unknown'),
            'reason': reason,
        }

    # ------------------------------------------------------------------
    def _trend_fallback(self, symbol, row, reason='thin_memory'):
        """Thin memory -> mild trend bias, still inside the HOLD zone logic."""
        prob = 0.5
        try:
            sma200 = row.get('sma_200')
            if sma200 is not None and not pd.isna(sma200):
                prob = 0.6 if row['close'] > sma200 else 0.4
        except Exception:
            pass
        signal = 'BUY' if prob > BUY_THRESHOLD else ('SELL' if prob < SELL_THRESHOLD else 'HOLD')
        return {'symbol': symbol, 'signal': signal, 'confidence': prob,
                'quality': 0.25, 'agreement': 0.0, 'neighbors_used': 0,
                'reason': f'trend_fallback ({reason})'}
