"""
src/models/meta_learner.py  (Brain v2.3.0)
======================================
Case-based reasoning over Qdrant memory. For the current market state it
retrieves the k most similar HISTORICAL states and averages their realised
forward returns at the configured prediction horizon
(config.FORWARD_HORIZON_HOURS, default 4h).

v2.2.1
------
- TREND FALLBACK IS NOW ADVISORY-ONLY (quality 0.10 < MIN_SIGNAL_QUALITY).
  The old fallback had quality 0.25 - above the 0.20 trade gate - so when
  the brain is blind (thin memory) the system machine-gunned sma_200
  fallback trades (798 trades, -61% in one backtest). A blind system must
  stand down, not gamble. Set ALLOW_TREND_FALLBACK_TRADES=True in config
  only if you explicitly want the old behaviour.

v2.3.0 (quality-formula fix, measured on 52,373 live readings Sep 15-Oct 13)
----------------------------------------------------------------------------
- THE SATURATION BUG: conviction was min(1, |prob-0.5| / 0.10) - full marks
  whenever prob moved just 10% off 0.5, i.e. |wmean| >= 0.004. Real weighted
  4h returns live at |wmean| ~ 0.007 (p90), so ~20% of ALL readings pinned
  conviction at 1.0 and q collapsed onto a flat 0.43-0.47 plateau
  (= 1.0 x |agree| x sim). Quality was effectively BINARY: the saturated
  cluster rubber-stamped every gate while genuinely good setups
  (q 0.10-0.30) were blocked. corr(q, |wmean|) was only 0.30.
- THE FIX: conviction = min(1, |prob-0.5| * 2) - a linear gradient that
  only reaches full marks at prob -> 0/1. On the same 52,373 readings:
  saturation 19.7% -> 0.0%, corr(q, |wmean|) 0.30 -> 0.77. Quality now
  actually ranks setups.
- Gates recalibrated in settings.py to the fixed distribution (quality is
  now computed from the PRE-NUDGE prob, so the saturating news feed can't
  inflate it): stock MIN = 0.07 (~p57 of stock signals), MEDIUM = 0.10,
  STRONG = 0.156 (the strong-evidence plateau, top ~23%), CRYPTO_MIN =
  0.029 (p85 of the smooth crypto gradient), TREND_REGIME_MIN = STRONG.
  Advisory fallback quality lowered 0.10 -> 0.005 so a blind brain still
  stands down under the new floor.
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
OUTCOME_KEY = f"forward_return_{config.FORWARD_HORIZON_HOURS}h"
ALLOW_FALLBACK_TRADES = getattr(config, 'ALLOW_TREND_FALLBACK_TRADES', False)


class MetaLearner:
    def __init__(self):
        self.encoder = VectorEncoder()
        self.memory = QdrantMemory()
        self.news = NewsOrchestrator() if config.USE_REAL_SENTIMENT else None

    # ------------------------------------------------------------------
    def _sigmoid(self, x):
        return 1 / (1 + np.exp(-x))

    def _quality_score(self, prob, agreement, sim_avg):
        # v2.3.0: linear conviction gradient. The old /0.10 scale gave full
        # marks at prob=0.60 (|wmean| >= 0.004) - 19.7% of live readings
        # saturated, and q became a binary stamp instead of a ranking.
        # *2 reaches full marks only at prob -> 0/1, so conviction tracks
        # the actual prediction magnitude across the whole observed range.
        conviction = min(1.0, abs(prob - 0.5) * 2)
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
                  if h.get(OUTCOME_KEY) is not None
                  and h['score'] >= config.MIN_NEIGHBOR_SIMILARITY]

        if len(usable) < 5:
            return self._trend_fallback(symbol, row_copy,
                                        reason=f'thin_memory({len(usable)})')

        # ---- 5. Robust weighting (positive squared scores) ----
        scores = np.array([max(h['score'], 0.0) for h in usable]) ** 2
        rets = np.array([h[OUTCOME_KEY] for h in usable], dtype=float)
        weights = scores / scores.sum()

        wmean = float(np.sum(rets * weights))
        agreement = float(np.sum(weights * np.sign(rets)))        # -1..1
        sim_avg = float(np.mean([h['score'] for h in usable]))

        prob = float(self._sigmoid(wmean * 100))

        # v2.3.0: quality is computed from the PRE-NUDGE prob. Quality must
        # measure the strength of the MEMORY EVIDENCE (wmean/agree/sim) only.
        # The NewsAPI keyword feed saturates at -1.00 in gloomy macro weeks;
        # letting the sentiment nudge inflate conviction would rubber-stamp
        # weak memory signals exactly when the feed is most extreme. The
        # nudge still influences DIRECTION below - it just can't buy quality.
        # (Backtests never nudge, so this also restores live<->backtest
        # quality parity.)
        quality = self._quality_score(prob, agreement, sim_avg)

        # Live sentiment nudge (bounded, off in backtests by default)
        if mode == 'live':
            prob = float(np.clip(prob + sentiment * config.SENTIMENT_BIAS, 0.0, 1.0))

        # ---- 6. Decision with HOLD zone + agreement gates ----
        # (quality was already computed above, from the pre-nudge prob)
        direction = 'HOLD'
        if prob > BUY_THRESHOLD:
            direction = 'BUY'
        elif prob < SELL_THRESHOLD:
            direction = 'SELL'

        # Agreement gate: |agreement| >= 0.10 == at least a 55/45 majority
        min_balance = 2 * config.MIN_NEIGHBOR_AGREEMENT - 1
        if direction != 'HOLD' and abs(agreement) < min_balance:
            direction = 'HOLD'

        # Direction alignment: the weighted majority must agree WITH the
        # signal (a SELL backed by mostly-positive outcomes is contradictory).
        if direction == 'BUY' and agreement <= 0:
            direction = 'HOLD'
        elif direction == 'SELL' and agreement >= 0:
            direction = 'HOLD'

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
        """
        Thin memory -> mild trend bias. ADVISORY ONLY: quality stays below
        MIN_SIGNAL_QUALITY unless ALLOW_TREND_FALLBACK_TRADES is set, so a
        blind system stands down instead of gambling.
        """
        prob = 0.5
        try:
            sma200 = row.get('sma_200')
            if sma200 is not None and not pd.isna(sma200):
                prob = 0.6 if row['close'] > sma200 else 0.4
        except Exception:
            pass
        signal = 'BUY' if prob > BUY_THRESHOLD else ('SELL' if prob < SELL_THRESHOLD else 'HOLD')
        # v2.3.0: advisory quality 0.10 -> 0.005. Gates were recalibrated to
        # the fixed quality scale (MIN_SIGNAL_QUALITY = 0.07), so 0.10 would
        # now PASS the entry floor and re-enable blind fallback trading.
        # 0.005 stays below any sane gate; opting in via ALLOW_TREND_FALLBACK_
        # TRADES sets 0.16 (just above the STRONG tier, 0.156).
        quality = 0.16 if ALLOW_FALLBACK_TRADES else 0.005
        return {'symbol': symbol, 'signal': signal, 'confidence': prob,
                'quality': quality, 'agreement': 0.0, 'neighbors_used': 0,
                'reason': f'trend_fallback ({reason})'}
