"""
src/models/meta_learner.py
FINAL CONFIGURATION:
- BUY_THRESHOLD = 0.52
- SELL_THRESHOLD = 0.48
- SENTIMENT_BIAS = 0.05
- NEIGHBORS = 50
- No symbol-specific overrides (apply globally).
"""
import numpy as np
import pandas as pd
import requests
from config.settings import config
from src.memory.vector_encoder import VectorEncoder
from src.news_engine.orchestrator import NewsOrchestrator
from utils.logger import setup_logger

logger = setup_logger("MetaLearner", "logs/meta_learner.log")

# --- LOCKED CONFIGURATION (Proven on AAPL 3-month) ---
BUY_THRESHOLD = 0.50      # back to original
SELL_THRESHOLD = 0.50     # back to original
NEIGHBORS = 100           # smoother average (increase from 50)
SENTIMENT_BIAS = 0.05     # now apply a small bias (use config value)
# ----------------------------------------------------

class MetaLearner:
    def __init__(self):
        self.collection_name = "market_memory"
        self.encoder = VectorEncoder()
        self.news = NewsOrchestrator()
        self.qdrant_url = f"http://{config.qdrant.host}:{config.qdrant.port}"
        
        try:
            self.encoder.transform_single(pd.Series())
        except Exception as e:
            logger.warning(f"PCA models not loaded. Will attempt during first prediction. Error: {e}")
    
    def get_signal(self, symbol: str, current_row: pd.Series, timestamp=None) -> dict:
        # 1. Get sentiment
        sentiment = self.news.get_sentiment_for_symbol(symbol, timestamp)
        
        # 2. Create a copy to avoid SettingWithCopyWarning
        row_copy = current_row.copy()
        row_copy['sentiment_score'] = sentiment
        
        # 3. Convert to vector
        try:
            current_vector = self.encoder.transform_single(row_copy)
        except Exception as e:
            logger.error(f"Vector encoding failed: {e}")
            return {'signal': 'HOLD', 'confidence': 0.5, 'reason': 'Encoding error'}
        
        # 4. Query Qdrant
        search_url = f"{self.qdrant_url}/collections/{self.collection_name}/points/search"
        payload = {
            "vector": current_vector.tolist(),
            "limit": NEIGHBORS,
            "with_payload": True
        }
        
        try:
            response = requests.post(search_url, json=payload)
            response.raise_for_status()
            search_data = response.json()
        except Exception as e:
            logger.error(f"Qdrant search failed: {e}")
            # Fallback: simple price trend based on current row's close vs a moving average?
            # Since we don't have historical close, we use a default probability.
            prob = 0.5
            signal = "HOLD"
            return {'signal': signal, 'confidence': prob, 'reason': 'Qdrant error fallback'}
        
        if not search_data.get('result'):
            # No similar states found – use a simple heuristic: if close > 200 SMA, bias bullish
            try:
                if row_copy.get('sma_200') is not None:
                    if row_copy['close'] > row_copy['sma_200']:
                        prob = 0.6
                    else:
                        prob = 0.4
                else:
                    prob = 0.5
                signal = "BUY" if prob > BUY_THRESHOLD else ("SELL" if prob < SELL_THRESHOLD else "HOLD")
                return {'signal': signal, 'confidence': prob, 'reason': 'No memory – trend fallback'}
            except:
                return {'signal': 'HOLD', 'confidence': 0.5, 'reason': 'No memory'}
        
        hits = search_data['result']
        forward_returns = []
        scores = []
        for hit in hits:
            if hit.get('payload', {}).get('forward_return_1h') is not None:
                forward_returns.append(hit['payload']['forward_return_1h'])
                scores.append(hit['score'])
        
        if not forward_returns:
            return {'signal': 'HOLD', 'confidence': 0.5, 'reason': 'Missing forward returns'}
        
        weights = np.array(scores) / np.sum(scores)
        weighted_avg_return = np.sum(np.array(forward_returns) * weights)
        
        # Sigmoid scaling
        probability = 1 / (1 + np.exp(-weighted_avg_return * 100))
        
        # Sentiment bias
        # sentiment_bias = sentiment * SENTIMENT_BIAS
        # probability = np.clip(probability + sentiment_bias, 0.0, 1.0)

        sentiment_boost = sentiment * SENTIMENT_BIAS * (1 + abs(sentiment))  # extra boost when |sentiment| is high
        probability = np.clip(probability + sentiment_boost, 0.0, 1.0)
        
        # Generate signal using the locked thresholds
        signal = "HOLD"
        if probability > BUY_THRESHOLD:
            signal = "BUY"
        elif probability < SELL_THRESHOLD:
            signal = "SELL"
        
        reason = f"Weighted avg return: {weighted_avg_return:.4f}, Sentiment: {sentiment:.2f}"
        logger.info(f"{symbol} | Signal: {signal} | Prob: {probability:.4f} | Reason: {reason}")
        
        return {
            'symbol': symbol,
            'signal': signal,
            'confidence': round(probability, 4),
            'weighted_avg_return': round(weighted_avg_return, 4),
            'sentiment': sentiment,
            'neighbors_used': len(forward_returns),
            'reason': reason
        }