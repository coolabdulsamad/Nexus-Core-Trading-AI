"""
src/memory/vector_encoder.py  (v3)
Compresses each market state into a PCA vector for the memory.

v2: all vector features are SCALE-FREE (ratios, %, ATR units, z-scores),
so states are comparable across symbols and price levels. Raw price-scale
columns stay in the DB but are NOT used in the vector.

v3: timeframe-aware - reads market_data{BAR_SUFFIX} / feature_cache{BAR_SUFFIX}
so the same encoder serves 5-min and 1h data layers.
"""
import os
import joblib
import numpy as np
import pandas as pd
import psycopg2
from sklearn.decomposition import PCA
from config.settings import config
from src.utils.logger import setup_logger

logger = setup_logger("VectorEncoder", "logs/memory.log")

# The exact, ordered feature set used in the vector (all scale-free)
FEATURE_COLS = [
    'rsi_14', 'macd_hist_pct', 'bb_pct_b', 'bb_width', 'atr_pct',
    'ret_1', 'ret_3', 'ret_12',
    'volume_profile_ratio', 'vol_z',
    'adx_14', 'dist_sma50', 'dist_sma200', 'dist_vwap',
    'hour_sin', 'hour_cos', 'sentiment_score',
]

_MODEL_DIR = "models/saved"


class VectorEncoder:
    def __init__(self):
        self.conn = psycopg2.connect(config.database.url)
        self.pca = None
        self.is_fitted = False
        self.feature_columns = FEATURE_COLS
        self.scaler_means = None
        self.scaler_stds = None

    # ------------------------------------------------------------------
    @staticmethod
    def _prepare(df: pd.DataFrame) -> pd.DataFrame:
        """Derives scale-free features from raw DB columns."""
        out = df.copy()
        close_safe = out['close'].replace(0, np.nan)
        out['macd_hist_pct'] = out['macd_hist'] / close_safe * 100
        out['sentiment_score'] = out.get('sentiment_score', 0.0)
        return out

    # ------------------------------------------------------------------
    def fetch_all_features(self, symbols=None) -> pd.DataFrame:
        if symbols is None:
            symbols = config.symbols

        suffix = config.BAR_SUFFIX
        query = f"""
            SELECT f.*, m.close, m.vwap
            FROM feature_cache{suffix} f
            JOIN market_data{suffix} m ON f.symbol = m.symbol AND f.time_bucket = m.time_bucket
            WHERE f.symbol = ANY(%s)
            ORDER BY f.time_bucket ASC
        """
        df = pd.read_sql(query, self.conn, params=(symbols,))
        if df.empty:
            logger.error("No feature data found in database.")
            return df

        df = self._prepare(df)
        df = df.dropna(subset=['vwap', 'close'])
        logger.info(f"Loaded {len(df)} feature rows from database (tables: *{suffix or ' (5-min)'}).")
        return df

    # ------------------------------------------------------------------
    def fit_transform(self, df: pd.DataFrame) -> np.ndarray:
        X_raw = df[self.feature_columns].astype(float).values
        X_raw = np.nan_to_num(X_raw)

        means = np.mean(X_raw, axis=0)
        stds = np.std(X_raw, axis=0)
        stds[stds == 0] = 1.0                      # zero-variance guard
        self.scaler_means, self.scaler_stds = means, stds

        X_scaled = (X_raw - means) / stds

        n_components = min(config.PCA_COMPONENTS, X_scaled.shape[1], X_scaled.shape[0])
        logger.info(f"Features: {X_scaled.shape[1]} | PCA components: {n_components}")
        self.pca = PCA(n_components=n_components)
        X_compressed = self.pca.fit_transform(X_scaled)
        self.is_fitted = True

        os.makedirs(_MODEL_DIR, exist_ok=True)
        joblib.dump(self.pca, f"{_MODEL_DIR}/pca.pkl")
        joblib.dump(self.feature_columns, f"{_MODEL_DIR}/feature_columns.pkl")
        joblib.dump(self.scaler_means, f"{_MODEL_DIR}/scaler_means.pkl")
        joblib.dump(self.scaler_stds, f"{_MODEL_DIR}/scaler_stds.pkl")
        logger.info(f"PCA fitted. Explained variance: {self.pca.explained_variance_ratio_.sum():.2%}")
        return X_compressed

    # ------------------------------------------------------------------
    def transform_single(self, row: pd.Series) -> np.ndarray:
        if not self.is_fitted:
            try:
                self.pca = joblib.load(f"{_MODEL_DIR}/pca.pkl")
                self.feature_columns = joblib.load(f"{_MODEL_DIR}/feature_columns.pkl")
                self.scaler_means = joblib.load(f"{_MODEL_DIR}/scaler_means.pkl")
                self.scaler_stds = joblib.load(f"{_MODEL_DIR}/scaler_stds.pkl")
                self.is_fitted = True
            except Exception as e:
                raise RuntimeError(f"PCA models not found. Run build_memory first. ({e})")

        df = pd.DataFrame([row])
        df = self._prepare(df)
        X_raw = np.array([[df.iloc[0].get(col, 0) for col in self.feature_columns]], dtype=float)
        X_raw = np.nan_to_num(X_raw)
        X_scaled = (X_raw - self.scaler_means) / self.scaler_stds
        return self.pca.transform(X_scaled).flatten()
