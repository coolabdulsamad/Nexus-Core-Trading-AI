"""
src/memory/vector_encoder.py
Responsibility: Load historical features, join VWAP, normalize, and compress.
FIXED: Handles zero-variance columns gracefully and robust model loading.
"""
import pandas as pd
import numpy as np
import psycopg2
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from config.settings import config
from utils.logger import setup_logger
import joblib
import os

logger = setup_logger("VectorEncoder", "logs/memory.log")

class VectorEncoder:
    def __init__(self):
        self.conn = psycopg2.connect(config.database.url)
        self.scaler = StandardScaler()
        self.pca = None
        self.is_fitted = False
        self.feature_columns = []
        self.scaler_means = None  # Store manually calculated means
        self.scaler_stds = None   # Store manually calculated stds

    def fetch_all_features(self, symbols=None) -> pd.DataFrame:
        """Fetches feature vectors + VWAP from the database."""
        if symbols is None:
            symbols = config.symbols
        
        # Join market_data to get VWAP
        query = """
            SELECT 
                f.symbol,
                f.time_bucket,
                f.rsi_14,
                f.macd_line,
                f.macd_signal,
                f.atr_14,
                f.bb_upper,
                f.bb_lower,
                f.volume_profile_ratio,
                f.sentiment_score,
                f.correlation_spy,
                m.vwap
            FROM feature_cache f
            JOIN market_data m ON f.symbol = m.symbol AND f.time_bucket = m.time_bucket
            WHERE f.symbol = ANY(%s)
            ORDER BY f.time_bucket ASC
        """
        df = pd.read_sql(query, self.conn, params=(symbols,))
        if df.empty:
            logger.error("No feature data found in database.")
            return df
        
        # Drop rows where VWAP is null (just in case)
        df = df.dropna(subset=['vwap'])
        
        # Fill any other stray NaNs with 0 (safety net)
        df = df.fillna(0)
        
        logger.info(f"Loaded {len(df)} feature rows from database.")
        return df

    def fit_transform(self, df: pd.DataFrame) -> np.ndarray:
        """
        Normalizes the data and runs PCA dynamically.
        Handles zero-variance columns by setting their std to 1.0 to avoid NaN.
        """
        # Store the feature column names
        self.feature_columns = [col for col in df.columns if col not in ['symbol', 'time_bucket']]
        X_raw = df[self.feature_columns].values

        # 1. Replace any inf/nan with 0
        X_raw = np.nan_to_num(X_raw)

        # 2. MANUAL SCALING to avoid division-by-zero errors from StandardScaler
        means = np.mean(X_raw, axis=0)
        stds = np.std(X_raw, axis=0)
        
        # CRITICAL FIX: If a column has zero standard deviation (all values identical), 
        # we set its std to 1.0 so that (value - mean) / 1.0 = 0. It contributes nothing.
        stds[stds == 0] = 1.0
        
        # Store these for future transform_single calls
        self.scaler_means = means
        self.scaler_stds = stds
        
        # Apply the scaling
        X_scaled = (X_raw - means) / stds

        # 3. Determine PCA components (max 64, but capped by available features)
        n_features = X_scaled.shape[1]
        n_components = min(64, n_features)
        
        logger.info(f"Input features: {n_features}. Setting PCA components to: {n_components}")
        
        # 4. Fit PCA
        self.pca = PCA(n_components=n_components)
        X_compressed = self.pca.fit_transform(X_scaled)
        
        self.is_fitted = True
        logger.info(f"PCA complete. Output dimensions: {X_compressed.shape[1]}")

        # 5. Save the models and scaler parameters to disk
        os.makedirs("models/saved", exist_ok=True)
        joblib.dump(self.pca, "models/saved/pca.pkl")
        joblib.dump(self.feature_columns, "models/saved/feature_columns.pkl")
        joblib.dump(self.scaler_means, "models/saved/scaler_means.pkl")
        joblib.dump(self.scaler_stds, "models/saved/scaler_stds.pkl")
        
        return X_compressed

    def transform_single(self, row: pd.Series) -> np.ndarray:
        """
        Transforms a SINGLE new row for live inference.
        Uses the pre-fitted PCA and custom scaler parameters.
        """
        if not self.is_fitted:
            try:
                self.pca = joblib.load("models/saved/pca.pkl")
                self.feature_columns = joblib.load("models/saved/feature_columns.pkl")
                self.scaler_means = joblib.load("models/saved/scaler_means.pkl")
                self.scaler_stds = joblib.load("models/saved/scaler_stds.pkl")
                self.is_fitted = True
                logger.info("Loaded pre-fitted models from disk.")
            except FileNotFoundError:
                raise RuntimeError("PCA models not found. Please run build_memory.py first.")
            except Exception as e:
                raise RuntimeError(f"Failed to load PCA models: {e}")

        # Ensure row has the exact columns, fill missing with 0
        X_raw = np.array([row.get(col, 0) for col in self.feature_columns]).reshape(1, -1)
        X_raw = np.nan_to_num(X_raw)
        
        # Apply the stored custom scaling
        X_scaled = (X_raw - self.scaler_means) / self.scaler_stds
        
        # Compress with PCA
        X_compressed = self.pca.transform(X_scaled)
        
        return X_compressed.flatten()