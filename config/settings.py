"""
config/settings.py
Single source of truth for Nexus Core.
Secrets come ONLY from environment / .env - never hardcode credentials here.
"""
import os
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()


class DatabaseSettings(BaseModel):
    url: str = os.getenv("DATABASE_URL", "postgresql://localhost:5432/nexus_core")


class QdrantSettings(BaseModel):
    host: str = os.getenv("QDRANT_HOST", "localhost")
    port: int = int(os.getenv("QDRANT_PORT", "6333"))


class PolygonSettings(BaseModel):
    api_key: str = os.getenv("POLYGON_API_KEY", "")


class GlobalConfig:
    database = DatabaseSettings()
    qdrant = QdrantSettings()
    polygon = PolygonSettings()

    # ----- Universe -----
    symbols = ["AAPL", "TSLA", "MSFT", "GOOGL", "NVDA"]
    BAR_MINUTES = 5                      # bar size used everywhere

    # ----- Brain (case-based memory) -----
    MEMORY_NEIGHBORS = 100
    MEMORY_MIN_AGE_MINUTES = 60
    MIN_NEIGHBOR_SIMILARITY = 0.50
    MIN_NEIGHBOR_AGREEMENT = 0.55
    REGIME_FILTER_ENABLED = True
    BUY_THRESHOLD = 0.52
    SELL_THRESHOLD = 0.48
    PCA_COMPONENTS = 64

    # ----- Sentiment -----
    USE_REAL_SENTIMENT = True
    SENTIMENT_BIAS = 0.05
    SENTIMENT_IN_BACKTEST = False

    # ----- Entry quality gate -----
    ENTRY_CONVICTION_MARGIN = 0.015
    MIN_SIGNAL_QUALITY = 0.20

    # ----- Signal-strength position sizing (quality tiers) -----
    QUALITY_STRONG = 0.60
    QUALITY_MEDIUM = 0.35
    RISK_PCT_STRONG = 0.020
    RISK_PCT_MEDIUM = 0.010
    RISK_PCT_WEAK = 0.005
    NOTIONAL_CAP_PCT = 0.75
    NOTIONAL_CAP_ABS = 75000

    # ----- Trade structure -----
    STOP_ATR_MULT = 3.0
    REWARD_RISK_RATIO = 3.0
    BREAKEVEN_ATR_MULTIPLE = 2.5
    SLIPPAGE_BPS = 0.0005
    COMMISSION_BPS = 0.0003
    TIME_LIMIT_BARS = 48
    COOLDOWN_BARS = 2

    # ----- SMA exit (fixed: buffered + confirmed) -----
    SMA_EXIT_ENABLED = True
    SMA_EXIT_BUFFER_ATR = 0.25
    SMA_EXIT_CONFIRM_BARS = 2

    # ----- Daily guards -----
    DAILY_LOSS_LIMIT_PCT = 0.05
    DAILY_PROFIT_TARGET_PCT = 0.02
    DAILY_TARGET_LOCK_BREAKEVEN = True

    # ----- Profit locking (live) -----
    ENABLE_PARTIAL_TAKE_PROFIT = True
    PARTIAL_TP_THRESHOLD = 0.60
    PARTIAL_CLOSE_PCT = 0.50
    ENABLE_TRAILING_TP = True
    TRAILING_TP_ATR_TRIGGER = 3.0
    TRAILING_TP_DISTANCE_ATR = 1.5
    ENABLE_PROFIT_DRAWDOWN_PROTECTION = True
    RETRACEMENT_HIGH_THRESHOLD = 0.70
    RETRACEMENT_LOCK_THRESHOLD = 0.50
    ENABLE_TIME_PARTIAL = True
    TIME_PARTIAL_BARS = 12
    TIME_PARTIAL_PROFIT_ATR = 0.5

    # ----- Data freshness -----
    MAX_DATA_AGE_SECONDS = 600
    DATA_FETCH_TIMEOUT = 30

    # ----- Circuit breakers -----
    MAX_DRAWDOWN_PCT = 0.10
    VOLATILITY_FILTER_ENABLED = True
    MIN_ATR_THRESHOLD = 0.5
    VOLATILITY_RATIO_MAX = 3.0

    # ----- Notifications -----
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    TELEGRAM_EOD_REPORT = True
    TELEGRAM_HEARTBEAT_CYCLES = 6

    # ----- Universe & daily selection (PR 2) -----
    UNIVERSE_MODE = os.getenv("UNIVERSE_MODE", "manual")  # 'manual' = active DB symbols | 'auto' = daily top-N selector
    TOP_N_SYMBOLS = 5                    # how many symbols the selector trades per day
    CRYPTO_ENABLED = True
    CRYPTO_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD"]
    SELECTOR_ATR_PCT_MIN = 0.0015        # volatility sweet spot for 5-min trading
    SELECTOR_ATR_PCT_MAX = 0.030

    # ----- Misc -----
    WIN_RATE_WINDOW = 20
    ENABLE_DYNAMIC_THRESHOLDS = False
    USE_BROKER_BRACKET_ORDERS = False


config = GlobalConfig()
