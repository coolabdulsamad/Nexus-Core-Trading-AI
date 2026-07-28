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
    MEMORY_NEIGHBORS = 100               # k nearest states to retrieve
    MEMORY_MIN_AGE_MINUTES = 60          # neighbor must be >= this old (its 1h outcome fully known) -> kills look-ahead
    MIN_NEIGHBOR_SIMILARITY = 0.50       # cosine floor; neighbors below this are ignored
    MIN_NEIGHBOR_AGREEMENT = 0.55        # weighted fraction of neighbors agreeing on direction (0.5 = coin flip)
    REGIME_FILTER_ENABLED = True         # prefer neighbors from the same market regime
    BUY_THRESHOLD = 0.52                 # HOLD zone between 0.48 and 0.52
    SELL_THRESHOLD = 0.48
    PCA_COMPONENTS = 64

    # ----- Sentiment -----
    USE_REAL_SENTIMENT = True            # FinBERT on real headlines (live only)
    SENTIMENT_BIAS = 0.05                # live bias strength
    SENTIMENT_IN_BACKTEST = False        # keep bias OFF in backtests (honesty)

    # ----- Entry quality gate -----
    ENTRY_CONVICTION_MARGIN = 0.015      # min |prob - 0.5|
    MIN_SIGNAL_QUALITY = 0.20            # composite quality floor (0..1) to allow any trade

    # ----- Signal-strength position sizing (quality tiers) -----
    QUALITY_STRONG = 0.60                # quality >= this -> strong tier
    QUALITY_MEDIUM = 0.35                # quality >= this -> medium tier (below -> weak tier)
    RISK_PCT_STRONG = 0.020              # 2.0% of slice risked on strong signals
    RISK_PCT_MEDIUM = 0.010              # 1.0%
    RISK_PCT_WEAK = 0.005                # 0.5%  (weak but tradable)
    NOTIONAL_CAP_PCT = 0.75              # max notional per position, % of per-symbol capital
    NOTIONAL_CAP_ABS = 75000             # absolute $ cap per position

    # ----- Trade structure -----
    STOP_ATR_MULT = 3.0                  # stop distance = 3 x ATR
    REWARD_RISK_RATIO = 3.0              # TP = 3R
    BREAKEVEN_ATR_MULTIPLE = 2.5         # move stop to entry after 2.5 x ATR profit
    SLIPPAGE_BPS = 0.0005
    COMMISSION_BPS = 0.0003
    TIME_LIMIT_BARS = 48                 # full exit after N bars
    COOLDOWN_BARS = 2

    # ----- SMA exit (fixed: buffered + confirmed) -----
    SMA_EXIT_ENABLED = True
    SMA_EXIT_BUFFER_ATR = 0.25           # price must close 0.25 x ATR beyond SMA...
    SMA_EXIT_CONFIRM_BARS = 2            # ...for 2 consecutive bars to trigger exit

    # ----- Daily guards -----
    DAILY_LOSS_LIMIT_PCT = 0.05          # stop trading the symbol day after -5%
    DAILY_PROFIT_TARGET_PCT = 0.02       # +2% day -> stop opening new trades (0 = disabled)
    DAILY_TARGET_LOCK_BREAKEVEN = True   # when target hit, move open stops to breakeven

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
    MAX_DATA_AGE_SECONDS = 600           # refuse to trade on bars older than 10 min
    DATA_FETCH_TIMEOUT = 30

    # ----- Circuit breakers -----
    MAX_DRAWDOWN_PCT = 0.10
    VOLATILITY_FILTER_ENABLED = True
    MIN_ATR_THRESHOLD = 0.5
    VOLATILITY_RATIO_MAX = 3.0

    # ----- Notifications -----
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    TELEGRAM_EOD_REPORT = True           # end-of-day analysis report
    TELEGRAM_HEARTBEAT_CYCLES = 6        # equity heartbeat every N cycles (~30 min)

    # ----- Misc -----
    WIN_RATE_WINDOW = 20
    ENABLE_DYNAMIC_THRESHOLDS = False
    USE_BROKER_BRACKET_ORDERS = False


config = GlobalConfig()
