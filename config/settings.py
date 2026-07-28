# import os
# from dotenv import load_dotenv
# from pydantic import BaseModel

# load_dotenv()

# class DatabaseSettings(BaseModel):
#     url: str = os.getenv("DATABASE_URL", "postgresql://nexus_admin:admin123%40A@localhost:5432/nexus_core")

# class QdrantSettings(BaseModel):
#     host: str = os.getenv("QDRANT_HOST", "localhost")
#     port: int = int(os.getenv("QDRANT_PORT", "6333"))

# class PolygonSettings(BaseModel):
#     api_key: str = os.getenv("POLYGON_API_KEY", "")

# class GlobalConfig:
#     database = DatabaseSettings()
#     qdrant = QdrantSettings()
#     polygon = PolygonSettings()
#     symbols = ["AAPL", "TSLA", "MSFT", "GOOGL", "NVDA"]
#     VOLATILITY_FILTER_ENABLED = False
#     USE_REAL_SENTIMENT = True
#     DAILY_LOSS_LIMIT_PCT = 0.03          # 3% daily loss limit (more room)
#     USE_LIMIT_ORDERS = True
#     LIMIT_ORDER_OFFSET_BPS = 10

#     WIN_RATE_WINDOW = 20               # number of recent trades to compute win rate
#     THRESHOLD_ADJUSTMENT_STEP = 0.05   # step size for dynamic threshold adjustment

#     TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
#     TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

#     # ---- New stable settings ----
#     # BASE_ENTRY_CONFIDENCE = 0.40          # higher threshold
#     # SENTIMENT_BIAS = 0.0                  # no sentiment influence
#     # BREAKEVEN_ATR_MULTIPLE = 1.5          # breakeven after 1.5×ATR profit
#     MAX_DRAWDOWN_PCT = 0.10                # disabled for backtest
#     DATA_FETCH_TIMEOUT = 30
#     USE_BROKER_BRACKET_ORDERS = False
#     ENABLE_DYNAMIC_THRESHOLDS = False     # disable adaptive threshold (keep constant)


#     # BASE_ENTRY_CONFIDENCE = 0.38          # allow more signals (was 0.45)
#     # SENTIMENT_BIAS = 0.0                  # keep neutral until we validate sentiment
#     BREAKEVEN_ATR_MULTIPLE = 1.5          # move to breakeven earlier (was 2.0)
#     # VOLATILITY_FILTER_ENABLED = True      # avoid crazy periods
#     DAILY_LOSS_LIMIT_PCT = 0.05           # room to breathe
#     USE_REAL_SENTIMENT = True             # keep if available, but bias = 0

#     BASE_ENTRY_CONFIDENCE = 0.35      # original value (allow more signals)
#     SENTIMENT_BIAS = 0.05             # now used
#     VOLATILITY_FILTER_ENABLED = True  # keep but relax threshold

# config = GlobalConfig()

import os
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()

class DatabaseSettings(BaseModel):
    url: str = os.getenv("DATABASE_URL", "postgresql://nexus_admin:admin123%40A@localhost:5432/nexus_core")

class QdrantSettings(BaseModel):
    host: str = os.getenv("QDRANT_HOST", "localhost")
    port: int = int(os.getenv("QDRANT_PORT", "6333"))

class PolygonSettings(BaseModel):
    api_key: str = os.getenv("POLYGON_API_KEY", "")

class GlobalConfig:
    database = DatabaseSettings()
    qdrant = QdrantSettings()
    polygon = PolygonSettings()
    symbols = ["AAPL", "TSLA", "MSFT", "GOOGL", "NVDA"]

    # ----- Filters & Risk -----
    VOLATILITY_FILTER_ENABLED = True
    USE_REAL_SENTIMENT = True
    DAILY_LOSS_LIMIT_PCT = 0.05
    USE_LIMIT_ORDERS = False          # <<< CHANGED to market orders
    LIMIT_ORDER_OFFSET_BPS = 15       # (unused now)

    WIN_RATE_WINDOW = 20
    THRESHOLD_ADJUSTMENT_STEP = 0.05
    ENABLE_DYNAMIC_THRESHOLDS = False

    # ----- Entry Confidence -----
    # NOTE: MetaLearner returns confidence == probability, where BUY means
    # prob>0.5 and SELL means prob<0.5. Gating on `confidence >= 0.50` therefore
    # makes SHORT entries impossible (SELL prob is always <0.5). The live trader
    # now gates on CONVICTION = abs(prob - 0.5) >= ENTRY_CONVICTION_MARGIN instead.
    BASE_ENTRY_CONFIDENCE = 0.50          # legacy; kept for reference only
    ENTRY_CONVICTION_MARGIN = 0.015       # how far prob must be from 0.5 to enter

    # ----- Risk Parameters -----
    BREAKEVEN_ATR_MULTIPLE = 2.5
    MAX_DRAWDOWN_PCT = 0.10
    DATA_FETCH_TIMEOUT = 30
    USE_BROKER_BRACKET_ORDERS = False

    # ----- Profit-locking / exit management (live) -----
    ENABLE_PARTIAL_TAKE_PROFIT = True
    PARTIAL_TP_THRESHOLD = 0.60           # take partial once profit reaches 60% of TP distance
    PARTIAL_CLOSE_PCT = 0.50              # close 50% of the position on partial TP

    ENABLE_TRAILING_TP = True
    TRAILING_TP_ATR_TRIGGER = 3.0         # start trailing TP after 3x ATR profit
    TRAILING_TP_DISTANCE_ATR = 1.5        # trail TP 1.5x ATR behind the best price

    ENABLE_PROFIT_DRAWDOWN_PROTECTION = True
    RETRACEMENT_HIGH_THRESHOLD = 0.70     # arm the lock once profit reaches 70% of TP
    RETRACEMENT_LOCK_THRESHOLD = 0.50     # then never give back below 50% of TP profit

    ENABLE_TIME_PARTIAL = True
    TIME_PARTIAL_BARS = 12                # after 12 bars (~1h on 5-min)
    TIME_PARTIAL_PROFIT_ATR = 0.5         # only if at least 0.5x ATR in profit

    # ----- Telegram -----
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

config = GlobalConfig()