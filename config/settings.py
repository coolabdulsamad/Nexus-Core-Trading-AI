"""
config/settings.py
Single source of truth for Nexus Core.
Secrets come ONLY from environment / .env - never hardcode credentials here.

v3: the system runs on 1-HOUR bars. Five-symbol honest backtests proved
5-minute similar-state recall has no edge after costs (PF 0.41-0.74 on
AAPL/TSLA/NVDA/MSFT/GOOGL) - losses came from noise-churning, not risk
management. Hourly bars cut trade frequency ~12x, shrink costs relative
to moves, and make recalled states actually comparable.

PR2: symbol universe (DB-backed, Alpaca-verified), daily top-N selection,
crypto support.

v3.1: UNIVERSE_POOL - the full candidate watchlist seeded into the DB
(python -m src.universe.seed_universe). Edge is symbol-specific and
time-specific, so the system watches a wide pool and lets the quality
gate decide what actually trades. The data/memory pipeline scripts are
DB-driven (every symbol in the DB gets backfilled, resampled, encoded),
so `symbols` below is now only the offline fallback.

v3.3: daily self-maintenance - the live trader launches the data pump +
Qdrant outcome sync once a day in the background (src/maintenance.py),
so memory outcomes can never silently go stale again.
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
    symbols = ["AAPL", "TSLA", "MSFT", "GOOGL", "NVDA"]   # offline fallback only
    BAR_MINUTES = 60                     # v3: 1-hour bars (60) or 5-min (5)
    BAR_SUFFIX = "_1h" if BAR_MINUTES == 60 else ""   # DB table suffix

    # ----- Candidate pool (v3.1) -----
    # Seeded into the DB universe by src/universe/seed_universe.py
    # (each name is Alpaca-verified before activation). In 'manual' mode
    # the trader watches EVERY active symbol and the quality gate decides
    # what trades; in 'auto' mode the DailySelector ranks the pool and
    # trades the top N. Liquid, diverse, all Alpaca-tradable.
    UNIVERSE_POOL = [
        # mega-cap tech / semis
        "AAPL", "MSFT", "GOOGL", "NVDA", "TSLA", "AMZN", "META",
        "AMD", "AVGO", "NFLX", "CRM", "ORCL", "ADBE", "QCOM",
        "INTC", "MU", "PLTR",
        # financials
        "JPM", "BAC", "GS", "V", "MA",
        # energy
        "XOM", "CVX",
        # healthcare
        "UNH", "JNJ", "LLY", "PFE",
        # consumer
        "HD", "MCD", "NKE", "SBUX", "DIS",
        # industrials
        "BA", "CAT",
        # high-beta / crypto proxies
        "COIN", "MSTR",
    ]

    # ----- Universe management (PR2) -----
    UNIVERSE_MODE = os.getenv("UNIVERSE_MODE", "manual")   # 'manual' = watch ALL active symbols | 'auto' = selector top-N daily
    TOP_N_SYMBOLS = 5                    # how many symbols the selector trades per day (auto mode)
    CRYPTO_ENABLED = True
    CRYPTO_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD"]
    SELECTOR_ATR_PCT_MIN = 0.0015        # volatility fit floor for the daily selector
    SELECTOR_ATR_PCT_MAX = 0.030         # volatility fit ceiling

    # ----- Brain (case-based memory) -----
    FORWARD_HORIZON_HOURS = 4            # prediction target: N hours ahead (4 bars on 1h)
    MEMORY_NEIGHBORS = 100               # k nearest states to retrieve
    # Neighbor must be older than the horizon (+1h buffer) so its outcome
    # was fully known at decision time -> kills look-ahead.
    MEMORY_MIN_AGE_MINUTES = (FORWARD_HORIZON_HOURS + 1) * 60
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
    MIN_SIGNAL_QUALITY = 0.35            # v3.6: was 0.20 - the 0.20-0.24 "weak" band had no edge (PF 0.59 week)
    QUALITY_MEMORY_REF_N = 80            # v3.6: quality is scaled by min(1, n/this) - thin memories can't enter

    # ----- Signal-strength position sizing (quality tiers) -----
    # v3.6.3: was 0.60 - unreachable (max q ever observed = 0.480), so the
    # STRONG tier never existed and everything was sized MEDIUM/WEAK.
    QUALITY_STRONG = 0.45                # quality >= this -> strong tier (~p95 of observed range)
    QUALITY_MEDIUM = 0.35                # quality >= this -> medium tier (below -> weak tier)
    RISK_PCT_STRONG = 0.020              # 2.0% of slice risked on strong signals
    RISK_PCT_MEDIUM = 0.010              # 1.0%
    RISK_PCT_WEAK = 0.005                # 0.5%  (weak but tradable)
    NOTIONAL_CAP_PCT = 0.75              # max notional per position, % of per-symbol capital
    NOTIONAL_CAP_ABS = 75000             # absolute $ cap per position

    # ----- v3.6: entry analysis gates -----
    # Evidence (Aug 27 - Sep 2 week): every loser came from the same cluster -
    # BUY in regime=trend_down with fearful sentiment on a thin (n=35) memory.
    # These gates make the brain prove the setup before money moves.
    SENTIMENT_VETO_LONG = -0.60          # sent <= this -> no LONG (extreme fear)
    SENTIMENT_VETO_SHORT = 0.60          # sent >= this -> no SHORT (extreme euphoria)
    TOXIC_REGIME_SENT = -0.30            # regime=trend_down AND sent <= this -> veto LONG at ANY quality
    # v3.6.3: was 0.60 - DEAD GATE. Measured over 39,570 live brain readings:
    # max q EVER observed is 0.480, so a 0.60 gate could never fire. 0.45 is
    # ~p95 of the observed range = genuinely "strong" but actually reachable.
    TREND_REGIME_MIN_QUALITY = 0.45      # LONG in trend_down (SHORT in trend_up) must be STRONG-tier
    CRYPTO_MOMENTUM_GATE = True          # crypto LONG needs price > sma200 AND 24h return > 0
    # v3.6.3: crypto quality runs structurally lower (p90 = 0.21 vs stocks 0.46;
    # no news feed, different memory). With the 0.35 floor crypto could never
    # trade. Crypto gets its own floor - the momentum gate + bar confirmations
    # below do the protective work instead.
    CRYPTO_MIN_SIGNAL_QUALITY = 0.25     # quality floor for crypto entries (stocks use MIN_SIGNAL_QUALITY)
    SESSION_OPEN_NO_ENTRY_MINUTES = 60   # no stock entries in the first 60 min of the US session
    ORDER_FILL_TIMEOUT_SECONDS = 90      # was 30s - 4 orders died unfilled at the volatile open

    # ----- v3.6.3: entry confirmation (the "analyse the bars before entering" layer) -----
    # The brain votes from memory; these make the CURRENT tape agree before
    # money moves. Each is independently switchable.
    ENTRY_BAR_CONFIRM_ENABLED = True     # last closed bar must move WITH the signal (LONG: ret_1 > 0)
    ENTRY_VWAP_CONFIRM_ENABLED = True    # LONG only above today's VWAP, SHORT only below (buyers/sellers in control)
    ENTRY_NO_CHASE_ENABLED = True        # skip if the last bar spiked > NO_CHASE_MAX_RANGE_ATR x ATR (never chase)
    ENTRY_NO_CHASE_MAX_RANGE_ATR = 1.5
    ENTRY_ADX_MIN = 20.0                 # with-trend entries need a real trend (ADX >= this; 0 = off)

    # ----- v3.6: loss cooldowns (the ETH 3-stops-in-7h fix) -----
    COOLDOWN_AFTER_CLOSE_BARS = 3        # was 1 (5 min) - ETH re-entered 10 min after a stop
    LOSS_COOLDOWN_HOURS = 24             # after a STOP_LOSS, symbol is banned this long
    REPEAT_LOSS_WINDOW_DAYS = 7          # 2 stop-outs inside this window ...
    REPEAT_LOSS_COOLDOWN_HOURS = 72      # ... bans the symbol this long

    # ----- Trade structure (1h recalibration) -----
    # 1h ATR is ~3-4x the 5-min ATR, so stops get tighter in ATR units:
    # SL 2xATR, TP 3xATR (R:R 1.5), breakeven at 1.5xATR.
    STOP_ATR_MULT = 2.0                  # stop distance = 2 x ATR(1h)
    REWARD_RISK_RATIO = 1.5              # TP = 1.5R (3 x ATR)
    BREAKEVEN_ATR_MULTIPLE = 1.5         # move stop to entry after 1.5 x ATR profit
    SLIPPAGE_BPS = 0.0005
    COMMISSION_BPS = 0.0003
    TIME_LIMIT_BARS = 16                 # v3.6: was 8 - TP needs 3 ATR, unreachable in 8h on crypto
    COOLDOWN_BARS = 3                  # v3.6: was 1 (see COOLDOWN_AFTER_CLOSE_BARS)

    # ----- SMA exit (DISABLED) -----
    # Structurally guaranteed-loss exit (entries require price beyond the
    # 200 SMA; exits fired beyond it in the losing direction).
    SMA_EXIT_ENABLED = False
    SMA_EXIT_BUFFER_ATR = 0.25
    SMA_EXIT_CONFIRM_BARS = 2

    # ----- Daily guards -----
    DAILY_LOSS_LIMIT_PCT = 0.05          # stop trading the symbol day after -5%
    DAILY_PROFIT_TARGET_PCT = 0.02       # +2% day -> stop opening new trades (0 = disabled)
    DAILY_TARGET_LOCK_BREAKEVEN = True   # when target hit, move open stops to breakeven

    # ----- Profit locking (v3.6 rebuild) -----
    # The old stack armed the retracement lock at +0.7 ATR and exited at +0.5
    # ATR: winners were cut to ~+$28 crumbs (median hold 1.6h) while losers
    # ran the full -2 ATR stop. Breakeven/trailing NEVER fired all week.
    # New stack: scale out, ratchet real profit, trail wide, lock 60% of peak.
    ENABLE_PARTIAL_TAKE_PROFIT = False   # replaced by SCALE_OUT below
    PARTIAL_TP_THRESHOLD = 0.60
    PARTIAL_CLOSE_PCT = 0.50
    ENABLE_TRAILING_TP = False           # replaced by the v3.6 trailing stop
    TRAILING_TP_ATR_TRIGGER = 3.0
    TRAILING_TP_DISTANCE_ATR = 1.5
    ENABLE_PROFIT_DRAWDOWN_PROTECTION = True
    RETRACEMENT_ARM_ATR = 2.0            # arm the lock only after +2 ATR peak (was 0.70)
    RETRACEMENT_KEEP_PCT = 0.60          # exit if profit falls to 60% of peak (was fixed 0.50 ATR)
    PROFIT_RATCHET_ATR = 1.5             # at +1.5 ATR the stop ratchets up ...
    RATCHET_LOCK_ATR = 0.50              # ... to entry + 0.5 ATR (locks real money, not breakeven)
    TRAILING_STOP_ACTIVATE_ATR = 2.5     # hard trailing starts at +2.5 ATR (was 4.0 - never fired)
    TRAILING_STOP_DISTANCE_ATR = 2.5     # trail 2.5 ATR behind the peak (was 6.0 - never mattered)
    SCALE_OUT_ENABLED = True             # sell 1/3 at +1 ATR and 1/3 at +2 ATR, trail the rest
    SCALE_OUT_1_ATR = 1.0
    SCALE_OUT_2_ATR = 2.0
    SCALE_OUT_PCT = 0.33
    # In-trade re-analysis: the brain re-judges every open position each cycle
    FLIP_EXIT_PROFIT_ATR = 0.5           # brain flips against + profit >= 0.5 ATR -> exit NOW (no 2-flip wait)
    FLIP_TIGHTEN_UNDERWATER = True       # brain flips against while underwater -> tighten stop to 1 ATR

    # ----- Live observability (v3.6.2) -----
    POSITION_HEARTBEAT_BARS = 4          # Telegram position status card every N bars per open trade
    TRAIL_ALERT_STEP_ATR = 0.25          # alert each time the trailing stop ratchets by >= this many ATR
    ENABLE_TIME_PARTIAL = True
    TIME_PARTIAL_BARS = 12
    TIME_PARTIAL_PROFIT_ATR = 0.5

    # ----- Data freshness -----
    MAX_DATA_AGE_SECONDS = 600           # refuse to trade on bars older than 10 min
    DATA_FETCH_TIMEOUT = 30

    # ----- Daily self-maintenance (v3.3) -----
    # The live trader runs `python -m src.maintenance` (data pump + Qdrant
    # outcome sync) once a day in the background. Trading never pauses.
    DAILY_MAINTENANCE_ENABLED = True     # set False to go back to manual runs
    DAILY_MAINTENANCE_UTC_HOUR = 7       # 07:00 UTC = 08:00 WAT - after US close, before next open
    MAINTENANCE_SYNC_DAYS = 10           # incremental outcome sync window (self-heals gaps <= 10 days)
    MAINTENANCE_TIMEOUT_SECONDS = 3600   # per-job timeout (pump / sync)

    # ----- Position adoption & honest sizing (v3.4) -----
    # The trader reconciles with the broker at startup AND every cycle:
    # positions opened before a restart (or manually at the broker) are
    # ADOPTED and managed with the exact same exit rules as new trades -
    # never closed blindly. Sizing uses the REAL account equity instead of
    # a fixed fictional slice, and every entry is capped by live buying power.
    ADOPT_BROKER_POSITIONS = True        # watch/manage positions that already exist at Alpaca
    ADOPTED_TIME_LIMIT_ENABLED = True    # adopted positions obey the same 8h time limit (False = exempt them)
    USE_REAL_ACCOUNT_SIZING = True       # slice = equity x SLICE_PCT_OF_EQUITY (still capped at $100k)
    SLICE_PCT_OF_EQUITY = 0.33           # ~ equity / MAX_POSITIONS -> no leverage by construction
    BUYING_POWER_USAGE_CAP = 0.95        # one entry may use at most 95% of live buying power

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
