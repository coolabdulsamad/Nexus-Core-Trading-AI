"""
config/settings.py - Nexus Core Trading AI configuration (v3.6)
"""
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabaseConfig:
    url: str = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/nexus_core")


@dataclass
class Settings:
    database: DatabaseConfig = field(default_factory=DatabaseConfig)

    # ----- API keys (from .env ONLY - never hardcode) -----
    ALPACA_API_KEY: str = os.getenv("ALPACA_API_KEY", "")
    ALPACA_SECRET_KEY: str = os.getenv("ALPACA_SECRET_KEY", "")
    ALPACA_PAPER: bool = os.getenv("ALPACA_PAPER", "true").lower() == "true"
    POLYGON_API_KEY: str = os.getenv("POLYGON_API_KEY", "")
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # ----- Timeframe -----
    BAR_MINUTES: int = 60
    BAR_SUFFIX: str = "_1h"

    # ----- Universe -----
    SYMBOLS: list = field(default_factory=lambda: ["AAPL", "MSFT", "NVDA", "TSLA", "AMD", "PLTR", "JPM"])
    CRYPTO_SYMBOLS: list = field(default_factory=lambda: ["BTC/USD", "ETH/USD"])
    SYMBOL_UNIVERSE_FROM_DB: bool = True
    VERIFY_SYMBOLS_ON_ALPACA: bool = True
    DAILY_TOP_N: int = 5

    # ----- Brain -----
    MEMORY_NEIGHBORS: int = 100
    FORWARD_HORIZON_HOURS: int = 4
    ENTRY_CONVICTION_MARGIN: float = 0.05
    SENTIMENT_BIAS: float = 0.05

    # ----- Signal quality gates (v3.6) -----
    # v3.6: floor raised 0.20 -> 0.35. The losing week proved q is NOT
    # predictive by itself (winners q~0.40, losers q~0.40), so the floor
    # works together with memory scaling + sentiment/regime vetoes below.
    MIN_SIGNAL_QUALITY: float = 0.35
    QUALITY_MEDIUM: float = 0.35
    QUALITY_STRONG: float = 0.60
    # Memory-scaled quality: eff_q = q x min(1, n / QUALITY_MEMORY_REF_N).
    # A q=0.46 built on 35 neighbors is NOT better than q=0.20 built on 100.
    QUALITY_MEMORY_REF_N: int = 80

    # ----- v3.6 entry analysis vetoes -----
    # The losing cluster was always the same: BUY + trend_down + fearful
    # sentiment + thin memory. These vetoes kill that cluster.
    SENTIMENT_VETO_LONG: float = -0.60    # no LONG when sentiment <= this (extreme fear)
    SENTIMENT_VETO_SHORT: float = 0.60    # no SHORT when sentiment >= this (extreme euphoria)
    TOXIC_REGIME_SENT: float = -0.30      # LONG + trend_down + sent <= this = veto (the losing combo)
    TREND_REGIME_MIN_QUALITY: float = 0.60  # counter-REGIME entries must be STRONG
    CRYPTO_MOMENTUM_GATE: bool = True     # crypto LONG needs price > sma200 AND 24h return > 0
    SESSION_OPEN_NO_ENTRY_MINUTES: int = 60   # no stock entries in the first hour of the US session
    ORDER_FILL_TIMEOUT_SECONDS: int = 90  # was 30 - open-print orders died unfilled at 30s

    # ----- Loss cooldowns (v3.6) -----
    # ETH was stopped 3x in 7h on a 5-min cooldown. Never again.
    LOSS_COOLDOWN_HOURS: int = 24          # after a STOP_LOSS, symbol banned this long
    REPEAT_LOSS_WINDOW_DAYS: int = 7       # rolling window for repeat offenders
    REPEAT_LOSS_COOLDOWN_HOURS: int = 72   # 2+ stops inside the window -> 72h ban
    COOLDOWN_BARS: int = 3                 # bars to wait after ANY close (was 1)

    # ----- Sizing -----
    RISK_PCT_STRONG: float = 0.02
    RISK_PCT_MEDIUM: float = 0.01
    RISK_PCT_WEAK: float = 0.005
    CAPITAL_PER_SYMBOL: float = 33000.0
    SLICE_PCT_OF_EQUITY: float = 0.33
    USE_REAL_ACCOUNT_SIZING: bool = True
    NOTIONAL_CAP_PCT: float = 0.95
    NOTIONAL_CAP_ABS: float = 100000.0
    BUYING_POWER_USAGE_CAP: float = 0.95
    MAX_POSITIONS: int = 4

    # ----- Risk / bracket -----
    STOP_ATR_MULT: float = 2.0
    REWARD_RISK_RATIO: float = 1.5
    COMMISSION_BPS: float = 0.0005
    SLIPPAGE_BPS: float = 0.0005

    # ----- Daily guards -----
    DAILY_LOSS_LIMIT_PCT: float = 0.03
    DAILY_PROFIT_TARGET_PCT: float = 0.02
    DAILY_TARGET_LOCK_BREAKEVEN: bool = True

    # ----- Profit locking (v3.6 rebuild) -----
    # The old stack armed the retracement lock at +0.7 ATR and exited at +0.5
    # ATR: winners were cut to ~+$28 crumbs (median hold 1.6h) while losers
    # ran the full -2 ATR stop. Breakeven/trailing NEVER fired all week.
    # New stack: scale out, ratchet real profit, trail wide, lock 60% of peak.
    ENABLE_PARTIAL_TAKE_PROFIT: bool = False   # replaced by SCALE_OUT below
    PARTIAL_TP_THRESHOLD: float = 0.60
    PARTIAL_CLOSE_PCT: float = 0.50
    ENABLE_TRAILING_TP: bool = False           # replaced by the v3.6 trailing stop
    TRAILING_TP_ATR_TRIGGER: float = 3.0
    TRAILING_TP_DISTANCE_ATR: float = 1.5
    ENABLE_PROFIT_DRAWDOWN_PROTECTION: bool = True
    RETRACEMENT_ARM_ATR: float = 2.0           # arm the lock only after +2 ATR peak (was 0.70)
    RETRACEMENT_KEEP_PCT: float = 0.60         # exit if profit falls to 60% of peak (was fixed 0.50 ATR)
    PROFIT_RATCHET_ATR: float = 1.5            # at +1.5 ATR the stop ratchets up ...
    RATCHET_LOCK_ATR: float = 0.50             # ... to entry + 0.5 ATR (locks real money, not breakeven)
    TRAILING_STOP_ACTIVATE_ATR: float = 2.5    # hard trailing starts at +2.5 ATR (was 4.0 - never fired)
    TRAILING_STOP_DISTANCE_ATR: float = 2.5    # trail 2.5 ATR behind the peak (was 6.0 - never mattered)
    SCALE_OUT_ENABLED: bool = True             # sell 1/3 at +1 ATR and 1/3 at +2 ATR, trail the rest
    SCALE_OUT_1_ATR: float = 1.0
    SCALE_OUT_2_ATR: float = 2.0
    SCALE_OUT_PCT: float = 0.33
    # In-trade re-analysis: the brain re-judges every open position each cycle
    FLIP_EXIT_PROFIT_ATR: float = 0.5          # brain flips against + profit >= 0.5 ATR -> exit NOW (no 2-flip wait)
    FLIP_TIGHTEN_UNDERWATER: bool = True       # brain flips against while underwater -> tighten stop to 1 ATR

    # ----- Live observability (v3.6.2) -----
    POSITION_HEARTBEAT_BARS: int = 4           # Telegram position status card every N bars per open trade
    TRAIL_ALERT_STEP_ATR: float = 0.25         # alert each time the trailing stop ratchets by >= this many ATR

    ENABLE_TIME_PARTIAL: bool = True
    TIME_PARTIAL_BARS: int = 12
    TIME_PARTIAL_PROFIT_ATR: float = 0.5

    # ----- Time limit -----
    TIME_LIMIT_BARS: int = 16                  # v3.6: was 8 - TP needs 3 ATR, unreachable in 8h on crypto

    # ----- SMA cross exit (disabled: structurally guaranteed-loss) -----
    SMA_EXIT_ENABLED: bool = False
    SMA_EXIT_CONFIRM_BARS: int = 3
    SMA_EXIT_BUFFER_ATR: float = 0.25

    # ----- Volatility filter -----
    VOLATILITY_FILTER_ENABLED: bool = True
    VOLATILITY_RATIO_MAX: float = 2.5

    # ----- Adoption (positions that existed before a restart) -----
    ADOPT_FALLBACK_SL_PCT: float = 0.03
    ADOPTED_TIME_LIMIT_ENABLED: bool = True

    # ----- Ghost-trader / runaway detection (v3.5.2) -----
    SIZE_DRIFT_TOLERANCE: float = 0.10
    RUNAWAY_NOTIONAL_MULT: float = 3.0
    RUNAWAY_ALERT_SECONDS: int = 3600
    ENTRY_FAIL_COOLDOWN_CYCLES: int = 6

    # ----- Backtest costs -----
    # (COMMISSION_BPS / SLIPPAGE_BPS above are shared)


config = Settings()
