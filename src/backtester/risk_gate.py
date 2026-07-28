"""
src/backtester/risk_gate.py
UPGRADED:
- Signal-strength position sizing: quality tiers map to different risk %
  (strong signal -> bigger position, medium -> normal, weak -> small).
- Higher configurable notional caps (use more capital when edge is strong).
- Daily PROFIT TARGET: once the day's PnL reaches the configured target,
  no new positions are opened for the rest of the day (lock the win).
- Daily loss limit retained.
"""
from typing import Tuple
from config.settings import config


class RiskGate:
    def __init__(self, initial_capital: float = 100000.0):
        self.capital = initial_capital
        self.slippage_bps = config.SLIPPAGE_BPS
        self.reward_risk_ratio = config.REWARD_RISK_RATIO
        self.stop_atr_mult = config.STOP_ATR_MULT
        self.breakeven_atr_multiple = config.BREAKEVEN_ATR_MULTIPLE

        # Daily tracking
        self.daily_loss_limit_pct = config.DAILY_LOSS_LIMIT_PCT
        self.daily_profit_target_pct = config.DAILY_PROFIT_TARGET_PCT
        self.today = None
        self.day_start_equity = initial_capital
        self.daily_realized_pnl = 0.0
        self.daily_unrealized_pnl = 0.0
        self.profit_target_hit = False

    # ------------------------------------------------------------------
    # Daily bookkeeping
    # ------------------------------------------------------------------
    def reset_daily_if_new_day(self, current_time):
        today = current_time.date() if hasattr(current_time, 'date') else current_time
        if self.today is None or today != self.today:
            self.today = today
            self.day_start_equity = self.capital
            self.daily_realized_pnl = 0.0
            self.daily_unrealized_pnl = 0.0
            self.profit_target_hit = False

    # Backwards-compatible alias used by the live trader
    def reset_daily_loss_if_new_day(self, current_time):
        self.reset_daily_if_new_day(current_time)

    def update_unrealized_pnl(self, current_equity):
        self.daily_unrealized_pnl = current_equity - self.day_start_equity - self.daily_realized_pnl

    def update_unrealized_loss(self, current_equity):
        # Backwards-compatible; keeps loss tracking working for the live trader
        self.update_unrealized_pnl(current_equity)

    def record_realized_pnl(self, pnl: float):
        self.daily_realized_pnl += pnl

    def record_realized_loss(self, loss_amount: float):
        if loss_amount < 0:
            self.daily_realized_pnl += loss_amount

    # ------------------------------------------------------------------
    # Daily guards
    # ------------------------------------------------------------------
    def check_daily_loss_limit(self, current_equity) -> bool:
        """True = allowed to keep trading."""
        day_pnl = current_equity - self.day_start_equity
        loss_pct = -day_pnl / self.day_start_equity if self.day_start_equity > 0 and day_pnl < 0 else 0.0
        return loss_pct < self.daily_loss_limit_pct

    def check_daily_profit_target(self, current_equity) -> bool:
        """True = profit target reached -> stop opening new trades today."""
        if self.daily_profit_target_pct <= 0:
            return False
        day_pnl_pct = (current_equity - self.day_start_equity) / self.day_start_equity
        if day_pnl_pct >= self.daily_profit_target_pct:
            if not self.profit_target_hit:
                self.profit_target_hit = True
            return True
        return False

    def can_open_new_position(self, current_equity) -> Tuple[bool, str]:
        if not self.check_daily_loss_limit(current_equity):
            return False, "daily_loss_limit"
        if self.check_daily_profit_target(current_equity):
            return False, "daily_profit_target"
        return True, "ok"

    # ------------------------------------------------------------------
    # Signal-strength tiers
    # ------------------------------------------------------------------
    def risk_pct_for_quality(self, quality: float) -> Tuple[float, str]:
        if quality >= config.QUALITY_STRONG:
            return config.RISK_PCT_STRONG, "STRONG"
        if quality >= config.QUALITY_MEDIUM:
            return config.RISK_PCT_MEDIUM, "MEDIUM"
        return config.RISK_PCT_WEAK, "WEAK"

    def _size(self, current_price, atr_value, quality, direction):
        if atr_value <= 0 or current_price <= 0:
            return 0, 0, 0, "INVALID"

        risk_pct, tier = self.risk_pct_for_quality(quality)
        risk_capital = self.capital * risk_pct
        stop_distance = atr_value * self.stop_atr_mult
        size = risk_capital / stop_distance

        max_notional = min(config.NOTIONAL_CAP_ABS, self.capital * config.NOTIONAL_CAP_PCT)
        if size * current_price > max_notional:
            size = max_notional / current_price

        if direction == 'LONG':
            stop_loss = current_price - stop_distance
            take_profit = current_price + stop_distance * self.reward_risk_ratio
        else:
            stop_loss = current_price + stop_distance
            take_profit = current_price - stop_distance * self.reward_risk_ratio

        return round(size, 2), round(stop_loss, 4), round(take_profit, 4), tier

    def calculate_long_position_size(self, current_price, atr_value, quality: float = None):
        if quality is None:
            quality = config.QUALITY_MEDIUM      # legacy callers -> medium tier
        size, sl, tp, tier = self._size(current_price, atr_value, quality, 'LONG')
        return size, sl, tp

    def calculate_short_position_size(self, current_price, atr_value, quality: float = None):
        if quality is None:
            quality = config.QUALITY_MEDIUM
        size, sl, tp, tier = self._size(current_price, atr_value, quality, 'SHORT')
        return size, sl, tp

    def size_with_tier(self, current_price, atr_value, quality, direction):
        """Full-info variant: returns (size, sl, tp, tier)."""
        return self._size(current_price, atr_value, quality, direction)

    # ------------------------------------------------------------------
    def apply_slippage(self, price: float, is_entry: bool = True) -> float:
        return price * (1 + self.slippage_bps) if is_entry else price * (1 - self.slippage_bps)

    def apply_commission(self, size: float, price: float) -> float:
        return max(size * price * config.COMMISSION_BPS, 0.01)

    def update_breakeven_stop(self, pos: dict, current_price: float, atr: float):
        if pos['type'] == 'LONG':
            if (current_price - pos['entry_price']) >= self.breakeven_atr_multiple * atr \
                    and pos['stop_loss'] < pos['entry_price']:
                pos['stop_loss'] = pos['entry_price']
                return True
        else:
            if (pos['entry_price'] - current_price) >= self.breakeven_atr_multiple * atr \
                    and pos['stop_loss'] > pos['entry_price']:
                pos['stop_loss'] = pos['entry_price']
                return True
        return False

    def lock_breakeven_all(self, positions: list):
        """Daily profit target hit -> move every open stop to breakeven."""
        moved = 0
        for pos in positions:
            if pos['type'] == 'LONG' and pos['stop_loss'] < pos['entry_price']:
                pos['stop_loss'] = pos['entry_price']; moved += 1
            elif pos['type'] == 'SHORT' and pos['stop_loss'] > pos['entry_price']:
                pos['stop_loss'] = pos['entry_price']; moved += 1
        return moved
