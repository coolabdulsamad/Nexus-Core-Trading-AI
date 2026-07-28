"""
src/backtester/risk_gate.py
UPDATED: Uses wider stop distance (3×ATR) and configurable breakeven.
"""
import math
from typing import Tuple
from config.settings import config

class RiskGate:
    def __init__(self, initial_capital: float = 100000.0):
        self.capital = initial_capital
        self.slippage_bps = 0.0005
        self.risk_per_trade_pct = 0.01          # 1% risk per trade
        self.reward_risk_ratio = 3.0            # 3:1 reward/risk

        # Daily VaR tracking
        self.daily_loss_limit_pct = config.DAILY_LOSS_LIMIT_PCT
        self.today = None
        self.day_start_equity = initial_capital
        self.daily_realized_loss = 0.0
        self.daily_unrealized_loss = 0.0

        # Breakeven multiple (from config)
        self.breakeven_atr_multiple = config.BREAKEVEN_ATR_MULTIPLE

    def reset_daily_loss_if_new_day(self, current_time):
        today = current_time.date()
        if self.today is None or today != self.today:
            self.today = today
            self.day_start_equity = self.capital
            self.daily_realized_loss = 0.0
            self.daily_unrealized_loss = 0.0

    def update_unrealized_loss(self, current_equity):
        self.daily_unrealized_loss = max(0, self.day_start_equity - current_equity)

    def check_daily_loss_limit(self, current_equity):
        total_loss = self.daily_realized_loss + self.daily_unrealized_loss
        loss_pct = total_loss / self.day_start_equity if self.day_start_equity > 0 else 0
        return loss_pct < self.daily_loss_limit_pct

    def record_realized_loss(self, loss_amount):
        if loss_amount < 0:
            self.daily_realized_loss += abs(loss_amount)

    def calculate_long_position_size(self, current_price: float, atr_value: float) -> Tuple[float, float, float]:
        """Returns: size, stop_loss_price, take_profit_price for LONG."""
        if atr_value <= 0 or current_price <= 0:
            return 0, 0, 0

        risk_capital = self.capital * self.risk_per_trade_pct
        # --- WIDER STOP: 3×ATR instead of 2×ATR ---
        stop_distance = atr_value * 3.0
        size = risk_capital / stop_distance

        # Cap notional to 50% of per-symbol capital (or $50k, whichever smaller)
        max_notional = min(50000, self.capital * 0.5)
        notional_value = size * current_price
        if notional_value > max_notional:
            size = max_notional / current_price

        stop_loss_price = current_price - stop_distance
        take_profit_price = current_price + (stop_distance * self.reward_risk_ratio)

        # Sanity check: TP must be > entry for long
        if take_profit_price <= current_price:
            take_profit_price = current_price + 0.5   # fallback

        return round(size, 2), round(stop_loss_price, 4), round(take_profit_price, 4)

    def calculate_short_position_size(self, current_price: float, atr_value: float) -> Tuple[float, float, float]:
        """Returns: size, stop_loss_price (above entry), take_profit_price (below entry) for SHORT."""
        if atr_value <= 0 or current_price <= 0:
            return 0, 0, 0

        risk_capital = self.capital * self.risk_per_trade_pct
        stop_distance = atr_value * 3.0          # wider stop
        size = risk_capital / stop_distance

        max_notional = min(50000, self.capital * 0.5)
        notional_value = size * current_price
        if notional_value > max_notional:
            size = max_notional / current_price

        stop_loss_price = current_price + stop_distance
        take_profit_price = current_price - (stop_distance * self.reward_risk_ratio)

        # Sanity check: TP must be < entry for short
        if take_profit_price >= current_price:
            take_profit_price = current_price - 0.5

        return round(size, 2), round(stop_loss_price, 4), round(take_profit_price, 4)

    def apply_slippage(self, price: float, is_entry: bool = True) -> float:
        if is_entry:
            return price * (1 + self.slippage_bps)
        else:
            return price * (1 - self.slippage_bps)

    def apply_commission(self, size: float, price: float) -> float:
        return max(size * price * 0.0003, 0.01)

    def update_breakeven_stop(self, pos: dict, current_price: float, atr: float):
        """
        If profit >= BREAKEVEN_ATR_MULTIPLE * ATR, move stop to entry price.
        Returns True if the stop was moved.
        """
        if pos['type'] == 'LONG':
            profit = current_price - pos['entry_price']
            if profit >= self.breakeven_atr_multiple * atr and pos['stop_loss'] < pos['entry_price']:
                pos['stop_loss'] = pos['entry_price']
                return True
        elif pos['type'] == 'SHORT':
            profit = pos['entry_price'] - current_price
            if profit >= self.breakeven_atr_multiple * atr and pos['stop_loss'] > pos['entry_price']:
                pos['stop_loss'] = pos['entry_price']
                return True
        return False