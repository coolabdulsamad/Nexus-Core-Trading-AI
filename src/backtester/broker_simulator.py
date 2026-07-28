"""
src/backtester/broker_simulator.py
UPDATED: Supports trailing stops, breakeven, profit locking.
"""
import pandas as pd
from typing import Dict, List
from utils.logger import setup_logger

logger = setup_logger("BrokerSimulator", "logs/backtester.log")

class BrokerSimulator:
    def __init__(self, initial_capital: float):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.open_positions: List[Dict] = []
        self.closed_trades: List[Dict] = []
        self.equity_curve: List[Dict] = []

    def get_total_equity(self, current_price: float) -> float:
        unrealized_pnl = 0
        for pos in self.open_positions:
            if pos['type'] == 'LONG':
                unrealized_pnl += (current_price - pos['entry_price']) * pos['size']
            elif pos['type'] == 'SHORT':
                unrealized_pnl += (pos['entry_price'] - current_price) * pos['size']
        return self.cash + unrealized_pnl

    def open_long(self, timestamp, entry_price, size, stop_loss, take_profit):
        if size <= 0 or entry_price * size > self.cash:
            return False
        self.cash -= size * entry_price
        self.open_positions.append({
            'type': 'LONG',
            'entry_time': timestamp,
            'entry_price': entry_price,
            'size': size,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'entry_cost': size * entry_price,
            'highest_price': entry_price,
            'lowest_price': entry_price,
            'atr': None
        })
        logger.info(f"LONG ENTRY: {size} shares @ ${entry_price:.2f}, SL: ${stop_loss:.2f}, TP: ${take_profit:.2f}")
        return True

    def open_short(self, timestamp, entry_price, size, stop_loss, take_profit):
        if size <= 0:
            return False
        self.cash += size * entry_price
        self.open_positions.append({
            'type': 'SHORT',
            'entry_time': timestamp,
            'entry_price': entry_price,
            'size': size,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'entry_cost': size * entry_price,
            'highest_price': entry_price,
            'lowest_price': entry_price,
            'atr': None
        })
        logger.info(f"SHORT ENTRY: {size} shares @ ${entry_price:.2f}, SL: ${stop_loss:.2f}, TP: ${take_profit:.2f}")
        return True

    def update_trailing_stop(self, pos, current_price, atr):
        """Update stop_loss using trailing logic. Returns True if stop was moved."""
        if pos['type'] == 'LONG':
            pos['highest_price'] = max(pos['highest_price'], current_price)
            profit_atr = (current_price - pos['entry_price']) / atr
            if profit_atr >= 1.5:                     # was 2.0
                if profit_atr >= 2.5:                 # was 3.0
                    trail_dist = 0.8 * atr            # was 1.0
                else:
                    trail_dist = 1.5 * atr            # was 2.0
                new_sl = pos['highest_price'] - trail_dist
                if new_sl > pos['stop_loss']:
                    pos['stop_loss'] = new_sl
                    return True
        elif pos['type'] == 'SHORT':
            pos['lowest_price'] = min(pos['lowest_price'], current_price)
            profit_atr = (pos['entry_price'] - current_price) / atr if atr > 0 else 0
            if profit_atr >= 2.0:
                if profit_atr >= 3.0:
                    trail_dist = 1.0 * atr
                else:
                    trail_dist = 2.0 * atr
                new_sl = pos['lowest_price'] + trail_dist
                if new_sl < pos['stop_loss']:
                    pos['stop_loss'] = new_sl
                    return True
        return False

    def check_positions(self, timestamp, current_price, atr=None) -> bool:
        """Check SL/TP and update trailing stops if ATR provided."""
        # Update trailing stop for all positions
        if atr is not None and atr > 0:
            for pos in self.open_positions:
                pos['atr'] = atr
                self.update_trailing_stop(pos, current_price, atr)

        closed_any = False
        positions_to_remove = []

        for i, pos in enumerate(self.open_positions):
            if pos['type'] == 'LONG':
                if current_price <= pos['stop_loss']:
                    self._close_position(pos, current_price, timestamp, "STOP_LOSS")
                    positions_to_remove.append(i)
                    closed_any = True
                elif current_price >= pos['take_profit']:
                    self._close_position(pos, current_price, timestamp, "TAKE_PROFIT")
                    positions_to_remove.append(i)
                    closed_any = True
            elif pos['type'] == 'SHORT':
                if current_price >= pos['stop_loss']:
                    self._close_position(pos, current_price, timestamp, "STOP_LOSS")
                    positions_to_remove.append(i)
                    closed_any = True
                elif current_price <= pos['take_profit']:
                    self._close_position(pos, current_price, timestamp, "TAKE_PROFIT")
                    positions_to_remove.append(i)
                    closed_any = True

        for i in sorted(positions_to_remove, reverse=True):
            del self.open_positions[i]

        return closed_any

    def _close_position(self, pos, current_price, timestamp, reason):
        if pos['type'] == 'LONG':
            gross_pnl = (current_price - pos['entry_price']) * pos['size']
            self.cash += pos['size'] * current_price
        elif pos['type'] == 'SHORT':
            gross_pnl = (pos['entry_price'] - current_price) * pos['size']
            self.cash -= pos['size'] * current_price

        commission = max(pos['size'] * current_price * 0.0003, 0.01)
        net_pnl = gross_pnl - commission
        self.cash += net_pnl

        self.closed_trades.append({
            'entry_time': pos['entry_time'],
            'exit_time': timestamp,
            'entry_price': pos['entry_price'],
            'exit_price': current_price,
            'type': pos['type'],
            'size': pos['size'],
            'pnl': net_pnl,
            'pnl_pct': (net_pnl / pos['entry_cost']) * 100,
            'reason': reason
        })
        logger.info(f"CLOSED {pos['type']} ({reason}): PnL = ${net_pnl:.2f} ({((net_pnl / pos['entry_cost']) * 100):.2f}%)")

    def force_close_all(self, timestamp, current_price):
        for pos in self.open_positions[:]:
            self._close_position(pos, current_price, timestamp, "END_OF_BACKTEST")
        self.open_positions.clear()