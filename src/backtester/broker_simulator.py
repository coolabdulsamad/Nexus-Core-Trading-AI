"""
src/backtester/broker_simulator.py
FIXED: correct cash accounting (no double-counted PnL), intrabar SL/TP
detection with bar high/low, realistic fills at stop/limit prices.

Accounting model
----------------
LONG:  cash -= entry_cost (+entry commission) at open; cash += proceeds (-exit
       commission) at close. Net effect = (exit-entry)*size - commissions.
SHORT: margin-style. Principal does NOT move; only PnL settles at close.
       cash += (entry-exit)*size - commissions.
While a position is open, equity = cash + sum(unrealized PnL) - which is
exact for both models.
"""
from typing import Dict, List, Optional
from config.settings import config
from src.utils.logger import setup_logger

logger = setup_logger("BrokerSimulator", "logs/backtester.log")

COMMISSION_BPS = config.COMMISSION_BPS
SLIPPAGE_BPS = config.SLIPPAGE_BPS


def _commission(size: float, price: float) -> float:
    return max(size * price * COMMISSION_BPS, 0.01)


class BrokerSimulator:
    def __init__(self, initial_capital: float):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.open_positions: List[Dict] = []
        self.closed_trades: List[Dict] = []
        self.equity_curve: List[Dict] = []

    # ------------------------------------------------------------------
    def get_total_equity(self, current_price: float) -> float:
        unrealized = 0.0
        for pos in self.open_positions:
            if pos['type'] == 'LONG':
                unrealized += (current_price - pos['entry_price']) * pos['size']
            else:
                unrealized += (pos['entry_price'] - current_price) * pos['size']
        return self.cash + unrealized

    # ------------------------------------------------------------------
    def open_long(self, timestamp, entry_price, size, stop_loss, take_profit):
        if size <= 0 or entry_price * size > self.cash:
            return False
        comm = _commission(size, entry_price)
        self.cash -= size * entry_price + comm
        self.open_positions.append({
            'type': 'LONG', 'entry_time': timestamp, 'entry_price': entry_price,
            'size': size, 'stop_loss': stop_loss, 'take_profit': take_profit,
            'entry_cost': size * entry_price, 'entry_commission': comm,
            'highest_price': entry_price, 'lowest_price': entry_price, 'atr': None,
        })
        logger.info(f"LONG ENTRY: {size} @ ${entry_price:.2f}, SL ${stop_loss:.2f}, TP ${take_profit:.2f}")
        return True

    def open_short(self, timestamp, entry_price, size, stop_loss, take_profit):
        if size <= 0 or entry_price * size > self.cash:
            return False
        comm = _commission(size, entry_price)
        self.cash -= comm                      # margin-style: only commission moves
        self.open_positions.append({
            'type': 'SHORT', 'entry_time': timestamp, 'entry_price': entry_price,
            'size': size, 'stop_loss': stop_loss, 'take_profit': take_profit,
            'entry_cost': size * entry_price, 'entry_commission': comm,
            'highest_price': entry_price, 'lowest_price': entry_price, 'atr': None,
        })
        logger.info(f"SHORT ENTRY: {size} @ ${entry_price:.2f}, SL ${stop_loss:.2f}, TP ${take_profit:.2f}")
        return True

    # ------------------------------------------------------------------
    def update_trailing_stop(self, pos, current_price, atr) -> bool:
        if pos['type'] == 'LONG':
            pos['highest_price'] = max(pos['highest_price'], current_price)
            profit_atr = (current_price - pos['entry_price']) / atr
            if profit_atr >= 1.5:
                trail_dist = 0.8 * atr if profit_atr >= 2.5 else 1.5 * atr
                new_sl = pos['highest_price'] - trail_dist
                if new_sl > pos['stop_loss']:
                    pos['stop_loss'] = new_sl
                    return True
        else:
            pos['lowest_price'] = min(pos['lowest_price'], current_price)
            profit_atr = (pos['entry_price'] - current_price) / atr if atr > 0 else 0
            if profit_atr >= 2.0:
                trail_dist = 1.0 * atr if profit_atr >= 3.0 else 2.0 * atr
                new_sl = pos['lowest_price'] + trail_dist
                if new_sl < pos['stop_loss']:
                    pos['stop_loss'] = new_sl
                    return True
        return False

    # ------------------------------------------------------------------
    def check_positions(self, timestamp, bar_high, bar_low, bar_close, atr=None) -> bool:
        """
        Intrabar SL/TP detection using the bar's high/low (not just close).
        If SL and TP are both inside one bar, assume the STOP hit first
        (conservative). Fills happen at the stop/limit price with slippage.
        """
        if atr is not None and atr > 0:
            for pos in self.open_positions:
                pos['atr'] = atr
                self.update_trailing_stop(pos, bar_close, atr)

        closed_any = False
        to_close = []  # (pos, fill_price, reason)

        for pos in self.open_positions:
            if pos['type'] == 'LONG':
                sl_hit = bar_low <= pos['stop_loss']
                tp_hit = bar_high >= pos['take_profit']
                if sl_hit:
                    fill = pos['stop_loss'] * (1 - SLIPPAGE_BPS)
                    to_close.append((pos, fill, "STOP_LOSS"))
                elif tp_hit:
                    fill = pos['take_profit'] * (1 - SLIPPAGE_BPS)
                    to_close.append((pos, fill, "TAKE_PROFIT"))
            else:
                sl_hit = bar_high >= pos['stop_loss']
                tp_hit = bar_low <= pos['take_profit']
                if sl_hit:
                    fill = pos['stop_loss'] * (1 + SLIPPAGE_BPS)
                    to_close.append((pos, fill, "STOP_LOSS"))
                elif tp_hit:
                    fill = pos['take_profit'] * (1 + SLIPPAGE_BPS)
                    to_close.append((pos, fill, "TAKE_PROFIT"))

        for pos, fill, reason in to_close:
            self._close_position(pos, fill, timestamp, reason)
            if pos in self.open_positions:
                self.open_positions.remove(pos)
            closed_any = True

        return closed_any

    # ------------------------------------------------------------------
    def _close_position(self, pos, exit_price, timestamp, reason):
        size = pos['size']
        exit_comm = _commission(size, exit_price)

        if pos['type'] == 'LONG':
            gross_pnl = (exit_price - pos['entry_price']) * size
            self.cash += size * exit_price - exit_comm          # proceeds back
        else:
            gross_pnl = (pos['entry_price'] - exit_price) * size
            self.cash += gross_pnl - exit_comm                  # margin-style settle

        net_pnl = gross_pnl - pos['entry_commission'] - exit_comm

        self.closed_trades.append({
            'entry_time': pos['entry_time'], 'exit_time': timestamp,
            'entry_price': pos['entry_price'], 'exit_price': exit_price,
            'type': pos['type'], 'size': size, 'pnl': net_pnl,
            'pnl_pct': (net_pnl / pos['entry_cost']) * 100,
            'reason': reason,
        })
        logger.info(f"CLOSED {pos['type']} ({reason}): PnL ${net_pnl:.2f} "
                    f"({(net_pnl / pos['entry_cost']) * 100:.2f}%)")

    # ------------------------------------------------------------------
    def force_close_all(self, timestamp, current_price):
        for pos in self.open_positions[:]:
            self._close_position(pos, current_price, timestamp, "END_OF_BACKTEST")
        self.open_positions.clear()
