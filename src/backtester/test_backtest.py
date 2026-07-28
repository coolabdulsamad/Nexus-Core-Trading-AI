#!/usr/bin/env python3
from src.backtester.engine import BacktesterEngine

symbol = "GOOGL"
start = "2026-03-24"
end = "2026-06-24"

engine = BacktesterEngine(symbol, initial_capital=100000.0, start_date=start, end_date=end)
engine.run()