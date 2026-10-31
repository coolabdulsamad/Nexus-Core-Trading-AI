#!/usr/bin/env bash
# Nexus Core trader supervisor (v3.7.4)
# Auto-restarts the trader if it crashes, or if the watchdog kills a hung
# process (silent network freeze). Positions are re-adopted on every boot,
# so a restart is always safe.
#
# Start:  nohup bash run_trader.sh > logs/trader_v374.log 2>&1 &
# Stop:   pkill -f run_trader.sh ; pkill -f live_paper_trader_multi
cd "$(dirname "$0")"
while true; do
  echo "[supervisor] $(date -u '+%Y-%m-%d %H:%M:%S') starting trader..."
  python -m src.live.live_paper_trader_multi
  code=$?
  echo "[supervisor] $(date -u '+%Y-%m-%d %H:%M:%S') trader exited (code $code) - restarting in 20s"
  sleep 20
done
