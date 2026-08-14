#!/usr/bin/env python3
"""
src/maintenance.py  (v3.3)
Daily self-maintenance for Nexus Core: data pump + Qdrant outcome sync.

Runs the two jobs SEQUENTIALLY as subprocesses so a failure in one is
reported cleanly:

  1. python -m src.ingestion.run_pump
       (fetch latest bars -> TimescaleDB -> encode new memory points)
  2. python -m src.memory.update_qdrant_payloads --days N
       (write realised forward returns onto recent memory points so the
        brain's similarity search always has usable outcomes)

The live trader launches this module once a day in the background
(DAILY_MAINTENANCE_ENABLED / DAILY_MAINTENANCE_UTC_HOUR in config), but it
is also runnable by hand from the project root:

    python -m src.maintenance            # incremental (last N days)
    python -m src.maintenance --full     # full resync of every memory point

State file: logs/.last_maintenance_ok (ISO timestamp of last success) -
the trader reads it to catch up after downtime.
"""
import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone

from config.settings import config
from src.utils.logger import setup_logger

logger = setup_logger("Maintenance", "logs/maintenance.log")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_FILE = os.path.join(PROJECT_ROOT, "logs", ".last_maintenance_ok")
TIMEOUT = getattr(config, 'MAINTENANCE_TIMEOUT_SECONDS', 3600)


def last_success():
    """ISO timestamp of the last fully successful maintenance, or None."""
    try:
        with open(STATE_FILE) as f:
            return datetime.fromisoformat(f.read().strip())
    except Exception:
        return None


def _record_success():
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, "w") as f:
            f.write(datetime.now(timezone.utc).isoformat())
    except Exception as e:
        logger.warning(f"Could not write state file: {e}")


def _notify(message: str, level: str):
    try:
        from src.utils.telegram import send_telegram
        send_telegram(message, level)
    except Exception as e:
        logger.warning(f"Telegram notify failed: {e}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Nexus Core daily maintenance")
    parser.add_argument('--full', action='store_true',
                        help='Full outcome resync of ALL memory points (slow, ~30 min). '
                             'Default is incremental (last MAINTENANCE_SYNC_DAYS days).')
    args = parser.parse_args()

    sync_cmd = [sys.executable, "-m", "src.memory.update_qdrant_payloads"]
    if not args.full:
        sync_cmd += ["--days", str(getattr(config, 'MAINTENANCE_SYNC_DAYS', 10))]

    jobs = [
        ("pump", [sys.executable, "-m", "src.ingestion.run_pump"]),
        ("qdrant_outcome_sync", sync_cmd),
    ]

    started = datetime.now(timezone.utc)
    mode = "FULL" if args.full else f"incremental (last {getattr(config, 'MAINTENANCE_SYNC_DAYS', 10)}d)"
    logger.info(f"===== DAILY MAINTENANCE STARTED {started.isoformat()} [{mode}] =====")

    failed = []
    for name, cmd in jobs:
        logger.info(f"[{name}] running: {' '.join(cmd)}")
        try:
            proc = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True,
                                  text=True, timeout=TIMEOUT)
            out_tail = (proc.stdout or "").strip().splitlines()[-15:]
            for line in out_tail:
                logger.info(f"[{name}] {line}")
            if proc.returncode != 0:
                failed.append(name)
                err_tail = (proc.stderr or "").strip().splitlines()[-15:]
                for line in err_tail:
                    logger.error(f"[{name}] {line}")
                logger.error(f"[{name}] FAILED (rc={proc.returncode}) - stopping here.")
                break                       # no point syncing if the pump died
        except subprocess.TimeoutExpired:
            failed.append(name)
            logger.error(f"[{name}] TIMEOUT after {TIMEOUT}s - stopping here.")
            break
        except Exception as e:
            failed.append(name)
            logger.error(f"[{name}] could not run: {e} - stopping here.")
            break

    minutes = (datetime.now(timezone.utc) - started).total_seconds() / 60
    if failed:
        logger.warning(f"===== DAILY MAINTENANCE FAILED ({', '.join(failed)}) after {minutes:.1f} min =====")
        _notify(f"Daily maintenance FAILED at: {', '.join(failed)} - check logs/maintenance.log", 'warning')
        return 1

    _record_success()
    logger.info(f"===== DAILY MAINTENANCE OK in {minutes:.1f} min [{mode}] =====")
    _notify(f"Daily maintenance done in {minutes:.0f} min: data pump + memory outcome sync OK", 'info')
    return 0


if __name__ == "__main__":
    sys.exit(main())
