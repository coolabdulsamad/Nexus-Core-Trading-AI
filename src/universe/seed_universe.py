#!/usr/bin/env python3
"""
src/universe/seed_universe.py
Seeds the candidate pool into the DB universe: config.UNIVERSE_POOL
(stocks) + config.CRYPTO_SYMBOLS (crypto). Every candidate is verified
against Alpaca first - verified symbols become ACTIVE (watched by the
trader / selector), unverified ones stay in the table but inactive.

Idempotent (upserts) - safe to re-run after editing UNIVERSE_POOL.

    python -m src.universe.seed_universe
"""
from config.settings import config
from src.universe.symbol_manager import SymbolManager
from src.utils.logger import setup_logger

logger = setup_logger("SeedUniverse", "logs/universe.log")


def main():
    mgr = SymbolManager()
    pool = [(s, 'stock') for s in config.UNIVERSE_POOL]
    if config.CRYPTO_ENABLED:
        pool += [(c, 'crypto') for c in config.CRYPTO_SYMBOLS]

    logger.info(f"Seeding {len(pool)} symbols into the universe...")
    for sym, typ in pool:
        mgr.add_symbol(sym, typ, verify=True)

    rows = mgr.list_symbols()
    active = [r for r in rows if r['active']]
    inactive = [r for r in rows if not r['active']]

    print(f"\n=== UNIVERSE: {len(active)} active / {len(inactive)} inactive ===")
    for r in rows:
        print(f"  {'ON ' if r['active'] else 'off'} {r['symbol']:<10} "
              f"{r['asset_type']:<7} verified={r['verified_broker']}")
    if inactive:
        print("\nInactive = Alpaca says not tradable. They stay out of trading.")
    print("\nNext: python -m src.ingestion.backfill_history 2")


if __name__ == "__main__":
    main()
