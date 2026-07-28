"""
src/universe/symbol_manager.py
DB-backed symbol universe with Alpaca verification.

- Symbols live in the `symbols` table (not hardcoded).
- add_symbol() verifies against Alpaca before activating.
- Falls back to config.symbols if the DB is unreachable, so the trader
  never dies because the universe table is missing.
"""
import os
import psycopg2
from config.settings import config
from src.utils.logger import setup_logger

logger = setup_logger("SymbolManager", "logs/universe.log")


class SymbolManager:
    def __init__(self):
        self.db_url = config.database.url

    def _conn(self):
        return psycopg2.connect(self.db_url)

    # ------------------------------------------------------------------
    # Alpaca verification
    # ------------------------------------------------------------------
    @staticmethod
    def verify_on_alpaca(symbol: str, asset_type: str = 'stock') -> bool:
        try:
            from alpaca.trading.client import TradingClient
            client = TradingClient(os.getenv("ALPACA_API_KEY"),
                                   os.getenv("ALPACA_SECRET_KEY"), paper=True)
            asset = client.get_asset(symbol)
            tradable = getattr(asset, 'tradable', False)
            logger.info(f"Alpaca verify {symbol}: tradable={tradable}")
            return bool(tradable)
        except Exception as e:
            logger.warning(f"Alpaca verification failed for {symbol}: {e}")
            return False

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------
    def add_symbol(self, symbol: str, asset_type: str = 'stock', verify: bool = True,
                   notes: str = '') -> bool:
        symbol = symbol.upper().strip()
        verified = self.verify_on_alpaca(symbol, asset_type) if verify else False
        if verify and not verified:
            logger.warning(f"{symbol} not verified on Alpaca - adding as inactive.")
        try:
            with self._conn() as conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO symbols (symbol, asset_type, active, verified_broker, notes)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, asset_type) DO UPDATE SET
                        active = EXCLUDED.active,
                        verified_broker = EXCLUDED.verified_broker,
                        notes = EXCLUDED.notes;
                """, (symbol, asset_type, verified if verify else True, verified, notes))
            logger.info(f"Symbol added: {symbol} ({asset_type}, active={verified if verify else True})")
            return True
        except Exception as e:
            logger.error(f"add_symbol failed: {e}")
            return False

    def remove_symbol(self, symbol: str, asset_type: str = 'stock') -> bool:
        """Soft remove (active=FALSE) so history stays intact."""
        try:
            with self._conn() as conn, conn.cursor() as cur:
                cur.execute("UPDATE symbols SET active = FALSE WHERE symbol = %s AND asset_type = %s",
                            (symbol.upper(), asset_type))
            return True
        except Exception as e:
            logger.error(f"remove_symbol failed: {e}")
            return False

    def list_symbols(self, active_only: bool = False):
        try:
            with self._conn() as conn, conn.cursor() as cur:
                q = "SELECT symbol, asset_type, active, verified_broker, added_at, notes FROM symbols"
                if active_only:
                    q += " WHERE active = TRUE"
                q += " ORDER BY asset_type, symbol"
                cur.execute(q)
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
        except Exception as e:
            logger.error(f"list_symbols failed (DB): {e} - falling back to config")
            fallback = [{'symbol': s, 'asset_type': 'stock', 'active': True,
                         'verified_broker': None, 'added_at': None, 'notes': 'config fallback'}
                        for s in config.symbols]
            if config.CRYPTO_ENABLED:
                fallback += [{'symbol': c, 'asset_type': 'crypto', 'active': True,
                              'verified_broker': None, 'added_at': None, 'notes': 'config fallback'}
                             for c in config.CRYPTO_SYMBOLS]
            return fallback

    def get_active(self, asset_type: str = None):
        rows = [r for r in self.list_symbols(active_only=True)
                if asset_type is None or r['asset_type'] == asset_type]
        return [r['symbol'] for r in rows]


if __name__ == "__main__":
    mgr = SymbolManager()
    for row in mgr.list_symbols():
        print(row)
