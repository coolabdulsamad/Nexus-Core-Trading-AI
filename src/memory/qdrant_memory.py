"""
src/memory/qdrant_memory.py
FIXED:
- Deterministic UNIQUE point IDs (uuid5 of symbol+timestamp). The old
  epoch-ms ID made AAPL/TSLA/MSFT/... overwrite each other at the same
  timestamp - the memory silently kept only ONE symbol per bar.
- Numeric epoch 'ts' payload field so searches can be time-filtered
  (this is what makes look-ahead-free backtesting possible).
- Regime label stored per point; searches can be regime-filtered.
- search_similar uses the modern query_points API (qdrant-client >=1.10
  deprecated and then REMOVED client.search); falls back to the legacy
  call only if an old client is installed.

NOTE: after upgrading, rebuild the memory (python -m src.memory.build_memory)
- old points use the colliding ID scheme.
"""
import uuid
from typing import List, Dict, Optional
import numpy as np
import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.http import models
from config.settings import config
from src.utils.logger import setup_logger

logger = setup_logger("QdrantMemory", "logs/memory.log")

_ID_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def make_point_id(symbol: str, ts_epoch: int) -> str:
    """Deterministic unique ID per (symbol, timestamp)."""
    return str(uuid.uuid5(_ID_NAMESPACE, f"{symbol}_{ts_epoch}"))


class QdrantMemory:
    def __init__(self):
        self.client = QdrantClient(host=config.qdrant.host, port=config.qdrant.port)
        self.collection_name = "market_memory"

    # ------------------------------------------------------------------
    def ensure_collection(self, vector_size: int):
        collections = self.client.get_collections()
        names = [c.name for c in collections.collections]

        if self.collection_name in names:
            info = self.client.get_collection(self.collection_name)
            current_size = info.config.params.vectors.size
            if current_size == vector_size:
                logger.info(f"Collection OK (vector size {vector_size})")
                return
            logger.warning(f"Vector size mismatch ({current_size} -> {vector_size}). Recreating collection.")
            self.client.delete_collection(self.collection_name)

        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=models.VectorParams(size=vector_size, distance=models.Distance.COSINE),
        )
        for field, schema in (("symbol", models.PayloadSchemaType.KEYWORD),
                              ("regime_label", models.PayloadSchemaType.KEYWORD),
                              ("ts", models.PayloadSchemaType.INTEGER)):
            try:
                self.client.create_payload_index(self.collection_name, field, schema)
            except Exception as e:
                logger.warning(f"Index on {field} skipped: {e}")
        logger.info("Collection created with symbol/regime/ts indexes.")

    # ------------------------------------------------------------------
    def upsert_batch(self, df: pd.DataFrame, vectors: np.ndarray):
        if len(df) != len(vectors):
            logger.error("DataFrame/vector length mismatch")
            return

        self.ensure_collection(vectors.shape[1])

        points = []
        for i, (_, row) in enumerate(df.iterrows()):
            ts_epoch = int(pd.Timestamp(row['time_bucket']).timestamp())
            points.append(models.PointStruct(
                id=make_point_id(row['symbol'], ts_epoch),
                vector=vectors[i].tolist(),
                payload={
                    "symbol": row['symbol'],
                    "timestamp": pd.Timestamp(row['time_bucket']).isoformat(),
                    "ts": ts_epoch,
                    "regime_label": row.get('regime_label', 'unknown') or 'unknown',
                    "forward_return_1h": None,
                },
            ))

        for i in range(0, len(points), 100):
            self.client.upsert(self.collection_name, points[i:i + 100])
        logger.info(f"Upserted {len(points)} vectors into Qdrant.")

    # ------------------------------------------------------------------
    def search_similar(self, vector: np.ndarray, symbol: str = None, limit: int = 10,
                       before_ts: Optional[int] = None, regime: Optional[str] = None) -> List[Dict]:
        """
        kNN search with optional filters:
        - before_ts: only states with ts <= before_ts (look-ahead guard)
        - regime: only states from the same market regime
        """
        must = []
        if symbol:
            must.append(models.FieldCondition(key="symbol", match=models.MatchValue(value=symbol)))
        if before_ts is not None:
            must.append(models.FieldCondition(key="ts", range=models.Range(lte=before_ts)))
        if regime and regime != "unknown":
            must.append(models.FieldCondition(key="regime_label", match=models.MatchValue(value=regime)))

        query_filter = models.Filter(must=must) if must else None

        # qdrant-client >=1.10: query_points (client.search was removed in
        # recent releases). Fall back to the legacy call for old clients.
        if hasattr(self.client, "query_points"):
            response = self.client.query_points(
                collection_name=self.collection_name,
                query=vector.tolist(),
                query_filter=query_filter,
                limit=limit,
                with_payload=True,
            )
            results = response.points
        else:
            results = self.client.search(
                collection_name=self.collection_name,
                query_vector=vector.tolist(),
                query_filter=query_filter,
                limit=limit,
                with_payload=True,
            )

        return [{
            'score': hit.score,
            'symbol': hit.payload.get('symbol'),
            'timestamp': hit.payload.get('timestamp'),
            'ts': hit.payload.get('ts'),
            'regime_label': hit.payload.get('regime_label'),
            'forward_return_1h': hit.payload.get('forward_return_1h'),
        } for hit in results]
