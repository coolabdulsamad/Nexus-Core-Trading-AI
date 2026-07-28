"""
src/memory/qdrant_memory.py
Responsibility: Interface with Qdrant. Dynamically handles vector sizes.
"""
from qdrant_client import QdrantClient
from qdrant_client.http import models
from config.settings import config
from utils.logger import setup_logger
import numpy as np
import pandas as pd
from typing import List, Dict

logger = setup_logger("QdrantMemory", "logs/memory.log")

class QdrantMemory:
    def __init__(self):
        self.client = QdrantClient(
            host=config.qdrant.host,
            port=config.qdrant.port
        )
        self.collection_name = "market_memory"

    def ensure_collection(self, vector_size: int):
        """
        Checks if collection exists with the correct vector size.
        If it doesn't match, it deletes and recreates it.
        """
        collections = self.client.get_collections()
        collection_names = [c.name for c in collections.collections]
        
        if self.collection_name in collection_names:
            # Check the current vector size
            collection_info = self.client.get_collection(self.collection_name)
            current_size = collection_info.config.params.vectors.size
            
            if current_size == vector_size:
                logger.info(f"Collection exists with correct vector size: {vector_size}")
                return
            else:
                logger.warning(f"Vector size mismatch (current: {current_size}, new: {vector_size}). Deleting and recreating.")
                self.client.delete_collection(self.collection_name)
        
        # Create the new collection
        logger.info(f"Creating collection: {self.collection_name} with vector size: {vector_size}")
        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=models.VectorParams(
                size=vector_size,
                distance=models.Distance.COSINE
            )
        )
        # Create a payload index for fast symbol filtering
        self.client.create_payload_index(
            collection_name=self.collection_name,
            field_name="symbol",
            field_schema=models.PayloadSchemaType.KEYWORD
        )
        logger.info("Collection created successfully.")

    def upsert_batch(self, df: pd.DataFrame, vectors: np.ndarray):
        """Upserts a batch of vectors into Qdrant."""
        if len(df) != len(vectors):
            logger.error("DataFrame and Vector length mismatch!")
            return

        # Ensure collection matches vector size
        vector_size = vectors.shape[1]
        self.ensure_collection(vector_size)

        points = []
        for i, (index, row) in enumerate(df.iterrows()):
            point_id = int(pd.Timestamp(row['time_bucket']).timestamp() * 1000)
            
            points.append(
                models.PointStruct(
                    id=point_id,
                    vector=vectors[i].tolist(),
                    payload={
                        "symbol": row['symbol'],
                        "timestamp": row['time_bucket'].isoformat(),
                        "forward_return_1h": None,
                        "regime_label": "unknown"
                    }
                )
            )

        # Upsert in chunks of 100
        for i in range(0, len(points), 100):
            chunk = points[i:i+100]
            self.client.upsert(
                collection_name=self.collection_name,
                points=chunk
            )
        logger.info(f"Upserted {len(points)} vectors into Qdrant.")

    def search_similar(self, vector: np.ndarray, symbol: str = None, limit: int = 10) -> List[Dict]:
        """Searches for the top-k most similar historical states."""
        search_filter = None
        if symbol:
            search_filter = models.Filter(
                must=[models.FieldCondition(
                    key="symbol",
                    match=models.MatchValue(value=symbol)
                )]
            )

        results = self.client.search(
            collection_name=self.collection_name,
            query_vector=vector.tolist(),
            query_filter=search_filter,
            limit=limit,
            with_payload=True
        )
        
        formatted = []
        for hit in results:
            formatted.append({
                'score': hit.score,
                'symbol': hit.payload['symbol'],
                'timestamp': hit.payload['timestamp'],
                'forward_return_1h': hit.payload.get('forward_return_1h')
            })
        return formatted