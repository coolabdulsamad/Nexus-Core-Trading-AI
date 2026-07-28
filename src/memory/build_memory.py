"""
src/memory/build_memory.py
Responsibility: Loads all data from TimescaleDB, compresses it to 64-dim vectors,
and pushes it all to Qdrant to build the historical memory.
"""
import pandas as pd
from src.memory.vector_encoder import VectorEncoder
from src.memory.qdrant_memory import QdrantMemory
from utils.logger import setup_logger

logger = setup_logger("MemoryBuilder", "logs/memory.log")

def main():
    logger.info("===== STARTING MEMORY BUILD =====")
    
    # 1. Load data from DB
    encoder = VectorEncoder()
    df = encoder.fetch_all_features()
    if df.empty:
        logger.error("No data to build memory.")
        return
    
    # 2. Compress to 64-dim vectors
    logger.info("Fitting PCA and transforming data...")
    vectors = encoder.fit_transform(df)
    
    # 3. Push to Qdrant
    qdrant = QdrantMemory()
    qdrant.upsert_batch(df, vectors)
    
    logger.info("===== MEMORY BUILD COMPLETED =====")
    logger.info(f"Total vectors stored: {len(vectors)}")

if __name__ == "__main__":
    main()