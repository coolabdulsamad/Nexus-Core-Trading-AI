#!/usr/bin/env python3
"""
src/memory/build_memory.py
Builds/rebuilds the brain's memory: DB features -> scaler+PCA -> Qdrant.
Run from project root:  python -m src.memory.build_memory
Then:                 python -m src.memory.backfill_forward_returns
                      python -m src.memory.update_qdrant_payloads
"""
from src.memory.vector_encoder import VectorEncoder
from src.memory.qdrant_memory import QdrantMemory
from src.utils.logger import setup_logger

logger = setup_logger("MemoryBuilder", "logs/memory.log")


def main():
    logger.info("===== MEMORY BUILD START =====")
    encoder = VectorEncoder()
    df = encoder.fetch_all_features()
    if df.empty:
        logger.error("No data to build memory.")
        return

    logger.info("Fitting scaler + PCA ...")
    vectors = encoder.fit_transform(df)

    qdrant = QdrantMemory()
    qdrant.upsert_batch(df, vectors)

    logger.info(f"===== MEMORY BUILD COMPLETE: {len(vectors)} states =====")
    logger.info("Next: backfill_forward_returns, then update_qdrant_payloads.")


if __name__ == "__main__":
    main()
