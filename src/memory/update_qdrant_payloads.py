"""
src/memory/update_qdrant_payloads.py
Responsibility: Fetch forward_return_1h from the database and update Qdrant payloads.
"""
import psycopg2
import pandas as pd
from qdrant_client import QdrantClient
from config.settings import config
from utils.logger import setup_logger

logger = setup_logger("UpdateQdrantPayloads", "logs/memory.log")

def main():
    logger.info("===== STARTING QDRANT PAYLOAD UPDATE =====")
    
    # 1. Connect to DB
    conn = psycopg2.connect(config.database.url)
    query = """
        SELECT symbol, time_bucket, forward_return_1h
        FROM feature_cache
        WHERE forward_return_1h IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    conn.close()
    logger.info(f"Loaded {len(df)} rows with forward returns.")
    
    if df.empty:
        logger.warning("No forward returns found. Run backfill first.")
        return
    
    # 2. Connect to Qdrant
    client = QdrantClient(host=config.qdrant.host, port=config.qdrant.port)
    collection_name = "market_memory"
    
    # 3. Update each point
    updated_count = 0
    for _, row in df.iterrows():
        point_id = int(pd.Timestamp(row['time_bucket']).timestamp() * 1000)
        try:
            client.set_payload(
                collection_name=collection_name,
                payload={"forward_return_1h": float(row['forward_return_1h'])},
                points=[point_id]
            )
            updated_count += 1
            if updated_count % 500 == 0:
                logger.info(f"Updated {updated_count} payloads...")
        except Exception as e:
            logger.error(f"Failed to update point {point_id}: {e}")
    
    logger.info(f"===== PAYLOAD UPDATE COMPLETED. Updated {updated_count} points. =====")

if __name__ == "__main__":
    main()