"""
Ingestion module for flight price pipeline.
"""

import os
import logging
from sqlalchemy import text
from .constants import get_mysql_connection, CSV_FILE_PATH, COLUMN_MAPPING
import pandas as pd

logger = logging.getLogger(__name__)


def ingest_csv_to_mysql(**context):
    """
    Task 1: Load CSV data into MySQL staging table.
    """
    logger.info("Starting CSV ingestion to MySQL...")
    
    try:
        # Check if CSV file exists
        if not os.path.exists(CSV_FILE_PATH):
            raise FileNotFoundError(f"CSV file not found at {CSV_FILE_PATH}")
        
        # Read CSV file in chunks for scalability
        chunk_size = 10000  # Adjust based on memory
        total_ingested = 0
        
        engine = get_mysql_connection()
        
        # Clear existing data
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE flight_staging"))
        
        for chunk in pd.read_csv(CSV_FILE_PATH, chunksize=chunk_size):
            # Rename columns
            chunk = chunk.rename(columns=COLUMN_MAPPING)
            
            # Add validation flags
            chunk['is_validated'] = False
            chunk['validation_errors'] = None
            
            # Insert chunk
            chunk.to_sql('flight_staging', engine, if_exists='append', index=False)
            total_ingested += len(chunk)
            logger.info(f"Ingested {total_ingested} records so far")
        
        logger.info(f"Successfully inserted {total_ingested} records into PostgreSQL staging")
        
        # Store metadata in XCom
        context['ti'].xcom_push(key='ingested_count', value=total_ingested)
        
        return {'status': 'success', 'records_ingested': total_ingested}
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        context['ti'].xcom_push(key='error', value=str(e))
        raise