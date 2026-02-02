
import os
import hashlib
import logging
from datetime import datetime
from sqlalchemy import text
from .constants import get_mysql_connection, CSV_FILE_PATH, COLUMN_MAPPING
from .schema_evolution import SchemaEvolutionHandler, validate_schema_compatibility
from .lineage import get_lineage_tracker, TransformationInfo
import pandas as pd

logger = logging.getLogger(__name__)


def compute_file_hash(file_path: str) -> str:
    # Compute MD5 hash of file for change detection.
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_last_ingestion_hash(engine) -> str:
    # Get the hash of the last ingested file.
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT file_hash FROM ingestion_metadata ORDER BY ingested_at DESC LIMIT 1"
            ))
            row = result.fetchone()
            return row[0] if row else None
    except Exception:
        return None


def record_ingestion_metadata(engine, file_hash: str, record_count: int, run_id: str):
    # Record metadata about the ingestion run.
    try:
        with engine.begin() as conn:
            # Create metadata table if not exists
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ingestion_metadata (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    file_hash VARCHAR(64),
                    record_count INT,
                    run_id VARCHAR(255),
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.execute(text("""
                INSERT INTO ingestion_metadata (file_hash, record_count, run_id)
                VALUES (:hash, :count, :run_id)
            """), {"hash": file_hash, "count": record_count, "run_id": run_id})
    except Exception as e:
        logger.warning(f"Could not record ingestion metadata: {e}")


def ingest_csv_to_mysql(**context):
    # Task 1: Load CSV data into MySQL staging table.

    
    
    logger.info("Starting CSV ingestion to MySQL...")
    start_time = datetime.now()
    run_id = context.get('run_id', 'unknown')
    dag_id = context.get('dag').dag_id if context.get('dag') else 'flight_price_pipeline'
    
    try:
        # Check if CSV file exists
        if not os.path.exists(CSV_FILE_PATH):
            raise FileNotFoundError(f"CSV file not found at {CSV_FILE_PATH}")
        
        engine = get_mysql_connection()
        
     
        # Idempotency Check: Compare file hash
       
        current_hash = compute_file_hash(CSV_FILE_PATH)
        last_hash = get_last_ingestion_hash(engine)
        
        force_reload = context.get('params', {}).get('force_reload', False)
        
        if current_hash == last_hash and not force_reload:
            logger.info("File unchanged since last ingestion, skipping...")
            context['ti'].xcom_push(key='ingestion_skipped', value=True)
            context['ti'].xcom_push(key='ingested_count', value=0)
            return {'status': 'skipped', 'reason': 'file_unchanged', 'file_hash': current_hash}
        
     
        # Schema Evolution Detection
      
        # Read first chunk to check schema
        sample_df = pd.read_csv(CSV_FILE_PATH, nrows=100)
        sample_df = sample_df.rename(columns=COLUMN_MAPPING)
        
        schema_handler = SchemaEvolutionHandler()
        schema_report = schema_handler.detect_schema_changes(sample_df)
        
        if schema_report.has_changes:
            logger.warning(f"Schema changes detected: {schema_report.new_columns} new, {schema_report.removed_columns} removed")
            context['ti'].xcom_push(key='schema_changes', value=schema_report.to_dict())
            
            if not schema_report.is_compatible:
                raise ValueError(f"Breaking schema changes detected: {schema_report.removed_columns}")
        
      
        # Chunked Ingestion with UPSERT Logic
        chunk_size = int(os.environ.get('FLIGHT_PIPELINE_CHUNK_SIZE', 10000))
        total_ingested = 0
        
        # Clear existing data (full refresh strategy)
        # For incremental, you would use UPSERT based on a unique key
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE flight_staging"))
        
        for chunk_idx, chunk in enumerate(pd.read_csv(CSV_FILE_PATH, chunksize=chunk_size)):
            # Rename columns
            chunk = chunk.rename(columns=COLUMN_MAPPING)
            
            # Handle schema evolution
            if schema_report.has_changes:
                chunk = schema_handler.adapt_dataframe(chunk, schema_report)
            
            # Add metadata columns
            chunk['is_validated'] = False
            chunk['validation_errors'] = None
            chunk['ingestion_run_id'] = run_id
            chunk['ingested_at'] = datetime.now()
            
            # Insert chunk
            chunk.to_sql('flight_staging', engine, if_exists='append', index=False)
            total_ingested += len(chunk)
            logger.info(f"Chunk {chunk_idx + 1}: Ingested {total_ingested} records so far")
        
        
        # Record Metadata & Lineage
     
        record_ingestion_metadata(engine, current_hash, total_ingested, run_id)
        
        # Track lineage
        lineage_tracker = get_lineage_tracker(dag_id, run_id)
        lineage_tracker.track_write(
            task_id='ingest_csv_to_mysql',
            target_name='flight_staging',
            target_namespace='mysql.staging',
            source_name='Flight_Price_Dataset_of_Bangladesh.csv',
            source_namespace='filesystem',
            row_count=total_ingested,
            columns=list(COLUMN_MAPPING.values())
        )
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Successfully ingested {total_ingested} records in {duration:.2f}s")
        
        # Store metadata in XCom
        context['ti'].xcom_push(key='ingested_count', value=total_ingested)
        context['ti'].xcom_push(key='ingestion_skipped', value=False)
        context['ti'].xcom_push(key='file_hash', value=current_hash)
        context['ti'].xcom_push(key='lineage', value=lineage_tracker.to_json())
        
        return {
            'status': 'success', 
            'records_ingested': total_ingested,
            'file_hash': current_hash,
            'duration_seconds': duration,
            'schema_changes': schema_report.has_changes
        }
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        context['ti'].xcom_push(key='error', value=str(e))
        raise