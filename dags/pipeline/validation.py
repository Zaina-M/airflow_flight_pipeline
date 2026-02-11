
import json
import logging
from datetime import datetime
from sqlalchemy import text
from airflow.exceptions import AirflowSkipException
from .constants import get_mysql_connection, REQUIRED_COLUMNS, COLUMN_MAPPING
from .data_quality import DataQualityValidator, create_flight_data_expectations
from .lineage import get_lineage_tracker
import pandas as pd

logger = logging.getLogger(__name__)


def validate_data(**context):
   
    logger.info("Starting data validation in MySQL...")
    start_time = datetime.now()
    run_id = context.get('run_id', 'unknown')
    dag_id = context.get('dag').dag_id if context.get('dag') else 'flight_price_pipeline'
    
    try:
        # Check if ingestion was skipped
        ingestion_skipped = context['ti'].xcom_pull(key='ingestion_skipped', task_ids='ingestion.ingest_csv_to_mysql')
        if ingestion_skipped:
            logger.info("Ingestion was skipped, using existing validated data")
            context['ti'].xcom_push(key='validation_skipped', value=True)
            raise AirflowSkipException("Ingestion skipped, no new data to validate")
        
        engine = get_mysql_connection()
        
        # Read data from staging
        df = pd.read_sql("SELECT * FROM flight_staging", engine)
        
        total_records = len(df)
        logger.info(f"Validating {total_records} records...")
      
        # Data Quality Framework Validation
    
        validator = create_flight_data_expectations(df)
        validation_result = validator.validate()
        
        # Track validation metrics
        missing_values_count = 0
        negative_fares_count = 0
        data_type_errors = 0
        validation_issues = []
        
        # Process framework results
        for result in validation_result.results:
            if not result.success:
                validation_issues.append({
                    'expectation': result.expectation_type,
                    'column': result.column,
                    'observed': str(result.observed_value),
                    'expected': str(result.expected_value)
                })
        
      
        # Legacy Validation (for corrections)
     
        
        # 1. Check for missing values in required columns
        required_db_cols = [COLUMN_MAPPING.get(col, col) for col in REQUIRED_COLUMNS]
        for col in required_db_cols:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    missing_values_count += null_count
                    logger.warning(f"Column '{col}' has {null_count} missing values")
        
        # 2. Handle missing values - fill with appropriate defaults
        df['airline'] = df['airline'].fillna('Unknown')
        df['source'] = df['source'].fillna('UNK')
        df['destination'] = df['destination'].fillna('UNK')
        df['base_fare_bdt'] = df['base_fare_bdt'].fillna(0)
        df['tax_surcharge_bdt'] = df['tax_surcharge_bdt'].fillna(0)
        df['total_fare_bdt'] = df['total_fare_bdt'].fillna(0)
        
        # 3. Validate numeric fare values
        fare_columns = ['base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt']
        for col in fare_columns:
            # Check for negative values
            negative_mask = df[col] < 0
            neg_count = negative_mask.sum()
            if neg_count > 0:
                negative_fares_count += neg_count
                logger.warning(f"Column '{col}' has {neg_count} negative values - setting to absolute")
                # Correct negative values by taking absolute
                df.loc[negative_mask, col] = df.loc[negative_mask, col].abs()
        
        # 4. Validate non-empty strings for categorical fields
        string_columns = ['airline', 'source', 'destination']
        for col in string_columns:
            empty_count = (df[col].str.strip() == '').sum()
            if empty_count > 0:
                data_type_errors += empty_count
                validation_issues.append({
                    'expectation': 'non_empty_string',
                    'column': col,
                    'observed': f"{empty_count} empty strings",
                    'expected': 'non-empty values'
                })
        
        # 5. Recalculate Total Fare if needed
        calculated_total = df['base_fare_bdt'] + df['tax_surcharge_bdt']
        fare_mismatch = (abs(df['total_fare_bdt'] - calculated_total) > 0.01).sum()
        if fare_mismatch > 0:
            logger.info(f"Recalculating {fare_mismatch} mismatched Total Fare values")
            df['total_fare_bdt'] = calculated_total
        
        # 6. Mark records as validated
        df['is_validated'] = True
        df['validation_errors'] = df.apply(
            lambda row: None if pd.notnull(row['airline']) and row['base_fare_bdt'] >= 0 
            else 'validation_issue',
            axis=1
        )
        df['validated_at'] = datetime.now()
        
        # Update staging table with validated data
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE flight_staging"))
        
        df.to_sql('flight_staging', engine, if_exists='append', index=False)
        
    
        # Calculate validation summary
    
        valid_records = total_records - missing_values_count - negative_fares_count - data_type_errors
        duration = (datetime.now() - start_time).total_seconds()
        
        validation_report = {
            'total_records': total_records,
            'valid_records': int(valid_records),
            'invalid_records': int(total_records - valid_records),
            'missing_values_count': int(missing_values_count),
            'negative_fares_count': int(negative_fares_count),
            'data_type_errors': int(data_type_errors),
            'fare_recalculations': int(fare_mismatch),
            'issues': validation_issues,
            'data_quality_result': validation_result.to_dict(),
            'duration_seconds': duration
        }
        
        logger.info(f"Validation complete: {valid_records}/{total_records} records valid in {duration:.2f}s")
        
      
        # Track Lineage
        
        lineage_tracker = get_lineage_tracker(dag_id, run_id)
        lineage_tracker.track_validation(
            task_id='validate_data',
            dataset_name='flight_staging',
            dataset_namespace='mysql.staging',
            validation_result={
                'total_records': int(total_records),
                'valid_records': int(valid_records),
                'data_quality_passed': bool(validation_result.success)
            }
        )
        
        # Store validation report in XCom
        context['ti'].xcom_push(key='validation_report', value=json.dumps(validation_report))
        context['ti'].xcom_push(key='validation_skipped', value=False)
        context['ti'].xcom_push(key='lineage', value=lineage_tracker.to_json())
        
        return validation_report
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        context['ti'].xcom_push(key='error', value=str(e))
        raise