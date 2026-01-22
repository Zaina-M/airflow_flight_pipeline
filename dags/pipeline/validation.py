"""
Validation module for flight price pipeline.
"""

import json
import logging
from sqlalchemy import text
from .constants import get_mysql_connection, REQUIRED_COLUMNS, COLUMN_MAPPING
import pandas as pd

logger = logging.getLogger(__name__)


def validate_data(**context):
    """
    Task 2: Validate data quality in staging table.
    
    Checks:
    - Required columns exist
    - Handle missing/null values
    - Validate data types
    - Flag negative fares or invalid data
    """
    logger.info("Starting data validation in MySQL...")
    
    try:
        engine = get_mysql_connection()
        
        # Read data from staging
        df = pd.read_sql("SELECT * FROM flight_staging", engine)
        
        total_records = len(df)
        validation_issues = []
        
        # Track validation metrics
        missing_values_count = 0
        negative_fares_count = 0
        data_type_errors = 0
        
        # 1. Check for missing values in required columns
        required_db_cols = [COLUMN_MAPPING.get(col, col) for col in REQUIRED_COLUMNS]
        for col in required_db_cols:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    missing_values_count += null_count
                    validation_issues.append(f"Column '{col}' has {null_count} missing values")
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
                validation_issues.append(f"Column '{col}' has {neg_count} negative values")
                logger.warning(f"Column '{col}' has {neg_count} negative values - setting to absolute")
                # Correct negative values by taking absolute
                df.loc[negative_mask, col] = df.loc[negative_mask, col].abs()
        
        # 4. Validate non-empty strings for categorical fields
        string_columns = ['airline', 'source', 'destination']
        for col in string_columns:
            empty_count = (df[col].str.strip() == '').sum()
            if empty_count > 0:
                data_type_errors += empty_count
                validation_issues.append(f"Column '{col}' has {empty_count} empty strings")
        
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
        
        # Update staging table with validated data
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE flight_staging"))
        
        df.to_sql('flight_staging', engine, if_exists='append', index=False)
        
        # Calculate validation summary
        valid_records = total_records - missing_values_count - negative_fares_count - data_type_errors
        
        validation_report = {
            'total_records': total_records,
            'valid_records': int(valid_records),
            'invalid_records': int(total_records - valid_records),
            'missing_values_count': int(missing_values_count),
            'negative_fares_count': int(negative_fares_count),
            'data_type_errors': int(data_type_errors),
            'issues': validation_issues
        }
        
        logger.info(f"Validation complete: {valid_records}/{total_records} records valid")
        
        # Store validation report in XCom
        context['ti'].xcom_push(key='validation_report', value=json.dumps(validation_report))
        
        return validation_report
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        context['ti'].xcom_push(key='error', value=str(e))
        raise