"""
Transformation module for flight price pipeline.
"""

import logging
from sqlalchemy import text
from .constants import get_postgres_connection, get_mysql_connection, PEAK_SEASONS
import pandas as pd

logger = logging.getLogger(__name__)


def transform_data(**context):
    """
    Task 3: Transform and clean data for analytics.
    
    Transformations:
    - Parse datetime fields
    - Add computed columns (is_peak_season, route)
    - Ensure Total Fare calculation
    """
    logger.info("Starting data transformation from MySQL to PostgreSQL...")
    
    mysql_engine = get_mysql_connection()
    postgres_engine = get_postgres_connection()
    
    # Read validated data from staging (MySQL)
    df = pd.read_sql("SELECT * FROM flight_staging WHERE is_validated = TRUE", mysql_engine)
    
    logger.info(f"Transforming {len(df)} validated records")
    
    # 1. Parse datetime fields
    df['departure_datetime'] = pd.to_datetime(df['departure_datetime'], errors='coerce')
    df['arrival_datetime'] = pd.to_datetime(df['arrival_datetime'], errors='coerce')
    
    # 2. Calculate Total Fare if not present or zero
    mask = (df['total_fare_bdt'].isnull()) | (df['total_fare_bdt'] == 0)
    df.loc[mask, 'total_fare_bdt'] = df.loc[mask, 'base_fare_bdt'] + df.loc[mask, 'tax_surcharge_bdt']
    
    # 3. Add is_peak_season flag
    df['is_peak_season'] = df['seasonality'].isin(PEAK_SEASONS)
    
    # 4. Create route column
    df['route'] = df['source'] + '-' + df['destination']
    
    # 5. Select columns for analytics table
    analytics_columns = [
        'airline', 'source', 'source_name', 'destination', 'destination_name',
        'departure_datetime', 'arrival_datetime', 'duration_hrs', 'stopovers',
        'aircraft_type', 'class', 'booking_source', 'base_fare_bdt',
        'tax_surcharge_bdt', 'total_fare_bdt', 'seasonality', 'days_before_departure',
        'is_peak_season', 'route'
    ]
    
    df_analytics = df[analytics_columns].copy()
    
    # 6. Load to PostgreSQL
    with postgres_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE flight_data_transformed"))
    
    df_analytics.to_sql('flight_data_transformed', postgres_engine, 
                        if_exists='append', index=False)
    
    logger.info(f"Successfully loaded {len(df_analytics)} transformed records to PostgreSQL")
    
    context['ti'].xcom_push(key='transformed_count', value=len(df_analytics))
    
    return {'status': 'success', 'records_transformed': len(df_analytics)}