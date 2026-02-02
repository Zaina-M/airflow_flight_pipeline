import logging
from datetime import datetime
from sqlalchemy import text
from .constants import get_postgres_connection, get_mysql_connection, PEAK_SEASONS
from .lineage import get_lineage_tracker, TransformationInfo
import pandas as pd

logger = logging.getLogger(__name__)


def transform_data(**context):
    
    
    logger.info("Starting data transformation from MySQL to PostgreSQL...")
    start_time = datetime.now()
    run_id = context.get('run_id', 'unknown')
    dag_id = context.get('dag').dag_id if context.get('dag') else 'flight_price_pipeline'
    
    mysql_engine = get_mysql_connection()
    postgres_engine = get_postgres_connection()
    
    # Read validated data from staging (MySQL)
    df = pd.read_sql("SELECT * FROM flight_staging WHERE is_validated = TRUE", mysql_engine)
    input_row_count = len(df)
    
    logger.info(f"Transforming {input_row_count} validated records")
    
    # Track transformations applied
    transformations = []
    
    # 1. Parse datetime fields
    df['departure_datetime'] = pd.to_datetime(df['departure_datetime'], errors='coerce')
    df['arrival_datetime'] = pd.to_datetime(df['arrival_datetime'], errors='coerce')
    transformations.append(TransformationInfo(
        name='parse_datetime',
        description='Convert string datetime to TIMESTAMP',
        input_columns=['departure_datetime', 'arrival_datetime'],
        output_columns=['departure_datetime', 'arrival_datetime'],
        logic="pd.to_datetime(col, errors='coerce')"
    ))
    
    # 2. Calculate Total Fare if not present or zero
    mask = (df['total_fare_bdt'].isnull()) | (df['total_fare_bdt'] == 0)
    df.loc[mask, 'total_fare_bdt'] = df.loc[mask, 'base_fare_bdt'] + df.loc[mask, 'tax_surcharge_bdt']
    transformations.append(TransformationInfo(
        name='calculate_total_fare',
        description='Calculate Total Fare = Base Fare + Tax & Surcharge',
        input_columns=['base_fare_bdt', 'tax_surcharge_bdt'],
        output_columns=['total_fare_bdt'],
        logic='total_fare_bdt = base_fare_bdt + tax_surcharge_bdt'
    ))
    
    # 3. Add is_peak_season flag
    df['is_peak_season'] = df['seasonality'].isin(PEAK_SEASONS)
    transformations.append(TransformationInfo(
        name='add_peak_season_flag',
        description=f'Flag peak seasons: {PEAK_SEASONS}',
        input_columns=['seasonality'],
        output_columns=['is_peak_season'],
        logic=f"seasonality IN {PEAK_SEASONS}"
    ))
    
    # 4. Create route column
    df['route'] = df['source'] + '-' + df['destination']
    transformations.append(TransformationInfo(
        name='create_route',
        description='Combine source and destination into route',
        input_columns=['source', 'destination'],
        output_columns=['route'],
        logic="source + '-' + destination"
    ))
    
    # 5. Select columns for analytics table
    analytics_columns = [
        'airline', 'source', 'source_name', 'destination', 'destination_name',
        'departure_datetime', 'arrival_datetime', 'duration_hrs', 'stopovers',
        'aircraft_type', 'class', 'booking_source', 'base_fare_bdt',
        'tax_surcharge_bdt', 'total_fare_bdt', 'seasonality', 'days_before_departure',
        'is_peak_season', 'route'
    ]
    
    df_analytics = df[analytics_columns].copy()
    output_row_count = len(df_analytics)
    
    # 6. Load to PostgreSQL
    with postgres_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE flight_data_transformed"))
    
    df_analytics.to_sql('flight_data_transformed', postgres_engine, 
                        if_exists='append', index=False)
    
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"Successfully loaded {output_row_count} transformed records to PostgreSQL in {duration:.2f}s")
    
   
    # Track Lineage
    lineage_tracker = get_lineage_tracker(dag_id, run_id)
    lineage_tracker.track_transform(
        task_id='transform_data',
        source_name='flight_staging',
        source_namespace='mysql.staging',
        target_name='flight_data_transformed',
        target_namespace='postgres.analytics',
        transformations=transformations,
        input_row_count=input_row_count,
        output_row_count=output_row_count
    )
    
    context['ti'].xcom_push(key='transformed_count', value=output_row_count)
    context['ti'].xcom_push(key='transformations_applied', value=len(transformations))
    context['ti'].xcom_push(key='lineage', value=lineage_tracker.to_json())
    
    return {
        'status': 'success', 
        'records_transformed': output_row_count,
        'transformations_applied': len(transformations),
        'duration_seconds': duration
    }