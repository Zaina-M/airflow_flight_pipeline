# KPIs module for flight price pipeline.
import json
import logging
from datetime import datetime
from sqlalchemy import text
from airflow.exceptions import AirflowSkipException
from .constants import get_postgres_connection, PEAK_SEASONS
from .lineage import get_lineage_tracker
import pandas as pd

logger = logging.getLogger(__name__)


def compute_kpis(**context):
    
    # Task 4: Compute KPI metrics using SQL pushdown for better performance.
    
    
    logger.info("Starting KPI computation with SQL pushdown...")
    start_time = datetime.now()
    run_id = context.get('run_id', 'unknown')
    dag_id = context.get('dag').dag_id if context.get('dag') else 'flight_price_pipeline'
    
    # Check if transformation was skipped - if so, skip KPI recomputation (data unchanged)
    transformation_skipped = context['ti'].xcom_pull(key='transformation_skipped', task_ids='transformation.transform_data')
    ingestion_skipped = context['ti'].xcom_pull(key='ingestion_skipped', task_ids='ingestion.ingest_csv_to_mysql')
    
    if transformation_skipped or ingestion_skipped:
        logger.info("Data unchanged, skipping KPI recomputation to save compute. Using cached KPIs.")
        context['ti'].xcom_push(key='kpi_skipped', value=True)
        raise AirflowSkipException("Data unchanged, using cached KPIs")
    
    postgres_engine = get_postgres_connection()

    try:
       
        # KPI 1: Average Fare by Airline 
    
        kpi1_query = """
            INSERT INTO kpi_avg_fare_by_airline (airline, avg_base_fare, avg_tax_surcharge, avg_total_fare, booking_count)
            SELECT 
                airline,
                ROUND(AVG(base_fare_bdt)::numeric, 2) as avg_base_fare,
                ROUND(AVG(tax_surcharge_bdt)::numeric, 2) as avg_tax_surcharge,
                ROUND(AVG(total_fare_bdt)::numeric, 2) as avg_total_fare,
                COUNT(*) as booking_count
            FROM flight_data_transformed
            GROUP BY airline
            ORDER BY avg_total_fare DESC
        """
        
        with postgres_engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE kpi_avg_fare_by_airline"))
            conn.execute(text(kpi1_query))
            result = conn.execute(text("SELECT COUNT(*) FROM kpi_avg_fare_by_airline"))
            kpi1_count = result.scalar()
        
        logger.info(f"KPI 1: Computed average fares for {kpi1_count} airlines (SQL pushdown)")
    
    except Exception as e:
        logger.error(f"KPI 1 failed: {e}")
        kpi1_count = 0
    
    
    # KPI 2: Seasonal Fare Variation 
    try:
        # Build peak seasons condition
        peak_seasons_str = ", ".join([f"'{s}'" for s in PEAK_SEASONS])
        
        kpi2_query = f"""
            INSERT INTO kpi_seasonal_fare_variation (season_type, season_name, avg_total_fare, min_fare, max_fare, booking_count)
            SELECT 
                CASE WHEN seasonality IN ({peak_seasons_str}) THEN 'peak' ELSE 'non_peak' END as season_type,
                COALESCE(seasonality, 'Unknown') as season_name,
                ROUND(AVG(total_fare_bdt)::numeric, 2) as avg_total_fare,
                ROUND(MIN(total_fare_bdt)::numeric, 2) as min_fare,
                ROUND(MAX(total_fare_bdt)::numeric, 2) as max_fare,
                COUNT(*) as booking_count
            FROM flight_data_transformed
            GROUP BY 
                CASE WHEN seasonality IN ({peak_seasons_str}) THEN 'peak' ELSE 'non_peak' END,
                seasonality
            ORDER BY season_type, avg_total_fare DESC
        """
        
        with postgres_engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE kpi_seasonal_fare_variation"))
            conn.execute(text(kpi2_query))
            result = conn.execute(text("SELECT COUNT(*) FROM kpi_seasonal_fare_variation"))
            kpi2_count = result.scalar()
        
        logger.info(f"KPI 2: Computed seasonal variation for {kpi2_count} seasons (SQL pushdown)")
    
    except Exception as e:
        logger.error(f"KPI 2 failed: {e}")
        kpi2_count = 0
    
   
    # KPI 3: Booking Count by Airline 
    
    try:
        kpi3_query = """
            INSERT INTO kpi_booking_count_by_airline (airline, total_bookings, economy_bookings, business_bookings, first_class_bookings)
            SELECT 
                airline,
                COUNT(*) as total_bookings,
                COUNT(*) FILTER (WHERE LOWER(class) LIKE '%economy%') as economy_bookings,
                COUNT(*) FILTER (WHERE LOWER(class) LIKE '%business%') as business_bookings,
                COUNT(*) FILTER (WHERE LOWER(class) LIKE '%first%') as first_class_bookings
            FROM flight_data_transformed
            GROUP BY airline
            ORDER BY total_bookings DESC
        """
        
        with postgres_engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE kpi_booking_count_by_airline"))
            conn.execute(text(kpi3_query))
            result = conn.execute(text("SELECT COUNT(*) FROM kpi_booking_count_by_airline"))
            kpi3_count = result.scalar()
        
        logger.info(f"KPI 3: Computed booking counts for {kpi3_count} airlines (SQL pushdown)")
    
    except Exception as e:
        logger.error(f"KPI 3 failed: {e}")
        kpi3_count = 0
    
 
    # KPI 4: Most Popular Routes 
   
    try:
        kpi4_query = """
            INSERT INTO kpi_popular_routes (source, destination, booking_count, route, source_name, destination_name, avg_fare, rank)
            SELECT 
                source,
                destination,
                COUNT(*) as booking_count,
                route,
                MAX(source_name) as source_name,
                MAX(destination_name) as destination_name,
                ROUND(AVG(total_fare_bdt)::numeric, 2) as avg_fare,
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rank
            FROM flight_data_transformed
            GROUP BY source, destination, route
            ORDER BY booking_count DESC
            LIMIT 20
        """
        
        with postgres_engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE kpi_popular_routes"))
            conn.execute(text(kpi4_query))
            result = conn.execute(text("SELECT COUNT(*) FROM kpi_popular_routes"))
            kpi4_count = result.scalar()
        
        logger.info(f"KPI 4: Identified top {kpi4_count} popular routes (SQL pushdown)")
    
    except Exception as e:
        logger.error(f"KPI 4 failed: {e}")
        kpi4_count = 0
    
    
    # Track Lineage
   
    duration = (datetime.now() - start_time).total_seconds()
    
    lineage_tracker = get_lineage_tracker(dag_id, run_id)
    lineage_tracker.track_aggregation(   
        task_id='compute_kpis',
        source_name='flight_data_transformed',
        source_namespace='postgres.analytics',
        kpi_tables=[
            'kpi_avg_fare_by_airline',
            'kpi_seasonal_fare_variation',
            'kpi_booking_count_by_airline',
            'kpi_popular_routes'
        ],
        target_namespace='postgres.analytics',
        metrics={
            'airlines_analyzed': kpi1_count,
            'seasons_analyzed': kpi2_count,
            'routes_identified': kpi4_count
        }
    )
    
    # Store KPI summary in XCom
    kpi_summary = {
        'airlines_analyzed': kpi1_count,
        'seasons_analyzed': kpi2_count,
        'booking_airlines': kpi3_count,
        'routes_identified': kpi4_count,
        'duration_seconds': duration,
        'method': 'sql_pushdown'
    }
    
    context['ti'].xcom_push(key='kpi_summary', value=json.dumps(kpi_summary))
    context['ti'].xcom_push(key='lineage', value=lineage_tracker.to_json())
    
    logger.info(f"KPI computation completed in {duration:.2f}s using SQL pushdown")
    
    return kpi_summary