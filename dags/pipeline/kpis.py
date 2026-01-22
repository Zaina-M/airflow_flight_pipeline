"""
KPIs module for flight price pipeline.
"""

import json
import logging
from sqlalchemy import text
from .constants import get_postgres_connection, PEAK_SEASONS
import pandas as pd

logger = logging.getLogger(__name__)


def compute_kpis(**context):
    """
    Task 4: Compute KPI metrics using SQL for better performance.
    
    KPIs:
    1. Average Fare by Airline
    2. Seasonal Fare Variation
    3. Booking Count by Airline
    4. Most Popular Routes
    """
    logger.info("Starting KPI computation...")
    
    postgres_engine = get_postgres_connection()
    
    # Read transformed data
    df = pd.read_sql("SELECT * FROM flight_data_transformed", postgres_engine)
    
    try:
        # ========================================
        # KPI 1: Average Fare by Airline
        # ========================================
        kpi_avg_fare = df.groupby('airline').agg({
            'base_fare_bdt': 'mean',
            'tax_surcharge_bdt': 'mean',
            'total_fare_bdt': 'mean',
            'airline': 'count'
        }).rename(columns={
            'base_fare_bdt': 'avg_base_fare',
            'tax_surcharge_bdt': 'avg_tax_surcharge',
            'total_fare_bdt': 'avg_total_fare',
            'airline': 'booking_count'
        }).reset_index()

        with postgres_engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE kpi_avg_fare_by_airline"))

        kpi_avg_fare.to_sql('kpi_avg_fare_by_airline', postgres_engine, 
                            if_exists='append', index=False)
        
        logger.info(f"KPI 1: Computed average fares for {len(kpi_avg_fare)} airlines")
    
    except Exception as e:
        logger.error(f"KPI 1 failed: {e}")
    
    # ========================================
    # KPI 2: Seasonal Fare Variation
    # ========================================
    # Peak seasons
    peak_df = df[df['is_peak_season'] == True]
    non_peak_df = df[df['is_peak_season'] == False]
    
    seasonal_data = []
    
    # Peak season aggregate
    if len(peak_df) > 0:
        for season in PEAK_SEASONS:
            season_df = df[df['seasonality'] == season]
            if len(season_df) > 0:
                seasonal_data.append({
                    'season_type': 'peak',
                    'season_name': season,
                    'avg_total_fare': season_df['total_fare_bdt'].mean(),
                    'min_fare': season_df['total_fare_bdt'].min(),
                    'max_fare': season_df['total_fare_bdt'].max(),
                    'booking_count': len(season_df)
                })
    
    # Non-peak (Regular) season
    if len(non_peak_df) > 0:
        seasonal_data.append({
            'season_type': 'non_peak',
            'season_name': 'Regular',
            'avg_total_fare': non_peak_df['total_fare_bdt'].mean(),
            'min_fare': non_peak_df['total_fare_bdt'].min(),
            'max_fare': non_peak_df['total_fare_bdt'].max(),
            'booking_count': len(non_peak_df)
        })
    
    kpi_seasonal = pd.DataFrame(seasonal_data)
    
    with postgres_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE kpi_seasonal_fare_variation"))
    
    if len(kpi_seasonal) > 0:
        kpi_seasonal.to_sql('kpi_seasonal_fare_variation', postgres_engine, 
                            if_exists='append', index=False)
    
    logger.info(f"KPI 2: Computed seasonal variation for {len(kpi_seasonal)} season types")
    
    # ========================================
    # KPI 3: Booking Count by Airline
    # ========================================
    kpi_bookings = df.groupby('airline').agg({
        'airline': 'count'
    }).rename(columns={'airline': 'total_bookings'}).reset_index()
    
    # Add class breakdown
    class_counts = df.groupby(['airline', 'class']).size().unstack(fill_value=0)
    class_counts.columns = [f"{col.lower().replace(' ', '_')}_bookings" for col in class_counts.columns]
    
    kpi_bookings = kpi_bookings.merge(
        class_counts.reset_index(), 
        on='airline', 
        how='left'
    )
    
    # Ensure required columns exist
    for col in ['economy_bookings', 'business_bookings', 'first_class_bookings']:
        if col not in kpi_bookings.columns:
            kpi_bookings[col] = 0
    
    kpi_bookings = kpi_bookings[['airline', 'total_bookings', 'economy_bookings', 
                                  'business_bookings', 'first_class_bookings']]
    
    with postgres_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE kpi_booking_count_by_airline"))
    
    kpi_bookings.to_sql('kpi_booking_count_by_airline', postgres_engine, 
                        if_exists='append', index=False)
    
    logger.info(f"KPI 3: Computed booking counts for {len(kpi_bookings)} airlines")
    
    # ========================================
    # KPI 4: Most Popular Routes
    # ========================================
    kpi_routes = df.groupby(['source', 'destination']).agg({
        'route': ['count', 'first'],
        'source_name': 'first',
        'destination_name': 'first',
        'total_fare_bdt': 'mean'
    }).reset_index()
    
    kpi_routes.columns = ['source', 'destination', 'booking_count', 'route', 
                          'source_name', 'destination_name', 'avg_fare']
    
    kpi_routes = kpi_routes.sort_values('booking_count', ascending=False).head(20)
    kpi_routes['rank'] = range(1, len(kpi_routes) + 1)
    
    with postgres_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE kpi_popular_routes"))
    
    kpi_routes.to_sql('kpi_popular_routes', postgres_engine, 
                      if_exists='append', index=False)
    
    logger.info(f"KPI 4: Identified top {len(kpi_routes)} popular routes")
    
    # Store KPI summary in XCom
    kpi_summary = {
        'airlines_analyzed': len(kpi_avg_fare),
        'seasons_analyzed': len(kpi_seasonal),
        'routes_identified': len(kpi_routes)
    }
    
    context['ti'].xcom_push(key='kpi_summary', value=json.dumps(kpi_summary))
    
    return kpi_summary