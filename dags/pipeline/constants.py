"""
Shared constants and utilities for the flight price pipeline.
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

# CSV file path
CSV_FILE_PATH = '/opt/airflow/data/raw/Flight_Price_Dataset_of_Bangladesh.csv'

# Column mapping from CSV to database
COLUMN_MAPPING = {
    'Airline': 'airline',
    'Source': 'source',
    'Source Name': 'source_name',
    'Destination': 'destination',
    'Destination Name': 'destination_name',
    'Departure Date & Time': 'departure_datetime',
    'Arrival Date & Time': 'arrival_datetime',
    'Duration (hrs)': 'duration_hrs',
    'Stopovers': 'stopovers',
    'Aircraft Type': 'aircraft_type',
    'Class': 'class',
    'Booking Source': 'booking_source',
    'Base Fare (BDT)': 'base_fare_bdt',
    'Tax & Surcharge (BDT)': 'tax_surcharge_bdt',
    'Total Fare (BDT)': 'total_fare_bdt',
    'Seasonality': 'seasonality',
    'Days Before Departure': 'days_before_departure'
}

# Required columns for validation
REQUIRED_COLUMNS = ['Airline', 'Source', 'Destination', 'Base Fare (BDT)', 
                    'Tax & Surcharge (BDT)', 'Total Fare (BDT)']

# Peak seasons definition
PEAK_SEASONS = ['Eid', 'Hajj', 'Winter']


def get_postgres_connection():
    """Get PostgreSQL connection using Airflow hook."""
    hook = PostgresHook(postgres_conn_id='postgres_analytics')
    return hook.get_sqlalchemy_engine()


def get_mysql_connection():
    """Get MySQL connection using Airflow hook."""
    hook = MySqlHook(mysql_conn_id='mysql_staging')
    return hook.get_sqlalchemy_engine()