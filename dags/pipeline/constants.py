# Provides database connections with pooling and configuration management.


import os
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

# Import configuration (with fallback for non-Airflow environments)
try:
    from .config import get_config, get_csv_file_path, get_peak_seasons, get_column_mapping, get_required_columns
    _config = get_config()
except ImportError:
    _config = None



# CSV file path - from config or environment
CSV_FILE_PATH = os.environ.get(
    'FLIGHT_PIPELINE_CSV_PATH',
    '/opt/airflow/data/raw/Flight_Price_Dataset_of_Bangladesh.csv'
)

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

# Peak seasons definition - from config or default
PEAK_SEASONS = os.environ.get('FLIGHT_PIPELINE_PEAK_SEASONS', 'Eid,Hajj,Winter').split(',')


# Database Configuration (from environment)

# PostgreSQL configuration
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = int(os.environ.get('POSTGRES_PORT', 5432))
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'analytics')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'airflow')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', '')

# MySQL configuration  
MYSQL_HOST = os.environ.get('MYSQL_HOST', 'mysql')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', 3306))
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'staging')
MYSQL_USER = os.environ.get('MYSQL_USER', 'airflow')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', '')

# Connection pool settings
POOL_SIZE = int(os.environ.get('DB_POOL_SIZE', 5))
MAX_OVERFLOW = int(os.environ.get('DB_MAX_OVERFLOW', 10))
POOL_TIMEOUT = int(os.environ.get('DB_POOL_TIMEOUT', 30))
POOL_RECYCLE = int(os.environ.get('DB_POOL_RECYCLE', 1800))  # 30 minutes


# Engine cache for connection reuse
_postgres_engine = None
_mysql_engine = None


def get_postgres_connection():
    
# Uses connection pooling for better performance.
    
    global _postgres_engine
    
    # Try Airflow hook first
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id='postgres_analytics')
        return hook.get_sqlalchemy_engine()
    except (ImportError, Exception):
        pass
    
    # Fallback to direct connection with pooling
    if _postgres_engine is None:
        connection_string = (
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
            f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
        _postgres_engine = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=POOL_SIZE,
            max_overflow=MAX_OVERFLOW,
            pool_timeout=POOL_TIMEOUT,
            pool_recycle=POOL_RECYCLE,
            pool_pre_ping=True  # Enable connection health checks
        )
    return _postgres_engine


def get_mysql_connection():
   
    global _mysql_engine
    
    # Try Airflow hook first
    try:
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        hook = MySqlHook(mysql_conn_id='mysql_staging')
        return hook.get_sqlalchemy_engine()
    except (ImportError, Exception):
        pass
    
    # Fallback to direct connection with pooling
    if _mysql_engine is None:
        connection_string = (
            f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
            f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
        )
        _mysql_engine = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=POOL_SIZE,
            max_overflow=MAX_OVERFLOW,
            pool_timeout=POOL_TIMEOUT,
            pool_recycle=POOL_RECYCLE,
            pool_pre_ping=True
        )
    return _mysql_engine


def reset_connections():
    # Reset cached database connections (useful for testing).
    global _postgres_engine, _mysql_engine
    if _postgres_engine:
        _postgres_engine.dispose()
        _postgres_engine = None
    if _mysql_engine:
        _mysql_engine.dispose()
        _mysql_engine = None