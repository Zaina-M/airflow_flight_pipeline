# Provides centralized configuration using Airflow Variables with fallback defaults.

import os
import json
import logging
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    # Database connection configuration.
    host: str
    port: int
    database: str
    user: str
    password: str
    
    def get_connection_string(self, dialect: str = "postgresql") -> str:
        # Generate SQLAlchemy connection string.
        return f"{dialect}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class PipelineConfig:
    
    
    # Default values (fallbacks)
    DEFAULTS = {
        # Pipeline settings
        'csv_file_path': '/opt/airflow/data/raw/Flight_Price_Dataset_of_Bangladesh.csv',
        'chunk_size': 10000,
        'max_retries': 3,
        'retry_delay_minutes': 5,
        
        # Peak seasons for KPI calculations
        'peak_seasons': ['Eid', 'Hajj', 'Winter'],
        
        # Validation thresholds
        'null_threshold': 0.05,  # Allow 5% nulls
        'negative_fare_threshold': 0.01,  # Allow 1% negative fares
        
        # Performance settings
        'batch_size': 5000,
        'enable_parallel_kpis': True,
        
        # Feature flags
        'enable_schema_evolution': True,
        'enable_lineage_tracking': True,
        'enable_data_quality_checks': True,
        
        # Database table names
        'staging_table': 'flight_staging',
        'transformed_table': 'flight_data_transformed',
        'kpi_tables': {
            'avg_fare': 'kpi_avg_fare_by_airline',
            'seasonal': 'kpi_seasonal_fare_variation',
            'bookings': 'kpi_booking_count_by_airline',
            'routes': 'kpi_popular_routes'
        }
    }
    
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
    REQUIRED_COLUMNS = [
        'Airline', 'Source', 'Destination', 
        'Base Fare (BDT)', 'Tax & Surcharge (BDT)', 'Total Fare (BDT)'
    ]
    
    _instance = None
    _config_cache: Dict[str, Any] = {}
    
    def __new__(cls):
        # Singleton pattern for configuration.
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance
    
    def _load_config(self) -> None:
        # Load configuration from all sources.
        self._config_cache = self.DEFAULTS.copy()
        
        # Try to load from Airflow Variables
        try:
            from airflow.models import Variable
            self._load_from_airflow_variables(Variable)
        except ImportError:
            logger.warning("Airflow not available, using environment variables")
            self._load_from_environment()
        except Exception as e:
            logger.warning(f"Could not load Airflow variables: {e}")
            self._load_from_environment()
    
    def _load_from_airflow_variables(self, Variable) -> None:
        # Load configuration from Airflow Variables.
        airflow_configs = {
            'csv_file_path': 'flight_pipeline_csv_path',
            'chunk_size': 'flight_pipeline_chunk_size',
            'peak_seasons': 'flight_pipeline_peak_seasons',
            'null_threshold': 'flight_pipeline_null_threshold',
            'enable_schema_evolution': 'flight_pipeline_schema_evolution',
            'enable_lineage_tracking': 'flight_pipeline_lineage_tracking',
            'enable_data_quality_checks': 'flight_pipeline_data_quality'
        }
        
        for config_key, var_name in airflow_configs.items():
            try:
                value = Variable.get(var_name, default_var=None)
                if value is not None:
                    # Handle JSON values
                    if config_key in ('peak_seasons', 'kpi_tables'):
                        value = json.loads(value)
                    elif config_key in ('chunk_size', 'batch_size', 'max_retries'):
                        value = int(value)
                    elif config_key in ('null_threshold', 'negative_fare_threshold'):
                        value = float(value)
                    elif config_key.startswith('enable_'):
                        value = value.lower() in ('true', '1', 'yes')
                    
                    self._config_cache[config_key] = value
                    logger.debug(f"Loaded config '{config_key}' from Airflow Variable")
            except Exception as e:
                logger.debug(f"Could not load Airflow Variable '{var_name}': {e}")
    
    def _load_from_environment(self) -> None:
        # Load configuration from environment variables.
        env_configs = {
            'csv_file_path': 'FLIGHT_PIPELINE_CSV_PATH',
            'chunk_size': 'FLIGHT_PIPELINE_CHUNK_SIZE',
            'peak_seasons': 'FLIGHT_PIPELINE_PEAK_SEASONS',
            'enable_schema_evolution': 'FLIGHT_PIPELINE_SCHEMA_EVOLUTION',
            'enable_lineage_tracking': 'FLIGHT_PIPELINE_LINEAGE_TRACKING',
        }
        
        for config_key, env_var in env_configs.items():
            value = os.environ.get(env_var)
            if value is not None:
                # Handle type conversions
                if config_key in ('peak_seasons',):
                    try:
                        value = json.loads(value)
                    except json.JSONDecodeError:
                        value = value.split(',')
                elif config_key in ('chunk_size', 'batch_size'):
                    value = int(value)
                elif config_key.startswith('enable_'):
                    value = value.lower() in ('true', '1', 'yes')
                
                self._config_cache[config_key] = value
                logger.debug(f"Loaded config '{config_key}' from environment")
    
    def get(self, key: str, default: Any = None) -> Any:
        # Get a configuration value.
        return self._config_cache.get(key, default)
    
    def get_int(self, key: str, default: int = 0) -> int:
        # Get an integer configuration value.
        value = self.get(key, default)
        return int(value) if value is not None else default
    
    def get_float(self, key: str, default: float = 0.0) -> float:
        # Get a float configuration value.
        value = self.get(key, default)
        return float(value) if value is not None else default
    
    def get_bool(self, key: str, default: bool = False) -> bool:
        # Get a boolean configuration value.
        value = self.get(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes')
        return bool(value)
    
    def get_list(self, key: str, default: Optional[List] = None) -> List:
        # Get a list configuration value.
        value = self.get(key, default or [])
        if isinstance(value, str):
            return value.split(',')
        return list(value) if value else []
    
    @property
    def csv_file_path(self) -> str:
        return self.get('csv_file_path')
    
    @property
    def chunk_size(self) -> int:
        return self.get_int('chunk_size', 10000)
    
    @property
    def peak_seasons(self) -> List[str]:
        return self.get_list('peak_seasons', ['Eid', 'Hajj', 'Winter'])
    
    @property
    def column_mapping(self) -> Dict[str, str]:
        return self.COLUMN_MAPPING.copy()
    
    @property
    def required_columns(self) -> List[str]:
        return self.REQUIRED_COLUMNS.copy()
    
    @property
    def staging_table(self) -> str:
        return self.get('staging_table', 'flight_staging')
    
    @property
    def transformed_table(self) -> str:
        return self.get('transformed_table', 'flight_data_transformed')
    
    @property
    def kpi_tables(self) -> Dict[str, str]:
        return self.get('kpi_tables', self.DEFAULTS['kpi_tables'])
    
    @property
    def enable_schema_evolution(self) -> bool:
        return self.get_bool('enable_schema_evolution', True)
    
    @property
    def enable_lineage_tracking(self) -> bool:
        return self.get_bool('enable_lineage_tracking', True)
    
    @property
    def enable_data_quality_checks(self) -> bool:
        return self.get_bool('enable_data_quality_checks', True)
    
    def reload(self) -> None:
        # Force reload of configuration.
        self._config_cache = {}
        self._load_config()
        logger.info("Configuration reloaded")
    
    def to_dict(self) -> Dict[str, Any]:
        # Export all configuration as dictionary.
        return {
            **self._config_cache,
            'column_mapping': self.COLUMN_MAPPING,
            'required_columns': self.REQUIRED_COLUMNS
        }


# Convenience function to get config instance
def get_config() -> PipelineConfig:
    # Get the pipeline configuration instance.
    return PipelineConfig()


# Export commonly used values for backward compatibility
def get_csv_file_path() -> str:
    return get_config().csv_file_path

def get_peak_seasons() -> List[str]:
    return get_config().peak_seasons

def get_column_mapping() -> Dict[str, str]:
    return get_config().column_mapping

def get_required_columns() -> List[str]:
    return get_config().required_columns
