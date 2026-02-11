# Includes unit tests, integration tests, and edge case coverage.


import pytest
import pandas as pd
import json
from unittest.mock import patch, MagicMock
from datetime import datetime

# Import modules to test
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

from pipeline.data_quality import DataQualityValidator, create_flight_data_expectations
from pipeline.schema_evolution import SchemaEvolutionHandler, validate_schema_compatibility
from pipeline.lineage import LineageTracker, MetadataCollector, TransformationInfo
from pipeline.config import PipelineConfig


class TestDataQualityValidator:
    # Test the data quality validation framework
    
    def test_expect_column_to_exist_success(self):
        # Test column existence check passes
        df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        validator = DataQualityValidator(df)
        
        validator.expect_column_to_exist('col1')
        result = validator.validate()
        
        assert result.success
        assert result.successful_expectations == 1
    
    def test_expect_column_to_exist_failure(self):
        # Test column existence check fails for missing column
        df = pd.DataFrame({'col1': [1, 2]})
        validator = DataQualityValidator(df)
        
        validator.expect_column_to_exist('missing_col')
        result = validator.validate()
        
        assert not result.success
        assert result.failed_expectations == 1
    
    def test_expect_values_not_null(self):
        # Test null value detection
        df = pd.DataFrame({'col1': [1, None, 3]})
        validator = DataQualityValidator(df)
        
        validator.expect_column_values_to_not_be_null('col1', mostly=1.0)
        result = validator.validate()
        
        assert not result.success  # Has null values
    
    def test_expect_values_not_null_with_threshold(self):
        # Test null value check with threshold
        df = pd.DataFrame({'col1': [1, None, 3, 4, 5]})  # 80% non-null
        validator = DataQualityValidator(df)
        
        validator.expect_column_values_to_not_be_null('col1', mostly=0.7)
        result = validator.validate()
        
        assert result.success  # 80% > 70% threshold
    
    def test_expect_values_between(self):
        # Test value range validation
        df = pd.DataFrame({'fare': [100, 200, -50, 300]})
        validator = DataQualityValidator(df)
        
        validator.expect_column_values_to_be_between('fare', min_value=0, mostly=1.0)
        result = validator.validate()
        
        assert not result.success  # Has negative value
    
    def test_expect_values_in_set(self):
        # Test value set membership
        df = pd.DataFrame({'class': ['Economy', 'Business', 'Unknown']})
        validator = DataQualityValidator(df)
        
        validator.expect_column_values_to_be_in_set('class', ['Economy', 'Business', 'First'])
        result = validator.validate()
        
        assert not result.success  # 'Unknown' not in set
    
    def test_expect_row_count_between(self):
        # Test row count validation
        df = pd.DataFrame({'col': range(100)})
        validator = DataQualityValidator(df)
        
        validator.expect_table_row_count_to_be_between(min_value=50, max_value=200)
        result = validator.validate()
        
        assert result.success
    
    def test_create_flight_data_expectations(self):
        # Test preset flight data expectations
        df = pd.DataFrame({
            'airline': ['Biman', 'US-Bangla'],
            'source': ['DAC', 'CGP'],
            'destination': ['CGP', 'DAC'],
            'base_fare_bdt': [5000.0, 6000.0],
            'tax_surcharge_bdt': [500.0, 600.0],
            'total_fare_bdt': [5500.0, 6600.0]
        })
        
        validator = create_flight_data_expectations(df)
        result = validator.validate()
        
        assert result.evaluated_expectations > 0
        assert result.success


class TestSchemaEvolution:
    # Test schema evolution detection and handling
    
    def test_detect_new_columns(self):
        # Test detection of new columns
        df = pd.DataFrame({
            'airline': ['Biman'],
            'source': ['DAC'],
            'destination': ['CGP'],
            'base_fare_bdt': [5000.0],
            'tax_surcharge_bdt': [500.0],
            'total_fare_bdt': [5500.0],
            'new_column': ['value']  # New column
        })
        
        handler = SchemaEvolutionHandler()
        report = handler.detect_schema_changes(df)
        
        assert report.has_changes
        assert 'new_column' in report.new_columns
    
    def test_detect_removed_columns(self):
        # Test detection of removed required columns
        df = pd.DataFrame({
            'airline': ['Biman'],
            'source': ['DAC'],
            # Missing 'destination'
            'base_fare_bdt': [5000.0],
            'tax_surcharge_bdt': [500.0],
            'total_fare_bdt': [5500.0]
        })
        
        handler = SchemaEvolutionHandler()
        report = handler.detect_schema_changes(df)
        
        assert report.has_changes
        assert 'destination' in report.removed_columns
        assert not report.is_compatible  # Required column missing
    
    def test_no_changes(self):
        # Test when schema matches expected
        df = pd.DataFrame({
            'airline': ['Biman'],
            'source': ['DAC'],
            'source_name': ['Dhaka'],
            'destination': ['CGP'],
            'destination_name': ['Chittagong'],
            'departure_datetime': ['2024-01-01'],
            'arrival_datetime': ['2024-01-01'],
            'duration_hrs': [1.5],
            'stopovers': ['0'],
            'aircraft_type': ['Boeing'],
            'class': ['Economy'],
            'booking_source': ['Online'],
            'base_fare_bdt': [5000.0],
            'tax_surcharge_bdt': [500.0],
            'total_fare_bdt': [5500.0],
            'seasonality': ['Regular'],
            'days_before_departure': [7]
        })
        
        handler = SchemaEvolutionHandler()
        report = handler.detect_schema_changes(df)
        
        assert not report.has_changes
        assert report.is_compatible
    
    def test_adapt_dataframe(self):
        # Test dataframe adaptation for schema changes
        df = pd.DataFrame({
            'airline': ['Biman'],
            'source': ['DAC'],
            'base_fare_bdt': [5000.0],
            'tax_surcharge_bdt': [500.0],
            'total_fare_bdt': [5500.0]
        })
        
        handler = SchemaEvolutionHandler()
        report = handler.detect_schema_changes(df)
        
        adapted_df = handler.adapt_dataframe(df, report)
        
        # Should add missing required columns
        assert 'destination' in adapted_df.columns


class TestLineageTracking:
    # Test lineage and metadata tracking
    
    def test_track_read_operation(self):
        # Test tracking of read operations
        tracker = LineageTracker('test_dag', 'run_123')
        
        event = tracker.track_read(
            task_id='test_task',
            source_name='test_table',
            source_namespace='mysql.staging',
            row_count=1000
        )
        
        assert event.event_type.value == 'read'
        assert len(tracker.events) == 1
    
    def test_track_write_operation(self):
        # Test tracking of write operations
        tracker = LineageTracker('test_dag', 'run_123')
        
        event = tracker.track_write(
            task_id='test_task',
            target_name='test_table',
            target_namespace='postgres.analytics',
            row_count=500
        )
        
        assert event.event_type.value == 'write'
        assert event.target_datasets[0].row_count == 500
    
    def test_track_transform_operation(self):
        # Test tracking of transformations
        tracker = LineageTracker('test_dag', 'run_123')
        
        transformations = [
            TransformationInfo(
                name='parse_datetime',
                description='Parse datetime',
                input_columns=['date_str'],
                output_columns=['date_ts']
            )
        ]
        
        event = tracker.track_transform(
            task_id='test_task',
            source_name='source_table',
            source_namespace='mysql.staging',
            target_name='target_table',
            target_namespace='postgres.analytics',
            transformations=transformations,
            input_row_count=1000,
            output_row_count=995
        )
        
        assert event.event_type.value == 'transform'
        assert len(event.transformations) == 1
    
    def test_lineage_summary(self):
        # Test lineage summary generation
        tracker = LineageTracker('test_dag', 'run_123')
        
        tracker.track_read('t1', 'source', 'mysql.staging')
        tracker.track_write('t2', 'target', 'postgres.analytics')
        
        summary = tracker.get_lineage_summary()
        
        assert summary['total_events'] == 2
        assert 'mysql.staging.source' in summary['datasets_read']
        assert 'postgres.analytics.target' in summary['datasets_written']
    
    def test_metadata_collector(self):
        # Test metadata collection
        collector = MetadataCollector()
        
        collector.set_pipeline_metadata('test_dag', 'run_123')
        collector.add_task_metrics('task1', {'rows': 1000})
        collector.add_data_quality_metrics(
            'task1',
            total_records=1000,
            valid_records=990,
            null_counts={'col1': 5}
        )
        
        metadata = collector.get_all_metadata()
        
        assert metadata['pipeline']['dag_id'] == 'test_dag'
        assert 'task1' in metadata['tasks']
        assert 'task1_quality' in metadata['tasks']


class TestConfiguration:
    # Test configuration management
    
    def test_default_values(self):
        # Test that default values are set
        config = PipelineConfig()
        
        assert len(config.peak_seasons) > 0
        assert config.column_mapping is not None
    
    def test_column_mapping(self):
        # Test column mapping retrieval
        config = PipelineConfig()
        mapping = config.column_mapping
        
        assert 'Airline' in mapping
        assert mapping['Airline'] == 'airline'
        assert 'Base Fare (BDT)' in mapping
    
    def test_required_columns(self):
        # Test required columns list
        config = PipelineConfig()
        required = config.required_columns
        
        assert 'Airline' in required
        assert 'Source' in required
        assert 'Total Fare (BDT)' in required


class TestValidateData:
    # Test data validation logic

    @patch('pipeline.validation.get_mysql_connection')
    def test_validate_data_success(self, mock_conn):
        # Test successful validation with clean data
        from pipeline.validation import validate_data
        
        mock_engine = MagicMock()
        mock_conn.return_value = mock_engine

        # Sample clean data
        sample_data = {
            'airline': ['Biman', 'US-Bangla'],
            'source': ['DAC', 'CGP'],
            'destination': ['CGP', 'DAC'],
            'base_fare_bdt': [5000.0, 6000.0],
            'tax_surcharge_bdt': [500.0, 600.0],
            'total_fare_bdt': [5500.0, 6600.0],
            'is_validated': [False, False],
            'validation_errors': [None, None]
        }
        df = pd.DataFrame(sample_data)

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = False
        
        with patch('pandas.read_sql', return_value=df):
            with patch.object(df, 'to_sql'):
                result = validate_data(**{
                    'ti': mock_ti,
                    'run_id': 'test_run',
                    'dag': MagicMock(dag_id='test_dag')
                })

                assert result['total_records'] == 2

    @patch('pipeline.validation.get_mysql_connection')
    def test_validate_data_with_negatives(self, mock_conn):
        # Test validation corrects negative fares
        from pipeline.validation import validate_data
        
        mock_engine = MagicMock()
        mock_conn.return_value = mock_engine

        # Data with negative fares
        sample_data = {
            'airline': ['Biman'],
            'source': ['DAC'],
            'destination': ['CGP'],
            'base_fare_bdt': [-5000.0],  # Negative
            'tax_surcharge_bdt': [500.0],
            'total_fare_bdt': [5500.0],
            'is_validated': [False],
            'validation_errors': [None]
        }
        df = pd.DataFrame(sample_data)
        
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = False

        with patch('pandas.read_sql', return_value=df):
            with patch.object(df, 'to_sql'):
                result = validate_data(**{
                    'ti': mock_ti,
                    'run_id': 'test_run',
                    'dag': MagicMock(dag_id='test_dag')
                })

                assert result['negative_fares_count'] == 1


class TestKPIComputation:
    # Test KPI computation logic
    
    @patch('pipeline.kpis.get_postgres_connection')
    def test_compute_kpis_sql_pushdown(self, mock_conn):
        # Test KPI computation uses SQL pushdown
        from pipeline.kpis import compute_kpis
        
        mock_engine = MagicMock()
        mock_conn.return_value = mock_engine
        
        # Mock the connection context manager
        mock_connection = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_connection)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        
        # Mock execute to return count
        mock_result = MagicMock()
        mock_result.scalar.return_value = 5
        mock_connection.execute.return_value = mock_result
        
        mock_ti = MagicMock()
        # Mock xcom_pull to return None (data was changed, so KPIs should compute)
        mock_ti.xcom_pull.return_value = None
        
        result = compute_kpis(**{
            'ti': mock_ti,
            'run_id': 'test_run',
            'dag': MagicMock(dag_id='test_dag')
        })
        
        # Verify SQL was executed
        assert mock_connection.execute.called
        assert result['method'] == 'sql_pushdown'


class TestTransformation:
    # Test data transformation logic
    
    def test_peak_season_flag(self):
        # Test peak season flagging logic
        from pipeline.constants import PEAK_SEASONS
        
        seasons = ['Eid', 'Regular', 'Winter', 'Hajj', 'Summer']
        expected = [True, False, True, True, False]
        
        result = [s in PEAK_SEASONS for s in seasons]
        
        assert result == expected
    
    def test_route_creation(self):
        # Test route column creation
        df = pd.DataFrame({
            'source': ['DAC', 'CGP'],
            'destination': ['CGP', 'DAC']
        })
        
        df['route'] = df['source'] + '-' + df['destination']
        
        assert df['route'].tolist() == ['DAC-CGP', 'CGP-DAC']


class TestEdgeCases:
    # Test edge cases and error handling
    
    def test_empty_dataframe_validation(self):
        # Test validation of empty dataframe
        df = pd.DataFrame()
        validator = DataQualityValidator(df)
        
        validator.expect_table_row_count_to_be_between(min_value=1)
        result = validator.validate()
        
        assert not result.success
    
    def test_all_null_column(self):
        # Test column with all null values
        df = pd.DataFrame({'col': [None, None, None]})
        validator = DataQualityValidator(df)
        
        validator.expect_column_values_to_not_be_null('col', mostly=0.5)
        result = validator.validate()
        
        assert not result.success
    
    def test_single_row_dataframe(self):
        # Test validation with single row
        df = pd.DataFrame({
            'airline': ['Biman'],
            'fare': [5000.0]
        })
        validator = DataQualityValidator(df)
        
        validator.expect_column_to_exist('airline')
        validator.expect_column_values_to_be_between('fare', min_value=0)
        result = validator.validate()
        
        assert result.success


if __name__ == '__main__':
    pytest.main([__file__, '-v'])