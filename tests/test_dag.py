"""
Unit tests for Flight Price Pipeline DAG
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from dags.flight_price_pipeline import validate_data, get_postgres_connection


class TestValidateData:
    """Test data validation logic"""

    @patch('dags.flight_price_pipeline.get_postgres_connection')
    def test_validate_data_success(self, mock_conn):
        """Test successful validation with clean data"""
        # Mock database connection and data
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

        # Mock read_sql to return our test data
        with patch('pandas.read_sql', return_value=df) as mock_read:
            with patch('sqlalchemy.engine.Engine.connect') as mock_connect:
                result = validate_data(**{'ti': MagicMock()})

                # Assertions
                assert 'total_records' in result
                assert result['total_records'] == 2
                assert result['valid_records'] == 2  # Should be valid
                assert result['invalid_records'] == 0

    @patch('dags.flight_price_pipeline.get_postgres_connection')
    def test_validate_data_with_negatives(self, mock_conn):
        """Test validation corrects negative fares"""
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

        with patch('pandas.read_sql', return_value=df) as mock_read:
            with patch('sqlalchemy.engine.Engine.connect') as mock_connect:
                result = validate_data(**{'ti': MagicMock()})

                # Should detect negative fares
                assert result['negative_fares_count'] == 1
                assert result['valid_records'] == 1  # Still valid after correction

    @patch('dags.flight_price_pipeline.get_postgres_connection')
    def test_validate_data_with_missing_values(self, mock_conn):
        """Test validation handles missing values"""
        mock_engine = MagicMock()
        mock_conn.return_value = mock_engine

        # Data with missing values
        sample_data = {
            'airline': [None, 'US-Bangla'],  # Missing airline
            'source': ['DAC', 'CGP'],
            'destination': ['CGP', 'DAC'],
            'base_fare_bdt': [5000.0, 6000.0],
            'tax_surcharge_bdt': [500.0, 600.0],
            'total_fare_bdt': [5500.0, 6600.0],
            'is_validated': [False, False],
            'validation_errors': [None, None]
        }
        df = pd.DataFrame(sample_data)

        with patch('pandas.read_sql', return_value=df) as mock_read:
            with patch('sqlalchemy.engine.Engine.connect') as mock_connect:
                result = validate_data(**{'ti': MagicMock()})

                # Should detect missing values
                assert result['missing_values_count'] > 0
                assert result['valid_records'] == 2  # Still valid after filling


class TestDatabaseConnection:
    """Test database connection functions"""

    @patch('dags.flight_price_pipeline.PostgresHook')
    def test_get_postgres_connection(self, mock_hook):
        """Test PostgreSQL connection creation"""
        mock_engine = MagicMock()
        mock_hook.return_value.get_sqlalchemy_engine.return_value = mock_engine

        engine = get_postgres_connection()

        assert engine == mock_engine
        mock_hook.assert_called_once_with(postgres_conn_id='postgres_analytics')


if __name__ == '__main__':
    pytest.main([__file__])