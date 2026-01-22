"""
Flight Price Data Pipeline DAG
==============================
End-to-end data pipeline for processing Bangladesh flight price data.

Pipeline Stages:
1. Ingest CSV data into MySQL staging
2. Validate data quality
3. Transform and clean data
4. Compute KPIs
5. Load to PostgreSQL analytics
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# Import functions from modules
from pipeline.ingestion import ingest_csv_to_mysql
from pipeline.validation import validate_data
from pipeline.transformation import transform_data
from pipeline.kpis import compute_kpis
from pipeline.reports import save_validation_report

# Configure logging
logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'email': 'your-email@example.com',  # Replace with actual email
    'retries': 3,  # Increased retries
    'retry_delay': timedelta(minutes=5),
}

# ========================================
# DAG Definition
# ========================================
with DAG(
    dag_id='flight_price_pipeline',
    default_args=default_args,
    description='End-to-end data pipeline for Bangladesh flight price analysis',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['flight', 'price', 'bangladesh', 'etl'],
) as dag:
    
    # Task 1: Ingest CSV to MySQL
    ingest_task = PythonOperator(
        task_id='ingest_csv_to_mysql',
        python_callable=ingest_csv_to_mysql,  # Keep name for now
        provide_context=True,
    )
    
    # Task 2: Validate Data
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True,
    )
    
    # Task 3: Transform Data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )
    
    # Task 4: Compute KPIs
    kpi_task = PythonOperator(
        task_id='compute_kpis',
        python_callable=compute_kpis,
        provide_context=True,
    )
    
    # Task 5: Save Validation Report
    report_task = PythonOperator(
        task_id='save_validation_report',
        python_callable=save_validation_report,
        provide_context=True,
    )
    
    # Define task dependencies
    ingest_task >> validate_task >> transform_task >> kpi_task >> report_task
