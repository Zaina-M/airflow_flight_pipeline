from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import logging

# Import functions from modules
from pipeline.ingestion import ingest_csv_to_mysql
from pipeline.validation import validate_data
from pipeline.transformation import transform_data
from pipeline.kpis import compute_kpis
from pipeline.reports import save_validation_report, send_pipeline_status

# Configure logging
logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['data@example.com'],  # Configure via Airflow Variables
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# DAG Definition with TaskGroups

with DAG(
    dag_id='flight_price_pipeline',
    default_args=default_args,
    description='End-to-end data pipeline for Bangladesh flight price analysis',
    schedule_interval='@daily', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['flight', 'price', 'bangladesh', 'etl'],
    doc_md="""
    
    This pipeline processes Bangladesh flight price data through the following stages:
    
    1. Ingestion: Load CSV data into MySQL staging (idempotent, with hash-based change detection)
    2. Quality: Validate data quality using the data quality framework
    3. Transform: Clean and enrich data, load to PostgreSQL
    4. Analytics: Compute KPIs using SQL pushdown
    5. Reporting: Save validation and quality reports
    
    
    
     Lineage
    All tasks track data lineage for auditing and debugging.
    """,
    # params={
    #     'force_reload': False,
    # },
) as dag:
    
    # TaskGroup: Data Ingestion
   
    with TaskGroup(group_id='ingestion', tooltip='Load raw data into staging') as ingestion_group:
        ingest_task = PythonOperator(
            task_id='ingest_csv_to_mysql',
            python_callable=ingest_csv_to_mysql,
            provide_context=True,
            doc_md="""
            Ingest CSV to MySQL
            - Loads raw CSV data into MySQL staging table
            - Loads full dataset into memory (suitable for datasets under 1GB)
            - Implements idempotency via file hash comparison
            - Detects schema evolution
            """
        )
    
    
    # TaskGroup: Data Quality - continues even if ingestion fails
   
    with TaskGroup(group_id='quality', tooltip='Validate and report on data quality') as quality_group:
        validate_task = PythonOperator(
            task_id='validate_data',
            python_callable=validate_data,
            provide_context=True,
            trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream failed
            doc_md="""
            Validate Data
            - Uses DataQualityValidator framework
            - Checks for nulls, negative values, type errors
            - Auto-corrects fixable issues
            - Recalculates Total Fare if mismatched
            """
        )
        
        report_task = PythonOperator(
            task_id='save_validation_report',
            python_callable=save_validation_report,
            provide_context=True,
            trigger_rule=TriggerRule.ALL_DONE,  # Run even if validation failed
            doc_md="""
             Save Validation Report
            - Persists validation results to PostgreSQL
            - Creates audit trail for each pipeline run
            """
        )
        
        validate_task >> report_task
    
   
    # TaskGroup: Transformation & Loading
  
    with TaskGroup(group_id='transformation', tooltip='Transform and load to analytics') as transform_group:
        transform_task = PythonOperator(
            task_id='transform_data',
            python_callable=transform_data,
            provide_context=True,
            doc_md="""
             Transform Data
            - Parses datetime fields
            - Adds peak season flags
            - Creates route column
            - Loads to PostgreSQL analytics
            """
        )
    
   
    # TaskGroup: Analytics & KPIs
    
    with TaskGroup(group_id='analytics', tooltip='Compute KPI metrics') as analytics_group:
        kpi_task = PythonOperator(
            task_id='compute_kpis',
            python_callable=compute_kpis,
            provide_context=True,
            doc_md="""
             Compute KPIs
            Uses SQL pushdown for performance:
            - Average Fare by Airline
            - Seasonal Fare Variation
            - Booking Count by Airline
            - Most Popular Routes
            """
        )
   
    # Define Task Dependencies with Status Report
 
    # Status report task - always runs to send summary
    pipeline_status = PythonOperator(
        task_id='pipeline_status_report',
        python_callable=send_pipeline_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,  # Always runs regardless of upstream status
        doc_md="""
         Pipeline Status Report
        - Summarizes all task statuses
        - Alerts on any failures
        - Always runs (even if other tasks fail)
        """
    )
    
    ingestion_group >> quality_group >> transform_group >> analytics_group >> pipeline_status
