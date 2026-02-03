"""
Reports module for flight price pipeline.
"""

import json
import logging
from datetime import datetime
from .constants import get_postgres_connection
import pandas as pd

logger = logging.getLogger(__name__)


def save_validation_report(**context):
    
    # Task 5: Save validation report to PostgreSQL.
    
    logger.info("Saving validation report...")
    
    postgres_engine = get_postgres_connection()
    
    # Get validation report from XCom
    validation_json = context['ti'].xcom_pull(key='validation_report', task_ids='validate_data') # saves validation report from previous task
    
    if validation_json:
        validation_report = json.loads(validation_json)
        
        report_df = pd.DataFrame([{
            'run_id': context['run_id'],
            'total_records': validation_report.get('total_records', 0),
            'valid_records': validation_report.get('valid_records', 0),
            'invalid_records': validation_report.get('invalid_records', 0),
            'missing_values_count': validation_report.get('missing_values_count', 0),
            'negative_fares_count': validation_report.get('negative_fares_count', 0),
            'data_type_errors': validation_report.get('data_type_errors', 0),
            'validation_details': json.dumps(validation_report.get('issues', []))
        }])
        
        report_df.to_sql('validation_report', postgres_engine, 
                         if_exists='append', index=False)
        
        logger.info("Validation report saved successfully")
    
    return {'status': 'complete'}


def send_pipeline_status(**context):
    """
    Send pipeline status summary - always runs regardless of upstream task status.
    This provides visibility into what succeeded/failed and can be used for alerting.
    """
    dag_run = context['dag_run']
    run_id = context.get('run_id', 'unknown')
    
    # Get status of all tasks
    task_instances = dag_run.get_task_instances()
    
    failed_tasks = []
    succeeded_tasks = []
    skipped_tasks = []
    upstream_failed_tasks = []
    
    for task_instance in task_instances:
        if task_instance.task_id == 'pipeline_status_report':
            continue  # Skip self
            
        state = str(task_instance.state) if task_instance.state else 'none'
        
        if state == 'failed':
            failed_tasks.append(task_instance.task_id)
        elif state == 'success':
            succeeded_tasks.append(task_instance.task_id)
        elif state == 'skipped':
            skipped_tasks.append(task_instance.task_id)
        elif state == 'upstream_failed':
            upstream_failed_tasks.append(task_instance.task_id)
    
    # Determine overall status
    if failed_tasks:
        overall_status = 'PARTIAL_FAILURE'
    elif upstream_failed_tasks:
        overall_status = 'UPSTREAM_FAILURE'
    elif skipped_tasks and not succeeded_tasks:
        overall_status = 'ALL_SKIPPED'
    else:
        overall_status = 'SUCCESS'
    
    status_report = {
        'run_id': run_id,
        'execution_date': str(context.get('execution_date')),
        'overall_status': overall_status,
        'succeeded': succeeded_tasks,
        'failed': failed_tasks,
        'skipped': skipped_tasks,
        'upstream_failed': upstream_failed_tasks,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    # Log the status
    logger.info(f"=" * 60)
    logger.info(f"PIPELINE STATUS REPORT")
    logger.info(f"=" * 60)
    logger.info(f"Run ID: {run_id}")
    logger.info(f"Overall Status: {overall_status}")
    logger.info(f"Succeeded Tasks ({len(succeeded_tasks)}): {succeeded_tasks}")
    
    if failed_tasks:
        logger.error(f" FAILED Tasks ({len(failed_tasks)}): {failed_tasks}")
    if skipped_tasks:
        logger.warning(f"Skipped Tasks ({len(skipped_tasks)}): {skipped_tasks}")
    if upstream_failed_tasks:
        logger.warning(f"Upstream Failed ({len(upstream_failed_tasks)}): {upstream_failed_tasks}")
    
    logger.info(f"=" * 60)
    
    # Save status report to PostgreSQL for audit trail
    try:
        postgres_engine = get_postgres_connection()
        status_df = pd.DataFrame([{
            'run_id': run_id,
            'overall_status': overall_status,
            'succeeded_count': len(succeeded_tasks),
            'failed_count': len(failed_tasks),
            'skipped_count': len(skipped_tasks),
            'details': json.dumps(status_report),
            'created_at': datetime.utcnow()
        }])
        status_df.to_sql('pipeline_run_status', postgres_engine, 
                         if_exists='append', index=False)
        logger.info("Pipeline status saved to database")
    except Exception as e:
        logger.warning(f"Could not save pipeline status to database: {e}")
    
    
    return status_report