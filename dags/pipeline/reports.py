"""
Reports module for flight price pipeline.
"""

import json
import logging
from .constants import get_postgres_connection
import pandas as pd

logger = logging.getLogger(__name__)


def save_validation_report(**context):
    """
    Task 5: Save validation report to PostgreSQL.
    """
    logger.info("Saving validation report...")
    
    postgres_engine = get_postgres_connection()
    
    # Get validation report from XCom
    validation_json = context['ti'].xcom_pull(key='validation_report', task_ids='validate_data')
    
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