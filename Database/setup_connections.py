"""
Airflow Connections Setup Script
================================
Run this script to create Airflow connections after the containers are up.
"""

from airflow.models import Connection
from airflow import settings
import os

def create_connections():
    # Create PostgreSQL and MySQL connections for Airflow.
    
    session = settings.Session()
    
    # PostgreSQL Analytics Connection
    postgres_conn = Connection(
        conn_id='postgres_analytics',
        conn_type='postgres',
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        schema=os.getenv('POSTGRES_DB', 'analytics'),
        login=os.getenv('POSTGRES_USER', 'airflow'),
        password=os.getenv('POSTGRES_PASSWORD', 'airflow_password'),
        port=int(os.getenv('POSTGRES_PORT', '5432'))
    )
    
    # MySQL Staging Connection
    mysql_conn = Connection(
        conn_id='mysql_staging',
        conn_type='mysql',
        host=os.getenv('MYSQL_HOST', 'mysql'),
        schema=os.getenv('MYSQL_DATABASE', 'staging'),
        login=os.getenv('MYSQL_USER', 'airflow'),
        password=os.getenv('MYSQL_PASSWORD', 'airflow_password'),
        port=int(os.getenv('MYSQL_PORT', '3306'))
    )
    
    connections = [postgres_conn, mysql_conn]
    
    for conn in connections:
        # Delete existing connection if it exists
        existing = session.query(Connection).filter(
            Connection.conn_id == conn.conn_id
        ).first()
        if existing:
            session.delete(existing)
        session.commit()
        
        # Add new connection
        session.add(conn)
        session.commit()
    
    print("Connections created successfully!")
    print(f"  - postgres_analytics: postgres://{postgres_conn.host}:{postgres_conn.port}/{postgres_conn.schema}")
    print(f"  - mysql_staging: mysql://{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}")


if __name__ == '__main__':
    create_connections()
