from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')

from pipelines.orchestrator import run_pipeline

default_args = {
    'owner': 'sammed',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['your-email@gmail.com']
}

with DAG(
    dag_id='metadata_driven_etl',
    default_args=default_args,
    description='Metadata-driven ETL pipeline: S3 → Staging → DWH → Datamart',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'redshift', 's3']
) as dag:

    # Task 1: Extract and load to staging
    extract_load_staging = PythonOperator(
        task_id='extract_load_staging',
        python_callable=run_pipeline,
    )

    # Task 2: Run SCD2 stored procedure
    load_dwh = BashOperator(
        task_id='load_dwh',
        bash_command="""
            python -c "
import psycopg2
from config import REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD
conn = psycopg2.connect(host=REDSHIFT_HOST, port=REDSHIFT_PORT, dbname=REDSHIFT_DB, user=REDSHIFT_USER, password=REDSHIFT_PASSWORD)
cursor = conn.cursor()
cursor.execute('CALL dwh.sp_load_customer_orders({{ run_id }});')
conn.commit()
cursor.close()
conn.close()
print('DWH load complete')
"
        """
    )

    # Task 3: Validate datamart
    validate_datamart = BashOperator(
        task_id='validate_datamart',
        bash_command="""
            python -c "
import psycopg2
from config import REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD
conn = psycopg2.connect(host=REDSHIFT_HOST, port=REDSHIFT_PORT, dbname=REDSHIFT_DB, user=REDSHIFT_USER, password=REDSHIFT_PASSWORD)
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM datamart.vw_customer_summary')
count = cursor.fetchone()[0]
print(f'Datamart row count: {count}')
assert count > 0, 'Datamart validation failed — no rows found'
cursor.close()
conn.close()
"
        """
    )

    # DAG dependency chain
    extract_load_staging >> load_dwh >> validate_datamart