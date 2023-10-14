from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from process import Extract, Load
from datetime import datetime
from lib import Notification
from airflow import DAG
import pendulum

# Set a variables
BUCKET_NAME = 'dibimbing_final_bucket'
CRED_PATH = '/other/google_secret.json'
DATA_PATH = '/data'

with DAG(
    'final_project',
    schedule_interval=None,
    catchup=False,
    default_args={
        'owner': 'muhidin',
        'start_date': pendulum.datetime(2023, 10, 1, tz='Asia/Jakarta'),
        'retries' : 1,
        'on_failure_callback': Notification.push,
        'on_retry_callback': Notification.push,
        'on_success_callback': Notification.push
    }
) as dag:

    def extract_func():
        extract = Extract(BUCKET_NAME, CRED_PATH, DATA_PATH)
        extract.extract_processing()

    def load_func():
        load = Load(DATA_PATH)
        load.load_procesing()

    # Extract data from GCS bucket (csv, parquet) and save to json
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_func
    )

    # Load extracted data from json to Postgres
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_func
    )

    # Create dim table and facts table in Postgres
    create_dim_facts = PostgresOperator(
        task_id='create_dim_facts',
        postgres_conn_id='connection_postgres',
        sql='sql/create_dim_facts.sql'
    )

    extract_task >> load_task >> create_dim_facts
