from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from lib import Notification, FakerGenerators
from process import Extract, Load
from datetime import datetime
from airflow import DAG
import pendulum

# Set a variables
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
        #'on_success_callback': Notification.push
    }
) as dag:

    def faker_func():
        FakerGenerators.create()

    def extract_func():
        extract = Extract(DATA_PATH)
        extract.extract_processing()

    def load_func():
        load = Load(DATA_PATH)
        load.load_procesing()

    # Create faker data for data raw
    create_faker_data = PythonOperator(
        task_id='create_faker_data',
        python_callable=faker_func
    )

    # Extract data from GCS bucket (csv, parquet) and save to json
    extract_to_json = PythonOperator(
        task_id='extract_to_json',
        python_callable=extract_func
    )

    # Load extracted data from json to Postgres
    load_to_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_func
    )

    # Create dim table and facts table in Postgres
    create_dim_facts = PostgresOperator(
        task_id='create_dim_facts',
        postgres_conn_id='connection_postgres',
        sql='sql/create_dim_facts.sql'
    )

    create_faker_data >> extract_to_json >> load_to_postgres >> create_dim_facts
