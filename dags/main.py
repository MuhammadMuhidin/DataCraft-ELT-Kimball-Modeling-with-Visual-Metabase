from airflow.providers.postgres.operators.postgres import PostgresOperator
from lib import Notification, FakerGenerators, MetabaseAPI
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from process import Extract
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

    def metabase_func():
        MetabaseAPI.send_report()

    # Create faker data for data raw
    create_faker_data = PythonOperator(
        task_id='create_faker_data',
        python_callable=faker_func
    )

    # Extract data and save to csv
    extract_to_csv = PythonOperator(
        task_id='extract_to_csv',
        python_callable=extract_func
    )

    # Load extracted data from csv to Postgres
    load_to_postgres = PostgresOperator(
        task_id='load_to_postgres',
        postgres_conn_id='connection_postgres',
        sql='/sql/ddl.sql'
    )

    # Transform data using dbt run
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /dbt && dbt run --project-dir . --profiles-dir .',
    )

    # Send report dashboard Metabase to Email
    send_report = PythonOperator(
        task_id='send_report',
        python_callable=metabase_func
    )

    create_faker_data >> extract_to_csv >> load_to_postgres >> dbt_run >> send_report
