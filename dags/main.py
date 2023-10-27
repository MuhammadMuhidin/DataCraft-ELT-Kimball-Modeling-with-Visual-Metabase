from airflow.providers.postgres.operators.postgres import PostgresOperator
from lib import Notification, FakerGenerators, MetabaseAPI
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from process import Extract
from airflow import DAG
import pendulum
import hashlib


# Set a variables
DATA_PATH = '/data'
keygen = '8c747032a1aa5af580f48ad2be75366bb517fe8b0990d10931eda23795f3cf26'

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

    def validation_func():
        # Simple validation to check if your secret code is valid
        try:
            # read secret.txt and check if it's valid direct to create_faker_data
            with open(DATA_PATH+'/secret_key.txt', 'r') as file:
                # generate hash key
                hash_key = hashlib.sha256(keygen.encode()).hexdigest()
                file_content = file.read()
                if file_content == hash_key:
                    print('your code is valid :)')
                    return 'Process.create_faker_data'
                else:
                    # if not valid, return invalid_code
                    print('your code not valid :( please try again')
                    return 'invalid_code'
        except:
            # if file not found, return not_found_secret_file
            print('File not found')
            return 'not_found_secret_file'

    def faker_func():
        FakerGenerators.create()

    def extract_func():
        extract = Extract(DATA_PATH)
        extract.extract_processing()

    def metabase_func():
        MetabaseAPI.send_report()

    # Branching for validation
    validation = BranchPythonOperator(
        task_id='validation',
        python_callable=validation_func
    )

    with TaskGroup('Process') as Process:
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

    # If file found but invalid
    invalid_code = DummyOperator(
        task_id='invalid_code',
    )

    # If file not found
    not_found_secret_file = DummyOperator(
        task_id='not_found_secret_file',
    )

    # End of DAG
    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed' # If none failed then end
    )

    validation >> [create_faker_data, invalid_code , not_found_secret_file]
    create_faker_data >> extract_to_csv >> load_to_postgres >> dbt_run >> send_report >> end
    invalid_code >> end
    not_found_secret_file >> end
    