from airflow import DAG
from datetime import datetime

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import pandas as pd

def process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user")
    if not user or "results" not in user:
        raise ValueError("No user data received from API")
    
    user = user["results"][0]
    processed_user = pd.DataFrame([{
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email'],
    }])

    processed_user.to_csv('/tmp/output_user.csv', index=False, header=False)

def store_user():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users_final (firstname, lastname, country, username, password, email) FROM stdin WITH DELIMITER ',' CSV;",
        filename='/tmp/output_user.csv'
    )

with DAG(
    dag_id="real_time_ingestion_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS users_final (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        '''
    )

    api_available = HttpSensor(
        task_id='api_available',
        http_conn_id='api_connect',
        endpoint='api/'
    )

    extract_user = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id='api_connect',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_user_task = PythonOperator(
        task_id='process_user',
        python_callable=process_user
    )

    store_user_task = PythonOperator(
        task_id='store_user',
        python_callable=store_user
    )

    create_table >> api_available >> extract_user >> process_user_task >> store_user_task
