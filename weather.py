from datetime import datetime
import logging
import json
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

def _process_weather(ti):
    info = ti.xcom_pull("extract_data")
    timestamp = info["dt"]
    temp = info["main"]["temp"]
    return timestamp, temp

def python_method(execution_date: pendulum.DateTime):
    # print("Running execution_date = ", kwargs['execution_date'])
    print(f"execution_date from task: {execution_date}")
    print(f"execution_date from task: {execution_date.int_timestamp}")
    print("Done")

with DAG(
    dag_id="weather_dag4",
    start_date=datetime(2025, 3, 5),
    schedule_interval="@daily",
    catchup=True,
) as dag:
    db_create = PostgresOperator(
        task_id="create_table_postgres",
        postgres_conn_id="wather_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS measures (
            timestamp TIMESTAMP,
            temp FLOAT);
            """,
    )

    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="http_weather_conn",
        endpoint="data/2.5/weather",
        request_params={"appid": Variable.get("WEATHER_API_KEY"), "q":"Lviv"}
    )

    extract_data = SimpleHttpOperator(
        task_id="extract_data",
        http_conn_id="http_weather_conn",
        endpoint="data/2.5/weather",
        data={"appid": Variable.get("WEATHER_API_KEY"), "q": "Lviv"},
        method="GET",
        response_filter=lambda x: json.loads(x.text),
        log_response=True
    )

    process_weather_data = PythonOperator(
        task_id="process_weather_data",
        python_callable=_process_weather
    )

    inject_data = PostgresOperator(
        task_id="inject_data",
        postgres_conn_id="wather_conn",
        sql="""
            INSERT INTO measures (timestamp, temp) VALUES
            (to_timestamp({{ti.xcom_pull(task_ids='process_weather_data')[0]}}),
            {{ti.xcom_pull(task_ids='process_weather_data')[1]}});
            """,
        )
    
    doit = PythonOperator(
        task_id='doit',
        provide_context=True,
        python_callable=python_method
    )

    db_create >> check_api >> extract_data >> process_weather_data >> inject_data >> doit