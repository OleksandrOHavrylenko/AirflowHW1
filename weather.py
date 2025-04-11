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

cities = {
    "Lviv" : {
        "lat" : 49.842957,
        "lon" : 24.031111
    }
}

def _process_weather(ti):
    info = ti.xcom_pull("extract_data")
    timestamp = info["data"][0]["dt"]
    temp = info["data"][0]["temp"]
    return timestamp, temp

def python_method(execution_date: pendulum.DateTime):
    # print("Running execution_date = ", kwargs['execution_date'])
    print(f"execution_date from task: {execution_date}")
    print(f"execution_date from task: {execution_date.int_timestamp}")
    print("Done")

with DAG(
    dag_id="weather_dag",
    start_date=datetime(2025, 4, 11),
    schedule_interval="@daily",
    catchup=False,
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
        endpoint="data/3.0/onecall/timemachine",
        request_params={"appid": Variable.get("WEATHER_API_KEY"), "lat": { cities["Lviv"]["lat"] },
                        "lon": { cities["Lviv"]["lon"] } , "dt": "{{ execution_date.int_timestamp }}"}
    )

    extract_data = SimpleHttpOperator(
        task_id="extract_data",
        http_conn_id="http_weather_conn",
        endpoint="data/3.0/onecall/timemachine",
        data={"appid": Variable.get("WEATHER_API_KEY"),  "lat": { cities["Lviv"]["lat"] },
                        "lon": { cities["Lviv"]["lon"] } , "dt": "{{ execution_date.int_timestamp }}"},
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