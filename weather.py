from datetime import datetime
import json
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

cities = {
    "Lviv": {
        "lat": 49.842957,
        "lon": 24.031111
        },
    "Kyiv": {
        "lat": 50.450001,
        "lon": 30.523333
        },
    "Kharkiv": {
        "lat": 50.001560,
        "lon": 36.231537
        },
    "Odesa": {
        "lat": 46.482952,
        "lon": 30.712481
        },
    "Zhmerynka": {
        "lat": 49.03705,
        "lon": 28.11201
        }             
}



def create_dag(dag_id, city, city_info, schedule_interval, default_args):

    def _process_weather(ti):
        info = ti.xcom_pull("extract_data")
        timestamp = info["data"][0]["dt"]
        temp = info["data"][0]["temp"]
        humidity = info["data"][0]["humidity"]
        cloudiness = info["data"][0]["clouds"]
        wind_speed = info["data"][0]["wind_speed"]
        return {
            "time": timestamp,
            "temp": temp,
            "humidity": humidity,
            "cloudiness": cloudiness,
            "wind_speed": wind_speed
            }

    generated_dag = DAG(dag_id, schedule_interval=schedule_interval, default_args=default_args, catchup=True)

    with generated_dag:
        db_create = PostgresOperator(
            task_id="create_table_postgres",
            postgres_conn_id="wather_conn",
            sql="""
                CREATE TABLE IF NOT EXISTS measures (
                timestamp TIMESTAMP,
                temp FLOAT,
                humidity FLOAT,
                cloudiness FLOAT,
                wind_speed FLOAT, 
                city varchar(100));
                """,
        )

        check_api = HttpSensor(
            task_id="check_api",
            http_conn_id="http_weather_conn",
            endpoint="data/3.0/onecall/timemachine",
            request_params={"appid": Variable.get("WEATHER_API_KEY"), "lat": { city_info["lat"] },
                            "lon": { city_info["lon"] } , "dt": "{{ execution_date.int_timestamp }}"}
        )

        extract_data = SimpleHttpOperator(
            task_id="extract_data",
            http_conn_id="http_weather_conn",
            endpoint="data/3.0/onecall/timemachine",
            data={"appid": Variable.get("WEATHER_API_KEY"),  "lat": { city_info["lat"] },
                            "lon": { city_info["lon"] } , "dt": "{{ execution_date.int_timestamp }}"},
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
            sql=f"""
                INSERT INTO measures (timestamp, temp, humidity, cloudiness, wind_speed, city) VALUES
                (to_timestamp({{{{ti.xcom_pull(task_ids='process_weather_data')["time"]}}}}),
                {{{{ti.xcom_pull(task_ids='process_weather_data')["temp"]}}}}, 
                {{{{ti.xcom_pull(task_ids='process_weather_data')["humidity"]}}}},
                {{{{ti.xcom_pull(task_ids='process_weather_data')["cloudiness"]}}}},
                {{{{ti.xcom_pull(task_ids='process_weather_data')["wind_speed"]}}}},
                '{city}');
                """,
        )

        db_create >> check_api >> extract_data >> process_weather_data >> inject_data

        return generated_dag

for city in cities:
    dag_id = f"weather_{city}"

    default_args = { "owner": "airflow",
                     "start_date": datetime(2025, 3, 5)
                    }
    schedule_interval = "@daily"

    globals()[dag_id] = create_dag(dag_id, city, cities[city], schedule_interval, default_args)