import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.check_operator import ValueCheckOperator
from scripts.weather_utils import etl

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "weather_etl"
default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2022, 11, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["weather"],
) as dag:

    create_weather_table = PostgresOperator(
            task_id="create_pet_table",
            sql="""
                CREATE TABLE IF NOT EXISTS weather (
                id SERIAL PRIMARY KEY,
                city text,
                current_temperature numeric,
                tomorrow_max_temperature numeric,
                tomorrow_min_temperature numeric,
                current_temperature_fahrenheit numeric);
            """,
        )

    # custom python operator
    # https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html#
    process_data = PythonOperator(
        task_id='etl',
        python_callable=etl
    )

    check_city_count = ValueCheckOperator(
        task_id='check_city_count',
        sql="SELECT COUNT(DISTINCT(city)) FROM weather",
        pass_value=1,
        conn_id="postgres_default"
    )

    create_weather_table >> process_data >> check_city_count
