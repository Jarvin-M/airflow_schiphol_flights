import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "weather_etl"
default_args = {
    "depends_on_past": False,
    "email": ["jmutatiina@deloitte.nl"],
    "email_on_failure": True,
    "email_on_retry": False,
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
                CREATE TABLE IF NOT EXISTS weather2 (
                id SERIAL PRIMARY KEY,
                weather_data JSON NOT NULL);
            """,
        )

    # custom python operator
    # https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html#
    @task
    def get_current_weather(ti=None):
        # Package the request, send the request and catch the response: r
        request_url = "http://weerlive.nl/api/json-data-10min.php?key=d207adc652&locatie=Amsterdam"

        parameters = {"t": "Current weather in Amsterdam"}

        response = requests.get(request_url, params=parameters)
        ti.xcom_push(key="current_weather", value=response.text)

    source_current_weather = get_current_weather()

    populate_weather_data = PostgresOperator(
        task_id="populate_pet_table",
        sql="""
            INSERT INTO weather2 (weather_data) VALUES
            (json('{{ti.xcom_pull(task_ids='get_current_weather', key='current_weather')}}'));
           """
    )

    create_weather_table >> source_current_weather >> populate_weather_data
