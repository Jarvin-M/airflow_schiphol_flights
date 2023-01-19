import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.check_operator import ValueCheckOperator
from scripts.flights_utils import etl
from scripts.flights_insights import get_insights
from airflow.operators.email_operator import EmailOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "flights_etl"
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
    tags=["Orchestration", ],
) as dag:

    #TODO: update the flights table with the right columns
    create_flights_table = PostgresOperator(
        task_id="create_flights_table",
        sql="""
                CREATE TABLE IF NOT EXISTS flights (
                "actualOffBlockTime" timestamptz NULL,
                "publicEstimatedOffBlockTime" timestamptz NULL,
                destinations text NULL,
                eu text NULL,
                visa bool NULL,
                delayed_minutes float8 NULL,
                delayed_status bool NULL,
                ingestion_timestamp timestamp NULL);
                
            """,
    )

    # custom python operator
    # https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html#
    process_data = PythonOperator(task_id="etl", python_callable=etl)

    # check_city_count = ValueCheckOperator(
    #     task_id="check_city_count",
    #     sql="SELECT COUNT(DISTINCT(city)) FROM weather",
    #     pass_value=1,
    #     conn_id="postgres_default",
    # )

    retrieve_insights = PythonOperator(task_id="insights", python_callable=get_insights, do_xcom_push=True)

    #TODO: extract and add the summary insights to the html content
    send_insights_email = EmailOperator(
        task_id="send_email",
        to="jmutatiina@deloitte.nl",
        subject="Summary: Schiphol Flight Delay insights {{ ds }}",
        html_content="<b><h1> {{ task_instance.xcom_pull(task_ids='insights') }} </h1></b>",
    )

    create_flights_table>>process_data>>retrieve_insights>>send_insights_email
