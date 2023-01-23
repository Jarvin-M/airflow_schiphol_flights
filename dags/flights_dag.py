from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts.flights_insights import generate_insights
from scripts.flights_utils import process_and_store_data
from scripts.flights_utils import build_and_send_email

DAG_ID = "flights_dag"
default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2022, 11, 1),
    schedule_interval="0 23 * * *",  # Daily interval at 23:00 
    catchup=False,
    tags=[
        "Orchestration",
        "flight_delays",
    ],
) as dag:

    # --1. create table in postgres database
    create_flights_table = PostgresOperator(
        task_id="create_flights_table",
        sql="""
                CREATE TABLE IF NOT EXISTS flights (
                "aircraftRegistration" text NULL,
                "flightName" text NULL,
                gate text NULL,
                "scheduleDateTime" timestamptz NULL,
                "publicEstimatedOffBlockTime" timestamptz NULL,
                destinations text NULL,
                eu text NULL,
                visa bool NULL,
                delayed_minutes float8 NULL,
                delayed_status bool NULL,
                ingestion_timestamp timestamp NULL);
            """,
    )

    # --2. Transform and process data
    ingest_n_process_data = PythonOperator(
        task_id="ingest_n_process_data", python_callable=process_and_store_data
    )

    # --3. Generate basic insights
    retrieve_insights = PythonOperator(
        task_id="generate_insights",
        python_callable=generate_insights,
        do_xcom_push=True,
    )

    # --4. Email insights
    send_insights_email = PythonOperator(
        task_id="send_insights_email",
        python_callable=build_and_send_email,
        provide_context=True,
    )

    # --5. Taskflow execution order
    create_flights_table >> ingest_n_process_data >> retrieve_insights >> send_insights_email
