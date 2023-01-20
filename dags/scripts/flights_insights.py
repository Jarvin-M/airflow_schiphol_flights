import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def generate_insights():
    # Configure connection between airflow and postgres database
    conn_string = (
        "postgresql+psycopg2://airflow:airflow@host.docker.internal:5961/airflow"
    )
    flights_db = create_engine(conn_string)
    conn = flights_db.connect()

    df_flights = pd.read_sql_query('select * from "flights"', con=flights_db)
    df_delays = (
        df_flights.groupby(["ingestion_timestamp", "gate"])
        .agg(
            {
                "delayed_status": "sum",
                "flightName": list,
            }
        )
        .reset_index()
    )
    delay_json = df_delays.astype(str).to_json()

    return delay_json
