import json
import re

import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine

def get_insights():
    # Configure connection between airflow and postgres database
    conn_string = (
        "postgresql+psycopg2://airflow:airflow@host.docker.internal:5961/airflow"
    )
    db = create_engine(conn_string)
    conn = db.connect()

    df_flights = pd.read_sql_query('select * from "flights"',con=db)
    df_delays = df_flights.groupby(['ingestion_timestamp'])['delayed_status'].sum().reset_index()
    delay_json = df_delays.to_json()
    return delay_json

