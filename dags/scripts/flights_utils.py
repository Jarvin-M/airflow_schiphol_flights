import json
import re

import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine


def get_todays_flights():
    # Package the request, send the request and catch the response: r
    # -- 1: API request to flights API
    url = "https://api.schiphol.nl/public-flights/flights?page=0"
    params = {
        "includedelays": "false",
        "sort": "+scheduleTime",
        "ScheduleDate": "2023-01-04",
        "flightDirection": "D",
        "route": "BRU, VIE, CPH, HEL, NCE, CDG, BER, FCO",
    }
    headers = {
        "app_id": "d4d55a09",
        "app_key": "ec5d34f4372b9a44fe6b3da9574396f7",
        "ResourceVersion": "v4",
        "Accept": "application/json",
    }
    response = requests.request("GET", url, headers=headers, params=params)

    # --2: Extract pagination with regex
    page_links = response.headers["link"]
    page_links_list = re.findall(r"\<(.*?)>", page_links)
    last_page = int(page_links_list[-1].split("page=")[-1])

    all_responses = json.loads(response.text)

    # -- 3: Extract pagination data via the recursive API query
    if last_page > 0:
        for page_num in range(1, last_page + 1):
            url = f"https://api.schiphol.nl/public-flights/flights?page={page_num}"
            partial_response = requests.request(
                "GET", url, headers=headers, params=params
            )
            partial_response = json.loads(partial_response.text)
            all_responses["flights"].extend(partial_response["flights"])

    return all_responses


def etl():

    flights_dict = get_todays_flights()
    df_flights_raw = pd.DataFrame(flights_dict["flights"])

    # Select columns
    df_flights = df_flights_raw[
        ["route", "actualOffBlockTime", "publicEstimatedOffBlockTime"]
    ]

    
    # Normalize the 'route' column
    df_flights = df_flights.join(pd.json_normalize(df_flights["route"])).drop(
        "route", axis="columns"
    )

    # Drop nulls
    df_flights = df_flights[~df_flights["publicEstimatedOffBlockTime"].isnull()]

    # Drop brackets in the 'destination' column
    df_flights["destinations"] = df_flights["destinations"].str[0]
    
    # Convert string to timestamp
    df_flights['actualOffBlockTime'] = pd.to_datetime(df_flights['actualOffBlockTime'],infer_datetime_format=True)
    df_flights['publicEstimatedOffBlockTime'] = pd.to_datetime(df_flights['publicEstimatedOffBlockTime'],infer_datetime_format=True)

    # Calculate delay minutes
    df_flights['delayed_minutes'] = ((df_flights['actualOffBlockTime'] - df_flights['publicEstimatedOffBlockTime'])) / pd.Timedelta(minutes=1)

    # Indicate delay status
    df_flights['delayed_status'] = df_flights['delayed_minutes'].apply(lambda x: x>10)

     # Add ingestion timestamp
    df_flights.loc[:, 'ingestion_timestamp'] = pd.Timestamp.now()

    # Drop duplicates
    df_flights = df_flights.drop_duplicates().reset_index(drop=True) 

    # Configure connection between airflow and postgres database
    conn_string = (
        "postgresql+psycopg2://airflow:airflow@host.docker.internal:5961/airflow"
    )
    db = create_engine(conn_string)
    conn = db.connect()

    # Write dataframe
    df_flights.to_sql("flights", db, if_exists="append", index=False)

