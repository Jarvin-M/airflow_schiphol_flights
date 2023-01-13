import json
import re

import numpy as np
import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine


def get_todays_flights():
    # Package the request, send the request and catch the response: r
    url = "https://api.schiphol.nl/public-flights/flights?page=0"

    params={
        'includedelays': 'false',
        'sort' : '+scheduleTime',
        'ScheduleDate': '2023-01-04',
        'flightDirection': 'D',
        'route': 'BRU, VIE, CPH, HEL, NCE, CDG, BER, FCO'
    }
    headers = {
    'app_id': 'd4d55a09',
    'app_key': 'ec5d34f4372b9a44fe6b3da9574396f7',
    'ResourceVersion': 'v4',
    'Accept': 'application/json',
    }

    response = requests.request("GET", url, headers=headers, params=params)

    #handle pagination with regex
    page_links=response.headers["link"]
    page_links_list = re.findall(r"\<(.*?)>",page_links)
    last_page = int(page_links_list[-1].split("page=")[-1])

    all_responses = json.loads(response.text)
    if last_page > 0:
        for page_num in range(1,last_page+1):
            url = f"https://api.schiphol.nl/public-flights/flights?page={page_num}"
            partial_response = requests.request("GET", url, headers=headers, params=params)
            partial_response = json.loads(partial_response.text)
            all_responses["flights"].extend(partial_response["flights"])

    return all_responses


def etl():
    flights_dict = get_todays_flights()
    df_flights = pd.DataFrame(flights_dict["flights"])
    
    # Select columns
    df_flights = df_flights[["route", "actualOffBlockTime", "publicEstimatedOffBlockTime"]]

    # Normalize the 'route' column
    df_flights = df_flights.join(pd.json_normalize(df_flights['route'])).drop('route', axis='columns')

    # Drop nulls
    df_flights = df_flights[~df_flights['publicEstimatedOffBlockTime'].isnull()]

    # Drop brackets in the column "destinations"
    df_flights['destinations'] = df_flights['destinations'].str[0]

    # Drop duplicates
    df_flights = df_flights.drop_duplicates().reset_index(drop=True)
    
    conn_string = (
        "postgresql+psycopg2://airflow:airflow@host.docker.internal:5961/airflow"
    )
    db = create_engine(conn_string)
    conn = db.connect()

    # our dataframe
    df_flights.to_sql("flights", db, if_exists="replace")
