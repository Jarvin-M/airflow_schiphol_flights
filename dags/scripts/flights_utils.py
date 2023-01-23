import json
import re
from datetime import datetime
from tempfile import NamedTemporaryFile

import pandas as pd
import requests
from airflow.operators.email_operator import EmailOperator
from sqlalchemy import create_engine

URL = "https://api.schiphol.nl/public-flights/flights?page=0"
PARAMS = {
    "includedelays": "false",
    "sort": "+scheduleTime",
    "ScheduleDate": f"{datetime.today().strftime('%Y-%m-%d')}",
    "flightDirection": "D",
    "route": "BRU, VIE, CPH, HEL, NCE, CDG, BER, FCO",
}
# TODO: store headers as secrets
HEADERS = {
    "app_id": "<api-id>",
    "app_key": "<api-key>",
    "ResourceVersion": "v4",
    "Accept": "application/json",
}


def extract_lastpagenumber(url: str, headers: dict, params: dict) -> int:
    """Extracts the last page number from the initial API request as the API
    response has pagination
    """

    # -- 1: API request to flights API
    response = requests.request("GET", url, headers=headers, params=params)

    # --2: Extract pagination with regex
    page_links = response.headers["link"]
    page_links_list = re.findall(r"\<(.*?)>", page_links)

    return int(page_links_list[-1].split("page=")[-1])


def get_todays_flights(url: str, headers: dict, params: dict) -> dict:
    """Unpack pagination API response and combine responses"""

    # --1. extract last page number from pagination
    last_page = extract_lastpagenumber(url, headers, params)

    response = requests.request("GET", url, headers=headers, params=params)
    all_responses = json.loads(response.text)

    # -- 2. Extract pagination data via the recursive API query
    if last_page > 0:
        for page_num in range(1, last_page + 1):
            url = f"https://api.schiphol.nl/public-flights/flights?page={page_num}"
            partial_response = requests.request(
                "GET", url, headers=headers, params=params
            )
            partial_response = json.loads(partial_response.text)
            all_responses["flights"].extend(partial_response["flights"])

    return all_responses


def transform_data(response_dict: dict):
    """Perform some transformations on the data from the API"""

    df_flights_raw = pd.DataFrame(response_dict["flights"])

    # Select columns
    df_flights = df_flights_raw[
        [
            "aircraftRegistration",
            "flightName",
            "gate",
            "route",
            "scheduleDateTime",
            "publicEstimatedOffBlockTime",
        ]
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
    df_flights["scheduleDateTime"] = pd.to_datetime(
        df_flights["scheduleDateTime"], infer_datetime_format=True
    )
    df_flights["publicEstimatedOffBlockTime"] = pd.to_datetime(
        df_flights["publicEstimatedOffBlockTime"], infer_datetime_format=True
    )

    # Calculate delay minutes
    df_flights["delayed_minutes"] = (
        (df_flights["publicEstimatedOffBlockTime"] - df_flights["scheduleDateTime"])
    ) / pd.Timedelta(minutes=1)

    # Indicate delay status
    df_flights["delayed_status"] = df_flights["delayed_minutes"].apply(lambda x: x > 10)

    # Add ingestion timestamp
    df_flights.loc[:, "ingestion_timestamp"] = pd.Timestamp.now()

    # Drop duplicates
    df_flights = df_flights.drop_duplicates().reset_index(drop=True)

    return df_flights


def process_and_store_data():
    """Ingest, transform and store today's flights data"""

    # --1. Extract today's flights
    flights_dict = get_todays_flights(url=URL, headers=HEADERS, params=PARAMS)

    # --2. Transform data for insights
    df_flights = transform_data(flights_dict)

    # -- 3. Configure connection between airflow and postgres database
    conn_string = (
        "postgresql+psycopg2://airflow:airflow@host.docker.internal:5961/airflow"
    )

    flights_db = create_engine(conn_string)
    conn = flights_db.connect()

    # --4. Write dataframe to postgres db
    df_flights.to_sql("flights", flights_db, if_exists="append", index=False)


def build_and_send_email(task_instance=None, **context):
    """Build and send insights as an email attachment"""
    todays_date = datetime.today().strftime("%Y-%m-%d")
    # --1. Open a temporary csv file
    with NamedTemporaryFile(
        mode="w+", suffix=".csv", prefix=("delays" + todays_date), delete=False
    ) as csvfile:

        # --2. pull the insights from previous task via xcom and write to csv
        insights_json = task_instance.xcom_pull(task_ids="generate_insights")
        insights_df = pd.read_json(insights_json)
        insights_df.to_csv(csvfile.name)

        # --3. Construct email and attach csv file with insights
        construct_email = EmailOperator(
            task_id="send_email",
            to="jmutatiina@deloitte.nl,jzhang5@deloitte.nl",
            subject=f"Summary: Schiphol Flight Delay insights- {todays_date}",
            html_content="""
                Hi,
                <p> Find attached the flight delay per gate summary for date -
                {{ ds }} attached </p>

                <p> Best regards,</p>
                Airflow
            """,
            files=[csvfile.name],
        )
        construct_email.execute(context)
