import requests 
import json
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

def _get_current_weather():
    # Package the request, send the request and catch the response: r
    request_url = "http://weerlive.nl/api/json-data-10min.php?key=d207adc652&locatie=Amsterdam"

    parameters = {"t": "Current weather in Amsterdam"}

    response = requests.get(request_url, params=parameters)
    return response



def etl():
	weather_dict = json.loads(_get_current_weather().text)
	df_weather = pd.DataFrame(weather_dict['liveweer'])
	df_weather_selected = df_weather[['plaats', 'temp', 'd1tmax', 'd1tmin']]
	df_weather_selected.columns = ["city", "current_temperature", "tomorrow_max_temperature", "tomorrow_minimum_temperature"]
	df_weather_selected[["current_temperature", "tomorrow_max_temperature", "tomorrow_minimum_temperature"]] = \
	df_weather_selected[["current_temperature", "tomorrow_max_temperature", "tomorrow_minimum_temperature"]].astype(np.float64)
	df_weather_selected["current_temperature_fahrenheit"] = df_weather_selected["current_temperature"].astype(np.float64) * 9/5 + 32

	conn_string = 'postgresql+psycopg2://airflow:airflow@host.docker.internal:5961/airflow'
	db = create_engine(conn_string)
	conn = db.connect()

	
	# our dataframe
	df_weather_selected.to_sql('weather', db, if_exists='replace')