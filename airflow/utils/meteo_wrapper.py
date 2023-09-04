import os
import urllib.parse
import json
from enum import Enum

import requests
from dotenv import load_dotenv


class HourlyWeatherVariables(Enum):
    TEMP = "temperature_2m"
    HUMIDITY = "relativehumidity_2m"
    DEWPOINT = "dewpoint_2m"
    APPARENT_TEMP = "apparent_temperature"
    CLOUD_COVER = "cloudcover"
    WIND_SPEED_10M = "windspeed_10m"
    WIND_DIRECTION_10M = "winddirection_10m"
    PRECIPITATION_PROB = "precipitation_probability"


class DailyWeatherVariables(Enum):
    MAX_TEMP = "temperature_2m_max"
    MIN_TEMP = "temperature_2m_min"


def base_url() -> str:
    return "https://api.open-meteo.com/v1/forecast"


def historical_url() -> str:
    return "https://archive-api.open-meteo.com/v1/era5"


def hourly_weather_7da(lat: float = 34.09, long: float = -117.8903) -> dict:
    hourly_query = [
        HourlyWeatherVariables.TEMP,
        HourlyWeatherVariables.APPARENT_TEMP,
        HourlyWeatherVariables.CLOUD_COVER,
        HourlyWeatherVariables.PRECIPITATION_PROB,
        HourlyWeatherVariables.WIND_SPEED_10M,
        HourlyWeatherVariables.DEWPOINT,
    ]

    daily_query = [DailyWeatherVariables.MAX_TEMP, DailyWeatherVariables.MIN_TEMP]

    hourly_params = (",").join([i.value for i in hourly_query])
    daily_params = (",").join([i.value for i in daily_query])
    temp_unit = "fahrenheit"
    timezone = urllib.parse.quote("America/Los_Angeles")
    r = requests.get(
        base_url(),
        params={
            "latitude": lat,
            "longitude": long,
            "hourly": hourly_params,
            "daily": daily_params,
            "timezone": timezone,
            "temperature_unit": temp_unit,
        },
    )
    request_json = json.loads(r.text)  # type: ignore
    data = request_json["hourly"]
    print(request_json["hourly"])
    print(request_json["daily"])
    return data


def hello():
    print("Hello world!")


def print_dotenv():
    success = load_dotenv(".env")
    print(os.environ.get("HOME_LAT"))


def open_meteo_covina():
    base_url = "https://api.open-meteo.com/v1/forecast"
    r = requests.get(
        base_url,
        params={"latitude": 34.09, "longitude": -117.8903, "hourly": "temperature_2m"},
    )
    print(r.text)
