import os
import sys

sys.path.append(os.path.abspath("./"))

from datetime import datetime, timedelta, timezone

import pandas as pd
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from utils.cockroach_wrapper import cockroach_connection, df_to_sql
from utils.meteo_wrapper import hourly_weather_7da
from utils.util import create_directory

CSV_PATH = "/opt/airflow/tmp/forecast_query_7da.csv"


def hit_open_meteo() -> None:
    data = hourly_weather_7da()
    df = pd.DataFrame(data)
    df["time"] = pd.to_datetime(df["time"])
    df["forecast_date"] = df["time"].dt.date
    df["hour"] = df["time"].dt.hour + 1
    df["reference_date"] = datetime.now().date()
    df["time_horizon"] = df.apply(
        lambda row: (row["forecast_date"] - row["reference_date"]).days, axis=1
    )

    df = df[
        [
            "reference_date",
            "forecast_date",
            "hour",
            "time_horizon",
            "apparent_temperature",
            "temperature_2m",
            "precipitation_probability",
            "dewpoint_2m",
            "windspeed_10m",
            "cloudcover",
        ]
    ]
    print(df.dtypes)
    print(df.head(48)[["reference_date", "forecast_date", "time_horizon"]])

    create_directory("/opt/airflow/tmp")
    df.to_csv(CSV_PATH, index=False)


def export_temps():
    engine = cockroach_connection()

    df = pd.read_csv(CSV_PATH)
    print(df)

    if not df_to_sql(engine, df, "hourly_weather"):
        print("failed")


default_args = {
    "owner": "scott",
    "retries": 0,
    "retry_delay": timedelta(seconds=2),
    "schedule_interval": "@daily",
}

# pylint: disable=unused-variable
with DAG(
    dag_id="7_da_temps",
    default_args=default_args,
    start_date=datetime(2022, 12, 29),
    # schedule_interval="0 2 * * 0",
    catchup=False,
) as dag:
    api_hit = PythonOperator(task_id="hit_open_meteo", python_callable=hit_open_meteo)
    create_table = PostgresOperator(
        task_id="create_table", postgres_conn_id="cockroach_db", sql="create_table.sql"
    )
    export_to_database = PythonOperator(
        task_id="export_to_cockroach", python_callable=export_temps
    )
    api_hit >> create_table >> export_to_database  # type: ignore
