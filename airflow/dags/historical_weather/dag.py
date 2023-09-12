import os
import sys

sys.path.append(os.path.abspath("./"))

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytz
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dotenv import load_dotenv
from utils.cockroach_wrapper import cockroach_connection, df_to_sql
from utils.meteo_wrapper import hourly_weather_historical
from utils.util import create_directory

from airflow import DAG

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
CSV_PATH = "/opt/airflow/tmp/historical_hourly_weather_query.csv"


def filter_new_dates():
    hook = PostgresHook(postgres_conn_id="cockroach_db")
    with open(f"{AIRFLOW_HOME}/dags/historical_weather/sql/get_dates.sql") as query:
        dates = hook.get_pandas_df(sql=query.read())
        return dates


def hit_open_meteo() -> None:
    last_date = filter_new_dates().values[0][0]
    if not last_date:
        last_date = datetime(2021, 12, 31)
    data = hourly_weather_historical(start_date=last_date + timedelta(days=1))
    df: pd.DataFrame = pd.DataFrame(data)
    df = df.dropna()
    df["time"] = pd.to_datetime(df["time"])  # type: ignore
    df["date"] = df["time"].dt.date
    df["hour"] = df.time.dt.hour + 1
    df["cloudcover"] = df["cloudcover"].astype("int32")

    df = df[
        [
            "date",
            "hour",
            "apparent_temperature",
            "temperature_2m",
            "precipitation",
            "dewpoint_2m",
            "windspeed_10m",
            "cloudcover",
        ]
    ].rename(
        columns={
            "apparent_temperature": "apparent_temp",
            "temperature_2m": "temp_2m",
            "precipitation": "precip",
        }
    )  # type: ignore

    print(df.dtypes)
    print(df.head())
    print(df.tail())
    create_directory("/opt/airflow/tmp")
    df.to_csv(CSV_PATH, index=False)


def export_df():
    engine = cockroach_connection()

    df = pd.read_csv(CSV_PATH)
    if df.empty:
        return
    if not df_to_sql(engine, df, "weather_historical"):
        print("failed")
        exit(-1)


default_args = {
    "owner": "scott",
    "retries": 0,
    "retry_delay": timedelta(seconds=2),
    "schedule_interval": "@daily",
}

with DAG(
    dag_id="historical_weather",
    default_args=default_args,
    start_date=datetime(2022, 12, 29),
    catchup=False,
) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="cockroach_db",
        sql="sql/create_table.sql",
    )

    api_hit = PythonOperator(task_id="hit_open_meteo", python_callable=hit_open_meteo)

    export_to_database = PythonOperator(
        task_id="export_to_cockroach", python_callable=export_df
    )

    create_table >> api_hit >> export_to_database  # type: ignore
