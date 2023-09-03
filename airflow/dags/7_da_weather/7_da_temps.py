import sys
import os

sys.path.append(os.path.abspath("./"))

from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dotenv import dotenv_values

from airflow import DAG


from utils.meteo_wrapper import (
    hello,
    print_dotenv,
    open_meteo_covina,
    temps_forecast_7da,
)
from utils.util import create_directory


def hit_open_meteo() -> None:
    data = temps_forecast_7da()
    df = pd.DataFrame(data)
    df["time"] = pd.to_datetime(df["time"])
    print(df.dtypes)
    create_directory("/opt/airflow/tmp")
    df.to_csv("/opt/airflow/tmp/forecast_7da.csv")


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
    task_1 = PythonOperator(task_id="hit_open_meteo", python_callable=hit_open_meteo)
