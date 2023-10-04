import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def cockroach_connection() -> Engine:
    if load_dotenv(".env"):
        print("Loaded environment variables")
    else:
        print("Failed to load environment variables")
    engine = create_engine(os.environ["COCKROACH_ALCHEMY"])
    return engine  # type: ignore


def df_to_sql(engine: Engine, df: pd.DataFrame, table: str) -> None:
    df.to_sql(table, engine, if_exists="append", index=False)
