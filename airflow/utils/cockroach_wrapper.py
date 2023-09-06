import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv(".env")


def cockroach_connection() -> Engine:
    engine = create_engine(os.environ["COCKROACH_ALCHEMY"])
    print(type(engine))
    return engine  # type: ignore


def df_to_sql(engine: Engine, df: pd.DataFrame, table: str) -> None:
    df.to_sql(table, engine, if_exists="replace", index=False)
