from typing import Dict, List
from logging_config import logger, log_execution_time
from env import DAILY_HISTORY_FOLDER

import pandas as pd
import pyarrow.dataset as ds
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn


# @log_execution_time
def find_next_available_daily_history(symbol: str, date: str, attempts: int = 0):
    # Якщо кількість спроб перевищує 10, припиняємо рекурсію
    if attempts >= 10:
        print(f"Reached maximum attempts for {symbol} starting from {date}.")
        return None

    curr_date = pd.to_datetime(date)
    if curr_date < pd.to_datetime("2020-01-01"):
        return None

    previous_date = str(curr_date - pd.Timedelta(days=1))

    df = read_daily_history(symbol, previous_date, date)

    if df.empty:
        print(f"Data for {symbol} from {previous_date} to {date} is empty. Retrying...")

        return find_next_available_daily_history(symbol, previous_date, attempts + 1)
    else:
        print(f"Next available daily data for {symbol} is {previous_date}")
        return previous_date


@log_execution_time
def read_daily_history(symbol: str, start_date: str, end_date: str):
    start_year = int(start_date.split("-")[0])
    end_year = int(end_date.split("-")[0])

    timestamp_start_ns = int(pd.to_datetime(start_date).timestamp() * 1_000_000_000)
    timestamp_end_ns = int(pd.to_datetime(end_date).timestamp() * 1_000_000_000)

    files = [
        os.path.join(DAILY_HISTORY_FOLDER, f"{year}-D.parquet")
        for year in range(start_year, end_year + 1)
        if os.path.exists(os.path.join(DAILY_HISTORY_FOLDER, f"{year}-D.parquet"))
    ]

    if not files:
        return pd.DataFrame()

    filter_expr = (
        (ds.field("ticker") == symbol)
        & (ds.field("window_start") >= timestamp_start_ns)
        & (ds.field("window_start") < timestamp_end_ns)
    )

    dataset = ds.dataset(files, format="parquet")
    table = dataset.to_table(filter=filter_expr)
    df = table.to_pandas()

    return df


app = FastAPI()
# Allowed sources (you can replace "*" with a list of domains)
origins = [
    "http://localhost",
    "http://localhost:3000",  # If the frontend is on React/Vue/Angular
    "http://127.0.0.1:8000",
    "*",  # Allow everyone (dangerous for production!)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Authorized sources
    allow_credentials=True,
    allow_methods=["*"],  # Allowed methods (GET, POST, PUT, DELETE, etc.)
    allow_headers=["*"],  # Allowed titles
)


@app.get("/history/daily")
def get_daily_history(symbol: str, start_date: str, end_date: str) -> List[Dict]:
    try:
        df = read_daily_history(symbol.strip().upper(), start_date, end_date)

        if df.empty:
            print(f"No data found for {symbol} from {start_date} to {end_date}")
            return []

        df = df.rename(columns={"window_start": "timestamp"})
        # Converting timestamp from nanoseconds to seconds
        df["timestamp"] = df["timestamp"] // 1_000_000_000

        return df.to_dict(orient="records")

    except KeyError as e:
        print(f"KeyError: Column not found - {e}")
        return []

    except ValueError as e:
        print(f"ValueError: Invalid data - {e}")
        return []

    except Exception as e:
        print(f"Unexpected error: {e}")
        return []


@app.get("/history/daily/next-available-date")
def get_daily_history_next_available_date(symbol: str, date: str):
    next_date: str | None = find_next_available_daily_history(
        symbol.strip().upper(), date
    )

    if next_date:
        ts = int(pd.to_datetime(next_date).timestamp())
        return {"date": next_date, "timestamp": ts}
    else:
        return {"date": None, "timestamp": None}


if __name__ == "__main__":
    # find_next_available_daily_history("SPY", "2023-09-25")
    # print(read_daily_history("AAL", "2023-09-01", "2023-10-01"))
    uvicorn.run(app, host="0.0.0.0", port=8000)  # hotreload unavailable
"""
run from cmd for hotreload
cd src
uvicorn api:app --reload
"""
