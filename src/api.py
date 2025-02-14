from logging_config import logger, log_execution_time
from env import DAILY_HISTORY_FOLDER

import pandas as pd
import pyarrow.dataset as ds
import os
from fastapi import FastAPI
import uvicorn


@log_execution_time
def read_daily_history(symbol, start_date, end_date):
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


@app.get("/history/daily")
def get_daily_history(symbol: str, start_date: str, end_date: str):
    df = read_daily_history(symbol.strip().upper(), start_date, end_date)
    df = df.rename(columns={"window_start": "timestamp"})
    # Convert timestamp from nanoseconds to seconds
    df["timestamp"] = df["timestamp"] // 1_000_000_000

    return df.to_dict(orient="records")


if __name__ == "__main__":
    # print(read_daily_history("AAL", "2020-01-01", "2025-01-01"))
    uvicorn.run(app, host="0.0.0.0", port=8000)  # hotreload unavailable
"""
run from cmd for hotreload
cd src
uvicorn api:app --reload
"""
