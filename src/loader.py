import os
import glob
import itertools
import pandas as pd
from datetime import datetime
import pytz
import pyarrow.dataset as ds

from logging_config import logger, log_execution_time
from env import FLAT_DAILY_DATA_FOLDER, DAILY_HISTORY_FOLDER


def extract_year_from_file_path(file_path):
    return file_path.split(os.sep)[-3]


def extract_filename_from_file_path(file_path):
    return os.path.basename(file_path)


def get_all_history_files(directory):
    files = glob.glob(os.path.join(directory, "**", "*.csv"), recursive=True)
    files.sort(
        key=lambda f: (
            extract_year_from_file_path(f),
            extract_filename_from_file_path(f),
        )
    )

    return files


# @log_execution_time
def load_history_csv(file):
    df = pd.read_csv(file)
    sorted_df = df.sort_values(by=["ticker", "window_start"])
    return sorted_df


@log_execution_time
def df_to_parquet_file(year, df):
    file = os.path.join(DAILY_HISTORY_FOLDER, f"{year}-D.parquet")
    df.to_parquet(file, engine="pyarrow", compression="snappy")
    print(file)


@log_execution_time
def convert_daily_history_csv_to_parquet():
    files = get_all_history_files(FLAT_DAILY_DATA_FOLDER)
    ny_tz = pytz.timezone("America/New_York")

    for year, group in itertools.groupby(files, key=extract_year_from_file_path):
        file_list = list(group)
        df_list = [load_history_csv(file) for file in file_list]
        df = pd.concat(df_list, ignore_index=True)
        df["ticker"] = df["ticker"].astype("category")
        # df["time"] = pd.to_datetime(df["window_start"])
        # df["time"] = df["time"].dt.tz_localize("UTC").dt.tz_convert(ny_tz)
        # df = df.drop(columns=["window_start"])

        print(f"Year {year}: {len(file_list)} files, total rows: {len(df)}")
        # for ticker, group in df.groupby("ticker"):
        #     print(ticker, group, len(group))
        #     break

        # save to parquet file
        df_to_parquet_file(year, df)

    return
    for source in files:
        df = load_history_csv(source)
        logger.info(f"{source}, {len(df)}")

        for ticker, group in df.groupby("ticker"):
            assert len(group) == 1


@log_execution_time
def read_daily_history(symbol, start_date, end_date):
    start_year = int(start_date.split("-")[0])
    end_year = int(end_date.split("-")[0])

    timestamp_start_sec = pd.to_datetime(start_date).timestamp()
    timestamp_start_ns = int(timestamp_start_sec * 1e9)

    timestamp_end_sec = pd.to_datetime(end_date).timestamp()
    timestamp_end_ns = int(timestamp_end_sec * 1e9)

    files = [
        os.path.join(DAILY_HISTORY_FOLDER, f"{year}-D.parquet")
        for year in range(start_year, end_year + 1)
        if os.path.exists(os.path.join(DAILY_HISTORY_FOLDER, f"{year}-D.parquet"))
    ]

    if not files:
        print(pd.DataFrame())
        return pd.DataFrame()

    filter_expr = (
        (ds.field("ticker") == symbol)
        & (ds.field("window_start") >= timestamp_start_ns)
        & (ds.field("window_start") < timestamp_end_ns)
    )

    dataset = ds.dataset(files, format="parquet")
    table = dataset.to_table(filter=filter_expr)
    df = table.to_pandas()

    print(df)
    return df


if __name__ == "__main__":
    # convert_daily_history_csv_to_parquet()
    read_daily_history("AAL", "2020-01-01", "2025-01-01")
