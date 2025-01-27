import os
import pandas as pd
from logging_config import logger, log_execution_time
from env import FLAT_DATA_FOLDER


def get_all_files(directory):
    files = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            files.append(os.path.join(root, filename))
    result = sorted(files, key=lambda x: os.path.basename(x))

    return result


# @log_execution_time
def load_history_csv(file):
    df = pd.read_csv(file)
    sorted_df = df.sort_values(by=["ticker", "window_start"])
    return sorted_df


@log_execution_time
def load():
    files = get_all_files(FLAT_DATA_FOLDER)
    for source in files:
        df = load_history_csv(source)
        logger.info(f"{source}, {len(df)}")

        for ticker, group in df.groupby("ticker"):
            assert len(group) == 1


if __name__ == "__main__":
    load()
