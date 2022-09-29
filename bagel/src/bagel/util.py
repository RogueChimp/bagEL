from datetime import datetime
import json

import pandas as pd


def format_table_name(name: str) -> str:
    return name.lower().replace(" ", "_").replace("-", "_")


def format_blob_name(
    system, table, timestamp, log=False, file_format=None, file_name=None
):
    if file_format is None:
        file_format = "log" if log else "json"

    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    day = timestamp.strftime("%d")

    full_date = format_timestamp_to_str(timestamp)

    _file_name = f"{table}_{full_date}"
    if file_name:
        _file_name += f"-{file_name}"

    if log:
        file_type = "log"
    else:
        file_type = "data"
    file_name = (
        f"{system}/{file_type}/{table}/{year}/{month}/{day}/{_file_name}.{file_format}"
    )
    return file_name


def format_timestamp_to_str(timestamp: datetime):
    full_date = timestamp.strftime("%Y_%m_%dT%H_%M_%S_%fZ")
    return full_date


def format_dict_to_json_binary(d):
    return bytes(json.dumps(d, default=str), "utf-8")


def extract_date_ranges(
    last_run_timestamp, current_timestamp, historical_batch, historical_frequency
):
    if historical_batch:
        date_ranges = get_historical_batch_ranges(
            last_run_timestamp, current_timestamp, historical_frequency
        )
    if not historical_batch or len(date_ranges) == 1:
        date_ranges = [last_run_timestamp, current_timestamp]

    return date_ranges


def get_historical_batch_ranges(start, end, freq=None):
    if not freq:
        freq = "D"
    return pd.date_range(start=start, end=end, freq=freq)


def get_current_timestamp():
    """Ccommon function to get the current time."""
    return datetime.utcnow()
