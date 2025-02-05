import os
import logging
import polars as pl
from typing import Optional

from .paths import DATA_DIR


def load_local_data(device_name: str) -> Optional[pl.DataFrame]:
    """
    Attempt to read the local Parquet file for a given device.
    Returns a Polars DataFrame if the file exists and is not empty, otherwise None.
    """
    filepath = os.path.join(DATA_DIR, f"{device_name}.parquet")
    if os.path.exists(filepath):
        try:
            df = pl.read_parquet(filepath)
            if df.height > 0:
                return df
        except Exception as e:
            logging.error("Error reading local data for %s: %s", device_name,
                          e)
    return None


def save_local_data(device_name: str, df: pl.DataFrame) -> None:
    """
    Save the given DataFrame to the device's local Parquet file.
    If a file already exists, combine the old and new data and write back.
    """
    filepath = os.path.join(DATA_DIR, f"{device_name}.parquet")
    existing_df = load_local_data(device_name)

    try:
        if existing_df is not None:
            # Combine and remove duplicates based on timestamp
            combined_df = pl.concat([existing_df, df],
                                    how="diagonal").unique(subset=["ts"])
        else:
            combined_df = df

        combined_df.write_parquet(filepath)
        logging.info("Saved data for device %s. Total rows now: %d",
                     device_name, combined_df.height)
    except Exception as e:
        logging.error("Error saving data for device %s: %s", device_name, e)


def get_local_latest_timestamp(device_name: str) -> Optional[int]:
    """
    Check if a local Parquet file exists and return the max timestamp.
    Assumes the data has a 'timestamp' column in Unix epoch seconds.
    Returns None if the file doesn't exist or is empty.
    """
    df = load_local_data(device_name)
    if df is None:
        logging.info(f"No local file for {device_name}.")
        return None
    try:
        if df.height == 0 or 'ts' not in df.columns:
            return None
        latest = df.select(pl.col("ts").max()).to_series()[0]
        logging.info(f"Latest local timestamp for {device_name}: {latest}")
        return latest
    except Exception as e:
        logging.error("Error getting latest timestamp for %s: %s", device_name,
                      e)
        return None
