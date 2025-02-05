import json
import os
import logging
import polars as pl

from .paths import CONFIG_DIR


def load_json_config(file_name: str) -> dict:
    """Load JSON file from config folder."""
    assert (file_name.endswith('.json'))
    assert (os.path.exists(os.path.join(CONFIG_DIR, file_name)))

    config_path = os.path.join(CONFIG_DIR, file_name)

    with open(config_path, 'r') as f:
        config = json.load(f)
    return config


def get_local_latest_timestamp(device_name):
    """
    Check if a local Parquet file exists and return the max timestamp.
    Assumes the data has a 'timestamp' column in Unix epoch seconds.
    Returns None if the file doesn't exist or is empty.
    """
    filename = os.path.join(DATA_DIR, f"{device_name}.parquet")
    if not os.path.exists(filename):
        logging.info(
            f"No local file for {device_name}. Starting from cloud earliest timestamp."
        )
        return None
    try:
        df = pl.read_parquet(filename)
        if df.height == 0 or 'creation_timestamp' not in df.columns:
            return None
        latest = df.select(pl.col("creation_timestamp").max()).to_series()[0]
        logging.info(f"Latest local timestamp for {device_name}: {latest}")
        return latest
    except Exception as e:
        logging.error(f"Error reading local file for {device_name}: {e}")
        return None
