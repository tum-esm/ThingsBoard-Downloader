import os
import logging
import polars as pl
from typing import Optional, Dict, List, Any

from .paths import DATA_DIR


def load_local_data(path: str, file_name: str) -> Optional[pl.DataFrame]:
    """
    Attempt to read the local Parquet file for a given device.
    Returns a Polars DataFrame if the file exists and is not empty, otherwise None.
    """

    filepath = os.path.join(path, f"{file_name}.parquet")

    if os.path.exists(filepath):
        try:
            df = pl.read_parquet(filepath)
            if df.height > 0:
                return df
        except Exception as e:
            logging.error(f"Error reading local data for {file_name}: {e}")
    return None


def save_local_data(path: str, file_name: str, df: pl.DataFrame) -> None:
    """
    Save the given DataFrame to the device's local Parquet file.
    If a file already exists, combine the old and new data and write back.
    """
    filepath = os.path.join(path, f"{file_name}.parquet")

    existing_df = load_local_data(path, file_name)

    try:
        if existing_df is not None:
            # Combine and remove duplicates based on timestamp
            combined_df = pl.concat([existing_df, df],
                                    how="diagonal").unique(subset=["ts"],
                                                           keep="last")
        else:
            combined_df = df

        combined_df.sort("ts").write_parquet(filepath)
        logging.info(
            f"Saved data for device {file_name}. Total rows now: {combined_df.height}"
        )
    except Exception as e:
        logging.error(f"Error saving data for device {file_name}: {e}")


def get_local_latest_timestamp(path: str, file_name: str) -> Optional[int]:
    """
    Check if a local Parquet file exists and return the max timestamp.
    Assumes the data has a 'timestamp' column in Unix epoch seconds.
    Returns None if the file doesn't exist or is empty.
    """
    df = load_local_data(path=path, file_name=file_name)
    if df is None:
        logging.info(f"No local file for {file_name}.")
        return None
    try:
        if df.height == 0 or 'ts' not in df.columns:
            return None
        latest = df.select(pl.col("ts").max()).to_series()[0]
        return latest
    except Exception as e:
        logging.error(f"Error getting latest timestamp for {file_name}: {e}")
        return None


def safe_convert_to_float(x: Any) -> Optional[float]:
    """
    Convert a value to float, handling boolean strings.
    
    - If x is already a bool, convert it to 1.0 (if True) or 0.0 (if False).
    - If x is a string and equals "true" or "false" (case-insensitive), return 1.0 or 0.0.
    - Otherwise, attempt to convert x to float.
    - If conversion fails, return None.
    """
    if isinstance(x, bool):
        return 1.0 if x else 0.0
    if isinstance(x, str):
        lower_x = x.lower().strip()
        if lower_x == "true":
            return 1.0
        elif lower_x == "false":
            return 0.0
    try:
        return round(float(x), 2)
    except (ValueError, TypeError):
        return None


def telemetry_to_dataframe(
        data: Dict[str, List[Dict[str, Any]]]) -> pl.DataFrame:
    """
    Convert telemetry data (a dict where each key maps to a list of measurements)
    into a Polars DataFrame where all measurements sharing the same timestamp are on the same row.
    
    Expected input structure:
    {
        "key1": [{"ts": 1738759266000, "value": "407.1"}, ...],
        "key2": [{"ts": 1738759266000, "value": "451.7"}, ...],
        ...
    }
    
    Returns a DataFrame with one column for the timestamp ('ts') and one column per key.
    The values are converted to floats.
    """
    # First, convert the dictionary into a list of rows in long format.
    rows = []
    for key, measurements in data.items():
        for m in measurements:
            # Convert the 'value' to float. Adjust conversion if needed.
            value = safe_convert_to_float(m["value"])
            rows.append({"ts": m["ts"], "key": key, "value": value})

    # Create a Polars DataFrame from the long list.
    # Force the schema so that "value" is always a float (Float64)
    df_long = pl.DataFrame(rows,
                           schema={
                               "ts": pl.Int64,
                               "key": pl.Utf8,
                               "value": pl.Float64
                           })

    return df_long
