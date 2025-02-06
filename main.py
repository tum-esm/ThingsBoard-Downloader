import os
import requests
import json
import time
import polars as pl
from datetime import datetime
import logging

from utils.thingsboard_api import get_jwt_token, get_telemetry_data, get_earliest_thingsboard_timestamp
from utils.config_files import load_json_config, get_keys_to_download
from utils.data_files import get_local_latest_timestamp, telemetry_to_dataframe, save_local_data
from utils.paths import LOG_DIR

# Create a log filename with the current date (YYYY-MM-DD)
log_filename = os.path.join(LOG_DIR,
                            f"{datetime.now().strftime('%Y-%m-%d')}.log")

logging.basicConfig(filename=log_filename,
                    level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# config
config = load_json_config("config.json")
devices = load_json_config("devices.json")
keys = get_keys_to_download()

# (debugging) read device ids from config file
device_name: str = "acropolis-6"
device_id: str = devices.get("acropolis-6")

# Create a persistent session.
with requests.Session() as session:
    # Retrieve the JWT token using the session.
    jwt_token: str = get_jwt_token(session=session)

    # Get the latest timesstamp to start downloading data from
    latest_local_ts = get_local_latest_timestamp(device_name)

    if latest_local_ts is None:
        cloud_earliest_ts = get_earliest_thingsboard_timestamp(
            jwt_token=jwt_token, device_id=device_id, session=session)
    else:
        cloud_earliest_ts = None

    # start timestamp for downloading data
    startTS: int = latest_local_ts or cloud_earliest_ts

    config_start_ts = config["download"]["start_unix_ms"]
    if config_start_ts and config_start_ts > startTS:
        startTS = config_start_ts

    logging.info(
        f"Timestamp to start downloading from: {datetime.fromtimestamp(startTS / 1000)}"
    )

    # end timestamp for downloading data
    endTS = int(time.time() * 1000)

    config_end_ts = config["download"]["end_unix_ms"]
    if config_end_ts and config_end_ts < endTS:
        endTS = config_end_ts

    logging.info(
        f"Timestamp to stop downloading at: {datetime.fromtimestamp(endTS / 1000)}"
    )

    # Download data for each key
    df_chunk = []

    for key in keys:
        print(f"Downloading data for key: {key}")

        current_timestamp = startTS

        while (True):
            # Start downloading data from ThingsBoard
            telemetry_data: json = get_telemetry_data(
                jwt_token=jwt_token,
                device_id=device_id,
                keys=key,
                startTS=current_timestamp,
                endTS=endTS,
                limit=1000,
                orderBy="ASC",
                session=session)

            if len(telemetry_data) == 0 or len(telemetry_data[key]) == 1:
                break

            df_key = telemetry_to_dataframe(telemetry_data)

            df_chunk.append(df_key)

            current_timestamp = df_key.select(
                pl.col("ts").max()).to_series()[0] + 1

    print(f"Starting pivot for device: {device_name}")
    # Pivot the DataFrame: index by "ts", columns are "key", values are "value".
    # This groups rows with the same timestamp into a single row.
    df_long = pl.concat(df_chunk)
    print(df_long)

    df_wide=df_long.sort("ts") \
        .pivot(index="ts", columns="key", values="value") \
        .with_columns(pl.from_epoch("ts", time_unit="ms").alias("datetime"))

    #.pivot(index="ts", columns="key", values="value") \

    print(df_wide.tail(20))

    # Save the data to a local Parquet file
    #save_local_data(device_name, df)
