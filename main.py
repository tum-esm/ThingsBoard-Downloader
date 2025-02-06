import os
import requests
import json
import time
import polars as pl
from datetime import datetime
import logging

from utils.thingsboard_api import get_jwt_token, get_telemetry_data, get_earliest_timestamp, get_telemetry_keys
from utils.config_files import load_json_config, add_missing_telemetry_keys, get_keys_to_download
from utils.data_files import get_local_latest_timestamp, telemetry_to_dataframe, save_local_data
from utils.paths import LOG_DIR

# Create a log filename with the current date (YYYY-MM-DD)
log_filename = os.path.join(LOG_DIR,
                            f"{datetime.now().strftime('%Y-%m-%d')}.log")

logging.basicConfig(filename=log_filename,
                    level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# read device ids from config file
devices: json = load_json_config("devices.json")
device_name: str = "acropolis-6"
device_id: str = devices.get("acropolis-6")

# Create a persistent session.
with requests.Session() as session:
    # Retrieve the JWT token using the session.
    jwt_token: str = get_jwt_token(session=session)

    latest_local_ts = get_local_latest_timestamp(device_name)

    if latest_local_ts is None:
        cloud_earliest_ts = get_earliest_timestamp(jwt_token=jwt_token,
                                                   device_id=device_id,
                                                   session=session)
    else:
        cloud_earliest_ts = None

    print(latest_local_ts, cloud_earliest_ts)

    # start and end timestamp for downloading data
    startTS: int = latest_local_ts or cloud_earliest_ts
    logging.info(
        f"Timestamp to start downloading from: {datetime.fromtimestamp(startTS / 1000)}"
    )
    endTS = int(time.time() * 1000)

    # Get all device specific telemetry keys from ThingsBoard
    keys = get_telemetry_keys(jwt_token, device_id, session=session)
    # Compares local and remote keys and add missing keys to the config file
    add_missing_telemetry_keys(keys)
    keys = get_keys_to_download()

    # Start downloading data from ThingsBoard
    telemetry_data: json = get_telemetry_data(jwt_token=jwt_token,
                                              device_id=device_id,
                                              keys=keys,
                                              startTS=startTS,
                                              endTS=endTS,
                                              limit=100,
                                              orderBy="ASC",
                                              session=session)
    #print(telemetry_data)

    # TODO: download for each key seperately until 1 line return, store in chunks, and then merge

    df = telemetry_to_dataframe(telemetry_data)
    df = df.with_columns(pl.from_epoch("ts", time_unit="ms").alias("datetime"))
    print(df.select("ts", "datetime", "gmp343_raw").sort("ts"))

    # Save the data to a local Parquet file
    save_local_data(device_name, df)
