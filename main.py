import os
import requests
import json
from typing import Optional
import polars as pl
from datetime import datetime
import logging

from utils.thingsboard_api import get_jwt_token, get_telemetry_data, get_earliest_timestamp, get_telemetry_keys
from utils.config_files import load_json_config, add_missing_telemetry_keys
from utils.data_files import get_local_latest_timestamp
from utils.paths import LOG_DIR

# Create a log filename with the current date (YYYY-MM-DD)
log_filename = os.path.join(LOG_DIR,
                            f"{datetime.now().strftime('%Y-%m-%d')}.log")

logging.basicConfig(filename=log_filename,
                    level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Get credentials from environment variables
THINGSBOARD_HOST = os.getenv("THINGSBOARD_HOST", "http://localhost:8080")
THINGSBOARD_USER_NAME = os.getenv("THINGSBOARD_USER_NAME", "username")
THINGSBOARD_USER_PASSWORD = os.getenv("THINGSBOARD_USER_PASSWORD", "password")

# read device ids from config file
devices: json = load_json_config("devices.json")
device_name: str = "acropolis-6"
device_id: str = devices.get("acropolis-6")

# Create a persistent session.
with requests.Session() as session:
    # Retrieve the JWT token using the session.
    jwt_token: str = get_jwt_token(THINGSBOARD_HOST,
                                   THINGSBOARD_USER_NAME,
                                   THINGSBOARD_USER_PASSWORD,
                                   session=session)

    latest_local_ts = get_local_latest_timestamp(device_name)

    if latest_local_ts is None:
        cloud_earliest_ts = get_earliest_timestamp(
            host=THINGSBOARD_HOST,
            jwt_token=jwt_token,
            device_id=device_id,
            session=session,
        )

    startTS: int = latest_local_ts or cloud_earliest_ts
    logging.info(f"Timestamp to start downloading from: {startTS}")

    keys = get_telemetry_keys(THINGSBOARD_HOST,
                              jwt_token,
                              device_id,
                              session=session)

    add_missing_telemetry_keys(keys)

    # Start downloading data from ThingsBoard
    # telemetry_data: json = get_telemetry_data(
    #     host=THINGSBOARD_HOST,
    #     jwt_token=jwt_token,
    #     device_id=device_id,
    #     keys=["gmp343_raw"],
    #     startTS=startTS,
    #     session=session,
    # )
