import os
import requests
import json
from typing import Optional
import polars as pl
from datetime import datetime
import logging

from utils.thingsboard_api import get_jwt_token, get_telemetry_data, get_earliest_timestamp
from utils.config_files import load_json_config
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

try:
    # Create a persistent session.
    with requests.Session() as session:
        # Retrieve the JWT token using the session.
        jwt_token: str = get_jwt_token(THINGSBOARD_HOST,
                                       THINGSBOARD_USER_NAME,
                                       THINGSBOARD_USER_PASSWORD,
                                       session=session)

        local_latest_ts = get_local_latest_timestamp(device_name)

        if local_latest_ts is None:
            cloud_earliest_ts = get_earliest_timestamp(
                host=THINGSBOARD_HOST,
                jwt_token=jwt_token,
                device_id=device_id,
                session=session,
            )
            logging.info(f"Cloud earliest timestamp: {cloud_earliest_ts}")

except requests.exceptions.RequestException as e:
    logging.error(f"An HTTP error occurred: {e}")
except ValueError as e:
    logging.error(f"Configuration error: {e}")
