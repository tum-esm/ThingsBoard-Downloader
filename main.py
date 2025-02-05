import os
import requests
from typing import Optional
import polars as pl
from datetime import datetime
import logging

from utils.thingsboard_api import get_jwt_token, get_telemetry_data, get_earliest_timestamp
from utils.file_operations import load_json_config
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
devices = pl.read_json("config/devices.json")

try:
    # Ensure all required environment variables are set
    if not all(
        [THINGSBOARD_HOST, THINGSBOARD_USER_NAME, THINGSBOARD_USER_PASSWORD]):
        raise ValueError(
            "Environment variables for ThingsBoard credentials are not properly set."
        )

    # Create a persistent session.
    with requests.Session() as session:
        # Retrieve the JWT token using the session.
        jwt_token: str = get_jwt_token(THINGSBOARD_HOST,
                                       THINGSBOARD_USER_NAME,
                                       THINGSBOARD_USER_PASSWORD,
                                       session=session)

        # Load devices configuration (assuming load_json_config is correctly implemented).
        from utils.file_operations import load_json_config
        devices = load_json_config("devices.json")
        # Replace "acropolis-6" with a valid key from your devices.json.
        device_id: str = devices.get("acropolis-6", "invalid_device_id")

        # Retrieve the earliest timestamp using the same session.
        earliest_ts: Optional[int] = get_earliest_timestamp(
            host=THINGSBOARD_HOST,
            jwt_token=jwt_token,
            device_id=device_id,
            key="gmp343_raw",
            session=session)

except requests.exceptions.RequestException as e:
    print(f"An HTTP error occurred: {e}")
except ValueError as e:
    print(f"Configuration error: {e}")
