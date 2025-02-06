import os
import requests
from datetime import datetime
import logging

from utils.thingsboard_api import get_jwt_token, get_telemetry_keys
from utils.config_files import load_json_config, add_missing_telemetry_keys
from utils.paths import LOG_DIR

# Create a log filename with the current date (YYYY-MM-DD)
log_filename = os.path.join(LOG_DIR,
                            f"{datetime.now().strftime('%Y-%m-%d')}.log")

logging.basicConfig(filename=log_filename,
                    level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# config
devices = load_json_config("devices.json")

# Create a persistent session.
with requests.Session() as session:
    # Retrieve the JWT token using the session.
    jwt_token: str = get_jwt_token(session=session)

    for device in devices.keys():
        # Get all device specific telemetry keys from ThingsBoard
        keys = get_telemetry_keys(jwt_token,
                                  devices.get(device),
                                  session=session)
        # Compares local and remote keys and add missing keys to the config file
        add_missing_telemetry_keys(keys)
