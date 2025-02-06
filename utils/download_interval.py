from datetime import datetime
import os
import time
import logging
import requests
from typing import Tuple

from utils.thingsboard_api import get_earliest_thingsboard_timestamp
from utils.data_files import get_local_latest_timestamp
from utils.config_files import load_json_config
from utils.paths import DATA_DIR

config = load_json_config("config.json")


def download_interval(jwt_token: str, device_name: str, device_id: str,
                      session: requests.Session) -> Tuple[int, int]:

    # Get the latest timesstamp to start downloading data from
    data_path = os.path.join(DATA_DIR, "2025")  # TODO make 2025 dynamic
    latest_local_ts = get_local_latest_timestamp(path=data_path,
                                                 file_name=device_name)

    if latest_local_ts is None:
        cloud_earliest_ts = get_earliest_thingsboard_timestamp(
            jwt_token=jwt_token, device_id=device_id, session=session)
    else:
        cloud_earliest_ts = None

    # start timestamp for downloading data
    startTS: int = latest_local_ts or cloud_earliest_ts or 0

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

    return startTS, endTS
