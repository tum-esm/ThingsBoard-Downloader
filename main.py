import os
import requests
import polars as pl
from datetime import datetime
import logging

from utils.thingsboard_api import get_jwt_token, get_telemetry_data
from utils.config_files import load_json_config, get_keys_to_download
from utils.data_files import telemetry_to_dataframe, save_local_data
from utils.download_interval import download_interval
from utils.os_functions import ensure_data_dir
from utils.paths import LOG_DIR, DATA_DIR

# Create a log file with the current date (YYYY-MM-DD)
log_filename = os.path.join(LOG_DIR,
                            f"{datetime.now().strftime('%Y-%m-%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format=
    "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(
        )  # This allows logs to be printed to the console as well
    ])

logging.info("=========================================")
logging.info("Starting data download from ThingsBoard")

# config
config = load_json_config("config.json")
devices = config["devices"]
keys = get_keys_to_download()

logging.info(f"Downloading data for keys: {keys}")

# Create a persistent session.
with requests.Session() as session:
    # Retrieve the JWT token using the session.
    jwt_token: str = get_jwt_token(session=session)

    for device_name, device_id in devices.items():
        try:
            # Get start and end timestamp for downloading data
            startTS, endTS = download_interval(jwt_token, device_name,
                                               device_id, session)

            logging.info(f"Downloading data for device: {device_name}.")

            df_chunk = []

            # Download data for each key
            for key in keys:
                current_timestamp = startTS

                # Download until no new data is returned & endTS is reached
                while (True):
                    telemetry_data = get_telemetry_data(
                        jwt_token=jwt_token,
                        device_id=device_id,
                        keys=key,
                        startTS=current_timestamp,
                        endTS=endTS,
                        agg=config["download"]["aggregation"],
                        interval=config["download"]["interval"],
                        limit=config["download"]["limit"] or 1000,
                        orderBy="ASC",
                        session=session)

                    if len(telemetry_data) == 0 or len(
                            telemetry_data[key]) == 1:
                        break

                    df_key = telemetry_to_dataframe(telemetry_data)
                    df_chunk.append(df_key)

                    # Update the current timestamp to the last timestamp in the downloaded chunk
                    current_timestamp = df_key.select(
                        pl.col("ts").max()).to_series()[0] + 1

            logging.info(f"Performing pivot for device: {device_name}")

            if len(df_chunk) == 0:
                logging.info(f"No data downloaded for device: {device_name}")
                continue

            df_long = pl.concat(df_chunk)

            # Pivot the DataFrame: index by "ts", columns: "key", values: "value".
            # This groups rows with the same timestamp into a single row.
            df_wide=df_long.sort("ts") \
                .pivot(index="ts", on="key", values="value") \
                .with_columns(pl.from_epoch("ts", time_unit="ms").alias("datetime"))

            # Save the data to a local Parquet file split by year
            for year in df_wide["datetime"].dt.year().unique().to_list():
                data_path = os.path.join(DATA_DIR, str(year))
                ensure_data_dir(data_path)

                save_local_data(
                    path=data_path,
                    file_name=device_name,
                    df=df_wide.filter(pl.col("datetime").dt.year() == year))
        except Exception as e:
            logging.error(f"Error downloading data for device: {device_name}")
            logging.error(e)
