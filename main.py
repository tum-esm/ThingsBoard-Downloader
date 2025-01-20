import os
import requests
import time
from typing import Dict, Any, Optional

# Get credentials from environment variables
THINGSBOARD_HOST = os.getenv("THINGSBOARD_HOST", "http://localhost:8080")
THINGSBOARD_USER_NAME = os.getenv("THINGSBOARD_USER_NAME", "username")
THINGSBOARD_USER_PASSWORD = os.getenv("THINGSBOARD_USER_PASSWORD", "password")


# Function to authenticate and retrieve JWT token
def get_jwt_token(host: str, username: str, password: str) -> str:
    login_url = f"{host}/api/auth/login"
    payload = {"username": username, "password": password}
    headers = {"Content-Type": "application/json"}

    response = requests.post(login_url, json=payload, headers=headers)
    response.raise_for_status()  # Raises HTTPError for 4xx/5xx responses
    return response.json().get("token")


# Function to fetch telemetry data
def get_telemetry_data(host: str,
                       jwt_token: str,
                       device_id: str,
                       keys: list[str],
                       interval: Optional[int] = None,
                       startTS: Optional[int] = None,
                       endTS: Optional[int] = None,
                       agg: Optional[str] = None,
                       limit: Optional[int] = None) -> Dict[str, Any]:
    """
    From ThingsBoard API documentation:
    
    keys - comma-separated list of telemetry keys to fetch.
    startTs - Unix timestamp that identifies the start of the interval in milliseconds.
    endTs - Unix timestamp that identifies the end of the interval in milliseconds.
    interval - the aggregation interval, in milliseconds.
    agg - the aggregation function. One of MIN, MAX, AVG, SUM, COUNT, NONE.
    limit - the max amount of data points to return or intervals to process."""

    telemetry_url = f"{host}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"

    # Convert keys to comma-separated string if provided as a list
    if isinstance(keys, list):
        keys = ",".join(keys)  # type: ignore

    # Build params dynamically, excluding None values
    params = {
        "keys": keys,
        "interval": interval,
        "startTs": startTS,
        "endTs": endTS,
        "agg": agg,
        "limit": limit
    }
    params = {key: value for key, value in params.items() if value is not None}

    headers = {
        "Content-Type": "application/json",
        "X-Authorization": f"Bearer {jwt_token}"
    }

    response = requests.get(telemetry_url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


# Main script
if __name__ == "__main__":
    try:
        # Ensure all required environment variables are set
        if not all([
                THINGSBOARD_HOST, THINGSBOARD_USER_NAME,
                THINGSBOARD_USER_PASSWORD
        ]):
            raise ValueError(
                "Environment variables for ThingsBoard credentials are not properly set."
            )

        # Authenticate and retrieve the JWT token
        jwt_token = get_jwt_token(THINGSBOARD_HOST, THINGSBOARD_USER_NAME,
                                  THINGSBOARD_USER_PASSWORD)
        print(f"JWT Token: {jwt_token}")

        # Define telemetry parameters
        device_id = "875886a0-cf6f-11ef-9db8-6112647d69f5"
        keys = ["gmp343_filtered", "sht45_humidity",
                "bme280_pressure"]  # Provide keys as a list
        interval = 60000
        startTS = int(time.time() * 1000 - 60000)
        endTS = int(time.time() * 1000)
        limit = 100

        # Fetch telemetry data
        telemetry_data = get_telemetry_data(
            host=THINGSBOARD_HOST,
            jwt_token=jwt_token,
            device_id=device_id,
            keys=keys,  # Pass keys as a list
            startTS=startTS,
            endTS=endTS,
            interval=interval,
            limit=limit,
        )
        print("Telemetry Data:", telemetry_data)

    except requests.exceptions.RequestException as e:
        print(f"An HTTP error occurred: {e}")
    except ValueError as e:
        print(f"Configuration error: {e}")
