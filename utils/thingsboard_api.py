import requests
import logging
import time
from typing import Dict, Any, Optional, List


# Function to authenticate and retrieve JWT token
def get_jwt_token(host: str,
                  username: str,
                  password: str,
                  session: Optional[requests.Session] = None) -> str:
    login_url: str = f"{host}/api/auth/login"
    payload: Dict[str, str] = {"username": username, "password": password}
    headers: Dict[str, str] = {"Content-Type": "application/json"}

    if session is None:
        response = requests.post(login_url, json=payload, headers=headers)
    else:
        response = session.post(login_url, json=payload, headers=headers)
    response.raise_for_status()  # Raises HTTPError for 4xx/5xx responses

    token: Optional[str] = response.json().get("token")
    if token is None:
        raise ValueError("JWT token not found in the response.")
    return token


# Function to fetch telemetry data
def get_telemetry_data(
        host: str,
        jwt_token: str,
        device_id: str,
        keys: List[str],
        interval: Optional[int] = None,
        startTS: Optional[int] = None,
        endTS: Optional[int] = None,
        agg: Optional[str] = None,
        limit: Optional[int] = None,
        orderBy: Optional[str] = None,
        session: Optional[requests.Session] = None) -> Dict[str, Any]:
    """
    From ThingsBoard API documentation:
    
    keys - comma-separated list of telemetry keys to fetch.
    startTs - Unix timestamp that identifies the start of the interval in milliseconds.
    endTs - Unix timestamp that identifies the end of the interval in milliseconds.
    interval - the aggregation interval, in milliseconds.
    agg - the aggregation function. One of MIN, MAX, AVG, SUM, COUNT, NONE.
    limit - the max amount of data points to return or intervals to process.
    orderBy - the order of results. One of ASC, DESC.
    """
    # Ensure host has no trailing slash.
    host = host.rstrip("/")
    telemetry_url: str = f"{host}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"

    # Convert keys list to a comma-separated string.
    if isinstance(keys, list):
        keys = ",".join(keys)

    params: Dict[str, Any] = {
        "keys": keys,
        "interval": interval,
        "startTs": startTS,
        "endTs": endTS,
        "agg": agg,
        "limit": limit,
        "orderBy": orderBy
    }
    params = {k: v for k, v in params.items() if v is not None}

    headers: Dict[str, str] = {
        "Content-Type": "application/json",
        "X-Authorization": f"Bearer {jwt_token}"
    }

    if session is None:
        response = requests.get(telemetry_url, headers=headers, params=params)
    else:
        response = session.get(telemetry_url, headers=headers, params=params)
    try:
        response.raise_for_status()
    except requests.HTTPError as http_err:
        logging.error("HTTP error occurred: %s - %s", response.status_code,
                      response.text)
        raise http_err
    return response.json()


def get_earliest_timestamp(
    host: str,
    jwt_token: str,
    device_id: str,
    session: Optional[requests.Session] = None,
    key: str = "gmp343_raw",
) -> Optional[int]:
    startTS: int = 0
    limit: int = 1

    data: Dict[str, Any] = get_telemetry_data(
        host=host,
        jwt_token=jwt_token,
        device_id=device_id,
        keys=[key],
        startTS=startTS,
        endTS=int(time.time() * 1000),
        limit=limit,
        orderBy="ASC",
        session=session,
    )

    # Expected structure: { "gmp343_raw": [{'ts': timestamp, 'value': ...}, ...] }
    if key in data and isinstance(data[key], list) and len(data[key]) > 0:
        earliest_timestamp: int = int(data[key][0]['ts'])
        logging.info(
            "Earliest ThingsBoard timestamp for device %s and key '%s': %d",
            device_id, key, earliest_timestamp)
        return earliest_timestamp
    else:
        logging.info("No telemetry data found for device %s with key '%s'.",
                     device_id, key)
        return None


# Function to fetch telemetry data
def get_telemetry_keys(
        host: str,
        jwt_token: str,
        device_id: str,
        session: Optional[requests.Session] = None) -> Dict[str, Any]:
    """
    From ThingsBoard API documentation:
    
    Returns a set of unique attribute key names for the selected entity. The response will include merged key names set for all attribute scopes:

    SERVER_SCOPE - supported for all entity types;
    CLIENT_SCOPE - supported for devices;
    SHARED_SCOPE - supported for devices.
    Referencing a non-existing entity Id or invalid entity type will cause an error.
    """
    # Ensure host has no trailing slash.
    host = host.rstrip("/")
    telemetry_url: str = f"{host}/api/plugins/telemetry/DEVICE/{device_id}/keys/timeseries"

    headers: Dict[str, str] = {
        "Content-Type": "application/json",
        "X-Authorization": f"Bearer {jwt_token}"
    }

    if session is None:
        response = requests.get(telemetry_url, headers=headers)
    else:
        response = session.get(telemetry_url, headers=headers)
    try:
        response.raise_for_status()
    except requests.HTTPError as http_err:
        logging.error("HTTP error occurred: %s - %s", response.status_code,
                      response.text)
        raise http_err
    return response.json()
