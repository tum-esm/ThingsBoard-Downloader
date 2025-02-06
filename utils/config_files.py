import json
import logging
import os
from typing import List, Optional, Dict, Any

from .paths import CONFIG_DIR


def load_json_config(file_name: str) -> dict:
    """Load JSON file from config folder."""
    assert (file_name.endswith('.json'))
    config_path = os.path.join(CONFIG_DIR, file_name)

    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    else:
        logging.error("Config file not found: %s", config_path)
        raise FileNotFoundError(f"Config file not found: {config_path}")


def dump_json_config(file_name: str, config: dict) -> None:
    """Load JSON file from config folder."""
    assert (file_name.endswith('.json'))
    config_path = os.path.join(CONFIG_DIR, file_name)

    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4)


def add_missing_telemetry_keys(telemetry_keys: Dict) -> None:
    """Add missing telemetry keys to the config file and set the default value to True.
    Will not overwrite existing keys and values."""

    # Load the current configuration from keys.json. If not found, start with an empty dict.
    config: Optional[Dict[str, Any]] = load_json_config("keys.json")
    if config is None:
        config = {}

    # Determine which keys are missing.
    missing_keys = [key for key in telemetry_keys if key not in config]

    # Add each missing key with default value True.
    for key in missing_keys:
        config[key] = True

    # If any keys were added, dump the updated config back to the file.
    if missing_keys:
        try:
            dump_json_config("keys.json", config)
            logging.info("Added missing telemetry keys %s to config file.",
                         missing_keys)
        except Exception as e:
            logging.error("Error saving updated telemetry keys config: %s", e)


def get_keys_to_download() -> List[str]:
    """Get the list of telemetry keys to download based on the config file."""
    config: Optional[Dict[str, Any]] = load_json_config("keys.json")
    if config is None:
        return []
    return [key for key, value in config.items() if value]
