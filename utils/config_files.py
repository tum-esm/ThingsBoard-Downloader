import json
import os

from .paths import CONFIG_DIR


def load_json_config(file_name: str) -> dict:
    """Load JSON file from config folder."""
    assert (file_name.endswith('.json'))
    assert (os.path.exists(os.path.join(CONFIG_DIR, file_name)))

    config_path = os.path.join(CONFIG_DIR, file_name)

    with open(config_path, 'r') as f:
        config = json.load(f)
    return config
