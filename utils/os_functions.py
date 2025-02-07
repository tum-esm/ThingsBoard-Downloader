import os
import logging
from pathlib import Path
from typing import Optional


def ensure_data_dir(dir_path: str) -> None:
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        logging.info(f"Created folder: {dir_path}")


def get_latest_year_folder(base_dir: str) -> Optional[str]:
    """
    Scans the given base_dir for subdirectories whose names are digits (e.g. "2025") and returns the
    path to the one with the highest value. If no such folder exists, returns None.
    """
    base = Path(base_dir)
    # Filter for directories with names that are all digits.
    year_dirs = [d for d in base.iterdir() if d.is_dir() and d.name.isdigit()]
    if not year_dirs:
        return None
    # Convert folder names to int and choose the maximum.
    latest_year_dir = max(year_dirs, key=lambda d: int(d.name))
    return str(latest_year_dir)
