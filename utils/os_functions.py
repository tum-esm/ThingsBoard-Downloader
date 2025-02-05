import os
import logging


def ensure_data_dir(dir_path: str) -> None:
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        logging.info(f"Created folder: {dir_path}")
