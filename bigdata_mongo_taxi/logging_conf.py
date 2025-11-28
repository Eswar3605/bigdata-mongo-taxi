import logging
from logging.handlers import RotatingFileHandler
import os

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)


def setup_logging() -> None:
    logger = logging.getLogger()
    if logger.handlers:
        return  # avoid duplicate handlers if called multiple times

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s - %(message)s"
    )

    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, "app.log"), maxBytes=1_000_000, backupCount=3
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
