import logging
from pythonjsonlogger import jsonlogger
import sys

def setup_logger(name: str, log_file: str = "logs/app.log"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Console Handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)

    # File Handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    # JSON Formatter
    formatter = jsonlogger.JsonFormatter(
        fmt='%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    console.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file_handler)
    return logger