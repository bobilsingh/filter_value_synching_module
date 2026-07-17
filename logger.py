import logging
import os
from logging.handlers import TimedRotatingFileHandler
from config import LOG_FILE_PATH, LOG_LEVEL

def setup_logger():
    """Sets up a rotating file logger and stream logger."""
    log_dir = os.path.dirname(LOG_FILE_PATH)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger("filter_sync")
    logger.setLevel(LOG_LEVEL)

    # Prevent duplicate handlers if setup_logger is called multiple times
    if not logger.handlers:
        # Create formatter with ThreadName to track parallel executions
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(threadName)s] (%(filename)s:%(lineno)d) - %(message)s"
        )

        # File Handler (rotating daily, keeping logs for 7 days)
        file_handler = TimedRotatingFileHandler(
            LOG_FILE_PATH, when="D", interval=1, backupCount=7
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console Handler (stdout)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

# Initialize and expose logger instance
logger = setup_logger()
