import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logging(log_filename="ingestion.log"):
    """Setup logging configuration with automatic file placement in logs/ directory."""

    # Ensure logs directory exists
    logs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
    os.makedirs(logs_dir, exist_ok=True)  # Create if missing

    # Define log file path
    log_file = os.path.join(logs_dir, log_filename)

    # Create a rotating file handler (Max 5MB per file, keeps last 20 logs)
    handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=20)

    # Configure logging
    logging.basicConfig(
        handlers=[handler],
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Return a named logger
    logger = logging.getLogger("IngestionLogger")
    return logger
