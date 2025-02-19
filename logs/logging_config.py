import logging
import os

def setup_logging(log_filename):
    """Setup logging configuration with just a filename, automatically placing it in logs/."""
    logs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))  # logs/ directory
    log_file = os.path.join(logs_dir, log_filename)  # Full log file path

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    return logging.getLogger()
