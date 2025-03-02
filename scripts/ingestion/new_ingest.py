import yaml
import os
import glob
import time
import pandas as pd

# **Set Kaggle API Credentials Before Importing Kaggle API**
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
credentials_path = os.path.join(project_root, "config", "credentials.yaml")

# Load credentials from YAML
with open(credentials_path, "r") as file:
    creds = yaml.safe_load(file)

# **Set Kaggle credentials as environment variables**
os.environ["KAGGLE_USERNAME"] = creds["kaggle"]["username"]
os.environ["KAGGLE_KEY"] = creds["kaggle"]["key"]

# **Now Import Kaggle API (After Setting Environment Variables)**
from kaggle.api.kaggle_api_extended import KaggleApi
from logs.logging_config import setup_logging

# Setup logging
logger = setup_logging("ingestion.log")

# Paths
kaggle_download_path = os.path.join(project_root, "data/raw/source_data")
local_file_path = os.path.join(project_root, "data/raw/local/customer_churn_dataset_local.csv")
processed_file_path = os.path.join(project_root, "data/raw/source_data/customer_data.csv")

# Ensure directories exist
os.makedirs(kaggle_download_path, exist_ok=True)
os.makedirs(os.path.dirname(local_file_path), exist_ok=True)


def ingest_kaggle_data(max_retries=3, delay=10):
    """Fetch data from Kaggle, ensuring old files are deleted and renamed properly."""
    attempt = 0
    while attempt < max_retries:
        try:
            # Initialize Kaggle API
            api = KaggleApi()
            api.authenticate()
            logger.info("Kaggle API authenticated successfully.")

            dataset = "blastchar/telco-customer-churn"
            logger.info(f"Downloading Kaggle dataset '{dataset}'...")

            # **Step 1: Delete all existing CSV files before downloading**
            old_csv_files = glob.glob(os.path.join(kaggle_download_path, "*.csv"))
            for file in old_csv_files:
                os.remove(file)
                logger.info(f"Deleted old CSV file: {file}")

            # **Step 2: Download dataset and unzip**
            api.dataset_download_files(dataset, path=kaggle_download_path, unzip=True)
            logger.info(f"Dataset '{dataset}' downloaded successfully to '{kaggle_download_path}'.")

            # **Step 3: Delete any leftover zip files**
            zip_files = glob.glob(os.path.join(kaggle_download_path, "*.zip"))
            for zip_file in zip_files:
                os.remove(zip_file)
                logger.info(f"Deleted zip archive: {zip_file}")

            # **Step 4: Validate and rename the CSV file**
            csv_files = [file for file in os.listdir(kaggle_download_path) if file.endswith(".csv")]

            if not csv_files:
                raise FileNotFoundError("No CSV file found in the Kaggle dataset directory.")

            original_file_path = os.path.join(kaggle_download_path, csv_files[0])
            new_file_path = os.path.join(kaggle_download_path, "customer_churn_dataset_kaggle.csv")

            # Ensure file is not empty before renaming
            if os.path.getsize(original_file_path) < 1024:
                raise ValueError(f"Downloaded CSV file is too small: {original_file_path}")

            os.rename(original_file_path, new_file_path)
            logger.info(f"File renamed from '{original_file_path}' to '{new_file_path}'.")

            return True  # Success

        except Exception as e:
            logger.error(f"Kaggle ingestion attempt {attempt + 1} failed: {str(e)}", exc_info=True)
            attempt += 1
            if attempt < max_retries:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.critical("Kaggle ingestion failed after multiple retries.")
                return False  # Failed after retries


# Run Ingestion Process
if __name__ == "__main__":
    kaggle_success = ingest_kaggle_data()

    if not kaggle_success:
        logger.critical("Kaggle data ingestion failed. Investigate logs immediately.")
