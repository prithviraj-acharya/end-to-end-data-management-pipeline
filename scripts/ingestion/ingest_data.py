import yaml
import os
import glob
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from logs.logging_config import setup_logging  # Import reusable logging

# Setup logging
logger = setup_logging("ingestion.log")

# Get the absolute path of the project root
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

# Paths
credentials_path = os.path.join(project_root, "config", "credentials.yaml")
kaggle_download_path = os.path.join(project_root, "data/raw/source_data")
local_file_path = os.path.join(project_root, "data/raw/local/customer_churn_dataset_local.csv")
processed_file_path = os.path.join(project_root, "data/raw/source_data/customer_data.csv")

# Ensure directories exist
os.makedirs(kaggle_download_path, exist_ok=True)
os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

# Load credentials
with open(credentials_path, "r") as file:
    creds = yaml.safe_load(file)


def ingest_kaggle_data():
    try:
        os.environ["KAGGLE_USERNAME"] = creds["kaggle"]["username"]
        os.environ["KAGGLE_KEY"] = creds["kaggle"]["key"]

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

        # **Step 4: Find and rename the new CSV file**
        csv_files = [file for file in os.listdir(kaggle_download_path) if file.endswith(".csv")]

        if len(csv_files) == 0:
            logger.error("No CSV file found in the Kaggle dataset directory.")
            return

        original_file_path = os.path.join(kaggle_download_path, csv_files[0])
        new_file_path = os.path.join(kaggle_download_path, "customer_churn_dataset_kaggle.csv")

        os.rename(original_file_path, new_file_path)
        logger.info(f"File renamed from '{original_file_path}' to '{new_file_path}'.")

    except Exception as e:
        logger.error(f"Kaggle ingestion failed: {str(e)}")


# Function to Ingest Local CSV File
def ingest_local_file():
    try:
        if not os.path.exists(local_file_path):
            logger.error(f"Local file not found: {local_file_path}")
            return

        df = pd.read_csv(local_file_path)
        df.to_csv(processed_file_path, index=False)

        logger.info(
            f"Local file '{local_file_path}' ingested successfully ({df.shape[0]} rows, {df.shape[1]} columns).")

    except Exception as e:
        logger.error(f"Local file ingestion failed: {str(e)}")


# Run Both Ingestion Processes
if __name__ == "__main__":
    ingest_kaggle_data()
    ingest_local_file()
