import os
import time
import pandas as pd
import sys
import shutil
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.append(project_root)
from exception import CustomException
from logger import setup_logging
from kaggle.api.kaggle_api_extended import KaggleApi
from datetime import datetime
from utils import get_latest_csv_file, connect_rds_to_pull_csv

# Setup logging
logger = setup_logging("ingestion")

# Paths
postfix_datetime = datetime.now().strftime('%Y%m%d%H%M%S')

kaggle_source_path = os.path.join(project_root, "data/raw/kaggle")
kaggle_file_name = "customer_churn_dataset_kaggle"
kaggle_dataset_name = "blastchar/telco-customer-churn"
kaggle_file_name_with_datetime = f"{kaggle_file_name}_{postfix_datetime}.csv"

rds_source_path = os.path.join(project_root, "data/raw/rds")
rds_file_name = "customer_churn_dataset_rds"
rds_dataset_name = "data/raw/rds/customer_churn_dataset_rds.csv"
rds_file_name_with_datetime = f"{rds_file_name}_{postfix_datetime}.csv"


# Ensure directories exist
os.makedirs(kaggle_source_path, exist_ok=True)
os.makedirs(rds_source_path, exist_ok=True)

def ingest_from_kaggle_api(kaggle_dataset_name, kaggle_source_path, kaggle_file_name_with_datetime, kaggle_completed_flag):
    try:
        logger.info("Triggered Data ingestion from Kaggle Source...")
        if kaggle_completed_flag == 0:
            # Initialize Kaggle API
            api = KaggleApi()
            api.authenticate()
            logger.info("Kaggle API authenticated successfully.")
            logger.info(f"Downloading Kaggle dataset '{kaggle_dataset_name}'...")

            # Download dataset and unzip**
            api.dataset_download_files(kaggle_dataset_name, path=kaggle_source_path, unzip=True)
            logger.info(f"Dataset '{kaggle_dataset_name}' downloaded successfully to '{kaggle_source_path}'.")

            # Rename the latest file generated 
            latest_file_created = get_latest_csv_file(kaggle_source_path)
            new_file_path = os.path.join(kaggle_source_path, kaggle_file_name_with_datetime)
            if latest_file_created is None:
                latest_file_created = new_file_path                 
            logger.info(f"Kaggle latest file - '{new_file_path}' downloaded successfully to '{kaggle_source_path}'.")
            if latest_file_created != new_file_path:
                os.rename(latest_file_created, new_file_path)
                logger.info(f"File renamed from '{latest_file_created}' to '{new_file_path}'.")
                kaggle_completed_flag = 1
            # Ensure file is not empty before renaming
            if os.path.getsize(new_file_path) < 1024:
                kaggle_completed_flag = -1
                raise ValueError(f"Downloaded CSV file from Kaggle source is too small: {latest_file_created}")
        else:
            logger.info("Kaggle latest file already processed!!")
        return kaggle_completed_flag

    except Exception as e:
        raise CustomException(e, sys)
    
def ingest_from_rds_api(rds_dataset_name, rds_source_path, rds_file_name_with_datetime, rds_completed_flag):
    try:
        logger.info("Triggered Data ingestion from RDS Source...")
        if rds_completed_flag == 0:
            # Initialize RDS connection
            logger.info("Connecting to RDS to pull data to local csv format...")
            connect_rds_to_pull_csv(rds_source_path, rds_file_name_with_datetime)

            # Rename the latest file generated 
            latest_file_created = get_latest_csv_file(rds_source_path)
            rds_file_name_with_datetime = f"{rds_file_name}_{postfix_datetime}.csv"
            new_file_path = os.path.join(rds_source_path, rds_file_name_with_datetime)
            if latest_file_created is None:
                latest_file_created = new_file_path
            logger.info(f"RDS latest file - '{new_file_path}' downloaded successfully to '{rds_source_path}'.")
            if latest_file_created != new_file_path:
                os.rename(latest_file_created, new_file_path)            
                logger.info(f"File renamed from '{latest_file_created}' to '{new_file_path}'.")
                rds_completed_flag = 1
            # Ensure file is not empty before renaming
            if os.path.getsize(new_file_path) < 1024:
                rds_completed_flag = -1
                raise ValueError(f"Downloaded CSV file from RDS source is too small: {latest_file_created}")
        else:
            logger.info("RDS latest file already processed!!")
        
        return rds_completed_flag

    except Exception as e:
        raise CustomException(e, sys)


def ingest_source_data(max_retries=3, delay=10):
    """Fetch data from Kaggle, ensuring old files are deleted and renamed properly."""
    rds_completed_flag = 0
    kaggle_completed_flag = 0
    attempt = 0
    while attempt < max_retries:
        try:
            # **Step 1: Ingesting Data from First Source**
            logger.info("First Dataset Ingestion triggered...")
            kaggle_completed_flag = ingest_from_kaggle_api(kaggle_dataset_name, kaggle_source_path, kaggle_file_name_with_datetime, kaggle_completed_flag)
            logger.info("First Dataset Ingestion Completed!")

            # **Step 2: Ingesting Data from Second Source**
            logger.info("Second Dataset Ingestion triggered...")
            rds_completed_flag = ingest_from_rds_api(rds_dataset_name, rds_source_path, rds_file_name_with_datetime, rds_completed_flag)
            logger.info("Second Dataset Ingestion Completed!")

            return True  # Success

        except Exception as e:
            logger.error(f"Source ingestion attempt {attempt + 1} failed: {str(e)}", exc_info=True)
            attempt += 1
            if attempt < max_retries:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.critical("Source ingestion failed after multiple retries.")
                return False  # Failed after retries


# Run Ingestion Process
if __name__ == "__main__":
    source_success = ingest_source_data()

    if not source_success:
        logger.critical("Kaggle data ingestion failed. Investigate logs immediately.")

