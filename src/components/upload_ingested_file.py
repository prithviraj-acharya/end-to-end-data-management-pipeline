import os
from datetime import datetime
import sys
import glob  # Import glob for file pattern matching

# Get the absolute path of the project root
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..', 'src'))
sys.path.append(project_root)

from exception import CustomException
from logger import setup_logging
from utils import connect_to_s3

# Setup logging
logger = setup_logging("upload_ingested")

# Paths
rds_source_path = os.path.join(project_root, "data/raw/rds")  # Path to rds data folder
rds_s3_path = "data/raw/rds"
kaggle_source_path = os.path.join(project_root, "data/raw/kaggle")  # Path to kaggle data folder
kaggle_s3_path = "data/raw/kaggle"

contetnt_present_flag = 0

logger.info("Uploading raw data to S3 triggered...")

def upload_to_s3(source_path, target_s3_path):
    try:
    
        # Find files without "Done" in their names
        files_to_upload = glob.glob(os.path.join(source_path, "*"))
        files_to_upload = [f for f in files_to_upload if "Done" not in os.path.basename(f) and os.path.isfile(f)]
        print(f"files_to_upload - {files_to_upload}")
        if not files_to_upload:  # Checks if the list is empty
            logger.info(f"No file present for upload!")
            print("The list is empty.")
        else:
            logger.info("Establishing Connection to S3 Bucket...")
            # Initialize S3 client
            s3_client, s3_bucket_name, contetnt_present_flag = connect_to_s3()
            logger.info("Connection to S3 eshtablished successfully!")
            # Get current timestamp to create unique folder (by day)
            current_timestamp = datetime.now()
            year = current_timestamp.year
            month = f"{current_timestamp.month:02d}"  # Two digits (01, 02, 03, ... )
            day = f"{current_timestamp.day:02d}"  # Two digits (01, 02, 03, ...)
            for file_path in files_to_upload:
                file_name = os.path.basename(file_path)

                # S3 object key (path structure: <source>/<year>/<month>/<day>/<file_name>)
                s3_key = f"{target_s3_path}/year={year}/month={month}/day={day}/{file_name}"
                print(f"Uploading {file_name} to s3://{s3_bucket_name}/{s3_key}")

                try:
                    # Upload file to S3
                    s3_client.upload_file(file_path, s3_bucket_name, s3_key)
                    logger.info(f"Successfully uploaded {file_name} to s3://{s3_bucket_name}/{s3_key}")

                    # Rename file locally by appending "Done"
                    new_file_path = os.path.join(source_path, f"{file_name}.Done")
                    os.rename(file_path, new_file_path)
                    logger.info(f"Renamed local file: {file_name} to {file_name}.Done")

                except Exception as upload_error:
                    logger.error(f"Failed to upload {file_name}: {str(upload_error)}")

    except Exception as e:
        logger.error(f"Upload process failed: {str(e)}")


# Run Upload to S3 process
if __name__ == "__main__":
    # Upload files from Local source
    logger.info("Uploading rds data to S3...")
    print(f"rds_source_path - {rds_source_path}")
    upload_to_s3(rds_source_path, rds_s3_path)
    logger.info("Upload for rds data to S3 completed!")

    # Upload files from Local source
    logger.info("Uploading kaggle data to S3...")
    print(f"kaggle_source_path - {kaggle_source_path}")
    upload_to_s3(kaggle_source_path, kaggle_s3_path)
    logger.info("Upload for Kaggle data to S3 completed!")