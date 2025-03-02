import yaml
import os
import boto3
from datetime import datetime
from logs.logging_config import setup_logging  # Import reusable logging

# Setup logging
logger = setup_logging("ingestion.log")

# Get the absolute path of the project root
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

# Paths
credentials_path = os.path.join(project_root, "config", "credentials.yaml")
kaggle_source_path = os.path.join(project_root, "data/raw/source_data")  # Path to Kaggle data folder
local_source_path = os.path.join(project_root, "data/raw/local")  # Path to local data folder

# Load credentials
with open(credentials_path, "r") as file:
    creds = yaml.safe_load(file)


def upload_to_s3():
    try:
        # Fetch AWS credentials
        aws_access_key = creds["aws"]["access_key"]
        aws_secret_key = creds["aws"]["secret_key"]
        bucket_name = "dmml-assignment1-bucket"

        # Initialize S3 client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
        )

        # Get current timestamp to create unique folder (by day)
        current_timestamp = datetime.now()
        year = current_timestamp.year
        month = f"{current_timestamp.month:02d}"  # Two digits (01, 02, 03, ... )
        day = f"{current_timestamp.day:02d}"  # Two digits (01, 02, 03, ...)

        # Function to upload files from a given source path
        def upload_from_source(source_path, source_name):
            for root, _, files in os.walk(source_path):
                for file in files:
                    # Local file path
                    file_path = os.path.join(root, file)

                    # S3 object key (path structure: <source>/<year>/<month>/<day>/<file_name>)
                    s3_key = f"{source_name}/year={year}/month={month}/day={day}/{file}"
                    print(f"Uploading {file} to s3://{bucket_name}/{s3_key}")

                    try:
                        # Upload file to S3
                        s3_client.upload_file(file_path, bucket_name, s3_key)
                        logger.info(f"Successfully uploaded {file} to s3://{bucket_name}/{s3_key}")
                    except Exception as upload_error:
                        logger.error(f"Failed to upload {file}: {str(upload_error)}")

        # Upload files from both Kaggle and Local sources
        logger.info("Uploading Kaggle data to S3...")
        upload_from_source(kaggle_source_path, "kaggle")

        logger.info("Uploading Local data to S3...")
        upload_from_source(local_source_path, "local")

    except Exception as e:
        logger.error(f"Upload process failed: {str(e)}")


# Run Upload to S3 process
if __name__ == "__main__":
    upload_to_s3()