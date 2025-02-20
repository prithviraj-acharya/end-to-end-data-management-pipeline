import yaml
import os
import boto3
from logs.logging_config import setup_logging  # Import reusable logging

# Setup logging
logger = setup_logging("ingestion.log")

# Get the absolute path of the project root
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

# Paths
credentials_path = os.path.join(project_root, "config", "credentials.yaml")
source_data_path = os.path.join(project_root, "data/raw/source_data")  # Path to source_data folder

# Load credentials
with open(credentials_path, "r") as file:
    creds = yaml.safe_load(file)


def upload_to_s3():
    try:

        # Fetch AWS credentials
        aws_access_key = creds["aws"]["access_key"]
        aws_secret_key = creds["aws"]["secret_key"]
        bucket_name = "dmml-assignment-bucket"

        # Initialize S3 client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
        )
        # Traverse through source_data folder
        for root, _, files in os.walk(source_data_path):
            for file in files:
                # Local file path
                file_path = os.path.join(root, file)

                # S3 object key (e.g., raw/filename.csv)
                s3_key = f"raw/{file}"
                print(f"Uploading {file} to s3://{bucket_name}/{s3_key}")

                try:
                    # Upload file to S3
                    s3_client.upload_file(file_path, bucket_name, s3_key)
                    logger.info(f"Successfully uploaded {file} to s3://{bucket_name}/{s3_key}")
                except Exception as upload_error:
                    logger.error(f"Failed to upload {file}: {str(upload_error)}")

    except Exception as e:
        logger.error(f"Upload process failed: {str(e)}")


# Run Upload to S3 process
if __name__ == "__main__":
    upload_to_s3()
