from dotenv import load_dotenv
from datetime import datetime, timezone
import requests
import os
import zipfile
import pg8000
import csv
import json
import sys

# ------------------ SETUP ------------------
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.append(project_root)
current_dir = os.path.dirname(os.path.abspath(__file__))
from logger import setup_logging
from utils import connect_to_s3
from exception import CustomException

# Ensure the logs folder exists
logger = setup_logging("RDS_Ingestion")

load_dotenv(r"C:\Users\gaura\Downloads\Sem_II\DM4ML\Assignment\end-to-end-data-management-pipeline\.env")

db_file_path = "data_temp"
#db_file_path = r"src\components\data\raw\rds"
#db_file_path = os.path.abspath(os.path.join(project_root, 'components', 'data', 'raw', 'rds'))


def upload_data_to_s3(file_path):
    logger.info("Uploading to S3...")

    now = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(timestamp)

    s3_client, aws_s3_bucket_name, contetnt_present_flag = connect_to_s3()

    S3_BUCKET = aws_s3_bucket_name
    S3_DB_FOLDER = "data/raw/rds"

    for file in os.listdir(file_path):
        local_file_path = os.path.join(file_path, file)
        file_name_with_timestamp = f"{os.path.basename(file_path)}_{timestamp}"
        s3_key = f"{S3_DB_FOLDER}/{now}/{file_name_with_timestamp}"

        try:
            s3_client.upload_file(local_file_path, S3_BUCKET, s3_key)
            logger.info(f"Uploaded {file} to s3://{S3_BUCKET}/{s3_key}")

        except Exception as e:
            logger.error(f"Failed to upload {file} to S3: {e}")


def write_csv(rows, cursor):
    logger.info("Writing to csv...")

    csv_file = db_file_path+"/customers_data.csv"
    os.makedirs("data/db_dataset", exist_ok=True)

    with open(csv_file, "w", newline="") as file:
        writer = csv.writer(file)
        column_names = [desc[0] for desc in cursor.description]
        writer.writerow(column_names)
        writer.writerows(rows)

    logger.info(f"Data saved to {csv_file}")


def connect_rds():
    logger.info("Connecting to RDS...")
    try:
        conn = pg8000.connect(
            database="dmml_db", #os.getenv("DB_NAME"),
            user="postgres", #os.getenv("DB_USER"),
            password="q1w2e3r4t5y6", #os.getenv("DB_PASSWORD"),
            host="dmml-database.ck7ceu68s82v.us-east-1.rds.amazonaws.com" #os.getenv("DB_HOST")

            # RDS Connection
            #DB_HOST=dmml-database.ck7ceu68s82v.us-east-1.rds.amazonaws.com
            #DB_NAME=dmml_db
            #DB_USER=postgres
            #DB_PASSWORD=q1w2e3r4t5y6

        )

        cur = conn.cursor()
        query = "SELECT * FROM customers;"
        cur.execute(query)
        rows = cur.fetchall()

        write_csv(rows, cur)

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Failed to connect: {e}")


def ingest_data():
    logger.info("Triggered Data ingestion from RDS")
    # logger.debug(f"Event: {json.dumps(event, indent=2)}")
    # logger.info(f"Function Name: {context.function_name}")
    # logger.info(f"Request ID: {context.aws_request_id}")

    connect_rds()
    upload_data_to_s3(db_file_path)


ingest_data()