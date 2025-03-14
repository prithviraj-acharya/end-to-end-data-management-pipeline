import sys
import os
import pandas as pd
import numpy as np
import dill
import boto3
import yaml
import glob
import pg8000
import csv
from dotenv import load_dotenv
from datetime import datetime, timezone

#print(sys.path)
# Add src folder to the system path
current_path = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(current_path, '..', 'src'))
sys.path.append(project_root)
credentials_path = os.path.join(project_root, '..', "config", "credentials.yaml")

from exception import CustomException
from logger import logging

def save_object(file_path, object_name):
    '''
    This function is responsible for saving the object in the file
    '''
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)

        with open(file_path, 'wb') as file_obj:
            #pickle.dump(object_name, file)
            dill.dump(object_name, file_obj)

    except Exception as e:
        raise CustomException(e, sys)


def load_object(file_path):
    try:
        with open(file_path, 'rb') as file_obj:
            object_name = dill.load(file_obj)
            return object_name
    except Exception as e:
        raise CustomException(e, sys)
    
def get_latest_csv_file(file_source_path):
    """
    Returns the path to the latest CSV file in the given directory.
    """
    try:
        csv_files = glob.glob(os.path.join(file_source_path, "*.csv"))
        if not csv_files:
            return None  # No CSV files found
        # Sort files by modification time (latest first)
        csv_files.sort(key=os.path.getmtime, reverse=True)
        return csv_files[0]  # Return the path to the latest file
    except Exception as e:
        raise CustomException(e, sys)

def connect_to_s3():
    '''
    This function is responsible to establish connection to aws s3 bucket
    '''
    try:
        contetnt_present_flag = 0
        # Load credentials
        with open(credentials_path, "r") as file:
            creds = yaml.safe_load(file)
        # Fetch AWS credentials
        aws_access_key = creds["aws"]["access_key"]
        aws_secret_key = creds["aws"]["secret_key"]
        aws_s3_bucket_name = creds["aws"]["s3_bucket_name"]
        region_name = creds["aws"]["region"]
        # Initialize S3 client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )
        # Validating the content of the s3 bucket
        s3_response = s3_client.list_objects_v2(Bucket=aws_s3_bucket_name)
        if 'Contents' in s3_response:
            for obj in s3_response['Contents']:
                contetnt_present_flag+=1
                #logging.info(f"S3 bucket content - {obj['Key']}...")
            else:
                contetnt_present_flag=-1
                logging.info("Bucket is empty or object not found.")
        return s3_client, aws_s3_bucket_name, contetnt_present_flag
    except Exception as e:
        logging.error(f"Upload process failed: {str(e)}")

def upload_data_to_s3(file_path, s3_file_prefix):
    now = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    s3_client, aws_s3_bucket_name, contetnt_present_flag = connect_to_s3()

    for file in os.listdir(file_path):
        local_file_path = os.path.join(file_path, file)
        file_name_with_timestamp = f"{os.path.basename(file_path)}_{timestamp}"
        s3_key = f"{s3_file_prefix}/{now}/{file_name_with_timestamp}"

        try:
            s3_client.upload_file(local_file_path, aws_s3_bucket_name, s3_key)
            logging.info(f"Uploaded {file} to s3://{aws_s3_bucket_name}/{s3_key}")

        except Exception as e:
            logging.error(f"Failure while Uploading file to S3: {str(e)}")

def write_csv(rows, cursor, file_path, file_name):
    try:
        csv_file = os.path.join(file_path, file_name)
        #print(f"csv_file - {csv_file}")
        os.makedirs(file_path, exist_ok=True)

        with open(csv_file, "w", newline="") as file:
            writer = csv.writer(file)
            # column_names = [desc[0] for desc in cursor.description]
            column_names = ["customerID", "gender", "SeniorCitizen", "Partner", "Dependents", "tenure", "PhoneService",
                            "MultipleLines", "InternetService", "OnlineSecurity", "OnlineBackup", "Churn"]
            writer.writerow(column_names)
            writer.writerows(rows)
    except Exception as e:
        logging.error(f"Failure while writing from RDS to local path: {str(e)}")

def connect_rds_to_pull_csv(local_file_path, file_name):
    try:
        #load_dotenv(r"C:\Users\gaura\Downloads\Sem_II\DM4ML\Assignment\end-to-end-data-management-pipeline\.env")
        load_dotenv()
        #print(f"load dotenv - {os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))}")
        conn = pg8000.connect(
            #database="dmml_db", #os.getenv("DB_NAME"),
            #user="postgres", #os.getenv("DB_USER"),
            #password="q1w2e3r4t5y6", #os.getenv("DB_PASSWORD"),
            #host="dmml-database.ck7ceu68s82v.us-east-1.rds.amazonaws.com" #os.getenv("DB_HOST")
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST")
        )

        cur = conn.cursor()
        logging.info("Connection extablished with RDS, pulling the data to load csv...")
        query = "SELECT * FROM customers;"
        cur.execute(query)
        rows = cur.fetchall()

        write_csv(rows, cur, local_file_path, file_name)

        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Connect to RDS process failed: {str(e)}")