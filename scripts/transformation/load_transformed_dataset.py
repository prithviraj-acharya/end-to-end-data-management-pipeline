import os
import pandas as pd
import numpy as np
import yaml
import boto3
import pyarrow.parquet as pq
import psycopg2
import warnings
from io import StringIO, BytesIO
from datetime import datetime, timedelta
import pytz

# ------------------ WARNING SUPPRESSION ------------------
warnings.filterwarnings("ignore", category=FutureWarning)

# ------------------ SETUP ------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

# Folder paths for transformation outputs
transformation_folder = os.path.join(project_root, "scripts", "transformation")
os.makedirs(transformation_folder, exist_ok=True)

# Load AWS credentials from config/credentials.yaml
credentials_path = os.path.join(project_root, "config", "credentials.yaml")
with open(credentials_path, "r") as file:
    creds = yaml.safe_load(file)
bucket_name = creds["aws"]["bucket_name"]
workgroup_name = creds["aws"]["redshift_workgroup_name"]
namespace_name = creds["aws"]["redshift_namespace_name"]
redshift_database = creds["aws"]["redshift_database"]
iam_role_arn = creds["aws"]["redshift_iam_role_arn"]
schema_name = creds["aws"]["redshift_schema_name"]
table_name = creds["aws"]["redshift_table_name"]
redshift_port = creds["aws"]["redshift_port"]
redshift_username = creds["aws"]["redshift_username"]
redshift_password = creds["aws"]["redshift_password"]
redshift_endpoint = creds["aws"]["redshift_endpoint"]

print(f"redshift_endpoint = {redshift_endpoint}  bucket_name = {bucket_name}  workgroup_name = {workgroup_name}  namespace_name = {namespace_name}  database = {redshift_database}   iam_role_arn = {iam_role_arn}  schema_name = {schema_name}  table_name = {table_name}")

# Initialize S3 client (for download/upload)
s3_client = boto3.client(
    "s3",
    aws_access_key_id=creds["aws"]["access_key"],
    aws_secret_access_key=creds["aws"]["secret_key"],
    region_name="ap-south-1"    # Region should be specified here
)

def get_latest_s3_object(prefix):
    print(f"Listing objects in bucket: {bucket_name} with prefix: {prefix}")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    objects = response.get("Contents", [])
    if not objects:
        raise ValueError(f"No objects found for prefix: {prefix}")
    latest_obj = max(objects, key=lambda x: x["LastModified"])
    return latest_obj["Key"]

# ------------------ STEP 1: LOAD THE LATEST MERGED FILE FROM S3 ------------------
try:

    print("Here 2")
    latest_transformed_key = get_latest_s3_object("transformed/")
    s3_transformed_path=f"s3://{bucket_name}/{latest_transformed_key}"
    print(f"Latest transformed file for loading: {s3_transformed_path}")


    # ------------------ NEW ADDITION: READ BACK THE UPLOADED PARQUET FILE AND VALIDATE ------------------
    # Read the Parquet file back from S3 to validate the upload
    # Retrieve the newly uploaded Parquet file from S3
    obj_uploaded_parquet = s3_client.get_object(Bucket=bucket_name, Key=latest_transformed_key)
    
    # Read the Parquet file data
    parquet_data_uploaded = obj_uploaded_parquet['Body'].read()
    
    # Convert to a PyArrow Table and then to a pandas DataFrame
    table_uploaded = pq.read_table(BytesIO(parquet_data_uploaded))
    df = table_uploaded.to_pandas()
    
    # Print the head of the DataFrame to validate the upload
    print("\nHead of the uploaded DataFrame from Parquet file:")
    print(df.head())

    # Block for reading csv format file
    #obj_transformed = s3_client.get_object(Bucket=bucket_name, Key=latest_transformed_key)
    #df = pd.read_csv(StringIO(obj_transformed['Body'].read().decode('utf-8')))
    #df = pd.read_parquet(s3_transformed_path, engine='pyarrow')
    #print(df.head())
except Exception as e:
    print(f"Error loading transformed file from S3: {e}")
    raise

# ----------------- Function to map pandas data types to Redshift data types -------------------
def map_dtype_to_redshift(dtype):
    """Maps pandas dtype to Redshift data types."""
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT8"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "VARCHAR"
    
"""
# Add new columns to the DataFrame for event_timestamp and created_timestamp
now = datetime.now(tz=pytz.UTC) # Use UTC time for consistency
df["event_timestamp"] = [now - timedelta(days=i % 30) for i in range(len(df))]
df["created_timestamp"] = now
"""

# Step 1: Use boto3 to get the Redshift Serverless endpoint
def load_parquet_to_redshift_serverless(redshift_endpoint,redshift_port,redshift_database,redshift_username,redshift_password,df):
    # Initialize psycopg2 cursor for Redshift Serverless
    try:
        redshift_conn = psycopg2.connect(
            host=redshift_endpoint,
            port=redshift_port,
            database=redshift_database,
            user=redshift_username,
            password=redshift_password
        )
        redshift_cursor=redshift_conn.cursor()
        
        # Create table (if it doesn't exists)
        select_query=f"""
            SELECT *
            FROM information_schema.tables 
            WHERE table_catalog = '{redshift_database}'
            AND table_schema = '{schema_name}'
            AND table_name = '{table_name}';
        """
        print(f"select query = {select_query}")
        redshift_cursor.execute(select_query)
        result = redshift_cursor.fetchall()
        print(f"Result = {result}")

        if not result:
            # Create table dynamically based on DataFrame schema
            columns = ", ".join([f"{col} {map_dtype_to_redshift(df[col].dtype)}" for col, dtype in zip(df.columns, df.dtypes)])
            
            # Create table query
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                {columns}
            );
            """
            print(f"Create Table Query: {create_table_query}")
            redshift_cursor.execute(create_table_query)
            redshift_conn.commit()

            print("Table created successfully")
        else:
            print(f"Table '{table_name}' already exists. Skipping table creation.")

        
        # Prepare COPY command to load the data into Redshift
        copy_command = f"""
        COPY {schema_name}.{table_name}
        FROM '{s3_transformed_path}'
        IAM_ROLE '{iam_role_arn}'
        FORMAT AS PARQUET;
        """
        print(f"COPY Command: {copy_command}")
        redshift_cursor.execute(copy_command)
        redshift_conn.commit()

        print("Data loaded into Redshift successfully!")
        
        # Step 3: Add the audit columns to the table if not already present
        alter_table_query = f"""
        ALTER TABLE {schema_name}.{table_name}
        ADD event_timestamp TIMESTAMP;
        ALTER TABLE {schema_name}.{table_name}
        ADD created_timestamp TIMESTAMP;
        """
        redshift_cursor.execute(alter_table_query)
        redshift_conn.commit()
        print("Audit columns added to Redshift table.")

        # Step 4: Update the audit columns with proper values
        update_query = f"""
        UPDATE {schema_name}.{table_name}
        SET event_timestamp = current_timestamp,
            created_timestamp = current_timestamp;
        """
        redshift_cursor.execute(update_query)
        redshift_conn.commit()

        print("Audit columns updated with current timestamps.")


    except psycopg2.Error as e:
        print(f"Error: {e}")
    finally:
        if redshift_conn:
            redshift_cursor.close()
            redshift_conn.close()

if __name__ == '__main__':
    load_parquet_to_redshift_serverless(redshift_endpoint,redshift_port,redshift_database,redshift_username,redshift_password,df)