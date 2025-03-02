import os
import pandas as pd
import yaml
import boto3
from datetime import datetime
from io import StringIO
from logs.logging_config import setup_logging  # Import your reusable logging configuration

# Setup logging
logger = setup_logging("ingestion.log")

# Get project root (adjust relative to this script)
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

# Load AWS credentials from YAML
credentials_path = os.path.join(project_root, "config", "credentials.yaml")
with open(credentials_path, "r") as file:
    creds = yaml.safe_load(file)

bucket_name = "dmml-assignment1-bucket"

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=creds["aws"]["access_key"],
    aws_secret_access_key=creds["aws"]["secret_key"],
)


def get_latest_s3_object(base_prefix):
    """
    List all objects under a given base prefix and return the key of the most recent file.
    """
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=base_prefix)
    objects = response.get("Contents", [])
    if not objects:
        raise ValueError(f"No objects found for prefix: {base_prefix}")
    latest_obj = max(objects, key=lambda x: x["LastModified"])
    return latest_obj["Key"]


# ----------------- MERGE DATASETS -----------------
# Retrieve the latest Kaggle and local dataset keys from S3
try:
    kaggle_key = get_latest_s3_object("kaggle/")
    local_key = get_latest_s3_object("local/")
    logger.info(f"Latest Kaggle file key: {kaggle_key}")
    logger.info(f"Latest local file key: {local_key}")
except Exception as e:
    logger.error(f"Failed to retrieve latest dataset keys from S3: {str(e)}")
    raise

try:
    # Retrieve Kaggle dataset from S3
    obj_kaggle = s3_client.get_object(Bucket=bucket_name, Key=kaggle_key)
    df_kaggle = pd.read_csv(obj_kaggle['Body'])

    # Retrieve local dataset from S3
    obj_local = s3_client.get_object(Bucket=bucket_name, Key=local_key)
    df_local = pd.read_csv(obj_local['Body'])

    logger.info("Successfully loaded Kaggle and local datasets from S3.")
except Exception as e:
    logger.error(f"Failed to load datasets from S3: {str(e)}")
    raise

# Standardize key columns for merging (assumes both datasets have "gender" and "tenure")
df_kaggle["gender"] = df_kaggle["gender"].astype(str).str.strip()
df_local["gender"] = df_local["gender"].astype(str).str.strip()

df_kaggle["tenure"] = pd.to_numeric(df_kaggle["tenure"], errors="coerce")
df_local["tenure"] = pd.to_numeric(df_local["tenure"], errors="coerce")

# Create a working copy of the local dataset (for removal of matched rows)
local_remaining = df_local.copy()
merged_rows = []  # This list will hold merged rows

# Iterate over Kaggle dataset (master)
for idx, kaggle_row in df_kaggle.iterrows():
    gender_val = kaggle_row["gender"]
    tenure_val = kaggle_row["tenure"]

    # Find matching local rows based on both gender and tenure
    matches = local_remaining[(local_remaining["gender"] == gender_val) &
                              (local_remaining["tenure"] == tenure_val)]
    if not matches.empty:
        # Use the first matching local row
        local_row = matches.iloc[0]
        # Remove this row from local_remaining so it isn't reused
        local_remaining.drop(local_row.name, inplace=True)
        # Merge the rows (Kaggle as primary, fill missing values from local)
        merged_row = kaggle_row.combine_first(local_row)
    else:
        merged_row = kaggle_row
    merged_rows.append(merged_row)

# Append any remaining local rows (unmatched) to the merged rows
for idx, local_row in local_remaining.iterrows():
    merged_rows.append(local_row)

# Create a merged DataFrame from the merged rows list
merged_df = pd.DataFrame(merged_rows)

# Ensure the final structure matches Kaggle's columns (then add extra local columns if any)
final_columns = df_kaggle.columns.tolist()
for col in merged_df.columns:
    if col not in final_columns:
        final_columns.append(col)
merged_df = merged_df[final_columns]

logger.info(
    f"Datasets merged successfully. Merged dataset has {merged_df.shape[0]} rows and {merged_df.shape[1]} columns.")

# Convert merged DataFrame to CSV string (in-memory)
csv_buffer = StringIO()
merged_df.to_csv(csv_buffer, index=False)

# ----------------- UPLOAD MERGED FILE TO S3 -----------------
# Create a timestamp folder key for S3
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
s3_key = f"merged/{timestamp}/merged_churn_data.csv"

try:
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    logger.info(f"Merged file successfully uploaded to s3://{bucket_name}/{s3_key}.")
    print(f"Merged file successfully uploaded to s3://{bucket_name}/{s3_key}.")
except Exception as e:
    logger.error(f"Failed to upload merged file: {str(e)}")
    print(f"Failed to upload merged file: {str(e)}")
