import os
import pandas as pd
import numpy as np
import yaml
import boto3
import warnings
from io import StringIO
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

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

# Initialize S3 client (for download/upload)
s3_client = boto3.client(
    "s3",
    aws_access_key_id=creds["aws"]["access_key"],
    aws_secret_access_key=creds["aws"]["secret_key"],
    region_name="ap-south-1"
)


def get_latest_s3_object(prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    objects = response.get("Contents", [])
    if not objects:
        raise ValueError(f"No objects found for prefix: {prefix}")
    latest_obj = max(objects, key=lambda x: x["LastModified"])
    return latest_obj["Key"]


# ------------------ STEP 1: LOAD THE LATEST MERGED FILE FROM S3 ------------------
try:
    latest_processed_key = get_latest_s3_object("processed/")
    print(f"Latest processed file for tranformation: s3://{bucket_name}/{latest_processed_key}")
    obj_processed = s3_client.get_object(Bucket=bucket_name, Key=latest_processed_key)
    df = pd.read_csv(StringIO(obj_processed['Body'].read().decode('utf-8')))
except Exception as e:
    print(f"Error loading processed file from S3: {e}")
    raise


def transform_processed_data(df):
    """Transforms the processed Telco Churn dataset."""

    # Interaction feature
    df['tenure_monthly_interaction'] = df['tenure'] * df['monthlycharges']

    # Service usage aggregation
    service_cols = [
        'onlinesecurity_yes', 'onlinebackup_yes', 'onlinebackup_yes',
        'techsupport_yes', 'streamingtv_yes', 'streamingmovies_yes'
    ]
    df['total_services'] = df[service_cols].sum(axis=1)

    # Charge ratio
    df['monthly_total_ratio'] = df['monthlycharges'] / (df['totalcharges'] + 1e-9) #added small number to avoid divide by zero.

    #Dependents Encoded.
    df['dependents_label'] = np.where(df['dependents_yes'] == True,1,0)

    #Partner Encoded.
    df['partner_label'] = np.where(df['partner_yes'] == True,1,0)

    #Family aggregation.
    df['family_label'] = df['partner_label'] + df['dependents_label']

    return df

# Load the prepared data
df_new = transform_processed_data(df)

# ------------------ STEP 4: UPLOAD TRANSFORMED FILE TO S3 ------------------
print("Starting upload of transformed file...")
try:
    csv_str = df_new.to_csv(index=False)
    print("CSV conversion completed.")
except Exception as e:
    print(f"Error during CSV conversion: {e}")
    raise

try:
    # Convert DataFrame to Parquet using pyarrow
    table = pa.Table.from_pandas(df)
    print("Parquet conversion completed.")
except Exception as e:
    print(f"Error during Parquet conversion: {e}")
    raise

upload_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Create a StringIO buffer to store the modified df in memory
csv_buffer = StringIO(csv_str)
transformed_s3_key_s3 = f"transformed/{upload_timestamp}/transformed_churn_data.csv"

# Create a BytesIO buffer to store the Parquet file in memory
parquet_buffer = BytesIO()
transformed_s3_key_parquet = f"transformed/{upload_timestamp}/transformed_churn_data.parquet"
# Write the Parquet file to the buffer
pq.write_table(table, parquet_buffer)
# Move the cursor back to the start of the buffer
parquet_buffer.seek(0)

try:
    #s3_client.put_object(Bucket=bucket_name, Key=transformed_s3_key_s3, Body=csv_buffer.getvalue())
    #print(f"Transformed file uploaded to s3://{bucket_name}/{transformed_s3_key_s3}")
    s3_client.put_object(Bucket=bucket_name, Key=transformed_s3_key_parquet, Body=parquet_buffer.getvalue())
    print(f"Parquet file successfully uploaded to s3://{bucket_name}/{transformed_s3_key_parquet}")
except Exception as e:
    print(f"Error uploading transformed file to S3: {e}")

print("\nAll steps completed.")


#--------------- Check before and after df correlation values --------------
# Correlation analysis
# Convert 'Churn' to numerical (0/1)
df['churn'] = df['churn'].map({'Yes': 1, 'No': 0})
# Convert column 'A' from float to int
df['churn'] = df['churn'].astype(int)
# Ensure all columns are numeric
for col in df.columns:
    if df[col].dtype == 'object':
        try:
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to numeric, coerce errors to NaN)
        except ValueError:
            print(f"Column '{col}' cannot be converted to numeric.")
            # Handle non-numeric columns (e.g., drop or encode further)
            df = df.drop(col, axis=1) # Drop the column that cannot be converted.


correlation_matrix_old = df.corr()
churn_correlations_old = correlation_matrix_old['churn'].sort_values(ascending=False)
print(churn_correlations_old)

for col in df_new.columns:
    if df_new[col].dtype == 'object':
        try:
            df_new[col] = pd.to_numeric(df_new[col], errors='coerce')  # Convert to numeric, coerce errors to NaN)
        except ValueError:
            print(f"Column '{col}' cannot be converted to numeric.")
            # Handle non-numeric columns (e.g., drop or encode further)
            df_new = df_new.drop(col, axis=1) # Drop the column that cannot be converted.


correlation_matrix_new = df_new.corr()
churn_correlations_new = correlation_matrix_new['churn'].sort_values(ascending=False)
print(churn_correlations_new)
