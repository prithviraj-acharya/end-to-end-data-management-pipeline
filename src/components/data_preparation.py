import os
import pandas as pd
import numpy as np
import yaml
import boto3
import warnings
from io import StringIO
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler, LabelEncoder
import sys

# ------------------ WARNING SUPPRESSION ------------------
warnings.filterwarnings("ignore", category=FutureWarning)

# ------------------ SETUP ------------------
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.append(project_root)
current_dir = os.path.dirname(os.path.abspath(__file__))
from logger import setup_logging
from utils import connect_to_s3
from exception import CustomException

# Setup logging
logger = setup_logging("data_preparation")

# Folder paths for EDA and processed outputs
eda_folder = os.path.join(project_root, '..', "artifacts", "eda")
processed_folder = os.path.join(project_root, "data", "processed")
os.makedirs(eda_folder, exist_ok=True)
os.makedirs(processed_folder, exist_ok=True)

# Initialize S3 client (for upload later)
s3_client, aws_s3_bucket_name, contetnt_present_flag = connect_to_s3()

def get_latest_s3_object(prefix):
    response = s3_client.list_objects_v2(Bucket=aws_s3_bucket_name, Prefix=prefix)
    objects = response.get("Contents", [])
    if not objects:
        raise ValueError(f"No objects found for prefix: {prefix}")
    latest_obj = max(objects, key=lambda x: x["LastModified"])
    return latest_obj["Key"]


# ------------------ STEP 1: LOAD THE LATEST MERGED FILE FROM S3 ------------------
try:
    latest_merge_key = get_latest_s3_object("merged/")
    print(f"Latest merged file for processing: s3://{aws_s3_bucket_name}/{latest_merge_key}")
    obj_merged = s3_client.get_object(Bucket=aws_s3_bucket_name, Key=latest_merge_key)
    df = pd.read_csv(StringIO(obj_merged['Body'].read().decode('utf-8')))
except Exception as e:
    print(f"Error loading merged file from S3: {e}")
    raise

# ------------------ STEP 2: EDA ON RAW MERGED DATA ------------------
df_original = df.copy()  # Preserve a copy for EDA

eda_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
plots_dir = os.path.join(eda_folder, f"eda_plots_raw_{eda_timestamp}")
os.makedirs(plots_dir, exist_ok=True)

print("\n--- EDA on Raw Merged Data ---")
logger.info("\n--- EDA on Raw Merged Data ---")
summary_stats = df_original.describe(include="all")
print("\nSummary Statistics:")
print(summary_stats)

logger.info("\nSummary Statistics:")
logger.info(summary_stats)
summary_stats.to_csv(os.path.join(plots_dir, f"summary_statistics_{eda_timestamp}.csv"))

# Identify numeric columns (assume "SeniorCitizen" is already categorical)
num_columns = df_original.select_dtypes(include=[np.number]).columns.tolist()
if "SeniorCitizen" in num_columns:
    num_columns.remove("SeniorCitizen")


def is_binary(series):
    return len(series.dropna().unique()) == 2


binary_cols = [col for col in num_columns if is_binary(df_original[col])]
non_binary_cols = [col for col in num_columns if col not in binary_cols]
target_var = "Churn"  # target column

# Generate plots for non-binary numeric columns
for col in non_binary_cols:
    plt.figure(figsize=(6, 4))
    sns.histplot(data=df_original, x=col, hue=target_var, kde=True)
    plt.title(f"Histogram of {col} by {target_var}")
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, f"hist_{col}.png"))
    plt.close()

    plt.figure(figsize=(6, 4))
    sns.boxplot(x=target_var, y=col, data=df_original)
    plt.title(f"Box Plot of {col} by {target_var}")
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, f"box_{col}.png"))
    plt.close()

# Generate count plots for binary numeric columns
for col in binary_cols:
    plt.figure(figsize=(6, 4))
    sns.countplot(data=df_original, x=col, hue=target_var)
    plt.title(f"Count Plot of {col} by {target_var}")
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, f"count_{col}.png"))
    plt.close()

print(f"EDA plots saved in: {plots_dir}")
logger.info(f"EDA plots saved in: {plots_dir}")

# ------------------ STEP 3: DATA PREPROCESSING ------------------
# Start with the merged DataFrame 'df' (which already has "Churn", "SeniorCitizen", and "TotalCharges" handled)

df_proc = df.copy()

# Drop the identifier column "customerID" (we drop it here before encoding)
# if "customerID" in df_proc.columns:
#     df_proc = df_proc.drop(columns=["customerID"])
#     print("Dropped customerID column.")
#     logger.info("Dropped customerID column.")

# Separate numeric and categorical columns.
numeric_cols = df_proc.select_dtypes(include=[np.number]).columns.tolist()
categorical_cols = df_proc.select_dtypes(include=["object", "category"]).columns.tolist()

# Exclude target "Churn" from encoding.
if "Churn" in categorical_cols:
    categorical_cols.remove("Churn")

if "customerID" in categorical_cols:
    categorical_cols.remove("customerID")

# (Assuming missing values are already imputed from previous steps; if not, fill them here.)
for col in numeric_cols:
    if df_proc[col].isnull().sum() > 0:
        df_proc[col].fillna(df_proc[col].median(), inplace=True)
for col in categorical_cols:
    if df_proc[col].isnull().sum() > 0:
        df_proc[col].fillna(df_proc[col].mode()[0], inplace=True)

# Scale numeric columns (excluding Churn if it's in numeric)
if "Churn" in numeric_cols:
    numeric_cols.remove("Churn")


# Standardize (normalize) numeric attributes.
scaler = StandardScaler()
df_proc[numeric_cols] = scaler.fit_transform(df_proc[numeric_cols])

df_proc = pd.get_dummies(df_proc, columns=categorical_cols, drop_first=True)
df_proc.columns = df_proc.columns.str.replace(" ", "_", regex=True)
df_proc.rename(columns={"PaymentMethod_Credit_card_(automatic)": "PaymentMethod_Credit_card_automatic"}, inplace=True)
df_proc.columns = df_proc.columns.str.lower()

print("Data preprocessing (cleaning, scaling, encoding) completed.")
logger.info("Data preprocessing (cleaning, scaling, encoding) completed.")
logger.info(f"Processed DataFrame shape: {df_proc.shape}")
print("Processed DataFrame shape:", df_proc.shape)

# ------------------ STEP 4: UPLOAD PROCESSED FILE TO S3 ------------------
print("Starting upload of processed file...")
logger.info("Starting upload of processed file...")
try:
    csv_str = df_proc.to_csv(index=False)
    print("CSV conversion completed.")
    logger.info("CSV conversion completed.")
except Exception as e:
    print(f"Error during CSV conversion: {e}")
    logger.info(f"Error during CSV conversion: {e}")
    raise

csv_buffer = StringIO(csv_str)
upload_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
processed_s3_key = f"processed/{upload_timestamp}/processed_churn_data.csv"

try:
    s3_client.put_object(Bucket=aws_s3_bucket_name, Key=processed_s3_key, Body=csv_buffer.getvalue())
    print(f"Processed file uploaded to s3://{aws_s3_bucket_name}/{processed_s3_key}")
    logger.info(f"Processed file uploaded to s3://{aws_s3_bucket_name}/{processed_s3_key}")
except Exception as e:
    print(f"Error uploading processed file to S3: {e}")
    logger.info(f"Error uploading processed file to S3: {e}")

print("\nAll steps completed.")
logger.info("\nAll steps completed.")
