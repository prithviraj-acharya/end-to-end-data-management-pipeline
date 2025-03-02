import os
import yaml
import boto3
import pandas as pd
import numpy as np
import warnings
from io import StringIO
from datetime import datetime

# Plotting/EDA libraries
import matplotlib.pyplot as plt
import seaborn as sns

# For scaling
from sklearn.preprocessing import StandardScaler

# ------------------ WARNING SUPPRESSION ------------------
warnings.filterwarnings(
    "ignore",
    message="use_inf_as_na option is deprecated and will be removed in a future version.",
    category=FutureWarning
)

# ------------------ SETUP ------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

# Pre-processing folder for saving EDA plots
preprocessing_folder = os.path.join(project_root, "scripts", "Pre-processing")
os.makedirs(preprocessing_folder, exist_ok=True)

# Load AWS credentials from config/credentials.yaml (assumed to include bucket_name, access_key, secret_key)
credentials_path = os.path.join(project_root, "config", "credentials.yaml")
with open(credentials_path, "r") as file:
    creds = yaml.safe_load(file)

# Use the bucket name from credentials
bucket_name = creds["aws"]["bucket_name"]

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=creds["aws"]["access_key"],
    aws_secret_access_key=creds["aws"]["secret_key"],
)


# ------------------ HELPER FUNCTION: GET LATEST S3 OBJECT ------------------
def get_latest_s3_object(base_prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=base_prefix)
    objects = response.get("Contents", [])
    if not objects:
        raise ValueError(f"No objects found for prefix: {base_prefix}")
    latest_obj = max(objects, key=lambda x: x["LastModified"])
    return latest_obj["Key"]


# ------------------ STEP 1: RETRIEVE LATEST MERGED FILE FROM S3 ------------------
try:
    merged_key = get_latest_s3_object("merged/")
    print(f"Latest merged file in S3: s3://{bucket_name}/{merged_key}")
except Exception as e:
    print(f"Error retrieving latest merged file key: {e}")
    raise

# ------------------ STEP 2: LOAD THE MERGED DATA ------------------
try:
    obj_merged = s3_client.get_object(Bucket=bucket_name, Key=merged_key)
    merged_csv = obj_merged['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(merged_csv))
    print(f"Successfully loaded merged dataset. Shape: {df.shape}")
except Exception as e:
    print(f"Error loading merged dataset from S3: {e}")
    raise

# ------------------ STEP 3: CONVERT CHURN TO "Yes"/"No" ------------------
if "Churn" in df.columns:
    df["Churn"] = df["Churn"].map({
        0: "No", 1: "Yes",
        "0": "No", "1": "Yes",
        "No": "No", "Yes": "Yes"
    })

# ------------------ SPECIAL HANDLING: CONVERT TOTALCHARGES TO NUMERIC ------------------
if "TotalCharges" in df.columns:
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors='coerce')

# ------------------ STEP 4: EDA ON ORIGINAL (UNSCALED) DATA ------------------
df_original = df.copy()  # Preserve original for EDA

eda_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
plots_folder = os.path.join(preprocessing_folder, f"eda_plots_original_{eda_timestamp}")
os.makedirs(plots_folder, exist_ok=True)

print("\n--- EDA on Original Data ---")
num_cols_original = df_original.select_dtypes(include=[np.number]).columns.tolist()


def is_binary(series):
    unique_vals = series.dropna().unique()
    return len(unique_vals) == 2


binary_numeric_cols = []
non_binary_numeric_cols = []
for col in num_cols_original:
    if is_binary(df_original[col]):
        binary_numeric_cols.append(col)
    else:
        non_binary_numeric_cols.append(col)

target_col = "Churn"
summary_stats_original = df_original.describe(include='all')
print("\n--- SUMMARY STATISTICS (Original Data) ---")
print(summary_stats_original)

for col in non_binary_numeric_cols:
    plt.figure(figsize=(6, 4))
    sns.histplot(data=df_original, x=col, hue=target_col, kde=True)
    plt.title(f"Histogram of {col} by {target_col}")
    plt.tight_layout()
    plt.savefig(os.path.join(plots_folder, f"hist_{col}.png"))
    plt.close()

    plt.figure(figsize=(6, 4))
    sns.boxplot(x=target_col, y=col, data=df_original)
    plt.title(f"Box Plot of {col} by {target_col}")
    plt.tight_layout()
    plt.savefig(os.path.join(plots_folder, f"box_{col}.png"))
    plt.close()

for col in binary_numeric_cols:
    plt.figure(figsize=(6, 4))
    sns.countplot(x=col, hue=target_col, data=df_original)
    plt.title(f"Count Plot of {col} by {target_col}")
    plt.tight_layout()
    plt.savefig(os.path.join(plots_folder, f"count_{col}.png"))
    plt.close()

print(f"EDA plots (original scale) saved in: {plots_folder}")

# ------------------ STEP 5: DATA PREPROCESSING ------------------
# Drop identifier columns that should not be encoded
if "customerID" in df.columns:
    df = df.drop(columns=["customerID"])
    print("Dropped customerID column.")

# Get numeric and categorical columns
num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
cat_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()

# Exclude target column from categorical encoding
if "Churn" in cat_cols:
    cat_cols.remove("Churn")

# Fill missing values: numeric with median; categorical with mode
for col in num_cols:
    if df[col].isnull().sum() > 0:
        df[col].fillna(df[col].median(), inplace=True)

for col in cat_cols:
    if df[col].isnull().sum() > 0:
        df[col].fillna(df[col].mode()[0], inplace=True)

# Scale numeric columns (excluding Churn if it's in numeric)
if "Churn" in num_cols:
    num_cols.remove("Churn")
scaler = StandardScaler()
df[num_cols] = scaler.fit_transform(df[num_cols])

# One-hot encode remaining categorical columns
df = pd.get_dummies(df, columns=cat_cols, drop_first=True)
print("Data preprocessing completed.")
print("DataFrame shape after preprocessing:", df.shape)

# ------------------ STEP 6: UPLOAD PROCESSED DATA TO S3 WITH TIMESTAMP ------------------
print("Starting upload of processed file...")

print("Converting DataFrame to CSV string...")
try:
    csv_str = df.to_csv(index=False)
    print("CSV conversion completed.")
except Exception as e:
    print(f"Error during CSV conversion: {e}")
    raise

# Use StringIO directly (S3 put_object will accept a string via getvalue())
csv_buffer = StringIO(csv_str)

upload_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
processed_key = f"processed/{upload_timestamp}/processed_churn_data.csv"

try:
    s3_client.put_object(Bucket=bucket_name, Key=processed_key, Body=csv_buffer.getvalue())
    print(f"Processed file uploaded to s3://{bucket_name}/{processed_key}")
except Exception as e:
    print(f"Error uploading processed file to S3: {e}")

print("\nAll steps completed.")
