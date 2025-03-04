import os
import yaml
import boto3
import pandas as pd
import numpy as np
import warnings
from io import StringIO
from datetime import datetime

# For scaling (if needed later)
from sklearn.experimental import enable_iterative_imputer  # noqa: F401
from sklearn.impute import IterativeImputer
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

# Load AWS credentials from config/credentials.yaml (if needed for other parts)
credentials_path = os.path.join(project_root, "config", "credentials.yaml")
with open(credentials_path, "r") as file:
    creds = yaml.safe_load(file)

bucket_name = creds["aws"]["bucket_name"]

# Initialize S3 client (for upload later)
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

# ------------------ MERGE DATASETS -----------------
# Retrieve the latest Kaggle and local dataset keys from S3
try:
    kaggle_key = get_latest_s3_object("kaggle/")
    local_key = get_latest_s3_object("local/")
    print(f"Latest Kaggle file key: {kaggle_key}")
    print(f"Latest local file key: {local_key}")
except Exception as e:
    print(f"Error retrieving latest dataset keys: {e}")
    raise

try:
    # Retrieve Kaggle dataset from S3
    obj_kaggle = s3_client.get_object(Bucket=bucket_name, Key=kaggle_key)
    df_kaggle = pd.read_csv(obj_kaggle['Body'])

    # Retrieve local dataset from S3
    obj_local = s3_client.get_object(Bucket=bucket_name, Key=local_key)
    df_local = pd.read_csv(obj_local['Body'])

    print("Successfully loaded Kaggle and local datasets from S3.")
except Exception as e:
    print(f"Error loading datasets from S3: {e}")
    raise

# ------------------ STANDARDIZE AND CONVERT COLUMNS ------------------
# Convert "Churn" to categorical "Yes"/"No"
if "Churn" in df_kaggle.columns:
    df_kaggle["Churn"] = df_kaggle["Churn"].map({0: "No", 1: "Yes", "0": "No", "1": "Yes", "No": "No", "Yes": "Yes"})
if "Churn" in df_local.columns:
    df_local["Churn"] = df_local["Churn"].map({0: "No", 1: "Yes", "0": "No", "1": "Yes", "No": "No", "Yes": "Yes"})

# Convert "SeniorCitizen" to categorical if present. If numeric, map 0->"No", 1->"Yes".
if "SeniorCitizen" in df_kaggle.columns:
    if pd.api.types.is_numeric_dtype(df_kaggle["SeniorCitizen"]):
        df_kaggle["SeniorCitizen"] = df_kaggle["SeniorCitizen"].map({0: "No", 1: "Yes"})
if "SeniorCitizen" in df_local.columns:
    if pd.api.types.is_numeric_dtype(df_local["SeniorCitizen"]):
        df_local["SeniorCitizen"] = df_local["SeniorCitizen"].map({0: "No", 1: "Yes"})

# Ensure "TotalCharges" is numeric so it is not imputed as a string.
if "TotalCharges" in df_kaggle.columns:
    df_kaggle["TotalCharges"] = pd.to_numeric(df_kaggle["TotalCharges"], errors="coerce")
if "TotalCharges" in df_local.columns:
    df_local["TotalCharges"] = pd.to_numeric(df_local["TotalCharges"], errors="coerce")

# ------------------ MERGE VIA VERTICAL CONCATENATION ------------------
# Assume Kaggle has more columns; local rows will get NaN for missing features.
merged_df = pd.concat([df_kaggle, df_local], axis=0, ignore_index=True, sort=False)

# Reorder columns so that Kaggle's column order is preserved.
final_columns = list(df_kaggle.columns)
for col in merged_df.columns:
    if col not in final_columns:
        final_columns.append(col)
merged_df = merged_df[final_columns]
print(f"Merged dataset shape: {merged_df.shape}")

# ------------------ IMPUTATION ------------------
# Separate the target ("Churn") from features.
if "Churn" in merged_df.columns:
    target = merged_df["Churn"]
    features = merged_df.drop(columns=["Churn"])
else:
    features = merged_df.copy()
    target = None

# NOTE: In this revised code, we are **not** dropping the customerID column,
# so that it remains in the final imputed DataFrame.
# (You mentioned that you will drop it later in data preparation.)

# Separate numeric and categorical features.
num_cols = features.select_dtypes(include=[np.number]).columns.tolist()
cat_cols = features.select_dtypes(include=["object", "category"]).columns.tolist()

# --- Impute Numeric Features using IterativeImputer ---
imp_numeric = IterativeImputer(random_state=0, max_iter=20)
features_num = pd.DataFrame(imp_numeric.fit_transform(features[num_cols]), columns=num_cols)
print("Numeric imputation completed. Numeric shape:", features_num.shape)

# --- Custom Imputation for Categorical Features ---
features_cat = features[cat_cols].copy()
cat_mappings = {}
for col in cat_cols:
    features_cat[col] = features_cat[col].astype("category")
    cat_mappings[col] = list(features_cat[col].cat.categories)
    codes = features_cat[col].cat.codes.replace(-1, np.nan)
    features_cat[col] = codes

for col in cat_cols:
    unique_vals = features_cat[col].dropna().unique()
    if len(unique_vals) > 1:
        imp_cat = IterativeImputer(random_state=0)
        col_array = features_cat[[col]].values
        imputed_col = imp_cat.fit_transform(col_array)
        imputed_col = np.rint(imputed_col).astype(int)
        features_cat[col] = imputed_col.ravel()
    else:
        features_cat[col].fillna(unique_vals[0], inplace=True)

for col in cat_cols:
    max_code = len(cat_mappings[col]) - 1
    features_cat[col] = features_cat[col].clip(0, max_code)
    features_cat[col] = features_cat[col].apply(lambda x: cat_mappings[col][x])
print("Categorical imputation completed. Categorical shape:", features_cat.shape)

# Combine imputed numeric and categorical features.
features_imputed = pd.concat([features_num, features_cat], axis=1)

# Add target back if available.
if target is not None:
    features_imputed["Churn"] = target.values

imputed_df = features_imputed
print("Final imputed DataFrame shape:", imputed_df.shape)

# ------------------ PUSH MERGED (AND IMPUTED) FILE TO S3 ------------------
csv_buffer = StringIO()
imputed_df.to_csv(csv_buffer, index=False)

push_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
push_s3_key = f"merged/{push_timestamp}/merged_churn_data.csv"

try:
    s3_client.put_object(Bucket=bucket_name, Key=push_s3_key, Body=csv_buffer.getvalue())
    print(f"Merged file after imputation uploaded to s3://{bucket_name}/{push_s3_key}")
except Exception as e:
    print(f"Error uploading merged file to S3: {e}")

print("\nMerge step completed.")
