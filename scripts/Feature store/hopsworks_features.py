import hopsworks
import pandas as pd
import os
import yaml
import glob

# Get the absolute path of the project root
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

# Paths
credentials_path = os.path.join(project_root, "config", "credentials.yaml")

# Load credentials
with open(credentials_path, "r") as file:
    creds = yaml.safe_load(file)

os.environ["HOPSWORKS_API_KEY"] = creds["hopsworks"]["api_key"]
# Log in to Hopsworks using the project name from your credentials
project = hopsworks.login(project=creds["hopsworks"]["project_name"])

# 2. Access the feature store
feature_store = project.get_feature_store()

# Pattern with wildcard
file_pattern = r"C:\Users\asood\PycharmProjects\EndtoEnd-data-management-pipeline\data\processed\processed_churn_data_*.csv"
files = glob.glob(file_pattern)

if not files:
    raise FileNotFoundError("No processed files found matching the pattern.")

# Get the latest file or the one you need (here we simply choose the first file)
file_path = files[0]
df = pd.read_csv(file_path)

# 4. Create or get a feature group
# Specify a primary key and (optionally) an event time column if available.
# Replace "id" and "event_time" with your actual column names.
feature_group = feature_store.get_or_create_feature_group(
    name="churn_feature_group",       # Name for your feature group
    version=1,
    description="Feature group for processed churn data",
    primary_key=["customerid"],               # List the column(s) that act as the primary key
    )

# 5. Insert the data into the feature group
feature_group.insert(df)

# 6. Verify the upload by reading back some data
retrieved_data = feature_group.read()
print(retrieved_data.head())
