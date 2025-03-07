import os
import pandas as pd
from datetime import datetime, timedelta
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int32, String
from logs.logging_config import setup_logging

logger = setup_logging("feature_store.log")

try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "../../"))
    data_dir = os.path.join(project_root, "data/raw/source_data")
    csv_path = os.path.join(data_dir, "customer_churn_dataset_kaggle.csv")
    parquet_path = os.path.join(data_dir, "customer_churn_dataset_kaggle.parquet")  # New Parquet file

    logger.info(f"Ensuring directory exists: {data_dir}")
    os.makedirs(data_dir, exist_ok=True)

    # Load and update CSV, then save as Parquet
    logger.info(f"Loading customer churn dataset from {csv_path}")
    df = pd.read_csv(csv_path)
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce").fillna(0.0)
    logger.info(f"Successfully loaded dataset from {csv_path}.")

    current_time = pd.Timestamp(datetime.now(), tz="UTC")
    df["event_timestamp"] = current_time
    df["created_timestamp"] = current_time

    df.to_parquet(parquet_path, index=False)  # Save as Parquet instead of CSV
    logger.info(f"Converted dataset to Parquet and saved to {parquet_path}")

    # Define Feast Components
    logger.info("Defining Feast entities and feature views.")
    customer = Entity(name="customer", join_keys=["customerID"], description="Customer entity")

    source = FileSource(
        path=parquet_path,  # Updated to Parquet path
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
    )

    customer_features = FeatureView(
        name="customer_features",
        entities=[customer],
        ttl=timedelta(days=365),
        schema=[
            Field(name="tenure", dtype=Int32),
            Field(name="TotalCharges", dtype=Float32),
            Field(name="Churn", dtype=String),
        ],
        source=source
    )
    logger.info("Feast feature definitions created successfully.")

except Exception as e:
    logger.critical(f"Script execution failed: {e}")
    raise