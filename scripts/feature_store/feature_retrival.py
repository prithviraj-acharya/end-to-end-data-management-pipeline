import os
import pandas as pd
from datetime import datetime
import pytz
from feast import FeatureStore
from scripts.feature_store.features import customer, customer_features
from logs.logging_config import setup_logging

logger = setup_logging("feature_store.log")

def get_online_features(fs, customer_ids):
    try:
        logger.info(f"Fetching online features for customers: {customer_ids}")
        features = fs.get_online_features(
            features=["customer_features:tenure", "customer_features:TotalCharges", "customer_features:Churn"],
            entity_rows=[{"customerID": cid} for cid in customer_ids]
        )
        features_df = features.to_df() if features else pd.DataFrame()
        logger.info(f"Online feature retrieval successful. Retrieved {len(features_df)} rows.")
        logger.info(f"Online features:\n{features_df.to_string(index=False)}")
        return features_df
    except Exception as e:
        logger.error(f"Online feature retrieval failed: {e}")
        raise

def get_offline_features(fs, start_date, end_date):
    try:
        logger.info(f"Fetching offline features from {start_date} to {end_date}")
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, "../../"))
        parquet_path = os.path.join(project_root, "data/raw/source_data", "customer_churn_dataset_kaggle.parquet")
        source_df = pd.read_parquet(parquet_path)
        all_customer_ids = source_df["customerID"].unique().tolist()

        entity_df = pd.DataFrame({
            "customerID": all_customer_ids,
            "event_timestamp": [end_date] * len(all_customer_ids)
        })
        logger.info(f"Entity DF sample:\n{entity_df.head(5).to_string()}")

        features = fs.get_historical_features(
            entity_df=entity_df,
            features=["customer_features:tenure", "customer_features:TotalCharges", "customer_features:Churn"]
        ).to_df()
        logger.info(f"Retrieved {len(features)} rows before filtering.")

        features = features[(features["event_timestamp"] >= start_date) &
                            (features["event_timestamp"] <= end_date)]
        logger.info(f"Filtered to {len(features)} rows within {start_date} to {end_date}.")
        logger.info(f"First 5 rows of offline features:\n{features.head(5).to_string(index=False)}")

        return features
    except Exception as e:
        logger.error(f"Offline feature retrieval failed: {e}")
        raise

if __name__ == "__main__":
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, "../../"))
        repo_path = os.path.join(project_root, "customer_churn_feast/feature_repo")
        logger.info(f"Initializing FeatureStore at '{repo_path}'")

        fs = FeatureStore(repo_path=repo_path)
        logger.info("FeatureStore initialized successfully.")

        start_date = datetime(2024, 3, 1, tzinfo=pytz.UTC)  # Matches current materialized data
        end_date = datetime.now(tz=pytz.UTC)

        offline_features = get_offline_features(fs, start_date, end_date)

        sample_customer_ids = ["7590-VHVEG", "5575-GNVDE", "3668-QPYBK"]
        online_features = get_online_features(fs, sample_customer_ids)

        logger.info("Feature retrieval completed successfully.")
    except Exception as e:
        logger.error(f"Feature retrieval process failed: {e}")
        raise