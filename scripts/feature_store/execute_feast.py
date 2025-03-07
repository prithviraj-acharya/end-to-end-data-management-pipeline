import os
import pandas as pd
from datetime import datetime
import pytz
from feast import FeatureStore
from scripts.feature_store.features import customer, customer_features
from logs.logging_config import setup_logging

logger = setup_logging("feast_apply.log")

try:
    # Define FeatureStore repo path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "../../"))
    repo_path = os.path.join(project_root, "customer_churn_feast/feature_repo")
    logger.info(f"Initializing FeatureStore at '{repo_path}'")

    # Initialize FeatureStore
    fs = FeatureStore(repo_path=repo_path)
    logger.info("FeatureStore initialized successfully.")

    # Apply feature definitions
    logger.info("Applying feature definitions to the store.")
    fs.apply([customer, customer_features])
    logger.info("Feature definitions applied successfully.")

    # Materialize incremental features
    end_time = datetime.now()
    logger.info(f"Starting feature materialization until {end_time}")
    fs.materialize_incremental(end_date=end_time)
    logger.info("Features materialized successfully.")

    print("Features created and materialized!")
    logger.info("Feast apply process completed successfully.")

except Exception as e:
    logger.error(f"An error occurred during the Feast apply process: {e}")
    raise