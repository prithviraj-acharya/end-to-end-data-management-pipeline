import os
from datetime import datetime
import pytz
from feast import FeatureStore
import sys

# ------------------ SETUP ------------------
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))
sys.path.append(project_root)
current_dir = os.path.dirname(os.path.abspath(__file__))
from logger import setup_logging
from utils import connect_to_s3
from exception import CustomException
from components.feature_store.features import customer, customer_features


logger = setup_logging("feast_apply")

try:
    # Define FeatureStore repo path
    repo_path = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')), "customer_churn_feast/feature_repo")
    print(f"repo_Path - {repo_path}")
    logger.info(f"Initializing FeatureStore at '{repo_path}'")

    # Initialize FeatureStore
    fs = FeatureStore(repo_path=repo_path)
    logger.info("FeatureStore initialized successfully.")

    # Apply feature definitions
    logger.info("Applying feature definitions to the store.")
    fs.apply([customer, customer_features])
    logger.info("Feature definitions applied successfully.")

    # Materialize incremental features dynamically
    end_time = datetime.now(tz=pytz.UTC)
    #start_time = datetime(2024, 3, 1, tzinfo=pytz.UTC)  # Hardcoded start date
    #logger.info(f"Starting feature materialization from {start_time} to {end_time}")
    fs.materialize_incremental(end_date=end_time)
    logger.info("Features materialized successfully.")

    print("Features created and materialized!")
    logger.info("Feast apply process completed successfully.")

except Exception as e:
    logger.error(f"An error occurred during the Feast apply process: {e}")
    raise