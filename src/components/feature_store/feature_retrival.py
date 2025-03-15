import os
import pandas as pd
from datetime import datetime
import pytz
from feast import FeatureStore
import sys
import json
import subprocess

# ------------------ SETUP ------------------
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))
sys.path.append(project_root)
current_dir = os.path.dirname(os.path.abspath(__file__))
from logger import setup_logging
from utils import connect_to_s3
from exception import CustomException
from components.feature_store.features import customer, customer_features

bash_script_path = os.path.join(current_dir, "set_specific_venv.sh")

# Define the path to the bash script and requirements file
bash_script = bash_script_path
print(f"bash_script_path - {bash_script_path}")
requirements_file_without_GE = "requirements_without_GE.txt"

# Run the bash script with the requirements file as an argument
#subprocess.run(["bash", bash_script, requirements_file_without_GE])

logger = setup_logging("feature_store")

artifacts_path = os.path.abspath(os.path.join(project_root, "..", "artifacts", "feature_metadata.json"))
print(f"artifacts_path - {artifacts_path}")
 
def extract_feature_metadata(fs: FeatureStore, output_file: str = None):
    try:
        print(f"output_file - {output_file}")
        # Get feature views from the registry
        feature_views = fs.list_feature_views()

        metadata = {}
        for fv in feature_views:
            feature_list = []
            for feature in fv.features:
                feature_list.append({
                    "name": feature.name,
                    "data_type": str(feature.dtype),
                    "description": feature.description if feature.description else "N/A",
                    "source": fv.batch_source.name,
                    "version": "v1"  # You may need to track versions separately
                })
            metadata[fv.name] = feature_list

        # Convert metadata to JSON
        metadata_json = json.dumps(metadata, indent=4)

        # Save to file if specified
        if output_file:
            with open(output_file, "w") as f:
                f.write(metadata_json)
            print(f"Feature metadata saved to {output_file}")

        return metadata

    except Exception as e:
        print(f"Error extracting feature metadata: {e}")
        return None

def get_online_features(fs, customer_ids):
    try:
        logger.info(f"Fetching online features for customers: {customer_ids}")
        features = fs.get_online_features(
            features=[
                "customer_features:tenure",
                "customer_features:totalcharges",
                "customer_features:churn"
            ],
            entity_rows=[{"customerid": cid} for cid in customer_ids]
        )
        features_df = features.to_df() if features else pd.DataFrame()
        logger.info(f"Online feature retrieval successful. Retrieved {len(features_df)} rows.")
        logger.info(f"Online features:\n{features_df.to_string(index=False)}")
        return features_df
    except Exception as e:
        logger.error(f"Online feature retrieval failed: {e}")
        raise

def fetch_offline_features(fs: FeatureStore):
    # Define entity query to fetch all distinct customer IDs from Redshift
    entity_query = "SELECT DISTINCT dm4ml_assignment_transformd_data.customerid, dm4ml_assignment_transformd_data.event_timestamp FROM public.dm4ml_assignment_transformd_data"

    # Fetch historical features using the correct feature view name
    entity_df = fs.get_historical_features(
        entity_df=entity_query,
        features=[
            "customer_features:paymentmethod_mailed_check",
            "customer_features:paymentmethod_electronic_check",
            "customer_features:paymentmethod_credit_card_automatic",
            "customer_features:paperlessbilling_yes",
            "customer_features:contract_two_year",
            "customer_features:contract_one_year",
            "customer_features:streamingmovies_yes",
            "customer_features:streamingmovies_no_internet_service",
            "customer_features:streamingtv_yes",
            "customer_features:streamingtv_no_internet_service",
            "customer_features:techsupport_yes",
            "customer_features:techsupport_no_internet_service",
            "customer_features:deviceprotection_yes",
            "customer_features:deviceprotection_no_internet_service",
            "customer_features:onlinebackup_yes",
            "customer_features:onlinebackup_no_internet_service",
            "customer_features:onlinesecurity_yes",
            "customer_features:onlinesecurity_no_internet_service",
            "customer_features:internetservice_no",
            "customer_features:internetservice_fiber_optic",
            "customer_features:multiplelines_yes",
            "customer_features:multiplelines_no_phone_service",
            "customer_features:phoneservice_yes",
            "customer_features:dependents_yes",
            "customer_features:partner_yes",
            "customer_features:seniorcitizen_yes",
            "customer_features:gender_male",
            "customer_features:family_label",
            "customer_features:partner_label",
            "customer_features:dependents_label",
            "customer_features:total_services",
            "customer_features:tenure",
            "customer_features:monthly_total_ratio",
            "customer_features:totalcharges",
            "customer_features:monthlycharges",
            "customer_features:churn",
        ]
    ).to_df()

    return entity_df

if __name__ == "__main__":
    try:
        repo_path = os.path.join(project_root, "..", "customer_churn_feast/feature_repo")
        logger.info(f"Initializing FeatureStore at '{repo_path}'")

        fs = FeatureStore(repo_path=repo_path)
        logger.info("FeatureStore initialized successfully.")

        sample_customer_ids = ["7729-JTEEC", "4909-JOUPP", "4657-FWVFY"]
        online_features = get_online_features(fs, sample_customer_ids)

        start_date = datetime(2024, 3, 1, tzinfo=pytz.UTC)
        end_date = datetime.now(tz=pytz.UTC)

        offline_features = fetch_offline_features(fs)
        logger.info(f"Offline features retrieved: {offline_features.shape[0]} rows.")
        logger.info(f"Offline features:\n{offline_features.head(5)}")

        logger.info(f"Creating feature metadata json file...")

        metadata = extract_feature_metadata(fs, output_file=artifacts_path)

        logger.info("Feature retrieval completed successfully.")
    except Exception as e:
        logger.error(f"Feature retrieval process failed: {e}")
        raise
