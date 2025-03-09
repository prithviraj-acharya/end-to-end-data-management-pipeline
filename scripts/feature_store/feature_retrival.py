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

import pandas as pd

import pandas as pd

import pandas as pd
from feast import FeatureStore

def fetch_offline_features(fs: FeatureStore):
    # Define entity query to fetch all distinct customer IDs from Redshift
    entity_query = "SELECT DISTINCT customer_warehouse.customerid, customer_warehouse.event_timestamp FROM public.customer_warehouse"

    # Fetch historical features using the correct feature view name
    entity_df = fs.get_historical_features(
        entity_df=entity_query,
        features=[
            "customer_features:gender",
            "customer_features:seniorcitizen",
            "customer_features:partner",
            "customer_features:dependents",
            "customer_features:tenure",
            "customer_features:phoneservice",
            "customer_features:multiplelines",
            "customer_features:internetservice",
            "customer_features:onlinesecurity",
            "customer_features:onlinebackup",
            "customer_features:deviceprotection",
            "customer_features:techsupport",
            "customer_features:streamingtv",
            "customer_features:streamingmovies",
            "customer_features:contract",
            "customer_features:paperlessbilling",
            "customer_features:paymentmethod",
            "customer_features:monthlycharges",
            "customer_features:totalcharges",
            "customer_features:churn",
        ]  # Explicitly list all features
    ).to_df()

    return entity_df

if __name__ == "__main__":
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, "../../"))
        repo_path = os.path.join(project_root, "customer_churn_feast/feature_repo")
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

        logger.info("Feature retrieval completed successfully.")
    except Exception as e:
        logger.error(f"Feature retrieval process failed: {e}")
        raise




#     CREATE TABLE public.customer - churn - warehouse (
#         customerid character varying(256) NOT NULL ENCODE lzo,
#     gender character varying(256) ENCODE lzo,
#     seniorcitizen bigint ENCODE az64,
#     partner character varying(256) ENCODE lzo,
#     dependents character varying(256) ENCODE lzo,
#     tenure bigint ENCODE az64,
#     phoneservice character varying(256) ENCODE lzo,
#     multiplelines character varying(256) ENCODE lzo,
#     internetservice character varying(256) ENCODE lzo,
#     onlinesecurity character varying(256) ENCODE lzo,
#     onlinebackup character varying(256) ENCODE lzo,
#     deviceprotection character varying(256) ENCODE lzo,
#     techsupport character varying(256) ENCODE lzo,
#     streamingtv character varying(256) ENCODE lzo,
#     streamingmovies character varying(256) ENCODE lzo,
#     contract character varying(256) ENCODE lzo,
#     paperlessbilling character varying(256) ENCODE lzo,
#     paymentmethod character varying(256) ENCODE lzo,
#     monthlycharges double precision ENCODE raw,
#     totalcharges double precision ENCODE raw,
#     churn character varying(256) ENCODE lzo,
#     event_timestamp timestamp without time zone ENCODE az64,
#     created_timestamp timestamp without time zone ENCODE az64,
#     PRIMARY KEY (customerid)
#     ) DISTSTYLE AUTO
# SORTKEY
# (customerid);