import os
import pandas as pd
from datetime import datetime, timedelta
import pytz
from feast import Entity, FeatureView, Field, FeatureService
from feast.types import Float32, Int32, String, UnixTimestamp
from feast.value_type import ValueType
from feast import RedshiftSource
import sys

# ------------------ SETUP ------------------
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))
sys.path.append(project_root)
current_dir = os.path.dirname(os.path.abspath(__file__))
from logger import setup_logging
from utils import connect_to_s3
from exception import CustomException

logger = setup_logging("feature_store")

try:
    # Define Feast Components
    logger.info("Defining Feast entities and feature views.")
    customer = Entity(
        name="customer",
        join_keys=["customerid"],
        description="Customer entity",
        value_type=ValueType.STRING
    )

    source = RedshiftSource(
        table="dm4ml_assignment_transformd_data",
        query="SELECT * FROM dm4ml_assignment_transformd_data",
        timestamp_field="event_timestamp",
    )

    customer_features = FeatureView(
        name="customer_features",
        entities=[customer],
        schema=[
            Field(name="paymentmethod_mailed_check", dtype=Int32),
            Field(name="paymentmethod_electronic_check", dtype=Int32),
            Field(name="paymentmethod_credit_card_automatic", dtype=Int32),
            Field(name="paperlessbilling_yes", dtype=Int32),
            Field(name="contract_two_year", dtype=Int32),
            Field(name="contract_one_year", dtype=Int32),
            Field(name="streamingmovies_yes", dtype=Int32),
            Field(name="streamingmovies_no_internet_service", dtype=Int32),
            Field(name="streamingtv_yes", dtype=Int32),
            Field(name="streamingtv_no_internet_service", dtype=Int32),
            Field(name="techsupport_yes", dtype=Int32),
            Field(name="techsupport_no_internet_service", dtype=Int32),
            Field(name="deviceprotection_yes", dtype=Int32),
            Field(name="deviceprotection_no_internet_service", dtype=Int32),
            Field(name="onlinebackup_yes", dtype=Int32),
            Field(name="onlinebackup_no_internet_service", dtype=Int32),
            Field(name="onlinesecurity_yes", dtype=Int32),
            Field(name="onlinesecurity_no_internet_service", dtype=Int32),
            Field(name="internetservice_no", dtype=Int32),
            Field(name="internetservice_fiber_optic", dtype=Int32),
            Field(name="multiplelines_yes", dtype=Int32),
            Field(name="multiplelines_no_phone_service", dtype=Int32),
            Field(name="phoneservice_yes", dtype=Int32),
            Field(name="dependents_yes", dtype=Int32),
            Field(name="partner_yes", dtype=Int32),
            Field(name="seniorcitizen_yes", dtype=Int32),
            Field(name="gender_male", dtype=Int32),
            Field(name="family_label", dtype=Int32),
            Field(name="partner_label", dtype=Int32),
            Field(name="dependents_label", dtype=Int32),
            Field(name="total_services", dtype=Int32),
            Field(name="tenure", dtype=Float32),
            Field(name="monthly_total_ratio", dtype=Float32),
            Field(name="totalcharges", dtype=Float32),
            Field(name="monthlycharges", dtype=Float32),
            Field(name="churn", dtype=String),
            Field(name="customerid", dtype=String),
            #Field(name="created_timestamp", dtype=UnixTimestamp),
            Field(name="event_timestamp", dtype=UnixTimestamp)
        ],
        source=source,
        online=True
    )

    logger.info("Feast feature definitions created successfully.")


    churn_model_svc = FeatureService(
        name="churn_model_svc",
        features=[customer_features],
        description="Feature service for churn prediction"
    )
    logger.info("Feast feature service created successfully.")

except Exception as e:
    logger.critical(f"Script execution failed: {e}")
    raise