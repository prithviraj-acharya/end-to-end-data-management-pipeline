import os
import pandas as pd
from datetime import datetime, timedelta
import pytz
from feast import Entity, FeatureView, Field, FeatureService
from feast.types import Float32, Int32, String, UnixTimestamp
from feast.value_type import ValueType
from feast import RedshiftSource
from logs.logging_config import setup_logging

logger = setup_logging("feature_store.log")

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
        table="customer_warehouse",
        query="SELECT * FROM customer_warehouse",
        timestamp_field="event_timestamp",
    )

    customer_features = FeatureView(
        name="customer_features",
        entities=[customer],
        schema=[
            Field(name="gender", dtype=String),
            Field(name="seniorcitizen", dtype=Int32),
            Field(name="partner", dtype=String),
            Field(name="dependents", dtype=String),
            Field(name="tenure", dtype=Int32),
            Field(name="phoneservice", dtype=String),
            Field(name="multiplelines", dtype=String),
            Field(name="internetservice", dtype=String),
            Field(name="onlinesecurity", dtype=String),
            Field(name="onlinebackup", dtype=String),
            Field(name="deviceprotection", dtype=String),
            Field(name="techsupport", dtype=String),
            Field(name="streamingtv", dtype=String),
            Field(name="streamingmovies", dtype=String),
            Field(name="contract", dtype=String),
            Field(name="paperlessbilling", dtype=String),
            Field(name="paymentmethod", dtype=String),
            Field(name="monthlycharges", dtype=Float32),
            Field(name="totalcharges", dtype=String),
            Field(name="churn", dtype=String),
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