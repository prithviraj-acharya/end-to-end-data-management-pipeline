import subprocess
import sys

subprocess.run([sys.executable, "-m", "pip", "freeze", "|", "xargs", "pip" "uninstall" "-y"])
subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])

import os
import yaml
import boto3
import warnings
import sys
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from datetime import datetime
from great_expectations.validator.metric_configuration import MetricConfiguration

# ------------------ SETUP ------------------
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.append(project_root)
current_dir = os.path.dirname(os.path.abspath(__file__))

from exception import CustomException
from logger import setup_logging
from utils import connect_to_s3

# Define the path to the bash script and requirements file
"""bash_script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'components'))
bash_script_path = bash_script_path.replace("\\", "/")
logger.info(f"bash_script_path - {bash_script_path}")
bash_script = "set_specific_venv.sh"
requirements_file_with_GE = "requirements_with_GE.txt"
requirements_file_without_GE = "requirements_without_GE.txt"

# Full path to the bash script
full_bash_script_path = f"{bash_script_path}/{bash_script}"
logger.info(f"full_bash_script_path - {full_bash_script_path}")
# Run the bash script with the requirements file as an argument
subprocess.run(["bash", full_bash_script_path, requirements_file_with_GE])
"""


# Ensure the logs folder exists
logger = setup_logging("validation_GE")

# Suppress python warnings (e.g. DeprecationWarnings)
warnings.filterwarnings("ignore")

# Initialize GE Data Context (assumes GE project is already set up)
context = ge.get_context()

# ------------------ EXPECTATION FUNCTIONS ------------------

def add_kaggle_expectations(validator):
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=15000)
    validator.expect_column_values_to_be_between("SeniorCitizen", min_value=0, max_value=1)
    validator.expect_column_values_to_be_between("tenure", min_value=0, max_value=100)
    validator.expect_column_values_to_be_between("MonthlyCharges", min_value=0, max_value=500)
    validator.expect_column_values_to_not_be_null("SeniorCitizen")
    validator.expect_column_values_to_not_be_null("tenure")
    validator.expect_column_values_to_not_be_null("MonthlyCharges")
    validator.expect_column_values_to_not_be_null("TotalCharges")
    validator.expect_column_values_to_be_in_set("gender", ["Male", "Female"])
    validator.expect_column_values_to_be_in_set("Partner", ["Yes", "No"])
    validator.expect_column_values_to_be_in_set("Dependents", ["Yes", "No"])
    validator.expect_column_values_to_be_in_set("PhoneService", ["Yes", "No"])
    validator.expect_column_values_to_be_in_set("MultipleLines", ["No phone service", "No", "Yes"])
    validator.expect_column_values_to_be_in_set("InternetService", ["DSL", "Fiber optic", "No"])
    validator.expect_column_values_to_be_in_set("OnlineSecurity", ["No", "Yes", "No internet service"])
    validator.expect_column_values_to_be_in_set("OnlineBackup", ["No", "Yes", "No internet service"])
    validator.expect_column_values_to_be_in_set("DeviceProtection", ["No", "Yes", "No internet service"])
    validator.expect_column_values_to_be_in_set("TechSupport", ["No", "Yes", "No internet service"])
    validator.expect_column_values_to_be_in_set("StreamingTV", ["No", "Yes", "No internet service"])
    validator.expect_column_values_to_be_in_set("StreamingMovies", ["No", "Yes", "No internet service"])
    validator.expect_column_values_to_be_in_set("Contract", ["Month-to-month", "One year", "Two year"])
    validator.expect_column_values_to_be_in_set("PaperlessBilling", ["Yes", "No"])
    validator.expect_column_values_to_be_in_set(
        "PaymentMethod",
        ["Electronic check", "Mailed check", "Bank transfer (automatic)", "Credit card (automatic)"]
    )
    validator.expect_column_values_to_be_in_set("Churn", ["No", "Yes"])


def add_local_expectations(validator):
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=15000)
    validator.expect_column_values_to_be_between("SeniorCitizen", min_value=0, max_value=1)
    validator.expect_column_values_to_be_between("tenure", min_value=0, max_value=100)
    validator.expect_column_values_to_be_between("Churn", min_value=0, max_value=1)
    validator.expect_column_values_to_not_be_null("SeniorCitizen")
    validator.expect_column_values_to_not_be_null("tenure")
    validator.expect_column_values_to_not_be_null("Churn")
    validator.expect_column_values_to_be_in_set("gender", ["Male", "Female"])
    validator.expect_column_values_to_be_in_set("Partner", ["Yes", "No"])
    validator.expect_column_values_to_be_in_set("Dependents", ["Yes", "No"])
    validator.expect_column_values_to_be_in_set("PhoneService", ["Yes", "No"])
    validator.expect_column_values_to_be_in_set("MultipleLines", ["Yes", "No", "No phone service"])
    validator.expect_column_values_to_be_in_set("InternetService", ["DSL", "Fiber optic", "No"])
    validator.expect_column_values_to_be_in_set("OnlineSecurity", ["Yes", "No", "No internet service"])
    validator.expect_column_values_to_be_in_set("OnlineBackup", ["Yes", "No", "No internet service"])


# ------------------ UTILITY FUNCTION ------------------

def get_latest_s3_object(base_prefix):
    # Initialize S3 client
    s3_client, s3_bucket_name, contetnt_present_flag = connect_to_s3()
    response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=base_prefix)
    objects = response.get("Contents", [])
    if not objects:
        raise ValueError(f"No objects found for prefix: {base_prefix}")
    latest_obj = max(objects, key=lambda x: x["LastModified"])
    return s3_bucket_name, latest_obj["Key"]


# ------------------ VALIDATION FUNCTION ------------------

def validate_latest_file(source):
    logger.info(f"Starting GE validation with {source}")

    if source == "kaggle":
        base_prefix = "data/raw/kaggle/"
        suite_name = "customer_churn_suite_kaggle"
    elif source == "rds":
        base_prefix = "data/raw/rds/"
        suite_name = "customer_churn_suite_rds"
    else:
        raise ValueError("Source must be 'kaggle' or 'rds'.")

    s3_bucket_name, key = get_latest_s3_object(base_prefix)
    s3_file_path = f"s3://{s3_bucket_name}/{key}"
    logger.info(f"Latest {source} file: {s3_file_path}")

    runtime_params = {"path": s3_file_path}

    logger.info(f"runtime_params: {runtime_params}")

    if source == "kaggle":
        runtime_params["reader_options"] = {"skipinitialspace": True}

    logger.info("Setting batch_request")

    batch_request = RuntimeBatchRequest(
        datasource_name="my_s3_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=f"customer_churn_dataset_{source}",
        runtime_parameters=runtime_params,
        batch_identifiers={"default_identifier_name": "default_batch"}
    )

    logger.info(f"batch_request: {batch_request}")

    context.create_expectation_suite(expectation_suite_name=suite_name, overwrite_existing=True)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )

    columns_metric = MetricConfiguration(
        metric_name="table.columns",
        metric_domain_kwargs={},
        metric_value_kwargs={}
    )
    actual_columns = validator.get_metric(metric=columns_metric)
    logger.info(f"Detected columns in {source} data: {actual_columns}")

    # Add core expectations
    if source == "kaggle":
        add_kaggle_expectations(validator)
    else:
        add_local_expectations(validator)

    results = validator.validate()
    logger.info(f"Validation results for {source} dataset:")
    logger.info(results)

    row_count_metric = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={},
        metric_value_kwargs={}
    )
    row_count = validator.get_metric(metric=row_count_metric)

    missing_counts = {}
    for col in actual_columns:
        missing_metric = MetricConfiguration(
            metric_name="column_values.null.count",
            metric_domain_kwargs={"column": col},
            metric_value_kwargs={}
        )
        missing_counts[col] = validator.get_metric(metric=missing_metric)

    logger.info(f"Total row count: {row_count}")
    logger.info("Missing values per column:")
    for col, missing in missing_counts.items():
        logger.info(f"  {col}: {missing}")

    validator.save_expectation_suite(discard_failed_expectations=False)

    # Build Data Docs and retrieve URLs
    context.build_data_docs()
    docs_urls = context.get_docs_sites_urls()
    logger.info("Data Docs available at:")

    if isinstance(docs_urls, dict):
        url = docs_urls.get("site_url")
        logger.info(url)
        logger.info(f"Data Docs available at: {url}")
    elif isinstance(docs_urls, list):
        # Assuming list items are dictionaries with a 'site_url' key:
        for doc in docs_urls:
            url = doc.get("site_url") if isinstance(doc, dict) else doc
            logger.info(url)
            logger.info(f"Data Docs available at: {url}")
    else:
        logger.info("Unexpected format for docs URLs:", docs_urls)
        logger.info(f"Unexpected format for docs URLs: {docs_urls}")

    logger.info(f"Validation complete for {source} dataset. Data quality report generated.")
    logger.info(f"Validation for {source} dataset completed and Data Docs built.")



# ------------------ MAIN FUNCTION ------------------

def main():
    for source in ["kaggle", "rds"]:
        validate_latest_file(source)


if __name__ == "__main__":
    main()

    # Run the bash script with the requirements file as an argument
    # subprocess.run(["bash", bash_script, requirements_file_without_GE])
