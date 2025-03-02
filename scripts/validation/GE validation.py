import os
import yaml
import boto3
import logging
import warnings
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from datetime import datetime
from great_expectations.validator.metric_configuration import MetricConfiguration

# ------------------ SETUP ------------------

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

# Ensure the logs folder exists
logs_folder = os.path.join(project_root, "logs")
os.makedirs(logs_folder, exist_ok=True)

# Configure logging
log_file_path = os.path.join(logs_folder, "ingestion.log")
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Suppress python warnings (e.g. DeprecationWarnings)
warnings.filterwarnings("ignore")

# Reduce Great Expectations logger level so that only errors appear
ge_logger = logging.getLogger("great_expectations")
ge_logger.setLevel(logging.ERROR)

# Load AWS credentials
credentials_path = os.path.join(project_root, "config", "credentials.yaml")
with open(credentials_path, "r") as file:
    creds = yaml.safe_load(file)

bucket_name = "dmml-assignment1-bucket"

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=creds["aws"]["access_key"],
    aws_secret_access_key=creds["aws"]["secret_key"],
)

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
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=base_prefix)
    objects = response.get("Contents", [])
    if not objects:
        raise ValueError(f"No objects found for prefix: {base_prefix}")
    latest_obj = max(objects, key=lambda x: x["LastModified"])
    return latest_obj["Key"]


# ------------------ VALIDATION FUNCTION ------------------

def validate_latest_file(source):
    if source == "kaggle":
        base_prefix = "kaggle/"
        suite_name = "customer_churn_suite_kaggle"
    elif source == "local":
        base_prefix = "local/"
        suite_name = "customer_churn_suite_local"
    else:
        raise ValueError("Source must be 'kaggle' or 'local'.")

    key = get_latest_s3_object(base_prefix)
    s3_file_path = f"s3://{bucket_name}/{key}"
    print(f"Latest {source} file: {s3_file_path}")

    runtime_params = {"path": s3_file_path}
    if source == "kaggle":
        runtime_params["reader_options"] = {"skipinitialspace": True}

    batch_request = RuntimeBatchRequest(
        datasource_name="my_s3_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=f"customer_churn_dataset_{source}",
        runtime_parameters=runtime_params,
        batch_identifiers={"default_identifier_name": "default_batch"}
    )

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
    print(f"Detected columns in {source} data: {actual_columns}")

    # Add core expectations
    if source == "kaggle":
        add_kaggle_expectations(validator)
    else:
        add_local_expectations(validator)

    results = validator.validate()
    print(f"Validation results for {source} dataset:")
    print(results)

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

    print("Total row count:", row_count)
    print("Missing values per column:")
    for col, missing in missing_counts.items():
        print(f"  {col}: {missing}")

    validator.save_expectation_suite(discard_failed_expectations=False)

    context.build_data_docs()
    context.open_data_docs()
    # Log only one line that validation is complete
    logging.info(f"Validation complete for {source} dataset. Data quality report generated.")
    print(f"Validation for {source} dataset completed and Data Docs built.")


# ------------------ MAIN FUNCTION ------------------

def main():
    for source in ["kaggle", "local"]:
        validate_latest_file(source)


if __name__ == "__main__":
    main()
