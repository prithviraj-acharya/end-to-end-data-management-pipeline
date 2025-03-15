import subprocess
import sys
subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements_mlFlow.txt"])

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.preprocessing import LabelEncoder
import os
import mlflow
import mlflow.sklearn
from feast import FeatureStore


# ------------------ SETUP ------------------
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.append(project_root)
current_dir = os.path.dirname(os.path.abspath(__file__))
from logger import setup_logging
from utils import connect_to_s3
from exception import CustomException
# Ensure the logs folder exists
logger = setup_logging("Models_MLflow")

# -------------------------------------------------------------------
# 1. MLflow Setup
# -------------------------------------------------------------------

# mlflow.set_tracking_uri(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')), "mlruns"))
mlflow.set_tracking_uri("file:./mlflow_runs")
logger.info(mlflow.get_tracking_uri())

experiment_name = "Churn_Prediction_Experiments"
experiment = mlflow.get_experiment_by_name(experiment_name)
if experiment is None:
    mlflow.create_experiment(experiment_name)
mlflow.set_experiment(experiment_name)

# Define registry model names (these remain constant, so each new run
# will create a new version under the same registered model)
lr_model_name = "Churn_LRModel"
rf_model_name = "Churn_RFModel"

# -------------------------------------------------------------------
# 2. Load and Prepare Data
# -------------------------------------------------------------------
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

repo_path = os.path.join(project_root, "..", "customer_churn_feast/feature_repo")
logger.info(f"Initializing FeatureStore at '{repo_path}'")

fs = FeatureStore(repo_path=repo_path)
logger.info("FeatureStore initialized successfully.")

data_original = fetch_offline_features(fs)

# Dropping Datetime field
data = data_original.drop(['event_timestamp'], axis=1)

# Convert integer columns to float64
data = data.astype({col: 'float64' for col in data.select_dtypes(include=['int64']).columns})

# Drop 'customerid' and separate features/target
X = data.drop(['churn', 'customerid'], axis=1)
y = data['churn']

# Convert 'churn' to numeric values
label_encoder = LabelEncoder()
y = label_encoder.fit_transform(y)

# Fill missing values (simple approach)
X = X.fillna(0)

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Prepare an example input for logging
input_example = X_test.iloc[0].to_dict()

# -------------------------------------------------------------------
# 3. Logistic Regression: Train, Log, and Register
# -------------------------------------------------------------------
with mlflow.start_run(run_name="LogisticRegression"):
    logger.info(f"Active run_id: {mlflow.active_run().info.run_id}")

    # Train the model
    lr_model = LogisticRegression(max_iter=1000, class_weight='balanced')
    lr_model.fit(X_train, y_train)

    # Predict and compute metrics
    lr_pred = lr_model.predict(X_test)
    lr_accuracy = accuracy_score(y_test, lr_pred)
    lr_precision = precision_score(y_test, lr_pred)
    lr_recall = recall_score(y_test, lr_pred)
    lr_f1 = f1_score(y_test, lr_pred)

    # Log parameters and metrics
    mlflow.log_param("Model", "Logistic Regression")
    mlflow.log_param("Max Iterations", 1000)
    mlflow.log_param("Class Weight", "Balanced")
    mlflow.log_metric("Accuracy", lr_accuracy)
    mlflow.log_metric("Precision", lr_precision)
    mlflow.log_metric("Recall", lr_recall)
    mlflow.log_metric("F1 Score", lr_f1)

    # Log the model artifact
    mlflow.sklearn.log_model(
        lr_model,
        artifact_path="logistic_regression_model",
        input_example=input_example
    )

    # Construct the model URI for the run we just logged
    model_uri = f"runs:/{mlflow.active_run().info.run_id}/logistic_regression_model"

    # Register the model in the Model Registry
    # This will create a new version each time this code runs
    result = mlflow.register_model(model_uri, lr_model_name)

    logger.info(f"Registered {lr_model_name} with version: {result.version}")

# -------------------------------------------------------------------
# 4. Random Forest: Train, Log, and Register
# -------------------------------------------------------------------
with mlflow.start_run(run_name="RandomForest"):
    logger.info(f"Active run_id: {mlflow.active_run().info.run_id}")

    # Train the model
    rf_model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
    rf_model.fit(X_train, y_train)

    # Predict and compute metrics
    rf_pred = rf_model.predict(X_test)
    rf_accuracy = accuracy_score(y_test, rf_pred)
    rf_precision = precision_score(y_test, rf_pred)
    rf_recall = recall_score(y_test, rf_pred)
    rf_f1 = f1_score(y_test, rf_pred)

    # Log parameters and metrics
    mlflow.log_param("Model", "Random Forest")
    mlflow.log_param("Number of Estimators", 100)
    mlflow.log_param("Class Weight", "Balanced")
    mlflow.log_metric("Accuracy", rf_accuracy)
    mlflow.log_metric("Precision", rf_precision)
    mlflow.log_metric("Recall", rf_recall)
    mlflow.log_metric("F1 Score", rf_f1)

    # Log the model artifact
    mlflow.sklearn.log_model(
        rf_model,
        artifact_path="random_forest_model",
        input_example=input_example
    )

    # Construct the model URI for the run we just logged
    model_uri = f"runs:/{mlflow.active_run().info.run_id}/random_forest_model"

    # Register the model in the Model Registry
    result = mlflow.register_model(model_uri, rf_model_name)

    logger.info(f"Registered {rf_model_name} with version: {result.version}")

# -------------------------------------------------------------------
# 5. Print Metrics for Quick Reference
# -------------------------------------------------------------------
logger.info("\n=== Final Metrics ===")
logger.info(f"Logistic Regression -> Accuracy: {lr_accuracy:.4f}, "
      f"Precision: {lr_precision:.4f}, Recall: {lr_recall:.4f}, F1 Score: {lr_f1:.4f}")
logger.info(f"Random Forest -> Accuracy: {rf_accuracy:.4f}, "
      f"Precision: {rf_precision:.4f}, Recall: {rf_recall:.4f}, F1 Score: {rf_f1:.4f}")


# subprocess.Popen(["mlflow", "ui", "--backend-store-uri", "/Users/lizapersonal/PycharmProjects/data-management-pipeline/mlflow_runs"])