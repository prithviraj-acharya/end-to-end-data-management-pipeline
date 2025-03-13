import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.preprocessing import LabelEncoder

import mlflow
import mlflow.sklearn

# -------------------------------------------------------------------
# 1. MLflow Setup
# -------------------------------------------------------------------
mlflow.set_tracking_uri("file:///C:/Users/asood/PycharmProjects/EndtoEnd-data-management-pipeline/mlruns")

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
file_path = "C:/Users/asood/PycharmProjects/EndtoEnd-data-management-pipeline/data/processed/processed_churn_data_20250307_024527.csv"
data = pd.read_csv(file_path)

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
    print(f"Active run_id: {mlflow.active_run().info.run_id}")

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

    print(f"Registered {lr_model_name} with version: {result.version}")

# -------------------------------------------------------------------
# 4. Random Forest: Train, Log, and Register
# -------------------------------------------------------------------
with mlflow.start_run(run_name="RandomForest"):
    print(f"Active run_id: {mlflow.active_run().info.run_id}")

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

    print(f"Registered {rf_model_name} with version: {result.version}")

# -------------------------------------------------------------------
# 5. Print Metrics for Quick Reference
# -------------------------------------------------------------------
print("\n=== Final Metrics ===")
print(f"Logistic Regression -> Accuracy: {lr_accuracy:.4f}, "
      f"Precision: {lr_precision:.4f}, Recall: {lr_recall:.4f}, F1 Score: {lr_f1:.4f}")
print(f"Random Forest -> Accuracy: {rf_accuracy:.4f}, "
      f"Precision: {rf_precision:.4f}, Recall: {rf_recall:.4f}, F1 Score: {rf_f1:.4f}")
