from prefect import task, flow
import os
import sys
import subprocess

# subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements_mlFlow.txt"])

# ------------------ SETUP ------------------
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.append(project_root)
current_dir = os.path.dirname(os.path.abspath(__file__))
from logger import setup_logging
from utils import connect_to_s3
from exception import CustomException

# Setup Logging
logger = setup_logging("pipeline_orchestrator")
logger.info("Starting Pipeline Orchestrator")

#FIX: Check and deactivate virtual environment properly
logger.info("Deactivating existing virtual environment...")
subprocess.run("deactivate", shell=True, check=False)

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Generic function for subprocess execution
def run_script(script_path):
    try:
        result = subprocess.run(["python", script_path], capture_output=True, text=True, check=True)
        logger.info(f"Script {script_path} executed successfully.")
        logger.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Script {script_path} failed: {e.stderr}")
        raise CustomException(f"Execution failed: {script_path}")  # Raise custom error
    except FileNotFoundError:
        logger.error(f"Script not found: {script_path}")
        raise CustomException(f"File not found: {script_path}")

# Define Prefect Tasks
# Task 1: Data Ingestion
@task(retries=3, retry_delay_seconds=5)
def ingest_task():
    return run_script(os.path.join(project_root, "src", "components", "ingestion.py"))

# Task 2: Upload to Raw Files to S3 Bucket
@task(retries=2)
def upload_to_s3_task():
    return run_script(os.path.join(project_root, "src", "components", "upload_ingested_file.py"))

# Task 3: Manual validation carried on each raw file
@task(retries=2)
def validation_manual_task():
    return run_script(os.path.join(project_root, "src", "components", "validation_manual.py"))

# Task 4: GE validation carried on each raw file
@task(retries=2)
def validation_ge_task():
    return run_script(os.path.join(project_root, "src", "components", "validation_GE.py"))

# Task 5: Merge files from both the sources
@task(retries=2)
def merger_task():
    return run_script(os.path.join(project_root, "src", "components", "merger.py"))

# Task 6: Data Preparation from both the sources
@task(retries=2)
def data_prep_task():
    return run_script(os.path.join(project_root, "src", "components", "data_preparation.py"))

@task(retries=2)
def feature_store_task():
    return run_script(os.path.join(project_root, "src", "components", "feature_store", "features.py"))

@task(retries=2)
def feast_task():
    return run_script(os.path.join(project_root, "src", "components", "feature_store", "execute_feast.py"))

@task(retries=2)
def feature_retrieval_task():
    return run_script(os.path.join(project_root, "src", "components", "feature_store", "feature_retrival.py"))

@task(retries=2)
def mlflow_task():
    return run_script(os.path.join(project_root, "src", "components", "Models_MLflow.py"))


# Define the Flow (DAG)
@flow
def data_pipeline():
    ingest_status = ingest_task()
    upload_to_s3_status = upload_to_s3_task(wait_for=[ingest_status])
    validation_manual_task_status = validation_manual_task(wait_for=[upload_to_s3_status])
    validation_ge_status = validation_ge_task(wait_for=[validation_manual_task_status])
    merger_task_status = merger_task(wait_for=[validation_ge_status])
    data_prep_status = data_prep_task(wait_for=[merger_task_status])
    feature_store_status = feature_store_task(wait_for=[data_prep_status])
    feast_status = feast_task(wait_for=[feature_store_status])
    feature_retrieval_status = feature_retrieval_task(wait_for=[feast_status])
    mlflow_status = mlflow_task(wait_for=[feature_retrieval_status])

    logger.info("Pipeline completed successfully.")


# Run the Flow
if __name__ == "__main__":
    data_pipeline.serve(name="DM4ML_Assignment_Gaurav")
    #prefect deployment build src/main.py:data_pipeline -n "DM4ML_Pipeline"
    #prefect deployment apply data_pipeline-deployment.yaml

    #prefect agent start -q "default"


