from apscheduler.schedulers.background import BackgroundScheduler
import subprocess
import time  # Import time module for sleep
from logs.logging_config import setup_logging  # Import reusable logging

# Setup logging for scheduler
logger = setup_logging("ingestion.log")

def execute_pipeline():
    """Runs the ingestion pipeline and uploads data to S3 at scheduled intervals."""
    try:
        logger.info("Starting data ingestion pipeline...")
        result_ingestion = subprocess.run(["python", "-m", "scripts.ingestion.ingest_data"], capture_output=True, text=True)

        if result_ingestion.returncode == 0:
            logger.info("Data ingestion pipeline executed successfully.")
        else:
            logger.error(f"Ingestion pipeline failed.\n{result_ingestion.stderr}")

        # Run Upload to S3 Process
        logger.info("Uploading data to AWS S3...")
        result_upload = subprocess.run(["python", "-m", "scripts.ingestion.upload_to_s3"], capture_output=True, text=True)

        if result_upload.returncode == 0:
            logger.info("Data upload to S3 completed successfully.")
        else:
            logger.error(f"Upload to S3 failed.\n{result_upload.stderr}")

    except Exception as e:
        logger.error(f"Error while running ingestion pipeline: {str(e)}")

# Run ingestion immediately before scheduling
execute_pipeline()

# Initialize Scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(execute_pipeline, "interval", minutes=1)  # Runs every 1 minute for testing
scheduler.start()

logger.info("Scheduler started. Running ingestion every 1 minute...")
print("Scheduler is running... Press Ctrl+C to stop.")

# Keep the script running
try:
    while True:
        time.sleep(1)  # Prevents script from exiting
except KeyboardInterrupt:
    logger.info("Scheduler stopped manually.")
    scheduler.shutdown(wait=True)
