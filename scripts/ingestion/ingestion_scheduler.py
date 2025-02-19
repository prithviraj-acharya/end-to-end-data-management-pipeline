from apscheduler.schedulers.background import BackgroundScheduler
import subprocess
import time  # Import time module for sleep
from logs.logging_config import setup_logging  # Import reusable logging

# Setup logging for scheduler
logger = setup_logging("ingestion.log")

def execute_pipeline():
    """Runs the ingestion pipeline script at scheduled intervals."""
    try:
        logger.info("Starting data ingestion pipeline...")
        result = subprocess.run(["python", "-m", "scripts.ingestion.ingest_data"], capture_output=True, text=True)

        if result.returncode == 0:
            logger.info("Data ingestion pipeline executed successfully.")
        else:
            logger.error(f"Ingestion pipeline failed.\n{result.stderr}")

    except Exception as e:
        logger.error(f"Error while running ingestion pipeline: {str(e)}")



# Run the ingestion pipeline immediately before scheduling
execute_pipeline()

# Initialize Scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(execute_pipeline, "interval", seconds=30)  # Runs every 30 seconds
scheduler.start()


logger.info("Scheduler started. Running ingestion every 30 seconds...")
print("Scheduler is running... Press Ctrl+C to stop.")

# Keep the script running
try:
    while True:
        time.sleep(1)  # Prevents script from exiting
except KeyboardInterrupt:
    logger.info("Scheduler stopped manually.")
    scheduler.shutdown()
