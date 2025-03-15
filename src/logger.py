import logging
import os
from datetime import datetime

def setup_logging(log_prefix="app"):
    """
    Sets up logging with a file prefix and creates the log file.

    Args:
        log_prefix (str): The prefix to use for the log file name.
    """

    LOG_File=f"{log_prefix}_{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"
    logs_path=os.path.join(os.getcwd(),'logs')
    os.makedirs(logs_path,exist_ok=True)

    LOG_FILE_PATH=os.path.join(logs_path,LOG_File)

    logging.basicConfig(
        filename=LOG_FILE_PATH,
        format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO
    )

    return logging.getLogger() #return the logger object.

"""
 ## To test this file working
if __name__=='__main__':
    logger = setup_logging(log_prefix="my_pipeline")
    logger.info("This is a test message")
"""