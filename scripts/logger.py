import logging
import os

# Ensure the logs directory exists
LOG_DIRECTORY = "logs"
os.makedirs(LOG_DIRECTORY, exist_ok=True)

# Configure logging
LOG_FILE_PATH = os.path.join(LOG_DIRECTORY, "app.log")
logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s"
)

# Get logger instance
logger = logging.getLogger(__name__)
