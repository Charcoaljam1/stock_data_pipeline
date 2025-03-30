import os
from dotenv import load_dotenv

load_dotenv()

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
DB_CONFIG = {
    'host': os.getenv('DB_ENDPOINT'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_NAME')
}

RAW_DATA_DIR = os.getenv('RAW_DATA_DIR')
PROCESSED_DATA_DIR = os.getenv('PROCESSED_DATA_DIR')