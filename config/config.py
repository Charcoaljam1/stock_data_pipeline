import os
from dotenv import load_dotenv

load_dotenv()

ALPHA_VANTAGE_API_KEY = os.getenv("alpha_vantage_api_key")
DB_CONFIG = {
    'host': os.getenv('db_endpoint'),
    'user': os.getenv('db_user'),
    'password': os.getenv('db_password'),
    'port': os.getenv('db_port'),
    'dbname': os.getenv('db_name')
}
