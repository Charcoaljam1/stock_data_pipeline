import psycopg2
from config import DB_CONFIG

def get_db_connection():
    """Creates and returns a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f'Error creating connection: {e}')
