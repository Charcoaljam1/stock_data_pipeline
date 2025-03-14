import psycopg2
from config import db_config

def get_db_connection():
    """Creates and returns a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except Exception as e:
        print(f'Error creating connection: {e}')
