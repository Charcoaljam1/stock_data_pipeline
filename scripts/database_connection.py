import psycopg2
from config.config import DB_CONFIG
from scripts.logger import logger
from contextlib import contextmanager

@contextmanager
def get_db_connection():
    """
    Creates a PostgreSQL database connection and ensures proper cleanup.

    Yields:
        psycopg2.connection: Active database connection.

    Raises:
        Exception: If connection fails.
    """

    conn = None
    try:
        logger.info("Attempting to create a database connection...")
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Database connection established successfully.")
        yield conn
    except Exception as e:
        logger.error(f"Error creating database connection: {e}")
        raise
    finally:
        if conn and not conn.closed:
            conn.close()
            logger.info("Database connection closed.")
