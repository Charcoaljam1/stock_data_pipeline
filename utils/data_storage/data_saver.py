import json
from config.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from config.config import DB_CONFIG
import os
from utils.logging.logger import log_info
from utils.exceptions.exception_handling import handle_exceptions
from sqlalchemy import create_engine, Column, Integer, String, DECIMAL, Date, ForeignKey, BIGINT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Database connection URL
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

# Create the engine
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Base class for ORM models
Base = declarative_base()

# Create a session factory
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)





class DataStorage:
    PROCESSED_DATA_DIR = PROCESSED_DATA_DIR
    RAW_DATA_DIR = RAW_DATA_DIR 

    def __init__(self):
        os.makedirs(self.RAW_DATA_DIR, exist_ok=True)
        os.makedirs(self.PROCESSED_DATA_DIR, exist_ok=True)

    @log_info
    @handle_exceptions
    def save_raw_data(self, data: dict):
        """Saves raw data to JSON files."""
        for symbols, symbol_data in data.items():
            for keys, values in symbol_data.items():
                file_path = os.path.join(self.RAW_DATA_DIR, f"{symbols}_{keys}.json")
                self._save_json(file_path, values)
    @log_info
    @handle_exceptions
    def save_processed_data(self, data: dict):
        """Saves processed data to CSV files."""
        for symbols, symbol_data in data.items():
            for keys, values in symbol_data.items():
                file_path = os.path.join(self.PROCESSED_DATA_DIR, f"{symbols}_{keys}.csv")
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                values.to_csv(file_path, index=False)

    def _save_json(self, file_path: str, data: dict):
        """Helper function to save data as JSON."""
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            json.dump(data, f)



