import json
import os
import pandas as pd
from utils.logging.logger import log_info
from utils.exceptions.exception_handling import handle_exceptions

@handle_exceptions
@log_info
def save_data(file_path: str, data: dict, data_type: str=None, symbol: str=None):
    """Saves data to a JSON file."""
    if isinstance(data, dict):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as raw_file:
            json.dump(data, raw_file, indent=4)
    elif isinstance(data, pd.Dataframe):
        data.to_csv(f"data/processed_data/{symbol}_{data_type}_statement.csv", index=False)