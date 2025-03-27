import requests
import json
from scripts.logger import log_info, handle_exceptions
import os
from config.config import ALPHA_VANTAGE_API_KEY
import pandas as pd
from collections import defaultdict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception


url = f"https://www.alphavantage.co/query"

class StockFetcher:
    DATA_DIR = "data/raw_data"
   

    def __init__(self, symbols, data_types, url):
        self.symbols = symbols
        self.data_types = data_types
        self.data = defaultdict(lambda: defaultdict(dict))
        self.url = url
        

    @handle_exceptions
    @log_info
    def get_data(self):
        
        validation_result = input_validation(self.data_types, self.symbols)
        if validation_result["error"]:
            raise ValueError(validation_result["message"])
        else:
            print("Validation passed!")
        
        
        for data_type in self.data_types:
            for symbol in self.symbols:
                params = build_parameters(symbol, data_type)
                response = fetch_api_response(self.url, params)
                if len(response) == 1:
                    raise ValueError("API rate limit has been reached")
                file_path = os.path.join(StockFetcher.DATA_DIR, f"{symbol}_{data_type}.json")
                save_data(file_path, response)
                self.data[symbol][data_type] = response

   
def build_parameters(symbol: str, data_type: str) -> dict:
    function_mapping = {
        "daily": "TIME_SERIES_DAILY",
        "income": "INCOME_STATEMENT",
        "balance": "BALANCE_SHEET",
        "cash": "CASH_FLOW",
        "eps": "EARNINGS",
        "info": "OVERVIEW"  
        }
    params = {
        "function": function_mapping[data_type],
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    if data_type == "daily":
        params.update({"datatype": "json", "outputsize": "full"})
    return params


def is_retryable_exception(exception):
    """Check if the exception is retryable."""
    if isinstance(exception, requests.exceptions.HTTPError) and getattr(exception.response, "status_code", None) in [429, 500, 503]:
        return True  
    if isinstance(exception, requests.exceptions.RequestException):
        return True 
    return False

@handle_exceptions
@log_info
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception(is_retryable_exception),
)
def fetch_api_response(url: str, params: dict) -> dict:
    response = requests.get(url, params=params)
    response.raise_for_status() 
    return response.json()

@handle_exceptions
@log_info
def save_data(file_path: str, data: dict):
    """Saves data to a JSON file."""
    if isinstance(data, dict):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as raw_file:
            json.dump(data, raw_file, indent=4)
    elif isinstance(data, pd.Dataframe):
        .to_csv(f"data/processed_data/{data['symbol']}_{data_type}_statement.csv", index=False)


def input_validation(function: str, symbol: str) -> dict:
    """Validates API key, function, and symbol."""

    function_mapping = {
        "daily": "TIME_SERIES_DAILY",
        "income": "INCOME_STATEMENT",
        "balance": "BALANCE_SHEET",
        "cash": "CASH_FLOW",
        "eps": "EARNINGS",
        "info": "OVERVIEW"  
        }

    if not ALPHA_VANTAGE_API_KEY:
        return {"error": True, "message": "API key is missing. Please check the 'config/config.py' file."}
    
    if isinstance(symbol, list):
        for tick in symbol:
            if len(tick) > 5:
                return {"error": True, "message": f"Invalid symbol '{tick}'. Maximum length is 5 characters."}
            if not tick.isalpha():
                return {"error": True, "message": f"Invalid symbol '{tick}'. Only alphabetic characters are allowed."}
    elif isinstance(symbol, str) and len(symbol) > 5:
        return {"error": True, "message": f"Invalid symbol '{symbol}'. Maximum length is 5 characters."}
    elif not isinstance(symbol, str) or not symbol.isalpha():
        return {"error": True, "message": f"Invalid symbol '{symbol}'. Only alphabetic characters are allowed."}
    
    if function not in function_mapping:
        return {"error": True, "message": f"Invalid function '{function}'. Valid options are: {', '.join(function_mapping.keys())}."}

    return {"error": False, "message": "Validation successful."}