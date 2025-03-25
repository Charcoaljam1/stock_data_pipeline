import requests
import json
from scripts.logger import log_info
import os
from config.config import ALPHA_VANTAGE_API_KEY
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception




function_mapping = {
"daily": "TIME_SERIES_DAILY",
"income": "INCOME_STATEMENT",
"balance": "BALANCE_SHEET",
"cash": "CASH_FLOW",
"eps": "EARNINGS",
"info": "OVERVIEW"  
}

url = f"https://www.alphavantage.co/query"  

@log_info('error')
def get_data(symbol: list | str, function: str):
    DATA_DIR = "data/raw_data"

    validation_result = input_validation(function, symbol)
    if validation_result["error"]:
        raise ValueError(validation_result["message"])
    else:
        print("Validation passed!")
    
    results = {}
    symbols = symbol if isinstance(symbol, list) else [symbol]

    for tick in symbols:
        params = build_parameters(tick, function)
        response = fetch_api_response(url, params)
        data = response
        file_path = os.path.join(DATA_DIR, f"{tick}_{function}.json")
        save_data(file_path, data)
        results[tick] = data
    return results if isinstance(symbol, list) else results[symbol]


@log_info('error', log_params=True)
def input_validation(function: str, symbol: str) -> dict:
    """Validates API key, function, and symbol."""

    if not ALPHA_VANTAGE_API_KEY:
        return {"error": True, "message": "API key is missing. Please check the 'config/config.py' file."}
    
    if isinstance(symbol, list):
        for tick in symbol:
            if not tick.isalpha():
                return {"error": True, "message": f"Invalid symbol '{tick}'. Only alphabetic characters are allowed."}
    elif not isinstance(symbol, str) or not symbol.isalpha():
        return {"error": True, "message": f"Invalid symbol '{symbol}'. Only alphabetic characters are allowed."}
    
    if function not in function_mapping:
        return {"error": True, "message": f"Invalid function '{function}'. Valid options are: {', '.join(function_mapping.keys())}."}

    return {"error": False, "message": "Validation successful."}


@log_info('error')
def build_parameters(symbol: str, function: str) -> dict:
    params = {
        "function": function_mapping[function],
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    if function == "daily":
        params.update({"datatype": "json", "outputsize": "full"})
    return params


def is_retryable_exception(exception):
    """Check if the exception is retryable."""
    if isinstance(exception, requests.exceptions.HTTPError) and exception.response.status_code in [429, 500, 503]:
        return True  
    if isinstance(exception, requests.exceptions.RequestException):
        return True 
    return False


@log_info('critical', log_params=True)
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception(is_retryable_exception),
)
def fetch_api_response(url: str, params: dict) -> dict:
    response = requests.get(url, params=params)
    response.raise_for_status() 
    return response.json()

    

@log_info('error', log_params=True)
def save_data(file_path: str, data: dict):
    """Saves data to a JSON file."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as raw_file:
        json.dump(data, raw_file, indent=4)

    


