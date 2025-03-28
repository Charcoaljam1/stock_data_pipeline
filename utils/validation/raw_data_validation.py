import re
from config.config import ALPHA_VANTAGE_API_KEY

date_pattern = r'^\d{4}-\d{2}-\d{2}$'  # Matches "YYYY-MM-DD"
daily_key_pattern = r'^\d\.\s(open|high|low|close|volume)$'  # Matches "1. open", etc.
value_pattern = r'^\d+(\.\d+)?$'  # Matches integers or decimal numbers
date_pattern = r'^\d{4}-\d{2}-\d{2}$'  # Matches YYYY-MM-DD format
key_pattern = r'^[a-zA-Z]+[a-zA-Z0-9]*$'  # Matches valid key names (no special characters)
value_none_pattern = r'^None$'  # Matches "None"
value_string_pattern = r'^[A-Z]{3}$'  # Matches string values like currency codes (e.g., "USD")


def raw_data_validation(data):
    """Validates the structure and content of raw data."""
    data_type_list = ['daily', 'income', 'balance', 'cash', 'info']
    if not isinstance(data, dict):
        return {"error": True, "message": f"Argument 'data' is not a dictionary, instead: {type(data)}"}
    if not data:
        return {"error": True, "message": "Variable cannot be empty"}
    
    for symbol, symbol_data in data.items():
        if not isinstance(symbol, str):
            return {"error": True, "message": f"Key is not a string, instead: {type(symbol)}"}
        if not isinstance(symbol_data, dict):
            return {"error": True, "message": f"Expected a dictionary as the value corresponding to key {symbol}, instead got a {type(symbol_data)}"}
        
        for data_type, values in symbol_data.items():
            if not isinstance(data_type, str):
                return {"error": True, "message": f"Key is not a string, instead: {type(data_type)}"}
            if not isinstance(values, dict):
                return {"error": True, "message": f"Expected a dictionary as the value corresponding to key {data_type}, instead got a {type(values)}"}
            if data_type not in data_type_list:
                return {"error": True, "message": f"Expected one of {data_type_list} as a key, instead got {data_type}"}
            if data_type == 'daily':
                result = daily_validation(values)
            elif data_type == 'info':
                result = info_validation(values)
            else:
                result = financials_validation(values)
            
            if result["error"]:
                return result

    return {"error": False, "message": "Validation successful."}



def daily_validation(daily_dict):
    """Validates daily data."""
    for keys, values in daily_dict.items():
        if not isinstance(keys, str):
            return {"error": True, "message": f"Key is not a string, instead: {type(keys)}"}
        if not isinstance(values, dict):
            return {"error": True, "message": f"Expected a dictionary as the value corresponding to key {keys}, instead got a {type(values)}"}
        if 'Time Series (Daily)' not in values:
            return {"error": True, "message": "Missing 'Time Series (Daily)' key."}

        
        for date, inner_dict in values.get('Time Series (Daily)').items():
            if not re.match(date_pattern, str(date)):
                return {"error": True, "message": f"Invalid date format: {date}"}
            for key, value in inner_dict.items():
                if not re.match(daily_key_pattern, str(key)):
                    return {"error": True, "message": f"Invalid key format: '{key}' (should match '{daily_key_pattern}')"}
                if not re.match(value_pattern, str(value)):
                    return {"error": True, "message": f"Invalid value format: '{value}' for key '{key}' (should be numeric)"}
    return {"error": False, "message": "Validation successful."}


def financials_validation(financials_dict):
    """Validates financial data."""
    if not isinstance(financials_dict, dict):
        return {"error": True, "message": "Financial data must be a dictionary."}
    
    if "annualReports" not in financials_dict.keys():
            return {"error": True, "message": "Missing 'annualReports' key."}
    
    for report in financials_dict.get("annualReports"):
        if not isinstance(report, dict):
            return {"error": True, "message": f"Expected a dictionary in 'annualReports', got {type(report)}."}
        for keys, values in report.items():
            if not isinstance(keys, str):
                return {"error": True, "message": f"Key is not a string, instead: {type(keys)}"}
            if not isinstance(values, (str, int, float)):
                return {"error": True, "message": f"Expected a string, int, or float as the value corresponding to key {keys}, instead got a {type(values)}"}
            
            if not re.match(key_pattern, keys):
                return {"error": True, "message": f"Invalid key format: {keys}"}
            if not (
                re.match(date_pattern, str(values)) or
                re.match(value_pattern, str(values)) or
                re.match(value_none_pattern, str(values)) or
                re.match(value_string_pattern, str(values))
            ):
                return {"error": True, "message": f'Invalid value format for key "{keys}": {values}'}

    return {"error": False, "message": "Validation successful."}


def info_validation(info_dict):
    """Validates info data."""
    required_keys = ['Name', 'SharesOutstanding', 'Symbol', 'Exchange', 'Currency', 'Country', 'Sector']
    if not isinstance(info_dict, dict):
        return {"error": True, "message": f"Expected a dictionary instead got a {type(info_dict)}"}
    
    for key in required_keys:
        if key not in info_dict:
            return {"error": True, "message": f"Missing required key: {key}"}
        
    for keys, values in info_dict.items():
        if not (
            re.match(value_string_pattern, str(values)) or
            re.match(value_pattern, str(values)) or
            re.match(date_pattern, str(values))
        ):
            return {"error": True, "message": f'Invalid value format for key "{keys}": {values}'}
    return {"error": False, "message": "Validation successful."}

        





def input_validation(function: str, symbol: str) -> dict:
    """Validates API key, function, and symbol."""

    function_mapping = {
        "daily": "TIME_SERIES_DAILY",
        "income": "INCOME_STATEMENT",
        "balance": "BALANCE_SHEET",
        "cash": "CASH_FLOW",
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
