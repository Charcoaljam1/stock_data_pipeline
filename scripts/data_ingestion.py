import requests
import json
from scripts.logger import logger
from config.config import ALPHA_VANTAGE_API_KEY

def get_data(symbol, function):
    """
        Fetch data from the Alpha Vantage API.
        
        Args:
            function (str): The type of data to request. Options are:
                            - "daily"
                            - "income"
                            - "balance"
                            - "cash"
                            - "eps"
                            - "info"
        symbol (str or list): The ticker symbol(s) of the stock(s) to fetch data for. Can be a single symbol as a string or a list of symbols.
    
        Returns:
            dict: The JSON response from the Alpha Vantage API if the request is successful.
            str: Error message if the request fails or invalid function is provided.
        """
    # Define the base URL for the API
    url = f"https://www.alphavantage.co/query"

    if not ALPHA_VANTAGE_API_KEY:
        logger.error("API key is missing. Please check the 'config.py' file.")
        raise ValueError("API key is missing. Please check the 'config.py' file.")

    
    # Map the functions to API parameters
    function_mapping = {
        "daily": "TIME_SERIES_DAILY",
        "income": "INCOME_STATEMENT",
        "balance": "BALANCE_SHEET",
        "cash": "CASH_FLOW",
        "eps": "EARNINGS",
        "info": "OVERVIEW"  
    }

    # Check if the function is valid
    if function not in function_mapping:
        logger.error(f"Invalid function '{function}'. Valid options are: {', '.join(function_mapping.keys())}.")
        raise ValueError(f"Invalid function '{function}'. Valid options are: {', '.join(function_mapping.keys())}.")

    logger.info(f"Fetching data for function '{function}' and symbol(s): {symbol}")

    # To handle a list of symbols
    if isinstance(symbol, list):
        # Store the data for each symbol
        results = {}
        for tick in symbol:
            parameters = {
                "function": function_mapping[function],
                "symbol": tick,
                "apikey": ALPHA_VANTAGE_API_KEY
            }
            
            # Add additional parameters for specific functions
            if function == "daily":
                parameters.update({
                    "datatype": "json",
                    "outputsize": "full"
                })
        
            # Make the API request
            try:
                response = requests.get(url, params=parameters)
                response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch data for symbol '{tick}'. Error: {e}")
                results[tick] = f"Failed to fetch data. Error: {e}"
                continue
        
            # Handle the API response
            if response.status_code == 200:
                results[tick] = response.json()
                file_path = f"data/raw_data/{tick}_{function}.json"
                with open(file_path, "w") as raw_file:
                    json.dump(results[tick], raw_file, indent=4)
                logger.info(f"Data for symbol '{tick}' saved to {file_path}")
            else:
                logger.error(f"Failed to fetch data for symbol '{tick}'. Status code: {response.status_code}. Reason: {response.reason}")
                results[tick] = f"Failed to fetch data. Status code: {response.status_code}. Reason: {response.reason}"

        return results  # Return all the results as a dictionary

    # Handle a single symbol
    else: 
        # Set up the parameters for the API request
        parameters = {
            "function": function_mapping[function],
            "symbol": symbol,
            "apikey": ALPHA_VANTAGE_API_KEY
        }
    
        # Add additional parameters for specific functions
        if function == "daily":
            parameters.update({
                "datatype": "json",
                "outputsize": "full"
            })
    
        # Make the API request
        try:
            response = requests.get(url, params=parameters)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for symbol '{symbol}'. Error: {e}")
            raise RuntimeError(f"Failed to fetch data for symbol '{symbol}'. Error: {e}")

    
        # Handle the API response
        if response.status_code == 200:
            data = response.json()
            file_path = f"data/raw_data/{symbol}_{function}.json"
            with open(file_path, "w") as raw_file:
                json.dump(data, raw_file, indent=4)
            logger.info(f"Data for symbol '{symbol}' saved to {file_path}")
            return data
        else:
            logger.error(f"Failed to fetch data for symbol '{symbol}'. Status code: {response.status_code}. Reason: {response.reason}")
            raise RuntimeError(f"Failed to fetch data for symbol '{symbol}'. Status code: {response.status_code}. Reason: {response.reason}")