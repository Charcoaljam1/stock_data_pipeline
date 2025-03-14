import requests
from config import ALPHA_VANTAGE_API_KEY

def get_data(function,symbol):
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
        return "API key is missing. Please check the 'config.py' file."

    
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
        return f"Invalid function '{function}'. Valid options are: {', '.join(function_mapping.keys())}."

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
            response = requests.get(url, params=parameters)
        
            # Handle the API response
            if response.status_code == 200:
                results[tick] = response.json()  # Store the response for this symbol
            else:
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
        response = requests.get(url, params=parameters)
    
        # Handle the API response
        if response.status_code == 200:
            return response.json()
        else:
            return f"Failed to fetch data. Status code: {response.status_code}. Reason: {response.reason}"


