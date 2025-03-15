import pandas as pd
from data_utils import clean_daily, clean_balance, clean_cash, clean_income, clean_info


def clean_data(df, data_type):
    """
    Cleans and processes data based on the specified type.

    Parameters:
    df (pd.DataFrame): The input data to be cleaned.
    data_type (str): The type of data ('daily', 'income', 'balance', 'cash', 'info').

    Returns:
    pd.DataFrame: The cleaned DataFrame.

    Raises:
    TypeError: If df is not a pandas DataFrame.
    ValueError: If an unknown data type is passed.
    """

    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")

    cleaning_functions = {
        'daily': clean_daily,
        'income': clean_income,
        'balance': clean_balance,
        'cash': clean_cash,
        'info': clean_info
        }

     if data_type not in cleaning_functions:
        raise ValueError(f"Unknown data type '{data_type}'. Expected one of {list(cleaning_functions.keys())}")

    return cleaning_functions[data_type](df)


def format_data(data,function,nested=False):
    """
    Format JSON data gotten from the Alpha Vantage API into a pandas DataFrame.
    
    Parameters:
    - data (dict): The raw JSON data from the Alpha Vantage API.
    - function (str): The type of data (e.g., 'daily', 'eps', 'income', 'balance', 'cash').
    - nested (bool): If False, expects a single time series. If True, expects a dictionary of time series.
    
    Returns:
    - pd.DataFrame or dict: A DataFrame or a dictionary of DataFrames depending on the 'nested' argument.

    Raises:
    - KeyError: If the expected keys are missing from the data.

    Examples:
        # Example for non-nested daily data
        format_data(data, function='daily', nested=False)

        # Example for nested EPS data
        format_data(data, function='eps', nested=True)
    """
    # Formats stock data
    if function == 'daily':
        if nested:
            stock_data = {}
            for key, values in data.items():
                time_series = values.get('Time Series (Daily)', None)
                if time_series is None:
                    raise KeyError(f"[{function}] Expected 'Time Series (Daily)' key not found in the nested data for {key}")
                stock_data[key] = pd.DataFrame.from_dict(time_series, orient='index')

            return stock_data
             
        else:
            time_series = data.get('Time Series (Daily)', None)
            if time_series is None:
                raise KeyError("Expected 'Time Series (Daily)' key not found in the data")
            stock_data = pd.DataFrame.from_dict(time_series, orient='index')
        
            return stock_data
    

    # Formats eps data
    elif function == 'eps':
        if nested:
            eps_data = {}
            for key, values in data.items():
                eps = values.get('annualEarnings', None)
                if eps is None:
                    raise KeyError(f"[{function}] Expected 'annualEarnings' key not found in the nested data for {key}")
                eps_data[key] = pd.DataFrame.from_dict(eps)

            return eps_data

        # Formats simple dictionaries
        else:
            eps = data.get('annualEarnings', None)
            if eps is None:
                raise KeyError("annualEarnings' key not found in the data")
            eps_data = pd.DataFrame.from_dict(eps)
        
            return eps_data

    # Formats info data
    elif function == 'info':
        # Formats nested dictionaries
        if nested:
            information = {}
            for key, values in data.items():
            info = data.get(['Name','SharesOutstanding'], None)
            if info is None:
                raise KeyError(f"[{function}] Expected 'Name' key not found in the nested data for {key}")
            information[key] = pd.DataFrame.from_dict(info)

            return information

        #Formats simple dictionaries
        else:
        info = data.get(['Name','SharesOutstanding'], None)
        if info is None:
            raise KeyError("'Name' key not found in the data")
        information = pd.DataFrame.from_dict(info)
    
        return information        

    # Formats income statement, cash flow, and balance sheet data
    else:
        # Formats nested dictionaries
        if nested:
            financial_data = {}
            for key, values in data.items():
                statement = values.get('annualReports', None)
                if statement is None:
                    raise KeyError(f"[{function}] Expected 'annualReports' key not found in the nested data for {key}")
                financial_data[key] = pd.DataFrame.from_dict(statement)

            return financial_data

        # Formats simple dictionaries
        else:
            statement = data.get('annualReports', None)
            if statement is None:
                raise KeyError("'annualReports' key not found in the data")
            financial_data = pd.DataFrame.from_dict(statement)
        
            return financial_data