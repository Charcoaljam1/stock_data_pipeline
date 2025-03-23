import pandas as pd
from scripts.logger import logger
from utils.data_utils import clean_daily, clean_balance, clean_cash, clean_income, clean_info


def clean_data(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
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
        logger.error("Input must be a pandas DataFrame")
        raise TypeError("Input must be a pandas DataFrame")

    cleaning_functions = {
        'daily': clean_daily,
        'income': clean_income,
        'balance': clean_balance,
        'cash': clean_cash,
        'info': clean_info
        }

    if data_type not in cleaning_functions:
        logger.error(f"Unknown data type '{data_type}'. Expected one of {list(cleaning_functions.keys())}")
        raise ValueError(f"Unknown data type '{data_type}'. Expected one of {list(cleaning_functions.keys())}")

    return cleaning_functions[data_type](df)


def format_data(data: pd.DataFrame,function: str,nested=False):
    """
    Format JSON data gotten from the Alpha Vantage API into a pandas DataFrame.
    
    Parameters:
    - data (dict): The raw JSON data from the Alpha Vantage API.
    - function (str): The type of data (e.g., 'daily', 'income', 'balance', 'cash', info).
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
                    logger.error(f"[{function}] Expected 'Time Series (Daily)' key not found in the nested data for {key}")
                    raise KeyError(f"[{function}] Expected 'Time Series (Daily)' key not found in the nested data for {key}")
                time_series_df = pd.DataFrame.from_dict(time_series, orient='index')
                cleaned_time_series_df = clean_data(time_series_df,function)
                cleaned_time_series_df.to_csv(f"data/processed_data/{key}_stock.csv")
                stock_data[key] = cleaned_time_series_df

            return stock_data
             
        else:
            time_series = data.get('Time Series (Daily)', None)
            if time_series is None:
                logger.error("Expected 'Time Series (Daily)' key not found in the data")
                raise KeyError("Expected 'Time Series (Daily)' key not found in the data")
            time_series_df = pd.DataFrame.from_dict(time_series, orient='index')
            cleaned_time_series_df = clean_data(time_series_df,function)
            cleaned_time_series_df.to_csv(f"data/processed_data/{data['Meta Data']['2. Symbol']}_stock.csv")
            stock_data = cleaned_time_series_df
    
            return stock_data
    

    # Formats info data
    elif function == 'info':
        # Formats nested dictionaries
        if nested:
            information = {}
            for key, values in data.items():
                keys = ['Name', 'SharesOutstanding', 'Symbol','Exchange','Currency','Country','Sector']
                info = {k: values.get(k, None) for k in keys}
                if info is None:
                    logger.error(f"[{function}] Expected 'Name' key not found in the nested data for {key}")
                    raise KeyError(f"[{function}] Expected 'Name' key not found in the nested data for {key}")
                info_df = pd.DataFrame([info])
                cleaned_info_df = clean_data(info_df,function)
                cleaned_info_df.to_csv(f"data/processed_data/{key}_info.csv", index=False)
                information[key] = cleaned_info_df

            return information

        #Formats simple dictionaries
        else:
            keys = ['Name', 'SharesOutstanding','Symbol', 'Exchange','Currency','Country','Sector']
            info = {key: data.get(key, None) for key in keys}
        if info is None:
            logger.error("key not found in the data")
            raise KeyError(" key not found in the data")
        info_df = pd.DataFrame([info])
        cleaned_info_df = clean_data(info_df,function)
        cleaned_info_df.to_csv(f"data/processed_data/{data.get('Symbol')}_info.csv", index=False)
        information = cleaned_info_df
    
        return information        

    # Formats income statement, cash flow, and balance sheet data
    else:
        # Formats nested dictionaries
        if nested:
            financial_data = {}
            for key, values in data.items():
                statement = values.get('annualReports', None)
                if statement is None:
                    logger.error(f"[{function}] Expected 'annualReports' key not found in the nested data for {key}")
                    raise KeyError(f"[{function}] Expected 'annualReports' key not found in the nested data for {key}")
                statement_df = pd.DataFrame.from_dict(statement)
                cleaned_statement_df = clean_data(statement_df,function)
                cleaned_statement_df.to_csv(f"data/processed_data/{key}_{function}_statement.csv", index=False)
                financial_data[key] = cleaned_statement_df

            return financial_data

        # Formats simple dictionaries
        else:
            statement = data.get('annualReports', None)
            if statement is None:
                logger.error("'annualReports' key not found in the data")
                raise KeyError("'annualReports' key not found in the data")
            statement_df = pd.DataFrame.from_dict(statement)
            cleaned_statement_df = clean_data(statement_df,function)
            cleaned_statement_df.to_csv(f"data/processed_data/{data['symbol']}_{function}_statement.csv", index=False)
            financial_data = cleaned_statement_df        

            return financial_data