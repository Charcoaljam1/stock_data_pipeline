import pandas as pd
from collections import defaultdict
from scripts.logger import log_info, handle_exceptions
from scripts.data_validation import raw_data_validation
from utils.data_utils import clean_daily, clean_balance, clean_cash, clean_income, clean_info

@handle_exceptions
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

def format_daily(data, data_type,symbol):
    """Format JSON stock data gotten from the Alpha Vantage API into a pandas DataFrame."""

    time_series = data.get('Time Series (Daily)', None)
    if time_series is None:
        raise KeyError("Expected 'Time Series (Daily)' key not found in the data")
    time_series_df = pd.DataFrame.from_dict(time_series, orient='index')
    cleaned_time_series_df = clean_data(time_series_df,data_type)
    save_data(data=cleaned_time_series_df, data_type=data_type, symbol= symbol)

    return cleaned_time_series_df

def format_info(data, data_type, symbol):
    """Format JSON company gotten from the Alpha Vantage API into a pandas DataFrame."""

    keys = ['Name', 'SharesOutstanding','Symbol', 'Exchange','Currency','Country','Sector']
    info = {key: data.get(key, None) for key in keys}
    if info is None:
        raise KeyError(f"{keys} keys not found in the data")
    info_df = pd.DataFrame([info])
    cleaned_info_df = clean_data(info_df,data_type)
    save_data(data=cleaned_info_df, data_type=data_type, symbol= symbol)

    return cleaned_info_df  

def format_financial(data, data_type,symbol):
    """Format JSON financial data gotten from the Alpha Vantage API into a pandas DataFrame."""

    statement = data.get('annualReports', None)
    if statement is None:
        raise KeyError("'annualReports' key not found in the data")
    statement_df = pd.DataFrame.from_dict(statement)
    cleaned_statement_df = clean_data(statement_df,data_type)
    save_data(data=cleaned_statement_df, data_type=data_type, symbol= symbol)
            

    return cleaned_statement_df 

class DataCleaner:

    formatting_functions = {
        'daily': format_daily,
        'income': format_financial,
        'balance': format_financial,
        'cash': format_financial,
        'info': format_info
        }
    
    def __init__(self, raw_data):
        self.raw_data = raw_data
        self.processed_data = defaultdict(lambda: defaultdict(dict))

    @handle_exceptions
    @log_info
    def transform(self):
        raw_data_validation(self.raw_data)
        for symbols, data in self.raw_data.items():
            for data_types, values in data.items():
                self.processed_data[symbols][data_types]=DataCleaner.formatting_functions[data_types](values,data_types,symbols)

