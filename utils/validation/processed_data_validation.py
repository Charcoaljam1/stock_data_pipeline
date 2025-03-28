import pandas as pd

def validate_processed_daily_data(df: pd.DataFrame) -> dict:
    """
    Validates the structure and content of the processed daily stock data.

    Parameters:
    df (pd.DataFrame): The processed daily stock data.

    Returns:
    dict: Validation result with 'error' and 'message' keys.
    """
    required_columns = ['Date', 'Open', 'Close', 'Volume']
    if not isinstance(df, pd.DataFrame):
        return {"error": True, "message": "Input data must be a pandas DataFrame."}

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        return {"error": True, "message": f"Missing required columns: {missing_columns}"}

    if not pd.api.types.is_datetime64_any_dtype(df['Date']):
        return {"error": True, "message": "Column 'Date' must be of datetime type."}

    numeric_columns = ['Open', 'Close', 'Volume']
    for col in numeric_columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            return {"error": True, "message": f"Column '{col}' must be numeric."}

    return {"error": False, "message": "Processed daily data validation successful."}


def validate_processed_info_data(df: pd.DataFrame) -> dict:
    """
    Validates the structure and content of the processed company info data.

    Parameters:
    df (pd.DataFrame): The processed company info data.

    Returns:
    dict: Validation result with 'error' and 'message' keys.
    """
    required_columns = ['name', 'total_shares', 'ticker_symbol', 'exchange', 'currency', 'country', 'sector']
    if not isinstance(df, pd.DataFrame):
        return {"error": True, "message": "Input data must be a pandas DataFrame."}

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        return {"error": True, "message": f"Missing required columns: {missing_columns}"}

    if not pd.api.types.is_numeric_dtype(df['total_shares']):
        return {"error": True, "message": "Column 'total_shares' must be numeric."}

    string_columns = ['name', 'ticker_symbol', 'exchange', 'currency', 'country', 'sector']
    for col in string_columns:
        if not pd.api.types.is_string_dtype(df[col]):
            return {"error": True, "message": f"Column '{col}' must be a string."}

    return {"error": False, "message": "Processed info data validation successful."}


def validate_processed_financial_data(df: pd.DataFrame, data_type: str) -> dict:
    """
    Validates the structure and content of the processed financial data.

    Parameters:
    df (pd.DataFrame): The processed financial data.
    data_type (str): The type of financial data ('income', 'balance', 'cash').

    Returns:
    dict: Validation result with 'error' and 'message' keys.
    """
    required_columns_map = {
        'income': ['fiscal_date_ending', 'total_revenue', 'gross_profit', 'operating_income', 'net_income', 'ebit'],
        'balance': ['fiscal_date_ending', 'total_current_assets', 'total_non_current_assets', 
                    'total_current_liabilities', 'total_non_current_liabilities', 'total_shareholder_equity'],
        'cash': ['fiscal_date_ending', 'operating_cashflow', 'capital_expenditures', 
                 'cashflow_from_investment', 'cashflow_from_financing', 'dividend_payout']
    }

    if data_type not in required_columns_map:
        return {"error": True, "message": f"Unknown data type '{data_type}'. Expected one of {list(required_columns_map.keys())}."}

    required_columns = required_columns_map[data_type]
    if not isinstance(df, pd.DataFrame):
        return {"error": True, "message": "Input data must be a pandas DataFrame."}

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        return {"error": True, "message": f"Missing required columns: {missing_columns}"}

    if not pd.api.types.is_datetime64_any_dtype(df['fiscal_date_ending']):
        return {"error": True, "message": "Column 'fiscal_date_ending' must be of datetime type."}

    numeric_columns = [col for col in required_columns if col != 'fiscal_date_ending']
    for col in numeric_columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            return {"error": True, "message": f"Column '{col}' must be numeric."}

    return {"error": False, "message": f"Processed {data_type} data validation successful."}


def validate_processed_data(df: pd.DataFrame, data_type: str) -> dict:
    """
    Validates the processed data based on its type.

    Parameters:
    df (pd.DataFrame): The processed data.
    data_type (str): The type of data ('daily', 'info', 'income', 'balance', 'cash').

    Returns:
    dict: Validation result with 'error' and 'message' keys.
    """
    if data_type == 'daily':
        return validate_processed_daily_data(df)
    elif data_type == 'info':
        return validate_processed_info_data(df)
    elif data_type in ['income', 'balance', 'cash']:
        return validate_processed_financial_data(df, data_type)
    else:
        return {"error": True, "message": f"Unknown data type '{data_type}'."}