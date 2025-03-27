import psycopg2
import warnings
from scripts.database_connection import get_db_connection
from scripts.logger import log_info, handle_exceptions
import pandas as pd

@handle_exceptions
@log_info
def load_data(data: pd.DataFrame, type: str, symbol=None, nested=False):
    """
    Loads data of a specified type for a given symbol into the database.

    This function uses a mapping of data types to their respective loading functions.
    It supports both simple and nested data loading based on the `nested` flag.

    Args:
        data: The data to be loaded. The structure of the data depends on the type.
        type (str): The type of data to load. Supported types are:
            - 'daily': Loads simple stock data.
            - 'info': Loads company information.
            - 'cash': Loads cash flow data.
            - 'balance': Loads balance sheet data.
            - 'income': Loads income statement data.
        symbol (str): The ticker symbol or identifier for the company or stock.
        nested (bool, optional): If True, handles data in a nested format. Defaults to False.

    Returns:
        int: Returns 0 if the data loading fails due to an invalid type or an exception.
             Returns the result of the loading function if successful.

    Raises:
        Logs errors but does not raise exceptions explicitly.

    Examples:
        >>> load_data(stock_data, 'daily', 'AAPL')
        # Loads simple stock data for AAPL.

        >>> load_data(cash_data, 'cash', nested=True)
        # Loads nested cash flow data.
    """
    loading_functions = {
        'daily': load_simple_stock,
        'info': load_company_info,
        'cash': load_cash_data,
        'balance': load_balance_data,
        'income': load_income_data
    }

    if type not in loading_functions:
        raise ValueError(f"Invalid data type: '{type}' is not a supported data type.")
     
    if nested:
        return(load_nested_data(data, type))
    else:
        return(loading_functions[type](data, symbol))
    


@handle_exceptions
@log_info
def load_simple_stock(data: pd.DataFrame, symbol: str, batch_size: int = 500) -> int:
    """
    Load stock data into the database for a given company symbol.

    Args:
        data (DataFrame): A DataFrame containing stock data with columns (date, open_price, close_price, volume).
        symbol (str): The ticker symbol of the company.

    Returns:
        int: The number of rows successfully inserted into the database.
    """
    # Validate inputs
    if not isinstance(data, pd.DataFrame):
        raise ValueError("Invalid data type: 'data' must be a pandas DataFrame.")
      
    if not isinstance(symbol, str):
        raise ValueError("Invalid data type: 'symbol' must be a string.")
    
    insert_query = '''
    INSERT INTO stocks (company_id, date, open_price,close_price,volume)
    VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING;
    '''
    conn = None
  
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Fetch company_id
            cur.execute('''SELECT company_id FROM companies WHERE ticker_symbol = %s''', (symbol,))
            result = cur.fetchone()
            if not result:
                raise ValueError(f"No company found with ticker symbol: {symbol}")
            company_id = result[0]

            # Prepare data for insertion
            values = [(company_id, *row) for row in data.itertuples(index=True, name=None)]
    
            # Batch insert stock data
            batch = []
            total_rows_inserted = 0

            for value in values:
                batch.append(value)
                if len(batch) == batch_size:
                    cur.executemany(insert_query, batch)
                    conn.commit()
                    total_rows_inserted += len(batch)
                    batch = []

            # Insert remaining rows
            if batch:
                cur.executemany(insert_query, batch)
                conn.commit()
                total_rows_inserted += len(batch)

            message = f"{symbol} stock data inserted successfully. Total rows inserted: {total_rows_inserted}"
            
            print(message)
            return {'info': True, 'message': message}
                

    

@handle_exceptions
@log_info
def load_nested_stock_data(nested_data: dict):
    """
    Processes and loads stock data for multiple companies stored in a dictionary.

    Parameters:
    nested_data (dict): Dictionary where keys are ticker symbols (str) and values are Pandas DataFrames.

    Raises:
    ValueError: If the input is not a dictionary or if any value is not a Pandas DataFrame.
    """
    if not isinstance(nested_data, dict):
        raise ValueError("Input must be a dictionary with ticker symbols as keys and DataFrames as values.")

    for symbol, df in nested_data.items():
        if not isinstance(df, pd.DataFrame):
            warnings.warn(f"Skipping {symbol}: Value is not a valid DataFrame.")
            continue  # Skip invalid data
            
       
        warnings.warn("Processing stock data for {symbol}.")
        load_simple_stock(df, symbol)




@handle_exceptions
@log_info
def load_company_info(data: pd.DataFrame, symbol: str) -> int:
    """
    Load company information into the companies table.

    Args:
        data (DataFrame): A DataFrame containing company information with columns:
                          ['name', 'total_shares', 'ticker_symbol', 'exchange', 'currency', 'country', 'sector'].

    Returns:
        int: The number of rows successfully inserted into the database.
    """
    required_columns = ['name', 'total_shares', 'ticker_symbol', 'exchange', 'currency', 'country', 'sector']

    if not input_validation(data, symbol, required_columns):
        raise ValueError("Invalid input data for loading company information.")

    insert_query = '''
    INSERT INTO companies (name, total_shares, ticker_symbol, exchange, currency, country, sector)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    '''
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Prepare data for insertion
            values = [(row) for row in data[required_columns].itertuples(index=False, name=None)]

            # Insert data
            cur.executemany(insert_query, values)
            conn.commit()
            total_rows_inserted = cur.rowcount

            message = f"{symbol} info data inserted successfully. Total rows inserted: {total_rows_inserted}"
            return {'info': True, 'message': message}

@handle_exceptions
@log_info
def load_nested_data(nested_data: dict, type: str) -> int:
    """
    Processes and loads financial data for multiple companies stored in a dictionary.

    Parameters:
        nested_data (dict): Dictionary where keys are ticker symbols (str) and values are Pandas DataFrames.
        type (str): The type of financial data being processed (e.g., 'cash', 'balance', 'income', 'stock', 'info').

    Returns:
        int: The total number of rows successfully inserted across all companies.
    """
    function_mapping = {
        'cash': 'cash flow',
        'balance': 'balance sheet',
        'income': 'income statement',
        'stock': 'stock',
        'info': 'company information'
    }
    data_type = function_mapping.get(type, f"'{type}' data")

    if not isinstance(nested_data, dict):
        raise ValueError("Input must be a dictionary with ticker symbols as keys and DataFrames as values.")

    total_rows_inserted = 0
    failed_companies = []

    for symbol, df in nested_data.items():
        if not isinstance(df, pd.DataFrame):
            warnings.warn(f"Skipping {symbol}: Value is not a valid DataFrame.")
            failed_companies.append(symbol)
            continue  # Skip invalid data

       
        print(f"Processing {data_type} for {symbol}.")
        rows_inserted = load_data(df, type, symbol)
        total_rows_inserted += rows_inserted
    return {'info': True, 'message': f"Finished processing {data_type}. Total rows inserted: {total_rows_inserted}. "
                f"Failed companies: {failed_companies}"}

@handle_exceptions
@log_info
def load_cash_data(data: pd.DataFrame, symbol: str) -> int:
    """
    Loads cash flow data into the cash_flows table.

    Args:
        data (DataFrame): A DataFrame containing cash flow data with columns:
                    ['fiscal_date_ending', 'operating_cashflow', 'capital_expenditures',
                    'cashflow_from_investment', 'cashflow_from_financing',
                    'dividend_payout', 'debt_repayments', 'free_cashflow']
    Returns:
        int: The number of rows successfully inserted into the database.
    """
    required_columns = ['fiscal_date_ending', 'operating_cashflow', 'capital_expenditures',
                        'cashflow_from_investment', 'cashflow_from_financing',
                        'dividend_payout', 'debt_repayments', 'free_cashflow']
    
    if not input_validation(data, symbol, required_columns):
        raise ValueError("Invalid input data for loading cash flow data.")

    insert_query = '''
    INSERT INTO cash_flows (company_id, date, operating_cash_flow, capital_expenditures,
                            cash_from_investing, cash_from_financing,
                            dividend_payments, debt_repayments, free_cash_flow)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (company_id, date) DO NOTHING;
    '''
    total_rows_inserted = insert_financial_data(data, 'cash', symbol, required_columns, insert_query)
    return {'info': True, 'message': f"Finished processing {symbol}. Total rows inserted: {total_rows_inserted}."}

@handle_exceptions
@log_info
def load_balance_data(data: pd.DataFrame, symbol: str) -> int:
    """
    Loads balance sheet data into the balance_sheets table.

    Args:
        data (DataFrame): A DataFrame containing balance sheet data with columns:
                    ['fiscal_date_ending', 'total_current_assets', 'total_non_current_assets',
                    'total_current_liabilities', 'total_non_current_liabilities',
                    'total_shareholder_equity', 'short_term_debt', 'long_term_debt',
                    'retained_earnings', 'cash_and_cash_equivalents'
                ]
    Returns:
        int: The number of rows successfully inserted into the database.
    """
    required_columns = [
            'fiscal_date_ending', 'total_current_assets', 'total_non_current_assets',
            'total_current_liabilities', 'total_non_current_liabilities',
            'total_shareholder_equity', 'short_term_debt', 'long_term_debt',
            'retained_earnings', 'cash_and_cash_equivalents'
        ]
    
    # Validate inputs
    if not input_validation(data, symbol, required_columns):
        raise ValueError("Invalid input data for loading balance sheet data.")
    

    insert_query = '''
    INSERT INTO balance_sheets (company_id, date, current_assets, non_current_assets,
                                current_liabilities, non_current_liabilities,
                                equity, short_term_debt, long_term_debt,
                                retained_earnings, cash_and_cash_equivalents)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (company_id, date) DO NOTHING;
    '''
    total_rows_inserted = insert_financial_data(data, 'balance', symbol, required_columns, insert_query)
    return {'info': True, 'message': f"Finished processing {symbol}. Total rows inserted: {total_rows_inserted}."}


@handle_exceptions
@log_info
def load_income_data(data: pd.DataFrame, symbol: str) -> int:
    """
    Loads balance income data into the income_statements table.

    Args:
        data (DataFrame): A DataFrame containing balance sheet data with columns:
                    ['fiscal_date_ending', 'total_revenue', 'gross_profit',
                    'operating_income', 'net_income', 'interest_and_debt_expense', 'ebit',
                    'gross_margin', 'operating_margin', 'ebit_margin']
    Returns:
        int: The number of rows successfully inserted into the database.
    """
    required_columns = [
            'fiscal_date_ending', 'total_revenue', 'gross_profit',
            'operating_income', 'net_income', 'interest_and_debt_expense', 'ebit',
            'gross_margin', 'operating_margin', 'ebit_margin'
        ]
    
    # Validate inputs
    if not input_validation(data, symbol, required_columns):
        raise ValueError("Invalid input data for loading income statement data.")
    
    insert_query = '''
    INSERT INTO income_statements (company_id, date, revenue, gross_profit,
                                operating_income, net_income, interest_expense, ebit,
                                gross_margin, operating_margin, ebit_margin)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (company_id, date) DO NOTHING;
    '''
    total_rows_inserted = insert_financial_data(data, 'income', symbol, required_columns, insert_query)
    return {'info': True, 'message': f"Finished processing {symbol}. Total rows inserted: {total_rows_inserted}."}


@handle_exceptions
@log_info
def input_validation (data: pd.DataFrame, symbol: str, required_columns: list) -> bool:
    """
    Validates the input data for loading into the database.

    Args:
        data (pd.DataFrame): The input data to validate.
        symbol (str): The ticker symbol of the company.
        required_columns (list): A list of required column names.

    Returns:
        bool: True if the input is valid, False otherwise.
    """
    if not isinstance(data, pd.DataFrame):
        warnings.warn("Invalid data type: 'data' must be a pandas DataFrame.")
        return False
    if not isinstance(symbol, str):
        warnings.warn("Invalid data type: 'symbol' must be a string.")
        return False
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        warnings.warn(f"Missing required columns in the DataFrame: {missing_columns}")
        return False
    return True


@handle_exceptions
@log_info
def insert_financial_data (data: pd.DataFrame, type: str, symbol: str, required_columns: list, insert_query: str) -> int:
    """
        Inserts financial data into the database for a specific company based on its ticker symbol.
        Args:
            data (pd.DataFrame): The financial data to be inserted, provided as a pandas DataFrame.
            type (str): The type of financial data being inserted (e.g., 'cash', 'balance', 'income', 'stock', 'info').
            symbol (str): The ticker symbol of the company for which the data is being inserted.
            required_columns (list): A list of column names from the DataFrame that are required for the insertion.
            insert_query (str): The SQL query used to insert the data into the database.
        Returns:
            int: The total number of rows successfully inserted into the database. Returns 0 if an error occurs.
        Raises:
            Exception: Logs any exceptions that occur during the database operation.
        Notes:
            - The function maps the `type` argument to a human-readable description using a predefined mapping.
            - It fetches the `company_id` from the database using the provided ticker symbol.
            - The data is prepared for insertion by combining the `company_id` with the required columns from the DataFrame.
            - If the ticker symbol does not exist in the database, the function logs an error and returns 0.
    """

    function_mapping = {
        'cash': 'cash flow',
        'balance': 'balance sheet',
        'income': 'income statement',
        'stock': 'stock',
        'info': 'company information'
    }
    data_type = function_mapping.get(type, f"'{type}' data")

    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Fetch company_id
            cur.execute('''SELECT company_id FROM companies WHERE ticker_symbol = %s''', (symbol,))
            result = cur.fetchone()
            if not result:
                raise ValueError(f"No company found with ticker symbol: {symbol}")
            company_id = result[0]

            # Prepare data for insertion
            values = [(company_id, *row) for row in data[required_columns].itertuples(index=False, name=None)]

            # Insert data
            cur.executemany(insert_query, values)
            conn.commit()
            total_rows_inserted = cur.rowcount

            message = f"{symbol} {data_type} data inserted successfully. Total rows inserted: {total_rows_inserted}"
            print(message)
            return {'info': True, 'message': message}

