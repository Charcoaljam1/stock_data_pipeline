import pandas as pd


def clean_daily(df):
    """
    Cleans a daily stock market DataFrame by:
    - Dropping unnecessary columns ('2. high', '3. low').
    - Converting the index to datetime format.
    - Renaming columns to more readable names.
    - Ensuring 'Open', 'Close', and 'Volume' columns have the correct data type (float64).

    Parameters:
    df (pd.DataFrame): DataFrame containing stock market data with specific column names.

    Returns:
    pd.DataFrame: Cleaned DataFrame with updated column names and data types.
    """

    df.drop(columns=[ '2. high', '3. low'], inplace=True)
    df.index = pd.to_datetime(df.index, errors="coerce")
    df.index.name = 'Date'

    df.rename(columns={
        "1. open": "Open",
        "4. close": "Close",
        "5. volume": "Volume"
    }, inplace=True)

    df[['Open','Close','Volume']] = df[['Open','Close','Volume']].astype('float64')

    return df

def clean_balance(df):
    """
    Cleans a balance sheet DataFrame by:
    - Renaming columns to more readable names.
    - Selecting only relevant columns.
    - Converting 'fiscalDateEnding' to datetime format.
    - Ensuring numeric columns are properly cast to float64.

    Parameters:
    df (pd.DataFrame): DataFrame containing balance sheet data.

    Returns:
    pd.DataFrame: Cleaned DataFrame with relevant columns and correct data types.
    """

    column_name_map = {
        'fiscalDateEnding': 'fiscal_date_ending',
        'reportedCurrency': 'reported_currency',
        'totalAssets': 'total_assets',
        'totalCurrentAssets': 'total_current_assets',
        'cashAndCashEquivalentsAtCarryingValue': 'cash_and_cash_equivalents',
        'cashAndShortTermInvestments': 'cash_and_short_term_investments',
        'inventory': 'inventory',
        'currentNetReceivables': 'current_net_receivables',
        'totalNonCurrentAssets': 'total_non_current_assets',
        'propertyPlantEquipment': 'property_plant_and_equipment',
        'accumulatedDepreciationAmortizationPPE': 'accumulated_depreciation_on_ppe',
        'intangibleAssets': 'intangible_assets',
        'intangibleAssetsExcludingGoodwill': 'intangible_assets_excl_goodwill',
        'goodwill': 'goodwill',
        'investments': 'investments',
        'longTermInvestments': 'long_term_investments',
        'shortTermInvestments': 'short_term_investments',
        'otherCurrentAssets': 'other_current_assets',
        'otherNonCurrentAssets': 'other_non_current_assets',
        'totalLiabilities': 'total_liabilities',
        'totalCurrentLiabilities': 'total_current_liabilities',
        'currentAccountsPayable': 'current_accounts_payable',
        'deferredRevenue': 'deferred_revenue',
        'currentDebt': 'current_debt',
        'shortTermDebt': 'short_term_debt',
        'totalNonCurrentLiabilities': 'total_non_current_liabilities',
        'capitalLeaseObligations': 'capital_lease_obligations',
        'longTermDebt': 'long_term_debt',
        'currentLongTermDebt': 'current_long_term_debt',
        'longTermDebtNoncurrent': 'non_current_long_term_debt',
        'shortLongTermDebtTotal': 'total_short_long_term_debt',
        'otherCurrentLiabilities': 'other_current_liabilities',
        'otherNonCurrentLiabilities': 'other_non_current_liabilities',
        'totalShareholderEquity': 'total_shareholder_equity',
        'treasuryStock': 'treasury_stock',
        'retainedEarnings': 'retained_earnings',
        'commonStock': 'common_stock',
        'commonStockSharesOutstanding': 'common_stock_shares_outstanding'
    }

    df.rename(columns=column_name_map, inplace=True)

    # Define and select relevant columns
    columns_to_keep = [
            'fiscal_date_ending', 'total_current_assets', 'total_non_current_assets',
            'total_current_liabilities', 'total_non_current_liabilities',
            'total_shareholder_equity', 'short_term_debt', 'long_term_debt',
            'retained_earnings', 'cash_and_cash_equivalents'
        ]
    
    # Using `.filter(items=...)` to prevent KeyError if a column is missing
    df.filter(items=columns_to_keep, inplace=True)
    
    # Use `.filter(items=...)` to prevent KeyError if a column is missing
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'])

    # Convert financial values to float64 safely
    numeric_columns = [
        'total_current_assets', 'total_non_current_assets', 'total_current_liabilities',
        'total_non_current_liabilities', 'total_shareholder_equity', 'short_term_debt',
        'long_term_debt', 'retained_earnings', 'cash_and_cash_equivalents'
    ]      
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")

    return df


def clean_data(df, type):
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")

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
                raise KeyError("annualReports' key not found in the data")
            financial_data = pd.DataFrame.from_dict(statement)
        
            return financial_data