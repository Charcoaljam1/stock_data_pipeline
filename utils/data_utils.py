import pandas as pd
import warnings

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
    required_columns = ['1. open', '4. close', '5. volume']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        warnings.warn(f"Warning: Missing columns {missing_columns} in data")

    df.drop(columns=[ '2. high', '3. low'], inplace=True, errors='ignore')
    df.index.name = 'Date'
    df.index = pd.to_datetime(df.index, errors="coerce")
    

    df.rename(columns={
        "1. open": "Open",
        "4. close": "Close",
        "5. volume": "Volume"
    }, inplace=True)

    df[['Open','Close','Volume']] = df[['Open','Close','Volume']].apply(pd.to_numeric, errors="coerce")

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
    
    # Using `.filter()` to prevent KeyError if a column is missing
    df = df.filter(items=columns_to_keep)
    
    df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])

    # Convert financial values to float64 safely
    numeric_columns = [
        'total_current_assets', 'total_non_current_assets', 'total_current_liabilities',
        'total_non_current_liabilities', 'total_shareholder_equity', 'short_term_debt',
        'long_term_debt', 'retained_earnings', 'cash_and_cash_equivalents'
    ]   

    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")

    return df

def clean_income(df):
    """
    Cleans an income statement DataFrame by:
    - Renaming columns to more readable names.
    - Selecting only relevant columns.
    - Converting 'fiscalDateEnding' to datetime format.
    - Ensuring numeric columns are properly cast to float64.
    - Engineering new columns.

    Parameters:
    df (pd.DataFrame): DataFrame containing balance sheet data.

    Returns:
    pd.DataFrame: Cleaned DataFrame with relevant columns and correct data types.
    """

    column_name_map = {
        'fiscalDateEnding': 'fiscal_date_ending',
        'reportedCurrency': 'reported_currency',
        'grossProfit': 'gross_profit',
        'totalRevenue': 'total_revenue',
        'costOfRevenue': 'cost_of_revenue',
        'costofGoodsAndServicesSold': 'cost_of_goods_and_services_sold',
        'operatingIncome': 'operating_income',
        'sellingGeneralAndAdministrative': 'selling_general_and_administrative',
        'researchAndDevelopment': 'research_and_development',
        'operatingExpenses': 'operating_expenses',
        'investmentIncomeNet': 'investment_income_net',
        'netInterestIncome': 'net_interest_income',
        'interestIncome': 'interest_income',
        'interestExpense': 'interest_expense',
        'nonInterestIncome': 'non_interest_income',
        'otherNonOperatingIncome': 'other_non_operating_income',
        'depreciation': 'depreciation',
        'depreciationAndAmortization': 'depreciation_and_amortization',
        'incomeBeforeTax': 'income_before_tax',
        'incomeTaxExpense': 'income_tax_expense',
        'interestAndDebtExpense': 'interest_and_debt_expense',
        'netIncomeFromContinuingOperations': 'net_income_from_continuing_operations',
        'comprehensiveIncomeNetOfTax': 'comprehensive_income_net_of_tax',
        'ebit': 'ebit',
        'ebitda': 'ebitda',
        'netIncome': 'net_income'
    }

    df.rename(columns=column_name_map, inplace=True)

    # Define and select relevant columns
    columns_to_keep = [
        'fiscal_date_ending', 'total_revenue', 'gross_profit',
        'operating_income', 'net_income',
        'interest_and_debt_expense', 'ebit'
    ]

    # Using `.filter()` to prevent KeyError if a column is missing
    df = df.filter(items=columns_to_keep)

    df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])

    # Convert financial values to float64 safely
    numeric_columns = [
        'total_revenue', 'gross_profit',
        'operating_income', 'net_income',
        'interest_and_debt_expense', 'ebit'
    ]

    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")

    # Engineer columns 
    df['gross_margin'] = (df['gross_profit']/df['total_revenue']) * 100
    df['operating_margin'] = (df['operating_income']/df['total_revenue']) * 100
    df['ebit_margin'] = (df['ebit']/df['total_revenue']) * 100

    return df 


def clean_cash(df):
    """
    Cleans an cash flow statement DataFrame by:
    - Renaming columns to more readable names.
    - Selecting only relevant columns.
    - Converting 'fiscalDateEnding' to datetime format.
    - Ensuring numeric columns are properly cast to float64.
    - Engineering new columns.

    Parameters:
    df (pd.DataFrame): DataFrame containing balance sheet data.

    Returns:
    pd.DataFrame: Cleaned DataFrame with relevant columns and correct data types.
    """

    column_name_map = {
        'fiscalDateEnding': 'fiscal_date_ending',
        'reportedCurrency': 'reported_currency',
        'operatingCashflow': 'operating_cashflow',
        'paymentsForOperatingActivities': 'payments_for_operating_activities',
        'proceedsFromOperatingActivities': 'proceeds_from_operating_activities',
        'changeInOperatingLiabilities': 'change_in_operating_liabilities',
        'changeInOperatingAssets': 'change_in_operating_assets',
        'depreciationDepletionAndAmortization': 'depreciation_depletion_and_amortization',
        'capitalExpenditures': 'capital_expenditures',
        'changeInReceivables': 'change_in_receivables',
        'changeInInventory': 'change_in_inventory',
        'profitLoss': 'profit_loss',
        'cashflowFromInvestment': 'cashflow_from_investment',
        'cashflowFromFinancing': 'cashflow_from_financing',
        'proceedsFromRepaymentsOfShortTermDebt': 'debt_repayments',
        'paymentsForRepurchaseOfCommonStock': 'payments_for_repurchase_of_common_stock',
        'paymentsForRepurchaseOfEquity': 'payments_for_repurchase_of_equity',
        'paymentsForRepurchaseOfPreferredStock': 'payments_for_repurchase_of_preferred_stock',
        'dividendPayout': 'dividend_payout',
        'dividendPayoutCommonStock': 'dividend_payout_common_stock',
        'dividendPayoutPreferredStock': 'dividend_payout_preferred_stock',
        'proceedsFromIssuanceOfCommonStock': 'proceeds_from_issuance_of_common_stock',
        'proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet': 'proceeds_from_issuance_of_long_term_debt_and_capital_securities_net',
        'proceedsFromIssuanceOfPreferredStock': 'proceeds_from_issuance_of_preferred_stock',
        'proceedsFromRepurchaseOfEquity': 'proceeds_from_repurchase_of_equity',
        'proceedsFromSaleOfTreasuryStock': 'proceeds_from_sale_of_treasury_stock',
        'changeInCashAndCashEquivalents': 'change_in_cash_and_cash_equivalents',
        'changeInExchangeRate': 'change_in_exchange_rate',
        'netIncome': 'net_income'
     }

    df.rename(columns=column_name_map, inplace=True)

    # Define and select relevant columns
    columns_to_keep = [
        'fiscal_date_ending', 'operating_cashflow', 'capital_expenditures',
        'cashflow_from_investment', 'cashflow_from_financing',
        'dividend_payout', 'debt_repayments'
    ]

    # Using `.filter()` to prevent KeyError if a column is missing
    df = df.filter(items=columns_to_keep)

    df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])

    # Convert financial values to float64 safely
    numeric_columns = [
        'operating_cashflow', 'capital_expenditures',
        'cashflow_from_investment', 'cashflow_from_financing',
        'dividend_payout', 'debt_repayments'
    ]

    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")

    # Engineer columns 
    df['free_cashflow'] = (df['operating_cashflow']-df['capital_expenditures'])

    return df

def clean_info(df):
    """
    Cleans company overview data.
    
    - Renames columns to follow snake_case naming convention.

    Parameters:
    df (pd.DataFrame): DataFrame containing company overview information.

    Returns:
    pd.DataFrame: Cleaned DataFrame with unique IDs assigned.
    """
    column_name_map = {
        'Name': 'name',
        'SharesOutstanding': 'total_shares',
        'Symbol': 'ticker_symbol',
        'Exchange':'exchange',
        'Currency': 'currency',
        'Country': 'country',
        'Sector': 'sector'
    }

    df.rename(columns=column_name_map, inplace=True)

    return df