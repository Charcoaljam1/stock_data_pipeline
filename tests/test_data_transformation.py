import pytest
import pandas as pd
from unittest.mock import patch
from scripts.data_transformation import clean_data, format_data
from utils.data_utils import clean_daily, clean_income, clean_balance, clean_cash, clean_info


# Mocking the clean functions since they are part of external modules
@pytest.fixture
def mock_clean_functions():
    with patch('utils.data_utils.clean_daily', return_value=pd.DataFrame({'A': [1, 2, 3]})), \
         patch('utils.data_utils.clean_income', return_value=pd.DataFrame({'A': [1, 2, 3]})), \
         patch('utils.data_utils.clean_balance', return_value=pd.DataFrame({'A': [1, 2, 3]})), \
         patch('utils.data_utils.clean_cash', return_value=pd.DataFrame({'A': [1, 2, 3]})), \
         patch('utils.data_utils.clean_info', return_value=pd.DataFrame({'A': [1, 2, 3]})):
        yield


# Test clean_data function
def test_clean_data_valid(mock_clean_functions):
    # Create a sample DataFrame for testing
    df = pd.DataFrame({'Date': ['2025-01-01', '2025-01-02'], 'Value': [100, 200]})

    # Test for valid data types
    cleaned_df = clean_data(df, 'daily')
    assert not cleaned_df.empty  # Check that the cleaned DataFrame is not empty
    assert set(cleaned_df.columns) == {'Date'}  # Check that the cleaned data has correct columns

    # Test for invalid data type
    with pytest.raises(ValueError):
        clean_data(df, 'unknown')

    # Test for invalid DataFrame
    with pytest.raises(TypeError):
        clean_data('invalid', 'daily')


# Test format_data function for 'daily' data
def test_format_data_daily(mock_clean_functions):
    # Create mock data for daily time series
    data = {
        'Meta Data': {'2. Symbol': 'AAPL'},
        'Time Series (Daily)': {
            '2025-01-01': {'1. open': 100, '2. high': 110, '3. low': 90, '4. close': 100, '5. volume': 1000},
            '2025-01-02': {'1. open': 101, '2. high': 111, '3. low': 91, '4. close': 101, '5. volume': 1100}
        }
    }

    # Test for non-nested daily data
    cleaned_data = format_data(data, function='daily', nested=False)
    assert not cleaned_data.empty
    assert 'Open' in cleaned_data.columns

    # Test for missing 'Time Series (Daily)' key
    with pytest.raises(KeyError):
        format_data({}, function='daily', nested=False)


# Test format_data function for 'info' data
def test_format_data_info(mock_clean_functions):
    # Create mock data for info
    data = {
        'Name': 'Apple Inc.',
        'SharesOutstanding': 5000,
        'Symbol': 'AAPL',
        'Exchange': 'NASDAQ',
        'Currency': 'USD',
        'Country': 'USA',
        'Sector': 'Technology'
    }

    # Test for non-nested info data
    cleaned_info = format_data(data, function='info', nested=False)
    assert 'A' in cleaned_info.columns  # Check if the cleaned data has correct columns

    # Test for missing keys in the data
    incomplete_data = {'Name': 'Apple Inc.'}  # Missing other keys
    with pytest.raises(KeyError):
        format_data(incomplete_data, function='info', nested=False)


# Test for handling nested data in format_data function
def test_format_data_nested(mock_clean_functions):
    # Create mock nested data for 'info'
    nested_data = {
        'AAPL': {
            'Name': 'Apple Inc.',
            'SharesOutstanding': 5000,
            'Symbol': 'AAPL',
            'Exchange': 'NASDAQ',
            'Currency': 'USD',
            'Country': 'USA',
            'Sector': 'Technology'
        },
        'GOOG': {
            'Name': 'Google',
            'SharesOutstanding': 3000,
            'Symbol': 'GOOG',
            'Exchange': 'NASDAQ',
            'Currency': 'USD',
            'Country': 'USA',
            'Sector': 'Technology'
        }
    }

    # Test for nested 'info' data
    cleaned_info = format_data(nested_data, function='info', nested=True)
    assert 'A' in cleaned_info['AAPL'].columns  # Check for 'AAPL'
    assert 'A' in cleaned_info['GOOG'].columns  # Check for 'GOOG'

    # Test for missing 'annualReports' key in nested financial data
    invalid_data = {'AAPL': {'Symbol': 'AAPL'}}  # Missing 'annualReports'
    with pytest.raises(KeyError):
        format_data(invalid_data, function='balance', nested=True)


# Test for edge cases, ensuring proper handling of unexpected input
def test_format_data_edge_cases(mock_clean_functions):
    # Test for empty data
    empty_data = {}
    cleaned_data = format_data(empty_data, function='daily', nested=False)
    assert cleaned_data.empty

    # Test for data with unexpected structure
    invalid_structure = {'unexpected_key': 'value'}
    with pytest.raises(KeyError):
        format_data(invalid_structure, function='daily', nested=False)


# Run all tests
if __name__ == '__main__':
    pytest.main()
