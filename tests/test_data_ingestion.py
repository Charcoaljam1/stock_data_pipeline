import pytest
import os
import json
import requests
from unittest.mock import patch, MagicMock
from scripts.data_ingestion import StockFetcher


@pytest.mark.parametrize("symbol,function,expected", [
    ("AAPL", "daily", {"error": False, "message": "Validation successful."}),
    (["GOOGL", "MSFT"], "income", {"error": False, "message": "Validation successful."}),
    ("INVALID1", "daily", {"error": True, "message": "Invalid symbol 'INVALID1'. Maximum length is 5 characters."}),
    ("AAPL", "wrong_function", {"error": True, "message": "Invalid function 'wrong_function'. Valid options are: daily, income, balance, cash, eps, info."}),
])

@pytest.fixture
def stock_fetcher_invalid_input(symbols=["INVALID_SYMBOL"], data_types=["daily"]):
    return StockFetcher(symbols=["INVALID_SYMBOL"], data_types=["daily"])

# Test if ValueError is raised for invalid input
def test_get_data_invalid_input(stock_fetcher_invalid_input):
    with patch('utils.validation.raw_data_validation.input_validation', return_value={"error": True, "message": "Invalid input!"}):
        with pytest.raises(ValueError, match="Maximum length is 5 characters."):
            stock_fetcher_invalid_input.get_data()

import pytest
from unittest.mock import patch

@pytest.fixture
def stock_fetcher_valid_input():
    return StockFetcher(symbols=["AAPL"], data_types=["daily"])

@pytest.fixture
def stock_fetcher_invalid_data():
    return StockFetcher(symbols=["AAPL"], data_types=["daily"])

# Test if warnings are raised for invalid data in the response
def test_get_data_warning(stock_fetcher_invalid_data, caplog):
    with patch('utils.fetching.api_utils.fetch_api_response', return_value={"data": "invalid data"}):
        # Simulate raw data validation error
        with patch('utils.validation.raw_data_validation', return_value={"error": True, "message": "Invalid data!"}):
            stock_fetcher_invalid_data.get_data()

    # Check if the warning was logged
    assert "Error validating AAPL daily data: Invalid data!" in caplog.text