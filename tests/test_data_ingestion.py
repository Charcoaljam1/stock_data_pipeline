import pytest
import os
import json
import requests
from unittest.mock import patch, MagicMock
from scripts.data_ingestion import get_data, input_validation, build_parameters, fetch_api_response, save_data


@pytest.mark.parametrize("symbol,function,expected", [
    ("AAPL", "daily", {"error": False, "message": "Validation successful."}),
    (["GOOGL", "MSFT"], "income", {"error": False, "message": "Validation successful."}),
    ("INVALID1", "daily", {"error": True, "message": "Invalid symbol 'INVALID1'. Maximum length is 5 characters."}),
    ("AAPL", "wrong_function", {"error": True, "message": "Invalid function 'wrong_function'. Valid options are: daily, income, balance, cash, eps, info."}),
])
def test_input_validation(symbol, function, expected):
    """Test input validation for function and symbol."""
    assert input_validation(function, symbol) == expected


@patch("scripts.data_ingestion.requests.get")
def test_fetch_api_response_success(mock_get):
    """Test successful API response handling."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"Time Series (Daily)": {"2025-03-24": {"open": "100", "close": "110"}}}
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    params = {"function": "TIME_SERIES_DAILY", "symbol": "AAPL", "apikey": "test_api_key"}
    response = fetch_api_response("https://www.alphavantage.co/query", params)

    assert isinstance(response, dict)
    assert "Time Series (Daily)" in response


@patch("scripts.data_ingestion.requests.get")
def test_fetch_api_response_fail(mock_get):
    """Test API failure scenarios."""
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("400 Client Error: Bad Request")
    mock_get.return_value = mock_response

    params = {"function": "TIME_SERIES_DAILY", "symbol": "INVALID", "apikey": "test_api_key"}

    with pytest.raises(requests.exceptions.HTTPError):
        fetch_api_response("https://www.alphavantage.co/query", params)


def test_build_parameters():
    """Test that API parameters are built correctly."""
    expected_params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": "AAPL",
        "apikey": "test_api_key",
        "datatype": "json",
        "outputsize": "full"
    }
    assert build_parameters("AAPL", "daily") == expected_params


@patch("scripts.data_ingestion.fetch_api_response")
@patch("scripts.data_ingestion.save_data")
def test_get_data(mock_save, mock_fetch):
    """Test `get_data` function end-to-end with mocked API and file saving."""
    mock_fetch.return_value = {"Time Series (Daily)": {"2025-03-24": {"open": "100", "close": "110"}}}

    result = get_data("AAPL", "daily")

    assert isinstance(result, dict)
    assert "Time Series (Daily)" in result
    mock_save.assert_called_once()


def test_save_data(tmp_path):
    """Test data saving function."""
    test_data = {"test": "data"}
    file_path = os.path.join(tmp_path, "test.json")

    save_data(file_path, test_data)

    assert os.path.exists(file_path)
    with open(file_path, "r") as f:
        saved_data = json.load(f)
    assert saved_data == test_data
