import pytest
from unittest.mock import patch, MagicMock
from collections import defaultdict
import warnings
from scripts.data_transformation import DataCleaner

# Sample valid and invalid data to be used in tests
VALID_RAW_DATA = {
  "AAPL":{ 'balance': {
    "annualReports": [
      {
        "fiscalDateEnding": "2024-09-30",
        "reportedCurrency": "USD",
        "totalAssets": "364980000000",
        "totalCurrentAssets": "152987000000",
        "cashAndCashEquivalentsAtCarryingValue": "29943000000",
        "cashAndShortTermInvestments": "65171000000",
        "inventory": "7286000000",
        "currentNetReceivables": "66243000000",
        "totalNonCurrentAssets": "211993000000",
        "propertyPlantEquipment": "45680000000",
        "accumulatedDepreciationAmortizationPPE": "73448000000",
        "intangibleAssets": "None",
        "intangibleAssetsExcludingGoodwill": "None",
        "goodwill": "None",
        "investments": "254763000000",
        "longTermInvestments": "91479000000",
        "shortTermInvestments": "35228000000",
        "otherCurrentAssets": "14287000000",
        "otherNonCurrentAssets": "74834000000",
        "totalLiabilities": "308030000000",
        "totalCurrentLiabilities": "176392000000",
        "currentAccountsPayable": "68960000000",
        "deferredRevenue": "21049000000",
        "currentDebt": "21023000000",
        "shortTermDebt": "9967000000",
        "totalNonCurrentLiabilities": "131638000000",
        "capitalLeaseObligations": "752000000",
        "longTermDebt": "96700000000",
        "currentLongTermDebt": "10912000000",
        "longTermDebtNoncurrent": "85750000000",
        "shortLongTermDebtTotal": "106629000000",
        "otherCurrentLiabilities": "78304000000",
        "otherNonCurrentLiabilities": "45888000000",
        "totalShareholderEquity": "56950000000",
        "treasuryStock": "None",
        "retainedEarnings": "-19154000000",
        "commonStock": "83276000000",
        "commonStockSharesOutstanding": "15116786000"
      },
      {
        "fiscalDateEnding": "2023-09-30",
        "reportedCurrency": "USD",
        "totalAssets": "352583000000",
        "totalCurrentAssets": "143566000000",
        "cashAndCashEquivalentsAtCarryingValue": "29965000000",
        "cashAndShortTermInvestments": "61555000000",
        "inventory": "6331000000",
        "currentNetReceivables": "60985000000",
        "totalNonCurrentAssets": "209017000000",
        "propertyPlantEquipment": "43715000000",
        "accumulatedDepreciationAmortizationPPE": "70884000000",
        "intangibleAssets": "None",
        "intangibleAssetsExcludingGoodwill": "None",
        "goodwill": "None",
        "investments": "264965000000",
        "longTermInvestments": "100544000000",
        "shortTermInvestments": "31590000000",
        "otherCurrentAssets": "14695000000",
        "otherNonCurrentAssets": "64758000000",
        "totalLiabilities": "290437000000",
        "totalCurrentLiabilities": "145308000000",
        "currentAccountsPayable": "62611000000",
        "deferredRevenue": "20161000000",
        "currentDebt": "15972000000",
        "shortTermDebt": "5985000000",
        "totalNonCurrentLiabilities": "145129000000",
        "capitalLeaseObligations": "859000000",
        "longTermDebt": "105103000000",
        "currentLongTermDebt": "9822000000",
        "longTermDebtNoncurrent": "95281000000",
        "shortLongTermDebtTotal": "111088000000",
        "otherCurrentLiabilities": "58829000000",
        "otherNonCurrentLiabilities": "49848000000",
        "totalShareholderEquity": "62146000000",
        "treasuryStock": "None",
        "retainedEarnings": "-214000000",
        "commonStock": "73812000000",
        "commonStockSharesOutstanding": "15550061000"
      }
    ]},
    "symbol": "AAPL"
  },
  "daily": {
    "Meta Data": {
      "1. Information": "Daily Prices (open, high, low, close) and Volumes",
      "2. Symbol": "AMZN",
      "3. Last Refreshed": "2025-03-24",
      "4. Output Size": "Full size",
      "5. Time Zone": "US/Eastern"
    },
    "Time Series (Daily)": {
      "2025-03-24": {
        "1. open": "200.0000",
        "2. high": "203.6400",
        "3. low": "199.9500",
        "4. close": "203.2600",
        "5. volume": "41625365"
      },
      "2025-03-21": {
        "1. open": "192.9000",
        "2. high": "196.9900",
        "3. low": "192.5200",
        "4. close": "196.2100",
        "5. volume": "60056917"
      }
    },
    "symbol": "AAPL"
  }
}


INVALID_RAW_DATA = {
  "AAPL": {
    "annualReports": [
      {
        "fiscalDateEnding": "2024-09-30",
        "reportedCurrency": "USD",
        "totalAssets": "364980000000",
        "totalCurrentAssets": "152987000000",
        "cashAndCashEquivalentsAtCarryingValue": "29943000000",
        "cashAndShortTermInvestments": "65171000000",
        "inventory": "7286000000",
        "currentNetReceivables": "66243000000",
        "totalNonCurrentAssets": "211993000000",
        "propertyPlantEquipment": "45680000000",
        "accumulatedDepreciationAmortizationPPE": "73448000000",
        "intangibleAssets": "None",
        "intangibleAssetsExcludingGoodwill": "None",
        "goodwill": "None",
        "investments": "254763000000",
        "longTermInvestments": "91479000000",
        "shortTermInvestments": "35228000000",
        "otherCurrentAssets": "14287000000",
        "otherNonCurrentAssets": "74834000000",
        "totalLiabilities": "308030000000",
        "totalCurrentLiabilities": "176392000000",
        "currentAccountsPayable": "68960000000",
        "deferredRevenue": "21049000000",
        "currentDebt": "21023000000",
        "shortTermDebt": "9967000000",
        "totalNonCurrentLiabilities": "131638000000",
        "capitalLeaseObligations": "752000000",
        "longTermDebt": "96700000000",
        "currentLongTermDebt": "10912000000",
        "longTermDebtNoncurrent": "85750000000",
        "shortLongTermDebtTotal": "106629000000",
        "otherCurrentLiabilities": "78304000000",
        "otherNonCurrentLiabilities": "45888000000",
        "totalShareholderEquity": "56950000000",
        "treasuryStock": "None",
        "retainedEarnings": "InvalidData",
        "commonStock": "83276000000",
        "commonStockSharesOutstanding": "15116786000"
      },
      {
        "fiscalDateEnding": "2023-09-30",
        "reportedCurrency": "USD",
        "totalAssets": "352583000000",
        "totalCurrentAssets": "143566000000",
        "cashAndCashEquivalentsAtCarryingValue": "29965000000",
        "cashAndShortTermInvestments": "61555000000",
        "inventory": "6331000000",
        "currentNetReceivables": "60985000000",
        "totalNonCurrentAssets": "209017000000",
        "propertyPlantEquipment": "43715000000",
        "accumulatedDepreciationAmortizationPPE": "70884000000",
        "intangibleAssets": "None",
        "intangibleAssetsExcludingGoodwill": "None",
        "goodwill": "None",
        "investments": "264965000000",
        "longTermInvestments": "100544000000",
        "shortTermInvestments": "31590000000",
        "otherCurrentAssets": "14695000000",
        "otherNonCurrentAssets": "64758000000",
        "totalLiabilities": "290437000000",
        "totalCurrentLiabilities": "145308000000",
        "currentAccountsPayable": "62611000000",
        "deferredRevenue": "20161000000",
        "currentDebt": "15972000000",
        "shortTermDebt": "5985000000",
        "totalNonCurrentLiabilities": "145129000000",
        "capitalLeaseObligations": "859000000",
        "longTermDebt": "105103000000",
        "currentLongTermDebt": "9822000000",
        "longTermDebtNoncurrent": "95281000000",
        "shortLongTermDebtTotal": "111088000000",
        "otherCurrentLiabilities": "58829000000",
        "otherNonCurrentLiabilities": "49848000000",
        "totalShareholderEquity": "InvalidEquityData",
        "treasuryStock": "None",
        "retainedEarnings": "-214000000",
        "commonStock": "73812000000",
        "commonStockSharesOutstanding": "InvalidShareCount"
      }
    ],
    "symbol": "AAPL"
  },
  "AMZN": {
    "Meta Data": {
      "1. Information": "Daily Prices (open, high, low, close) and Volumes",
      "2. Symbol": "AMZN",
      "3. Last Refreshed": "2025-03-24",
      "4. Output Size": "Full size",
      "5. Time Zone": "US/Eastern"
    },
    "Time Series (Daily)": {
      "2025-03-24": {
        "1. open": "200.0000",
        "2. high": "203.6400",
        "3. low": "199.9500",
        "4. close": "NaN",
        "5. volume": "41625365"
      },
      "2025-03-21": {
        "1. open": "InvalidOpenValue",
        "2. high": "196.9900",
        "3. low": "192.5200",
        "4. close": "196.2100",
        "5. volume": "60056917"
      }
    },
    "symbol": "AMZN"
  }
}


# Test class for DataCleaner
class TestDataCleaner:

    # Test initialization
    def test_initialization(self):
        data_cleaner = DataCleaner(VALID_RAW_DATA)
        assert data_cleaner.raw_data == VALID_RAW_DATA
        assert isinstance(data_cleaner.processed_data, defaultdict)

    # Test transformation with valid data
    def test_transform_valid_data(self):
        data_cleaner = DataCleaner(VALID_RAW_DATA)

        # Mocking the formatting function to avoid actual logic execution
        with patch.object(DataCleaner, 'formatting_functions', {'daily': MagicMock(return_value={"formatted": True})}):
            data_cleaner.transform()

        # Check if processed data is populated after transformation
        assert data_cleaner.processed_data != defaultdict
        assert 'AAPL' in data_cleaner.processed_data
        assert 'annualReports' in data_cleaner.processed_data['AAPL']

    # Test transformation with invalid data, expecting warnings
    @pytest.mark.filterwarnings("ignore::UserWarning")
    def test_transform_invalid_data(self):
        data_cleaner = DataCleaner(INVALID_RAW_DATA)

        # Mocking the formatting function to simulate transformation
        with patch.object(DataCleaner, 'formatting_functions', {'daily': MagicMock(return_value={"formatted": True})}):
            with patch('warnings.warn') as warn_mock:
                data_cleaner.transform()
                warn_mock.assert_called_once_with('Invalid fiscalDateEnding format')

    # Test exception handling during transformation
    @patch('your_module.handle_exceptions')
    def test_exception_handling(self, mock_handle_exceptions):
        data_cleaner = DataCleaner(VALID_RAW_DATA)

        # Mock an exception in the transformation
        mock_handle_exceptions.side_effect = Exception("Mock exception")
        
        with pytest.raises(Exception, match="Mock exception"):
            data_cleaner.transform()

    # Test validation of processed data (successful validation)
    def test_validate_processed_data_success(self):
        data_cleaner = DataCleaner(VALID_RAW_DATA)

        # Mocking the validate_processed_data function to simulate successful validation
        with patch('your_module.validate_processed_data', return_value={"error": False}):
            result = data_cleaner.transform()

        # Assert that the data is validated successfully without warnings
        assert result is None

    # Test validation of processed data (failed validation)
    @pytest.mark.filterwarnings("ignore::UserWarning")
    def test_validate_processed_data_failure(self):
        data_cleaner = DataCleaner(VALID_RAW_DATA)

        # Mocking the validate_processed_data function to simulate failed validation
        with patch('your_module.validate_processed_data', return_value={"error": True, "message": "Invalid data format"}):
            with patch('warnings.warn') as warn_mock:
                data_cleaner.transform()
                warn_mock.assert_called_once_with("Invalid data format")

    # Test that the appropriate formatting functions are called
    def test_formatting_function_called(self):
        data_cleaner = DataCleaner(VALID_RAW_DATA)

        with patch.object(DataCleaner, 'formatting_functions', {'daily': MagicMock(return_value={"formatted": True})}) as mock_formatting:
            data_cleaner.transform()
            mock_formatting['daily'].assert_called()

    # Test case when `processed_data` is correctly populated for valid data
    def test_processed_data_population(self):
        data_cleaner = DataCleaner(VALID_RAW_DATA)

        # Mocking the formatting function
        with patch.object(DataCleaner, 'formatting_functions', {'daily': MagicMock(return_value={"formatted": True})}):
            data_cleaner.transform()

        # Check that processed_data has been populated correctly
        assert "AAPL" in data_cleaner.processed_data
        assert "annualReports" in data_cleaner.processed_data["AAPL"]
