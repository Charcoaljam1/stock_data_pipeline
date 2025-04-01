import pytest
from unittest.mock import patch
from utils.validation.raw_data_validation import input_validation, raw_data_validation
from utils.fetching.api_utils import build_parameters, fetch_api_response
from scripts.data_ingestion import StockFetcher
from requests.exceptions import HTTPError
from tenacity import RetryError


mock_data = {
        'AAPL':{
            'daily':{
                  "Meta Data": {
        "1. Information": "Daily Prices (open, high, low, close) and Volumes",
        "2. Symbol": "AAPL",
        "3. Last Refreshed": "2025-03-28",
        "4. Output Size": "Full size",
        "5. Time Zone": "US/Eastern"
    },
    "Time Series (Daily)": {
        "2025-03-28": {
            "1. open": "221.6700",
            "2. high": "223.8100",
            "3. low": "217.6800",
            "4. close": "217.9000",
            "5. volume": "39818617"
        },
        "2025-03-27": {
            "1. open": "221.3900",
            "2. high": "224.9900",
            "3. low": "220.5601",
            "4. close": "223.8500",
            "5. volume": "37094774"
        },
        "2025-03-26": {
            "1. open": "223.5100",
            "2. high": "225.0200",
            "3. low": "220.4700",
            "4. close": "221.5300",
            "5. volume": "34532656"
        },
        "2025-03-25": {
            "1. open": "220.7700",
            "2. high": "224.1000",
            "3. low": "220.0800",
            "4. close": "223.7500",
            "5. volume": "34493583"
        },
        "2025-03-24": {
            "1. open": "221.0000",
            "2. high": "221.4800",
            "3. low": "218.5800",
            "4. close": "220.7300",
            "5. volume": "44299483"
        },
        "2025-03-21": {
            "1. open": "211.5600",
            "2. high": "218.8400",
            "3. low": "211.2800",
            "4. close": "218.2700",
            "5. volume": "94127768"
        },
        "2025-03-20": {
            "1. open": "213.9900",
            "2. high": "217.4899",
            "3. low": "212.2200",
            "4. close": "214.1000",
            "5. volume": "48862947"
        }
            }},
            'info':{
    "Symbol": "AAPL",
    "AssetType": "Common Stock",
    "Name": "Apple Inc",
    "Description": "Apple Inc. is an American multinational technology company that specializes in consumer electronics, computer software, and online services. Apple is the world's largest technology company by revenue (totalling $274.5 billion in 2020) and, since January 2021, the world's most valuable company. As of 2021, Apple is the world's fourth-largest PC vendor by unit sales, and fourth-largest smartphone manufacturer. It is one of the Big Five American information technology companies, along with Amazon, Google, Microsoft, and Facebook.",
    "SharesOutstanding": "320193",
    "Exchange": "NASDAQ",
    "Currency": "USD",
    "Country": "USA",
    "Sector": "TECHNOLOGY",
    "Industry": "ELECTRONIC COMPUTERS",
    "Address": "ONE INFINITE LOOP, CUPERTINO, CA, US",
 },
            'cash':{
                 "symbol": "AAPL",
                 "annualReports": [
        {
            "fiscalDateEnding": "2024-09-30",
            "reportedCurrency": "USD",
            "operatingCashflow": "118254000000",
            "paymentsForOperatingActivities": "1900000000",
            "proceedsFromOperatingActivities": "None",
            "changeInOperatingLiabilities": "21572000000",
            "changeInOperatingAssets": "17921000000",
            "depreciationDepletionAndAmortization": "11445000000",
            "capitalExpenditures": "9447000000",
            "changeInReceivables": "5144000000",
            "changeInInventory": "1046000000",
            "profitLoss": "93736000000",
            "cashflowFromInvestment": "2935000000",
            "cashflowFromFinancing": "-121983000000",
            "proceedsFromRepaymentsOfShortTermDebt": "7920000000",
            "paymentsForRepurchaseOfCommonStock": "94949000000",
            "paymentsForRepurchaseOfEquity": "94949000000",
            "paymentsForRepurchaseOfPreferredStock": "None",
            "dividendPayout": "15234000000",
            "dividendPayoutCommonStock": "None",
            "dividendPayoutPreferredStock": "None",
            "proceedsFromIssuanceOfCommonStock": "None",
            "proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet": "0",
            "proceedsFromIssuanceOfPreferredStock": "None",
            "proceedsFromRepurchaseOfEquity": "-94949000000",
            "proceedsFromSaleOfTreasuryStock": "None",
            "changeInCashAndCashEquivalents": "None",
            "changeInExchangeRate": "None",
            "netIncome": "93736000000"
        }]
            },
            'balance':{
    "symbol": "AAPL",
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
        }]
            },
            'income':{
                "symbol": "AAPL",
                "annualReports": [
                    {
                        "fiscalDateEnding": "2024-09-30",
                        "reportedCurrency": "USD",
                        "grossProfit": "180683000000",
                        "totalRevenue": "391035000000",
                        "costOfRevenue": "236449000000",
                        "costofGoodsAndServicesSold": "210352000000",
                        "operatingIncome": "123216000000",
                        "sellingGeneralAndAdministrative": "26097000000",
                        "researchAndDevelopment": "31370000000",
                        "operatingExpenses": "57467000000",
                        "investmentIncomeNet": "None",
                        "netInterestIncome": "None",
                        "interestIncome": "None",
                        "interestExpense": "None",
                        "nonInterestIncome": "391035000000",
                        "otherNonOperatingIncome": "None",
                        "depreciation": "8200000000",
                        "depreciationAndAmortization": "None",
                        "incomeBeforeTax": "123485000000",
                        "incomeTaxExpense": "29749000000",
                        "interestAndDebtExpense": "None",
                        "netIncomeFromContinuingOperations": "93736000000",
                        "comprehensiveIncomeNetOfTax": "98016000000",
                        "ebit": "123216000000",
                        "ebitda": "None",
                        "netIncome": "93736000000"
        }]
            }
        },
        'MSFT': {
                    'daily':{
                        "Meta Data": {
                "1. Information": "Daily Prices (open, high, low, close) and Volumes",
                "2. Symbol": "MSFT",
                "3. Last Refreshed": "2025-03-28",
                "4. Output Size": "Full size",
                "5. Time Zone": "US/Eastern"
            },
            "Time Series (Daily)": {
                "2025-03-28": {
                    "1. open": "221.6700",
                    "2. high": "223.8100",
                    "3. low": "217.6800",
                    "4. close": "217.9000",
                    "5. volume": "39818617"
                },
                "2025-03-27": {
                    "1. open": "221.3900",
                    "2. high": "224.9900",
                    "3. low": "220.5601",
                    "4. close": "223.8500",
                    "5. volume": "37094774"
                },
                "2025-03-26": {
                    "1. open": "223.5100",
                    "2. high": "225.0200",
                    "3. low": "220.4700",
                    "4. close": "221.5300",
                    "5. volume": "34532656"
                },
                "2025-03-25": {
                    "1. open": "220.7700",
                    "2. high": "224.1000",
                    "3. low": "220.0800",
                    "4. close": "223.7500",
                    "5. volume": "34493583"
                },
                "2025-03-24": {
                    "1. open": "221.0000",
                    "2. high": "221.4800",
                    "3. low": "218.5800",
                    "4. close": "220.7300",
                    "5. volume": "44299483"
                },
                "2025-03-21": {
                    "1. open": "211.5600",
                    "2. high": "218.8400",
                    "3. low": "211.2800",
                    "4. close": "218.2700",
                    "5. volume": "94127768"
                },
                "2025-03-20": {
                    "1. open": "213.9900",
                    "2. high": "217.4899",
                    "3. low": "212.2200",
                    "4. close": "214.1000",
                    "5. volume": "48862947"
                }
                    },
            'info':{
    "Symbol": "MSFT",
    "AssetType": "Common Stock",
    "Name": "Apple Inc",
    "Description": "Apple Inc. is an American multinational technology company that specializes in consumer electronics, computer software, and online services. Apple is the world's largest technology company by revenue (totalling $274.5 billion in 2020) and, since January 2021, the world's most valuable company. As of 2021, Apple is the world's fourth-largest PC vendor by unit sales, and fourth-largest smartphone manufacturer. It is one of the Big Five American information technology companies, along with Amazon, Google, Microsoft, and Facebook.",
    "CIK": "320193",
    "Exchange": "NASDAQ",
    "Currency": "USD",
    "Country": "USA",
    "Sector": "TECHNOLOGY",
    "Industry": "ELECTRONIC COMPUTERS",
    "Address": "ONE INFINITE LOOP, CUPERTINO, CA, US",
    "OfficialSite": "https://www.apple.com",
    "FiscalYearEnd": "September",
    "LatestQuarter": "2024-12-31",
    "MarketCapitalization": "3273315582000",
  
},
            'cash':{
                 "symbol": "MSFT",
    "annualReports": [
        {
            "fiscalDateEnding": "2024-09-30",
            "reportedCurrency": "USD",
            "operatingCashflow": "118254000000",
            "paymentsForOperatingActivities": "1900000000",
            "proceedsFromOperatingActivities": "None",
            "changeInOperatingLiabilities": "21572000000",
            "changeInOperatingAssets": "17921000000",
            "depreciationDepletionAndAmortization": "11445000000",
            "capitalExpenditures": "9447000000",
            "changeInReceivables": "5144000000",
            "changeInInventory": "1046000000",
            "profitLoss": "93736000000",
            "cashflowFromInvestment": "2935000000",
            "cashflowFromFinancing": "-121983000000",
            "proceedsFromRepaymentsOfShortTermDebt": "7920000000",
            "paymentsForRepurchaseOfCommonStock": "94949000000",
            "paymentsForRepurchaseOfEquity": "94949000000",
            "paymentsForRepurchaseOfPreferredStock": "None",
            "dividendPayout": "15234000000",
            "dividendPayoutCommonStock": "None",
            "dividendPayoutPreferredStock": "None",
            "proceedsFromIssuanceOfCommonStock": "None",
            "proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet": "0",
            "proceedsFromIssuanceOfPreferredStock": "None",
            "proceedsFromRepurchaseOfEquity": "-94949000000",
            "proceedsFromSaleOfTreasuryStock": "None",
            "changeInCashAndCashEquivalents": "None",
            "changeInExchangeRate": "None",
            "netIncome": "93736000000"
        }]
            },
            'balance':{
                "symbol": "MSFT",
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
                }]
            },
            'income':{
    "symbol": "MSFT",
    "annualReports": [
        {
            "fiscalDateEnding": "2024-09-30",
            "reportedCurrency": "USD",
            "grossProfit": "180683000000",
            "totalRevenue": "391035000000",
            "costOfRevenue": "236449000000",
            "costofGoodsAndServicesSold": "210352000000",
            "operatingIncome": "123216000000",
            "sellingGeneralAndAdministrative": "26097000000",
            "researchAndDevelopment": "31370000000",
            "operatingExpenses": "57467000000",
            "investmentIncomeNet": "None",
            "netInterestIncome": "None",
            "interestIncome": "None",
            "interestExpense": "None",
            "nonInterestIncome": "391035000000",
            "otherNonOperatingIncome": "None",
            "depreciation": "8200000000",
            "depreciationAndAmortization": "None",
            "incomeBeforeTax": "123485000000",
            "incomeTaxExpense": "29749000000",
            "interestAndDebtExpense": "None",
            "netIncomeFromContinuingOperations": "93736000000",
            "comprehensiveIncomeNetOfTax": "98016000000",
            "ebit": "123216000000",
            "ebitda": "None",
            "netIncome": "93736000000"
        }]
            
         }
        }
    }
}

missing_key_data = {
    'AAPL':{
            'daily':{
                  "Meta Data": {
        "1. Information": "Daily Prices (open, high, low, close) and Volumes",
        "2. Symbol": "AAPL",
        "3. Last Refreshed": "2025-03-28",
        "4. Output Size": "Full size",
        "5. Time Zone": "US/Eastern"
    },
    "Time Series (Daily)": {
        "2025-03-28": {
            "1. open": "221.6700",
            "2. high": "223.8100",
            "3. low": "217.6800",
            "4. close": "217.9000",
            "5. volume": "39818617"
        },
        "2025-03-27": {
            "1. open": "221.3900",
            "2. high": "224.9900",
            "3. low": "220.5601",
            "4. close": "223.8500",
            "5. volume": "37094774"
        },
        "2025-03-26": {
            "1. open": "223.5100",
            "2. high": "225.0200",
            "3. low": "220.4700",
            "4. close": "221.5300",
            "5. volume": "34532656"
        }
            }},
            'info':{
    "Symbol": "AAPL",
    "AssetType": "Common Stock",
    "Name": "Apple Inc",
    "Description": "Apple Inc. is an American multinational technology company that specializes in consumer electronics, computer software, and online services. Apple is the world's largest technology company by revenue (totalling $274.5 billion in 2020) and, since January 2021, the world's most valuable company. As of 2021, Apple is the world's fourth-largest PC vendor by unit sales, and fourth-largest smartphone manufacturer. It is one of the Big Five American information technology companies, along with Amazon, Google, Microsoft, and Facebook.",
    "CIK": "320193",
    "Exchange": "NASDAQ",
    "Currency": "USD",
    "Country": "USA",
    "Sector": "TECHNOLOGY",
    "Industry": "ELECTRONIC COMPUTERS",
    "Address": "ONE INFINITE LOOP, CUPERTINO, CA, US",
 
},
            'cash':{
                 "symbol": "AAPL",
                 "annualReports": [
        {
            "fiscalDateEnding": "2024-09-30",
            "reportedCurrency": "USD",
            "operatingCashflow": "118254000000",
            "paymentsForOperatingActivities": "1900000000",
            "proceedsFromOperatingActivities": "None",
            "changeInOperatingLiabilities": "21572000000",
            "changeInOperatingAssets": "17921000000",
            "depreciationDepletionAndAmortization": "11445000000",
            "capitalExpenditures": "9447000000",
            "changeInReceivables": "5144000000",
            "changeInInventory": "1046000000",
            "profitLoss": "93736000000",
            "cashflowFromInvestment": "2935000000",
          
        }]
            },
            'balance':None,
            },
    'MSFT':{
        'deely':79898,
        'balance':{
                "symbol": "MSFT",
            "annualReports": [
                {
                   
                    "totalAssets": "364980000000",
                    "totalCurrentAssets": "152987000000",
                    "cashAndCashEquivalentsAtCarryingValue": "29943000000",
                    "cashAndShortTermInvestments": "65171000000",
                    "inventory": "7286000000",
                    "currentNetReceivables": "66243000000",
                    "totalNonCurrentAssets": "211993000000",
                    "propertyPlantEquipment": "45680000000",
           
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
                }]
            },
            'income':{
    "symbol": "MSFT",
    "annualReports": [
        {
            "fiscalDateEnding": "2024-09-30",
            "reportedCurrency": "USD",
            "grossProfit": "180683000000",
            "totalRevenue": "391035000000",
            "investmentIncomeNet": "None",
            "netInterestIncome": "None",
            "interestIncome": "None",
            "interestExpense": "None",
            "nonInterestIncome": "391035000000",
            "otherNonOperatingIncome": "None",
            "depreciation": "8200000000",
            "depreciationAndAmortization": "None",
            "incomeBeforeTax": "123485000000",
               }]
            
         }
    }
    
}


@pytest.fixture
def symbols():
    return 'AAPL'

@pytest.fixture
def data_types():
    return 'daily'

@pytest.fixture
def data_get(symbols,data_types):
    return StockFetcher(symbols,data_types)

@pytest.fixture
def parameters(symbols, data_types):
    return build_parameters(symbols, data_types) 

        
@pytest.fixture
def url():
    return "https://www.alphavantage.co/query"

@patch('requests.get')
def test_get_data(mock_get,url,parameters):
    """Test for successful API respsone"""
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_data

    
    data = fetch_api_response(url,parameters)
        
   
    assert 'AAPL' in data # Expects AAPL to be a key
    assert isinstance(data, dict) # Expects data to be a dictionary
    assert 'daily' in data['AAPL'] # Expects daily key to be in AAPL dictionary

def test_validation():
    """Test for proper validation of data from API"""
    result = []
    for symbol, data_types in mock_data.items():
        for key, values in data_types.items():
            result.append(raw_data_validation(values,key))

    assert isinstance(result[1], dict) # Expects each element in the list to be a dictionary
    assert result[0]['error'] == False # Expects the error key to have a value of False

def test_validation_with_missing_key_data():
    """Test for expected behaviour when handling a dictionary with at least one missing key"""
    result = []
    for symbol, data_types in missing_key_data.items():
        for key, values in data_types.items():
            result.append(raw_data_validation(values,key))

    assert isinstance(result[4], dict) # Expects each element in the list to be a dictionary
    assert result[2]['error'] == True  # Expects the error key to have a value of False

def test_validation_with_None():
    """"Test for when both arguments are None"""
    data = raw_data_validation(None,None)

    assert data['error'] == True # Expects the error key to have a value of True
    assert 'not a dictionary' in data['message'] # Expects the error message to contain string: 'not a dictionary'


def test_validation_with_None_data_types():
    """Tests for when data_type argument is None"""
    for symbol, data_types in missing_key_data.items():
        for key, values in data_types.items():
            result = raw_data_validation(values,None)

    assert result['error'] == True  # Expects the error key to have a value of True
    assert 'Expected one of' in result['message'] # Expects the error message to contain string: 'Expected one of'

def test_validation_with_corrupt_data():
    """Test for proper validation with corrupted data"""
    result = raw_data_validation({8465444: 'HELLO World'},'daily')

    assert result['error'] == True  # Expects the error key to have a value of True
    assert "Missing 'Time Series (Daily)' key." in result['message'] # Expects a certain error message

def test_input_validation():
    """Test for proper validation of parameter inputs"""
    result = []
    for tick in ['TSLA','MSFT', '3435',4545,'Invalid']:
        for data_types in ['daily', 335,'INFO']:
            result.append(input_validation(tick, data_types))

    assert result[1]['error'] == True
    assert result[7]['error'] == True

@patch('requests.get')
def test_error(mock_get,url,parameters):
    """Test for proper exception raising"""
    mock_get.return_value.status_code = 404
    mock_get.return_value.raise_for_status.side_effect = HTTPError("404 Client Error: Not Found")

        
    with pytest.raises(HTTPError):
       fetch_api_response(url,parameters)

@patch('requests.get')
def test_retry_error(mock_get,url,parameters):
    """Test for proper exception raising after RetryError"""
    mock_get.return_value.raise_for_status.side_effect = HTTPError("404 Client Error: Not Found")

    mock_get.side_effect = RetryError(last_attempt=mock_get.return_value.raise_for_status.side_effect)  # Simulate a retry failure

    with pytest.raises(HTTPError):
        fetch_api_response(url, parameters)

def test_stock_fetcher(data_get):
    """Test for proper object behaviour"""
    data_get.get_data()

    assert isinstance(data_get.data, dict)