from scripts.data_ingestion import get_data
import json
import os

symbols = ['AAPL', 'GOOGL', 'AMZN', 'META']

MSFT_data_daily = get_data('MSFT', 'daily')
MSFT_data_balance = get_data('MSFT', 'balance')
MSFT_data_income = get_data('MSFT', 'income')
MSFT_data_cash = get_data('MSFT', 'cash')
MSFT_data_info = get_data('MSFT', 'info')

raw_data_daily = get_data(symbols, 'daily')
raw_data_balance = get_data(symbols, 'balance')
raw_data_income = get_data(symbols, 'income')
raw_data_cash = get_data(symbols, 'cash')   
raw_data_info = get_data(symbols, 'info')

