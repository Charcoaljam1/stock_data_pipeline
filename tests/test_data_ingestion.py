from scripts.data_ingestion import get_data
import json
import os

# symbols = ['AAPL', 'GOOGL', 'AMZN', 'META']

# MSFT_data_daily = get_data('MSFT', 'daily')
# MSFT_data_balance = get_data('MSFT', 'balance')
# MSFT_data_income = get_data('MSFT', 'income')
# MSFT_data_cash = get_data('MSFT', 'cash')
# MSFT_data_info = get_data('MSFT', 'info')

# raw_data_daily = get_data(symbols, 'daily')
# raw_data_balance = get_data(symbols, 'balance')
# raw_data_income = get_data(symbols, 'income')
# raw_data_cash = get_data(symbols, 'cash')   
# raw_data_info = get_data(symbols, 'info')

# Dictionary mapping function names to their corresponding dictionaries
symbols = ['AAPL', 'GOOGL', 'AMZN', 'META']
functions = ['daily', 'balance', 'income', 'cash', 'info']

raw_data_daily = {}
raw_data_balance = {}
raw_data_income = {}
raw_data_cash = {}
raw_data_info = {}

# Dictionary mapping function names to their corresponding dictionaries
data_storage = {
    'daily': raw_data_daily,
    'balance': raw_data_balance,
    'income': raw_data_income,
    'cash': raw_data_cash,
    'info': raw_data_info
}

# Load JSON files into their respective dictionaries
for symbol in symbols:
    for function in functions:
        file_path = f"data/raw_data/{symbol}_{function}.json"
        if os.path.exists(file_path):
            with open(file_path, "r") as file:
                data_storage[function][symbol] = json.load(file)
        else:
            print(f"Warning: {file_path} not found. Skipping.")

MSFT_data_daily = 0
MSFT_data_balance = 0
MSFT_data_income = 0
MSFT_data_cash = 0
MSFT_data_info = 0

data= {}



for function in functions:
    file_path = f"data/raw_data/MSFT_{function}.json"
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            data[function] = json.load(file)
    else:
        print(f"Warning: {file_path} not found. Skipping.")

MSFT_data_daily = data['daily']
MSFT_data_balance = data['balance']
MSFT_data_income = data['income']
MSFT_data_cash = data['cash']
MSFT_data_info = data['info']

print(MSFT_data_info)