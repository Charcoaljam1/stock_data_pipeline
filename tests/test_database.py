from scripts.database_setup import create_tables
from scripts.data_loading_oltp import load_data
from tests import test_data_transformation as tdt



create_tables()



load_data(tdt.formatted_MSFT_data_info, 'info', symbol='MSFT')
load_data(tdt.formatted_MSFT_data_daily, 'daily', symbol='MSFT')           
load_data(tdt.formatted_MSFT_data_balance, 'balance', symbol='MSFT')
load_data(tdt.formatted_MSFT_data_income, 'income', symbol='MSFT')
load_data(tdt.formatted_MSFT_data_cash, 'cash', symbol='MSFT')

load_data(tdt.formatted_data_info, 'info', nested=True)
load_data(tdt.formatted_data_daily, 'daily', nested=True)
load_data(tdt.formatted_data_balance, 'balance', nested=True)   
load_data(tdt.formatted_data_income, 'income', nested=True)
load_data(tdt.formatted_data_cash, 'cash', nested=True)

   




