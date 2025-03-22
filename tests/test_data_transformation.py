from scripts.data_transformation import format_data
from tests import test_data_ingestion as tdi

formatted_MSFT_data_daily = format_data(tdi.MSFT_data_daily, 'daily')
formatted_MSFT_data_balance = format_data(tdi.MSFT_data_balance, 'balance')
formatted_MSFT_data_income = format_data(tdi.MSFT_data_income, 'income')
formatted_MSFT_data_cash = format_data(tdi.MSFT_data_cash, 'cash')
formatted_MSFT_data_info = format_data(tdi.MSFT_data_info, 'info')

formatted_data_daily = format_data(tdi.raw_data_daily, 'daily', nested=True)
formatted_data_balance = format_data(tdi.raw_data_balance, 'balance', nested=True)
formatted_data_income = format_data(tdi.raw_data_income, 'income', nested=True)
formatted_data_cash = format_data(tdi.raw_data_cash, 'cash', nested=True)
formatted_data_info = format_data(tdi.raw_data_info, 'info', nested=True)
