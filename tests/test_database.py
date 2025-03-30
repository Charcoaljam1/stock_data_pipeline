from scripts.data_ingestion import StockFetcher
from scripts.data_transformation import DataCleaner
from scripts.data_saving import DataStorage
from utils.logging.logger import log_info
from utils.exceptions.exception_handling import handle_exceptions
import json



# fetch = StockFetcher(['AAPL'],['daily'])
# fetch.get_data()
save = DataStorage()
with open('/home/charcoal_jam/projects/stock_pipeline/data/raw_data/AAPL_info.json', 'r') as file:
    stock_data = json.load(file)
data = {'AAPL': {'info': stock_data}}


# save.save_raw_data(fetch.data)
clean = DataCleaner(data)
clean.transform()
clean.processed_data
save.save_processed_data(clean.processed_data)

