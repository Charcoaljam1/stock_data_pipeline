from utils.logging.logger import log_info, handle_exceptions
import os
from utils.validation.data_validation import input_validation
from utils.fetching.api_utils import build_parameters, fetch_api_response
from utils.data_storage.data_saver import save_data
from collections import defaultdict


#url = f"https://www.alphavantage.co/query"

class StockFetcher:
    DATA_DIR = "data/raw_data"
   

    def __init__(self, symbols, data_types, url="https://www.alphavantage.co/query"):
        self.symbols = symbols
        self.data_types = data_types
        self.data = defaultdict(lambda: defaultdict(dict))
        self.url = url
        

    @handle_exceptions
    @log_info
    def get_data(self):
        
        validation_result = input_validation(self.data_types, self.symbols)
        if validation_result["error"]:
            raise ValueError(validation_result["message"])
        else:
            print("Validation passed!")
        
        
        for data_type in self.data_types:
            for symbol in self.symbols:
                params = build_parameters(symbol, data_type)
                response = fetch_api_response(self.url, params)
                file_path = os.path.join(StockFetcher.DATA_DIR, f"{symbol}_{data_type}.json")
                save_data(file_path, response)
                self.data[symbol][data_type] = response
