from utils.logging.logger import log_info
from utils.exceptions.exception_handling import handle_exceptions
import warnings
from utils.validation.raw_data_validation import input_validation, raw_data_validation
from utils.fetching.api_utils import build_parameters, fetch_api_response
from collections import defaultdict


class StockFetcher:
    
   

    def __init__(self, symbols, data_types, url="https://www.alphavantage.co/query"):
        self.symbols = symbols
        self.data_types = data_types
        self.data = defaultdict(lambda: defaultdict(dict))
        self.url = url
        

    @log_info
    @handle_exceptions
    def get_data(self):
        
        validation_result = input_validation(self.data_types, self.symbols)
        if validation_result["error"]:
            raise ValueError(validation_result["message"])
        else:
            print(validation_result["message"])
        
        for symbol in self.symbols:
            for data_type in self.data_types:
                params = build_parameters(symbol, data_type)
                response = fetch_api_response(self.url, params)
                validation_result = raw_data_validation(response, data_type)
                if validation_result["error"]:
                    warnings.warn(f'Error validating {symbol} {data_type} data: {validation_result["message"]}')
                    continue
                elif symbol not in self.data:
                    self.data[symbol] = {}
                if data_type not in self.data[symbol]:
                    self.data[symbol][data_type] = {}
                self.data[symbol][data_type] = response
