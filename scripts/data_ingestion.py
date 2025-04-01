from utils.logging.logger import log_info
from utils.exceptions.exception_handling import handle_exceptions
import warnings
from utils.validation.raw_data_validation import input_validation, raw_data_validation
from utils.fetching.api_utils import build_parameters, fetch_api_response
from collections import defaultdict


class StockFetcher:
    
   

    def __init__(self, symbols, data_types, url="https://www.alphavantage.co/query"):
         # Ensure symbols are uppercase if it's a string or a list of strings
        if isinstance(symbols, str):
            self.symbols = [symbols.upper()]
        elif isinstance(symbols, list):
            self.symbols = [symbol.upper() for symbol in symbols]
        else:
            raise TypeError("symbols must be a string or a list of strings")
        
        # Ensure data_types are lowercase if it's a string or a list of strings
        if isinstance(data_types, str):
            self.data_types = [data_types.lower()]
        elif isinstance(data_types, list):
            self.data_types = [dtype.lower() for dtype in data_types]
        else:
            raise TypeError("data_types must be a string or a list of strings")
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
                
                self.data[symbol][data_type] = response
