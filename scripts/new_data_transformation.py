from collections import defaultdict
import warnings
from utils.logging.logger import log_info
from utils.exceptions.exception_handling import handle_exceptions
from utils.validation.raw_data_validation import raw_data_validation, validate_processed_data
from utils.cleaning.data_cleaners import format_daily, format_financial, format_info

class DataCleaner:

    formatting_functions = {
        'daily': format_daily,
        'income': format_financial,
        'balance': format_financial,
        'cash': format_financial,
        'info': format_info
        }
    
    def __init__(self, raw_data):
        self.raw_data = raw_data
        self.processed_data = defaultdict(lambda: defaultdict(dict))

    @log_info
    @handle_exceptions
    def transform(self):
        for symbols, data in self.raw_data.items():
            for data_types, values in data.items():

                result = DataCleaner.formatting_functions[data_types](values,data_types,symbols)
                validation_result = validate_processed_data(result, data_types)
                if validation_result["error"]:
                    warnings.warn(validation_result["message"])
                    continue
                else:
                    self.processed_data[symbols][data_types]

