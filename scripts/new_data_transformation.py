from collections import defaultdict
from utils.logging.logger import log_info
from utils.exceptions.exception_handling import handle_exceptions
from utils.validation.data_validation import raw_data_validation
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

    @handle_exceptions
    @log_info
    def transform(self):
        raw_data_validation(self.raw_data)
        for symbols, data in self.raw_data.items():
            for data_types, values in data.items():
                self.processed_data[symbols][data_types]=DataCleaner.formatting_functions[data_types](values,data_types,symbols)

