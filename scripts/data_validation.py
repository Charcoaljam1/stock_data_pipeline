import re

date_pattern = r'^\d{4}-\d{2}-\d{2}$'  # Matches "YYYY-MM-DD"
daily_key_pattern = r'^\d\.\s(open|high|low|close|volume)$'  # Matches "1. open", etc.
value_pattern = r'^\d+(\.\d+)?$'  # Matches integers or decimal numbers
date_pattern = r'^\d{4}-\d{2}-\d{2}$'  # Matches YYYY-MM-DD format
key_pattern = r'^[a-zA-Z]+[a-zA-Z0-9]*$'  # Matches valid key names (no special characters)
value_none_pattern = r'^None$'  # Matches "None"
value_string_pattern = r'^[A-Z]{3}$'  # Matches string values like currency codes (e.g., "USD")


def raw_data_validation(data):
    data_type_list = ['daily', 'income', 'balance', 'cash', 'info']
    if not isinstance(data, dict):
        raise TypeError(f"Argument 'data' is not a dictionary, instead:{type(data)}")
    if not data:
        raise ValueError("Variable cannot be empty")
    
    for symbol, symbol_data in data.items():
        if not isinstance(symbol, str):
            raise TypeError(f"Key is not a string, instead: {type(symbol)}")
        if not isinstance(symbol_data, dict):
            raise TypeError(f"Expected a dictionary as the value corresponding to key {symbol}, instead got a {type(symbol_data)}")
        
        for data_type, values in symbol_data.items():
            if not isinstance(data_type, str):
                raise TypeError(f"Key is not a string, instead: {type(data_type)}")
            if not isinstance(values, dict):
                raise TypeError(f"Expected a dictionary as the value corresponding to key {data_type}, instead got a {type(values)}")
            if data_type not in data_type_list:
                raise KeyError(f"Expected one of {data_type_list} as a key, instead got {data_type}")
            if data_type == 'daily':
                daily_validation(values)
            elif data_type == 'info':
                info_validation(values)
            else:
                financials_validation(values)

    return True
        
            
# if len(data) == 1:
#     print("API rate limit has been reached")

def daily_validation(daily_dict):
    for keys, values in daily_dict.items():
       
        if not isinstance(keys, str):
            raise TypeError(f"Key is not a string, instead: {type(keys)}")
        if not isinstance(values, dict):
            raise TypeError(f"Expected a dictionary as the value corresponding to key {keys}, instead got a {type(values)}")
        
        for date, inner_dict in values['Time Seried Daily'].items():
            if not re.match(date_pattern, date):
                raise ValueError(f"Invalid date format: {date}")
            for key, value in inner_dict.items():
                if not re.match(daily_key_pattern, key):
                    raise KeyError(f"Invalid key format: {key}")
                if not re.match(value_pattern, value):
                    raise ValueError(f"Invalid value format: {value}")
    return True

def financials_validation(financials_dict):

    for report in financials_dict["annualReports"]:
        for keys, values in report.items():
            if not isinstance(keys, str):
                raise TypeError(f"Key is not a string, instead: {type(keys)}")
            if not isinstance(values, dict):
                raise TypeError(f"Expected a dictionary as the value corresponding to key {keys}, instead got a {type(values)}")
            
            if not re.match(key_pattern, keys):
                raise ValueError(f"Invalid key format: {keys}")
            # Validate the value
            if re.match(date_pattern, values):
                pass
            elif re.match(value_pattern, values):
               pass
            elif re.match(value_none_pattern, values):
                pass
            elif re.match(value_string_pattern, values):
                pass
            else:
                raise ValueError(f'Invalid value format for key "{keys}": {values}')

    return True

def info_validation(info_dict):
    for keys, values in info_dict.items():
        if re.match(value_string_pattern, values):
            pass
        elif re.match(value_pattern, values):
            pass
        elif re.match(date_pattern, values):
            pass
        else:
            raise ValueError(f'Invalid value format for key "{keys}": {values}')
