import json
import traceback
from functools import wraps
import requests
from tenacity import RetryError
from utils.logging.logger import configure_logger, sanitize_args
import pandas as pd

def handle_exceptions(func):
    """"Decorator to handle exceptions and log them."""
    
    
    status_code_to_message = {
    503: "Service unavailable. Please try again later.",
    429: "Rate limit exceeded. Please wait for a few seconds before retrying.",
    401: "Invalid API key. Please check the 'config/config.py' file.",
    404: "Resource not found. Please check the function and symbol.",
    400: "Bad request. Please check the function and symbol.",
    403: "Forbidden. Please check the API key and URL.",
    500: "Internal server error. Please try again later.",
    502: "Bad gateway. Please try again later.",
    504: "Gateway timeout. Please try again later."
    }

    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = configure_logger(func.__module__)

      #  sanitized_args, sanitized_kwargs = sanitize_args(args, kwargs)
        actual_func_name = func.__name__

        error_data = {
            "status": "error",
            "function": actual_func_name,
            # "args": sanitized_args,
            # "kwargs": sanitized_kwargs
        }
      #  error_data["args"] = error_data["args"].to_dict() if isinstance(error_data["args"], pd.DataFrame) else error_data["args"]

        try:
          return func(*args, **kwargs)

        except requests.exceptions.Timeout as e:
            error_data["error_type"] = type(e).__name__
            error_data["error_message"] = f'Timeout occured while fetching data. {str(e)}. Retrying...'
        
            logger.error(json.dumps(error_data, indent=4), extra={"custom_funcName": actual_func_name})
            logger.debug(traceback.format_exc())

            raise  # Re-raise exception
        except requests.exceptions.HTTPError as e:
            error_data["error_type"] = type(e).__name__
            error_data["error_message"] = f"HTTP Error"

            if e.response.status_code in status_code_to_message:
                error_data["error_message"] += f" ({status_code_to_message[e.response.status_code]})"
            elif e.response:
                error_data["error_message"] += f": {e.response.status_code}. {e.response.reason}" 
            
            logger.error(json.dumps(error_data, indent=4), extra={"custom_funcName": actual_func_name})
            logger.debug(traceback.format_exc())

            raise  # Re-raise exception
        except requests.exceptions.RequestException as e:
            error_data["error_type"] = type(e).__name__
            error_data["error_message"] = f"Request failed: {str(e)}"

            logger.critical(json.dumps(error_data, indent=4), extra={"custom_funcName": actual_func_name})
            logger.debug(traceback.format_exc())

            raise  # Re-raise exception
        except RetryError as e:
            original_exception = e.last_attempt.exception()
            
            error_data["error_type"] = type(e).__name__
            error_data["error_message"] = f"Request failed: {original_exception}"

            if isinstance(original_exception, requests.exceptions.HTTPError):
                error_data["error_message"] = f"HTTPError: {original_exception}"

            logger.critical(json.dumps(error_data, indent=4), extra={"custom_funcName": actual_func_name})
            logger.debug(traceback.format_exc())

            raise  original_exception
        except Exception as e:
            error_data["error_type"] = type(e).__name__
            error_data["error_message"] = str(e)


            logger.error(json.dumps(error_data, indent=4), extra={"custom_funcName": actual_func_name})
            logger.debug(traceback.format_exc())

            raise  # Re-raise exception
    return wrapper
