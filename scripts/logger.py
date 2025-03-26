import os
import json
import logging
import traceback
import requests
from tenacity import RetryError
import time
from functools import wraps
from logging.handlers import RotatingFileHandler

# Global logger configuration
LOG_DIRECTORY = "logs"
os.makedirs(LOG_DIRECTORY, exist_ok=True)

def configure_logger(module_name: str) -> logging.Logger:
    """Configures a logger for a specific function."""
    LOG_FILE_PATH = os.path.join(LOG_DIRECTORY, f"{module_name.split('.')[-1]}.log")
    
    logger = logging.getLogger(module_name)

    # Prevent multiple handlers from being added
    if not logger.hasHandlers():
        handler = RotatingFileHandler(LOG_FILE_PATH, maxBytes=5 * 1024 * 1024, backupCount=5)
        formatter = logging.Formatter("%(asctime)s - %(custom_funcName)s - %(levelname)s - %(message)s ")
        handler.setFormatter(formatter)

        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        logger.propagate = False  # Prevent handler duplication

    return logger

def sanitize_args(args, kwargs, sensitive_keys=None):
    """Remove sensitive data from logged arguments."""
    if sensitive_keys is None:
        sensitive_keys = {"apikey", "password", "token", "secret", "key", "alpha_vantage_api_key"}

    sanitized_kwargs = {
        k: "[REDACTED]" if k.lower() in sensitive_keys else v
        for k, v in kwargs.items()
    }

    sanitized_args = tuple(
        "[REDACTED]" if isinstance(arg, str) and any(s in arg.lower() for s in sensitive_keys) else arg
        for arg in args
    )

    return sanitized_args, sanitized_kwargs

def log_info(func):
    """Decorator to log function execution start and completion."""
    @wraps(func)  # Preserves function metadata
    def wrapper(*args, **kwargs):
        logger = configure_logger(func.__module__)
        actual_func_name = func.__name__
        sanitized_args, sanitized_kwargs = sanitize_args(args, kwargs)

        info_data = {
            "status": "starting",
            "function": actual_func_name,
            "args": sanitized_args,
            "kwargs": sanitized_kwargs,
            "_info_message": f"{actual_func_name} function is starting"
        }
        logger.info(json.dumps(info_data, indent=4), extra={"custom_funcName": actual_func_name})

        result = func(*args, **kwargs)  # Execute the function

        info_data["status"] = "completed"
        info_data["_info_message"] = f"{actual_func_name} function has completed"
        logger.info(json.dumps(info_data, indent=4), extra={"custom_funcName": actual_func_name})

        return result  # Preserve function output

    return wrapper

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

        sanitized_args, sanitized_kwargs = sanitize_args(args, kwargs)
        actual_func_name = func.__name__

        error_data = {
            "status": "error",
            "function": actual_func_name,
            "args": sanitized_args,
            "kwargs": sanitized_kwargs,
            "error_type": type(e).__name__,
            "error_message":f'Timeout occured while fetching data. {str(e)}. Retrying...'
        }

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
