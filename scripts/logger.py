import os
import json
import logging
import traceback
import requests
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


def log_info(log_level='error', log_params=True, re_raise=True):
    """
    Decorator for logging function calls and exceptions.

    Parameters:
    - log_level: Logging level (e.g., ERROR, WARNING, CRITICAL)
    - log_params: Whether to log function arguments
    - re_raise: Whether to re-raise the exception after logging
    """
    level_map = {
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    log_level = level_map.get(log_level.lower(), logging.INFO)
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = configure_logger(func.__module__)

            start_time = time.time()
            sanitized_args, sanitized_kwargs = sanitize_args(args, kwargs)
            actual_func_name = func.__name__

            try:
                logger.info(f"Function {actual_func_name} called",
                 extra={"custom_funcName": actual_func_name})
                result = func(*args, **kwargs)

                execution_time = round(time.time() - start_time, 3)

                if isinstance(result, dict) and result.get("error", False):
                    logger.warning(f"Validation failed in {actual_func_name}: {result['message']}", extra={"custom_funcName": actual_func_name})
               
                if isinstance(result, dict) and result.get("info", False):
                    logger.info(f"Process completed in {actual_func_name}: {result['message']}", extra={"custom_funcName": actual_func_name})

                logger.info(f"Function {actual_func_name} completed action in {execution_time} seconds",
                 extra={"custom_funcName": actual_func_name})

                return result

            except requests.exceptions.Timeout as e:
                error_data = {
                    "status": "error",
                    "function": actual_func_name,
                    "error_type": type(e).__name__,
                    "error_message": f'Timeout occured while fetching data. {str(e)}. Retrying...'
                }
                
                if log_params:
                    error_data.update({"args": sanitized_args, "kwargs": sanitized_kwargs})

                logger.log(level_map['warning'], json.dumps(error_data, indent=4), extra={"custom_funcName": actual_func_name})
                logger.debug(traceback.format_exc())

                if re_raise:
                    raise  # Re-raise exception
            except requests.exceptions.HTTPError as e:
                error_data = {
                    "status": "warning",
                    "function": actual_func_name,
                    "error_type": type(e).__name__,
                    "error_message": f"HTTP Error"
                }
                
                if e.response:
                    error_data["error_message"] += f": {e.response.status_code}. {e.response.reason}" 

                if e.response.status_code == 503:
                    error_data["error_message"] += f" (Service unavailable. Please try again later.)"
                elif e.response.status_code == 429:
                    error_data["error_message"] += f" (Rate limit exceeded. Please wait for a few seconds before retrying.)"  
                elif e.response.status_code == 401:
                    error_data["error_message"] += f" (Invalid API key. Please check the 'config/config.py' file.)"
                elif e.response.status_code == 404:
                    error_data["error_message"] += f" (Resource not found. Please check the function and symbol.)"
                elif e.response.status_code == 400:
                    error_data["error_message"] += f" (Bad request. Please check the function and symbol.)"
                elif e.response.status_code == 403:
                    error_data["error_message"] += f" (Forbidden. Please check the API key and URL.)"
                elif e.response.status_code == 500:
                    error_data["error_message"] += f" (Internal server error. Please try again later.)"
                elif e.response.status_code == 502:
                    error_data["error_message"] += f" (Bad gateway. Please try again later.)"
                elif e.response.status_code == 504:
                    error_data["error_message"] += f" (Gateway timeout. Please try again later.)"
            
                
                if log_params:
                    error_data.update({"args": sanitized_args, "kwargs": sanitized_kwargs})

                logger.log(level_map['error'], json.dumps(error_data, indent=4), extra={"custom_funcName": actual_func_name})
                logger.debug(traceback.format_exc())

                if re_raise:
                    raise  # Re-raise exception
            except requests.exceptions.RequestException as e:
                error_data = {
                    "status": "critical",
                    "function": actual_func_name,
                    "error_type": type(e).__name__,
                    "error_message": f"Request failed: {str(e)}"
                }
                
                if log_params:
                    error_data.update({"args": sanitized_args, "kwargs": sanitized_kwargs})

                logger.log(level_map['critical'], json.dumps(error_data, indent=4), extra={"custom_funcName": actual_func_name})
                logger.debug(traceback.format_exc())

                if re_raise:
                    raise  # Re-raise exception
            except Exception as e:
                error_data = {
                    "status": "error",
                    "function": actual_func_name,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                }
                
                if log_params:
                    error_data.update({"args": sanitized_args, "kwargs": sanitized_kwargs}, extra={"custom_funcName": actual_func_name})

                logger.log(log_level, json.dumps(error_data, indent=4),
                 extra={"custom_funcName": actual_func_name})
                logger.debug(traceback.format_exc())

                if re_raise:
                    raise  # Re-raise exception
        return wrapper
    return decorator
