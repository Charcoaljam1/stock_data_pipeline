import os
import json
import logging
import traceback
import requests

from functools import wraps
from logging.handlers import RotatingFileHandler
import pandas as pd

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

