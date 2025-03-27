import requests
from config.config import ALPHA_VANTAGE_API_KEY
from utils.logging.logger import log_info
from utils.exceptions.exception_handling import handle_exceptions
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception


def build_parameters(symbol: str, data_type: str) -> dict:
    function_mapping = {
        "daily": "TIME_SERIES_DAILY",
        "income": "INCOME_STATEMENT",
        "balance": "BALANCE_SHEET",
        "cash": "CASH_FLOW",
        "eps": "EARNINGS",
        "info": "OVERVIEW"  
        }
    params = {
        "function": function_mapping[data_type],
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    if data_type == "daily":
        params.update({"datatype": "json", "outputsize": "full"})
    return params


def is_retryable_exception(exception):
    """Check if the exception is retryable."""
    if isinstance(exception, requests.exceptions.HTTPError) and getattr(exception.response, "status_code", None) in [429, 500, 503]:
        return True  
    if isinstance(exception, requests.exceptions.RequestException):
        return True 
    return False

@handle_exceptions
@log_info
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception(is_retryable_exception),
)
def fetch_api_response(url: str, params: dict) -> dict:
    response = requests.get(url, params=params)
    response.raise_for_status() 
    return response.json()