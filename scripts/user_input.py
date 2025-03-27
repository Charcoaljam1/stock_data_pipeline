from logger import log_info, handle_exceptions
import warnings

@handle_exceptions
@log_info
def get_stock_symbols():
    """Prompt the user to enter stock symbol(s) and validate the input."""

    attempts = 3  # Allow the user up to 3 tries
    
    for attempt in range(attempts):
        user_input = input("Enter stock symbol(s) (comma-separated for multiple, e.g., AAPL,MSFT): ").strip().upper()

        # Ensure input is not empty
        if not user_input:
            warnings.warn(f"Invalid input: Stock symbol(s) cannot be empty. Attempts left: {attempts - attempt - 1}")
            print(f"Invalid input: Stock symbol(s) cannot be empty. Attempts left: {attempts - attempt - 1}")
            continue  # Retry
        
        # Ensure input contains only valid stock symbol characters (letters, commas, and spaces)
        if not all(char.isalpha() or char in {',', ' '} for char in user_input):
            warnings.warn(f"Invalid input: Stock symbols must contain only letters and commas. Attempts left: {attempts - attempt - 1}")
            print(f"Invalid input: Stock symbols must contain only letters and commas. Attempts left: {attempts - attempt - 1}")
            continue  # Retry
        
        # If valid, process input
        symbols = [symbol.strip() for symbol in user_input.split(',')]
        print("Valid stock symbols:", symbols)
        return symbols  # Exit function with valid input

    # If all attempts fail, raise an error
    raise ValueError("Too many invalid attempts. Exiting...")

