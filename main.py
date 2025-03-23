from scripts.user_input import get_stock_symbols
from scripts.data_ingestion import get_data
from scripts.data_transformation import format_data
from scripts.data_loading_oltp import load_data
from scripts.database_setup import create_tables
from scripts.logger import logger

def main():
    try:
        # Step 1: Set up the database
        logger.info("Setting up the database...")
        # create_tables()
        logger.info("Database setup complete.")

        # Step 2: Define the symbols and functions based on user input
        try:
            symbols = get_stock_symbols()
            print("Valid stock symbols:", symbols)
        except ValueError as e:
            print(e)
        
        functions = ['daily', 'balance', 'income', 'cash', 'info'] 
        nested = len(symbols) > 1

        # Step 3: Data Ingestion
        logger.info("Starting data ingestion...")
        raw_data = {}
        for function in functions:
            raw_data[function] = get_data(symbols, function)
        logger.info("Data ingestion complete.")

        # Step 4: Data Transformation
        logger.info("Starting data transformation...")
        nested = isinstance(symbols, list)
        transformed_data = {}
        for function in functions:
            transformed_data[function] = format_data(raw_data[function], function, nested=nested)
        logger.info("Data transformation complete.")

        # Step 5: Data Loading
        logger.info("Starting data loading...")
        for function, data in transformed_data.items():
            load_data(data, function, nested=nested)
        logger.info("Data loading complete.")

        logger.info("Pipeline execution completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during pipeline execution: {e}")

if __name__ == "__main__":
    main()