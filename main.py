from scripts.data_ingestion import StockFetcher
from scripts.data_transformation import DataCleaner
from scripts.data_saving import DataStorage
from utils.logging.logger import log_info
from utils.exceptions.exception_handling import handle_exceptions

@log_info()
@handle_exceptions
def main():
    
    fetch = StockFetcher(['AAPL','MSFT','IBM','TSLA'],['info','daily','cash','income','balance'])
    clean = DataCleaner(fetch.data)
    save = DataStorage()
    save.save_raw_data(fetch.data)
    save.save_processed_data(clean.processed_data)
    save.create_tables()
    save.save_to_database(clean.processed_data)
 

if __name__ == "__main__":
    main()