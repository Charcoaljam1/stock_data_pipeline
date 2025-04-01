from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import my classes
from scripts.data_ingestion import StockFetcher
from scripts.data_transformation import DataCleaner
from scripts.data_saving import DataStorage

# Define the default arguments for the DAG
default_args = {
    'owner': 'user',
    'start_date': datetime(2024, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

#Define the DAG
dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    schedule='@quarterly',
    catchup=False
)

symbols = ['AAPL', 'MSFT', 'IBM', 'TSLA']
data_types = ['info', 'daily', 'cash', 'income', 'balance']
fetcher = StockFetcher(symbols, data_types)  
cleaner = DataCleaner(fetcher.data) 
storer = DataStorage()  

# Define the tasks using PythonOperator
fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetcher.get_data,  # Calls get_data method of StockFetcher
    dag=dag
)

save_raw_data_task = PythonOperator(
    task_id='save_raw_data',
    python_callable=storer.save_raw_data,  # Calls save_raw_data method of DataStorage
    op_args=[fetcher.data],  # Passes fetcher's data as an argument
    dag=dag
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=cleaner.transform,  # Calls transform method of DataCleaner
    op_args=[fetcher.data],  # Passes fetcher's data to DataCleaner
    dag=dag
)

save_processed_data_task = PythonOperator(
    task_id='save_processed_data',
    python_callable=storer.save_processed_data,  # Calls save_processed_data method of DataStorage
    op_args=[cleaner.processed_data],  # Passes cleaned data to save_processed_data
    dag=dag
)

create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=storer.create_tables,  # Calls create_tables method of DataStorage
    dag=dag
)

save_to_db_task = PythonOperator(
    task_id='save_to_db',
    python_callable=storer.save_to_database,  # Calls save_to_database method of DataStorage
    op_args=[cleaner.processed_data],  # Passes cleaned data to save_to_database
    dag=dag
)

# Set task dependencies (order of execution)
fetch_task >> save_raw_data_task >> clean_task >> save_processed_data_task >> create_tables_task >> save_to_db_task