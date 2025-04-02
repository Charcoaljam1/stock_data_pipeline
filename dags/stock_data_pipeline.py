from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import my classes
from scripts.data_ingestion import StockFetcher
from scripts.data_transformation import DataCleaner
from scripts.data_saving import DataStorage

# Define default arguments for the DAG
default_args = {
    'owner': 'user',
    'start_date': datetime(2024, 4, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    schedule_interval='@quarterly',
    catchup=False
)

# Define the symbols and data types
symbols = ['AAPL', 'MSFT', 'IBM', 'TSLA']
data_types = ['info', 'daily', 'cash', 'income', 'balance']

# Task Functions
def fetch_data(**kwargs):
    fetcher = StockFetcher(symbols, data_types)
    data = fetcher.get_data()
    kwargs['ti'].xcom_push(key='raw_data', value=data)

def save_raw_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='fetch_data', key='raw_data')
    storer = DataStorage()
    storer.save_raw_data(raw_data)

def clean_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='fetch_data', key='raw_data')
    cleaner = DataCleaner(raw_data)
    cleaned_data = cleaner.transform()
    ti.xcom_push(key='cleaned_data', value=cleaned_data)

def save_processed_data(**kwargs):
    ti = kwargs['ti']
    cleaned_data = ti.xcom_pull(task_ids='clean_data', key='cleaned_data')
    storer = DataStorage()
    storer.save_processed_data(cleaned_data)

def create_tables():
    storer = DataStorage()
    storer.create_tables()

def save_to_database(**kwargs):
    ti = kwargs['ti']
    cleaned_data = ti.xcom_pull(task_ids='clean_data', key='cleaned_data')
    storer = DataStorage()
    storer.save_to_database(cleaned_data)

# Define the tasks using PythonOperator
fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag
)

save_raw_data_task = PythonOperator(
    task_id='save_raw_data',
    python_callable=save_raw_data,
    provide_context=True,
    dag=dag
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    provide_context=True,
    dag=dag
)

save_processed_data_task = PythonOperator(
    task_id='save_processed_data',
    python_callable=save_processed_data,
    provide_context=True,
    dag=dag
)

create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag
)

save_to_db_task = PythonOperator(
    task_id='save_to_db',
    python_callable=save_to_database,
    provide_context=True,
    dag=dag
)

# Set task dependencies
fetch_task >> save_raw_data_task >> clean_task >> save_processed_data_task >> create_tables_task >> save_to_db_task
