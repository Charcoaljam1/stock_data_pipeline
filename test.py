from scripts.database_setup import create_tables
from scripts.database_connection import get_db_connection
from scripts.data_ingestion import get_data
from scripts.data_transformation import format_data

import pandas as pd

#create_tables()

#data = get_data('daily','MSFT')

stock_data = pd.read_csv('data/processed_data/MSFT_stock.csv')
# cash_data = format_data(get_data('cash','MSFT'),'cash')
# balance_data = format_data(get_data('balance','MSFT'),'balance')
# income_data = format_data(get_data('income','MSFT'),'income')

values = [row for row in stock_data.itertuples(index=False, name=None)]
stock_data_to_insert = values = [tuple(row) for row in stock_data.itertuples(index=False, name=None)]
# cash_data_to_insert = values = [tuple(row) for row in cash_data.itertuples(index=False, name=None)]
# balance_data_to_insert = values = [tuple(row) for row in balance_data.itertuples(index=False, name=None)]
# income_data_to_insert = values = [tuple(row) for row in income_data.itertuples(index=False, name=None)]

print(values)

try:
    conn = get_db_connection()
   
    cur = conn.cursor()
    cur.execute('''SELECT * FROM companies''')
    table = cur.fetchall()
    print(table)

    cur.close()
    conn.close()

except Exception as e:
    print(f'Error: {e}')

company_id = table[0][0]
stock_symbol = 'MSFT'
stock_data_with_company_id = [(company_id, stock_symbol, *row) for row in stock_data_to_insert]
# cash_data_with_company_id = [(company_id, *row) for row in cash_data_to_insert]
# balance_data_with_company_id = [(company_id, *row) for row in balance_data_to_insert]
# income_data_with_company_id = [(company_id, *row) for row in income_data_to_insert]


try:
    conn = get_db_connection()
    cur = conn.cursor()

    # Define the SQL query 
    insert_query = '''
    INSERT INTO stocks (company_id, ticker_symbol, date, open_price,close_price,volume)
    VALUES (%s,%s,%s,%s,%s,%s);
    '''

    # Execute the query with data
    # cur.executemany(insert_query,stock_data_with_company_id)

    # # Commit changes
    # conn.commit()

    print("Data inserted successfully!")

    cur.execute('''SELECT * FROM stocks LIMIT 10''')
    tables = cur.fetchall()
    print(tables)

    # Close cursor and connection
    cur.close()
    conn.close()

except Exception as e:
    print(f'Error: {e}')
    conn.rollback()


