import psycopg2
from scripts.database_connection import get_db_connection


def load_data(data, type, symbol, nested=False):


def load_simple_stock(data, symbol):
    
    insert_query = '''
    INSERT INTO stocks (company_id, date, open_price,close_price,volume)
    VALUES (%s,%s,%s,%s,%s);
    '''

    try:
        conn = get_db_connection()
    
        cur = conn.cursor()
        cur.execute('''SELECT company_id FROM companies WHERE ticker_symbol = %s''', (symbol,))
        company_id = cur.fetchall()

        cur.close()
        conn.close()

    except Exception as e:
        print(f'Error: {e}')

    company_id = company_id[0]

    values = [row for row in data.itertuples(index=True, name=None)]

    values_with_id = [(company_id, *row) for row in values]

    try: 
        conn = get_db_connection()
        cur = conn.cursor()

        cur.executemany(insert_query,values_with_id)
        conn.commit() 

        print(f'{symbol} stock data inserted successfully')

        cur.close()
        conn.close()

    except Exception as e:
        print(f'Error: {e}')
        conn.rollback()