import psycopg2
from psycopg2 import sql
from config import DB_CONFIG

def create_tables():
    """Create tables in the PostgreSQL database."""

    create_tables = '''
        DROP TABLE IF EXISTS Companies;

        CREATE TABLE IF NOT EXISTS Companies(
            company_id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            total_shares BIGINT
        );

        DROP TABLE IF EXISTS Stocks;
            
        CREATE TABLE IF NOT EXISTS Stocks(
            stock_id SERIAL PRIMARY KEY,
            company_id INT REFERENCES Companies(company_id) ON DELETE CASCADE,
            ticker_symbol VARCHAR(10) NOT NULL,
            date DATE NOT NULL,
            open_price DECIMAL(10,2),
            close_price DECIMAL(10,2),
            volume BIGINT,
            UNIQUE (company_id, date)
        );

        CREATE TABLE IF NOT EXISTS Balance_sheets(
            statement_id SERIAL PRIMARY KEY,
            company_id INT REFERENCES Companies(company_id) ON DELETE CASCADE,
            date DATE NOT NULL,
            current_assets DECIMAL(15,2),
            non_current_assets DECIMAL(15,2),
            current_liabilities DECIMAL(15,2),
            non_current_liabilities DECIMAL(15,2),
            equity DECIMAL(15,2)
        );

        CREATE TABLE IF NOT EXISTS Income_statements(
            statement_id SERIAL PRIMARY KEY,
            company_id INT REFERENCES Companies(company_id) ON DELETE CASCADE,
            date DATE NOT NULL,
            revenue DECIMAL(15,2),
            gross_profit DECIMAL(15,2),
            gross_margin DECIMAL(5,2),
            operating_income DECIMAL(15,2),
            operating_margin DECIMAL (5,2),
            net_income DECIMAL(15,2),
            earnings_per_share DECIMAL(15,2),
            price_to_earnings DECIMAL(10,2),
            interest_expense DECIMAL(15,2),
            depreciation_amortization DECIMAL(15,2)
        );

        CREATE TABLE IF NOT EXISTS Cash_flows(
            statement_id SERIAL PRIMARY KEY,
            company_id INT REFERENCES Companies(company_id) ON DELETE CASCADE,
            date DATE NOT NULL,
            operating_cash_flow DECIMAL(15,2),
            capital_expenditures DECIMAL(15,2),
            cash_from_investing DECIMAL(15,2),
            cash_from_financing DECIMAL(15,2),
            debt_repayments DECIMAL(15,2),
            dividend_payments DECIMAL(15,2)
        );

        '''

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute(create_tables)
        
        # Commit and close
        conn.commit()
        print("Tables created successfully!")

        cur.close()
        conn.close()
    
    except Exception as e:
        print(f"Error creating tables: {e}")

if __name__ == "__main__":
    create_tables() 