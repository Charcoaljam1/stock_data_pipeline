import psycopg2
from psycopg2 import sql

from utils.exceptions.exception_handling import handle_exceptions
from utils.logging.logger import log_info
from scripts.database_connection import get_db_connection





@handle_exceptions
@log_info
def create_tables():
    """Create tables in the PostgreSQL database."""

    create_tables = '''
            CREATE TABLE IF NOT EXISTS companies(
            company_id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            total_shares BIGINT,
            ticker_symbol VARCHAR(10) NOT NULL,
            exchange VARCHAR(255),
            currency VARCHAR(25),
            country VARCHAR(255),
            sector VARCHAR(255)
        );

            
        CREATE TABLE IF NOT EXISTS stocks(
            stock_id SERIAL PRIMARY KEY,
            company_id INT REFERENCES Companies(company_id) ON DELETE CASCADE,
            date DATE NOT NULL,
            open_price DECIMAL(10,2),
            close_price DECIMAL(10,2),
            volume BIGINT,
            UNIQUE (company_id, date)
        );

        CREATE TABLE IF NOT EXISTS balance_sheets(
            statement_id SERIAL PRIMARY KEY,
            company_id INT REFERENCES Companies(company_id) ON DELETE CASCADE,
            date DATE NOT NULL,
            current_assets DECIMAL(15,2),
            non_current_assets DECIMAL(15,2),
            current_liabilities DECIMAL(15,2),
            non_current_liabilities DECIMAL(15,2),
            equity DECIMAL(15,2),
            short_term_debt DECIMAL(15,2),
            long_term_debt DECIMAL(15,2),
            retained_earnings DECIMAL(15,2),
            cash_and_cash_equivalents  DECIMAL(15,2),
            UNIQUE (company_id, date)
        );

        CREATE TABLE IF NOT EXISTS income_statements(
            statement_id SERIAL PRIMARY KEY,
            company_id INT REFERENCES Companies(company_id) ON DELETE CASCADE,
            date DATE NOT NULL,
            revenue DECIMAL(15,2),
            gross_profit DECIMAL(15,2),
            operating_income DECIMAL(15,2),
            net_income DECIMAL(15,2),
            interest_expense DECIMAL(15,2),
            ebit DECIMAL(15,2),
            gross_margin DECIMAL(5,2),
            operating_margin DECIMAL (5,2),
            ebit_margin DECIMAL (5,2),
            UNIQUE (company_id, date)
        );

        CREATE TABLE IF NOT EXISTS cash_flows(
            statement_id SERIAL PRIMARY KEY,
            company_id INT REFERENCES Companies(company_id) ON DELETE CASCADE,
            date DATE NOT NULL,
            operating_cash_flow DECIMAL(15,2),
            capital_expenditures DECIMAL(15,2),
            cash_from_investing DECIMAL(15,2),
            cash_from_financing DECIMAL(15,2),
            dividend_payments DECIMAL(15,2),
            debt_repayments DECIMAL(15,2),
            free_cash_flow DECIMAL (15,2),
            UNIQUE (company_id, date)   
        );

        '''

    try:
        # Connect to PostgreSQL
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                logger.info("Creating tables...")
                # Execute the query
                cur.execute(create_tables)
        
                # Commit and close
                conn.commit()
                logger.info("Schema created successfully!")
    
    except Exception as e:
        logger.error(f"Error creating tables: {e}")

if __name__ == "__main__":
    create_tables() 