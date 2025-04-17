import json
from config.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
import os
from utils.logging.logger import log_info
from utils.exceptions.exception_handling import handle_exceptions
from config.config import DB_CONFIG
from sqlalchemy import create_engine, Column, Integer, String, Date, DECIMAL, BigInteger, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


# Define base for ORM models
Base = declarative_base()

DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

class Company(Base):
    __tablename__ = "companies"
    
    company_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)
    total_shares = Column(BigInteger)
    ticker_symbol = Column(String(10), nullable=False)
    exchange = Column(String(255))
    currency = Column(String(25))
    country = Column(String(255))
    sector = Column(String(255))

    # Relationships
    stocks = relationship("Stock", back_populates="company", cascade="all, delete-orphan")
    balance_sheets = relationship("BalanceSheet", back_populates="company", cascade="all, delete-orphan")
    income_statements = relationship("IncomeStatement", back_populates="company", cascade="all, delete-orphan")
    cash_flows = relationship("CashFlow", back_populates="company", cascade="all, delete-orphan")


class Stock(Base):
    __tablename__ = "stocks"
    
    stock_id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.company_id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    open = Column(DECIMAL(15,2))
    close = Column(DECIMAL(15,2))
    volume = Column(BigInteger)

    __table_args__ = (UniqueConstraint("company_id", "date", name="uq_stock_date"),)

    company = relationship("Company", back_populates="stocks")


class BalanceSheet(Base):
    __tablename__ = "balance_sheets"

    statement_id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.company_id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    total_current_assets = Column(DECIMAL(20,2))
    total_non_current_assets = Column(DECIMAL(20,2))
    total_current_liabilities = Column(DECIMAL(20,2))
    total_non_current_liabilities = Column(DECIMAL(20,2))
    total_shareholder_equity = Column(DECIMAL(20,2))
    short_term_debt = Column(DECIMAL(20,2))
    long_term_debt = Column(DECIMAL(20,2))
    retained_earnings = Column(DECIMAL(20,2))
    cash_and_cash_equivalents = Column(DECIMAL(20,2))

    __table_args__ = (UniqueConstraint("company_id", "date", name="uq_balance_sheet_date"),)

    company = relationship("Company", back_populates="balance_sheets")


class IncomeStatement(Base):
    __tablename__ = "income_statements"

    statement_id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.company_id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    total_revenue = Column(DECIMAL(20,2))
    gross_profit = Column(DECIMAL(20,2))
    operating_income = Column(DECIMAL(20,2))
    net_income = Column(DECIMAL(20,2))
    interest_and_debt_expense = Column(DECIMAL(20,2))
    ebit = Column(DECIMAL(20,2))
    gross_margin = Column(DECIMAL(20,2))
    operating_margin = Column(DECIMAL(20,2))
    ebit_margin = Column(DECIMAL(20,2))

    __table_args__ = (UniqueConstraint("company_id", "date", name="uq_income_statement_date"),)

    company = relationship("Company", back_populates="income_statements")


class CashFlow(Base):
    __tablename__ = "cash_flows"

    statement_id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.company_id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    operating_cash_flow = Column(DECIMAL(20,2))
    capital_expenditures = Column(DECIMAL(20,2))
    cashflow_from_investment = Column(DECIMAL(20,2))
    cashflow_from_financing = Column(DECIMAL(20,2))
    dividend_payout = Column(DECIMAL(20,2))
    debt_repayments = Column(DECIMAL(20,2))
    free_cashflow = Column(DECIMAL(20,2))

    __table_args__ = (UniqueConstraint("company_id", "date", name="uq_cash_flow_date"),)

    company = relationship("Company", back_populates="cash_flows")



class DataStorage:
    PROCESSED_DATA_DIR = PROCESSED_DATA_DIR
    RAW_DATA_DIR = RAW_DATA_DIR 

    def __init__(self):
        os.makedirs(self.RAW_DATA_DIR, exist_ok=True)
        os.makedirs(self.PROCESSED_DATA_DIR, exist_ok=True)
        self.engine = create_engine(DATABASE_URL)
        self.Session = sessionmaker(bind=self.engine)

    @log_info
    @handle_exceptions
    def save_raw_data(self, data: dict):
        """Saves raw data to JSON files."""
        for symbols, symbol_data in data.items():
            for keys, values in symbol_data.items():
                file_path = os.path.join(self.RAW_DATA_DIR, f"{symbols}_{keys}.json")
                self._save_json(file_path, values)
    @log_info
    @handle_exceptions
    def save_processed_data(self, data: dict):
        """Saves processed data to CSV files."""
        for symbols, symbol_data in data.items():
            for keys, values in symbol_data.items():
                file_path = os.path.join(self.PROCESSED_DATA_DIR, f"{symbols}_{keys}.csv")
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                values.to_csv(file_path, index=False)



    @log_info
    @handle_exceptions
    def create_tables(self):
        """Create tables in the database."""
        Base.metadata.create_all(self.engine)

    @log_info
    @handle_exceptions
    def save_to_database(self, data_dict):
        """Load processed stock data from dictionary into the PostgreSQL database."""
        session = self.Session()

        TABLE_MAPPING = {
        "daily": Stock,
        "income": IncomeStatement,
        "balance": BalanceSheet,
        "cash": CashFlow,
        "info": Company
        }
        
        try:
            for symbol, data in data_dict.items():
                print(f"Processing data for: {symbol}")

                # Process Company Info first to get company_id
                if "info" in data:
                    company_df = data["info"]
                    company_data = company_df.to_dict(orient="records")[0]  # Convert first row to dict

                    # Insert company and get company_id 
                    existing_company = session.query(Company).filter_by(ticker_symbol=symbol).first()
                    if existing_company:
                        company_id = existing_company.company_id
                    else:
                        new_company = Company(**company_data)
                        session.add(new_company)
                        session.flush()  # Get company_id before commit
                        company_id = new_company.company_id

                # Insert other data types using bulk inserts
                for key, df in data.items():
                    if key == "info":
                        continue 

                    table = TABLE_MAPPING.get(key)
                    if table is None:
                        print(f"Skipping unrecognized key: {key}")
                        continue

                    # Add company_id to the DataFrame
                    df["company_id"] = company_id
                    
                    # Convert DataFrame to list of dictionaries
                    records = df.to_dict(orient="records")

                    # Perform bulk insert
                    session.bulk_insert_mappings(table, records)
                    session.flush()

                session.commit()
                print(f"{symbol} data successfully loaded into database.")

        except Exception as e:
            session.rollback()
            print(f"Error loading data: {e}")
        finally:
            session.close()


    def _save_json(self, file_path: str, data: dict):
        """Helper function to save data as JSON."""
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)


