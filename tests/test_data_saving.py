import pytest
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from scripts.data_saving import Base, DataStorage, Stock, IncomeStatement, BalanceSheet, CashFlow, Company 
from unittest.mock import patch, MagicMock


@pytest.fixture
def sample_data():
    return {
        "AAPL": {
            "daily": {"date": "2024-03-31", "price": 150.5},
            "weekly": {"date": "2024-03-30", "price": 148.2}
        }
    }

@patch("os.path.join")
@patch("scripts.data_saving.DataStorage._save_json")  
def test_save_raw_data(mock_save_json, mock_path_join, sample_data):
    mock_path_join.side_effect = lambda *args: "/".join(args)  # Simulate path joining
    
    obj = DataStorage()
    obj.save_raw_data(sample_data)

    expected_calls = [
        ("data/raw_data/AAPL_daily.json", sample_data["AAPL"]["daily"]),
        ("data/raw_data/AAPL_weekly.json", sample_data["AAPL"]["weekly"]),
    ]

    # Ensure _save_json is called with the correct arguments
    for call, (expected_path, expected_data) in zip(mock_save_json.call_args_list, expected_calls):
        assert call[0][0] == expected_path  # File path
        assert call[0][1] == expected_data  # Data content


@patch("os.makedirs")
@patch("os.path.join")
@patch("pandas.DataFrame.to_csv")
def test_save_processed_data(mock_to_csv, mock_path_join, mock_makedirs, sample_data):
    mock_path_join.side_effect = lambda *args: "/".join(args)  # Simulate path joining
    mock_df = MagicMock()
    mock_to_csv.return_value = None

    # Convert sample_data values to mock DataFrame
    processed_data = {k: {k2: mock_df for k2 in v} for k, v in sample_data.items()}

    obj = DataStorage()  # Replace with actual class instantiation
    obj.save_processed_data(processed_data)

    expected_calls = [
        ("/processed_data/AAPL_daily.csv",),
        ("/processed_data/AAPL_weekly.csv",)
    ]

    # Ensure to_csv is called correctly
    for call, (expected_path,) in zip(mock_to_csv.call_args_list, expected_calls):
        assert call[0][0] == expected_path  # File path
        assert "index" in call[1] and call[1]["index"] is False  # Ensure index=False


# Use an in-memory SQLite database for testing
TEST_DATABASE_URL = "sqlite:///:memory:"

@pytest.fixture(scope="function")
def test_db():
    """Creates a new database session for testing."""
    engine = create_engine(TEST_DATABASE_URL)
    Base.metadata.create_all(engine)  # Create tables in test DB
    TestingSessionLocal = sessionmaker(bind=engine)
    session = TestingSessionLocal()

    try:
        yield session  # Provide the session to the test
    finally:
        session.rollback()
        session.close()  # Cleanup
        Base.metadata.drop_all(engine)  # Reset database after test

@pytest.fixture
def date():
    return   datetime.strptime("2025-02-08", "%Y-%m-%d")

@pytest.fixture
def company(test_db):
    company = Company(name="Microsoft", ticker_symbol='MSFT')
    test_db.add(company)
    test_db.commit()
    return company

def test_create_stock(test_db, date, company):
    """Test that an stock can be created and retrieved."""
    # Create a stock
    stock = Stock(date=date, volume=30004,company_id=company.company_id)
    test_db.add(stock)
    test_db.commit()


    # Retrieve stock
    retrieved_stock = (
    test_db.query(Stock)
    .filter(func.strftime('%Y-%m-%d', Stock.date) == date.strftime('%Y-%m-%d'))
    .first()
    )

    test_db.refresh(retrieved_stock)  # Ensures fresh data from DB


    assert retrieved_stock is not None  # Ensure it exists
    assert retrieved_stock.volume == 30004
    assert retrieved_stock.company_id == stock.company_id
    assert retrieved_stock.company_id == company.company_id

def test_balance_sheet_company_relationship(test_db, date, company):
    """Test that an stock is correctly linked to their stock."""
    bs = BalanceSheet(date=date, current_assets=300004, non_current_assets=2339984,company_id=company.company_id)
    test_db.add(bs)
    test_db.commit()
    
    # Fetch from DB
    retrieved_bs = test_db.query(BalanceSheet).filter_by(company_id=company.company_id).first()
    test_db.refresh(retrieved_bs)  # Ensures fresh data from DB
    
    assert retrieved_bs is not None  # Entry should exist
    assert retrieved_bs.current_assets == 300004
    assert retrieved_bs.non_current_assets == 2339984
    assert retrieved_bs.company_id == company.company_id  # Ensures correct linkage
