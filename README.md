# Stock Market Data Pipeline

## Project Overview
This project implements a streamlined data pipeline to fetch stock market data from an API, transform it, and load it into a PostgreSQL database. The pipeline is designed to be modular, ensuring maintainability and scalability. It includes robust error handling, logging, and validation to ensure smooth operation and data integrity.

## Features
- **Data Ingestion:** Fetch stock market data from an API endpoint.
- **Data Transformation:** Clean and preprocess raw data for storage and analysis.
- **Database Loading:** Store transformed data in a PostgreSQL database.
- **Custom Logging:** Comprehensive logging for debugging and monitoring pipeline operations.
- **Validation:** Input and data validation to ensure data integrity and consistency.

## Folder Structure
```
stock_pipeline_project/
│── main.py                    # Entry point of the project
│── README.md                  # Project documentation
│── requirements.txt           # List of required Python packages
│── setup.py                   # Optional: For packaging the project
│── .env                       # Environment variables
│── .gitignore                 # Git ignore file
│── .python-version            # Python version file
│
├── config/                    # Configuration files
│   ├── config.py              # Stores API keys, database settings & configurations
│
├── data/                      # Data storage
│   ├── raw_data/              # Raw data files
│   ├── processed_data/        # Processed data files
│
├── logs/                      # Log files
│   ├── app.log                # Application logs
│   ├── data_ingestion.log     # Logs for data ingestion
│   ├── data_transformation.log # Logs for data transformation
│   ├── database_operations.log # Logs for database operations
│
├── scripts/                   # Core scripts for the pipeline
│   ├── __init__.py            # Marks this directory as a package
│   ├── user_input.py          # Handles user input for stock symbols
│   ├── data_ingestion.py      # Fetches stock data from the API
│   ├── data_transformation.py # Cleans, formats & processes stock data
│   ├── data_loading_oltp.py   # Loads data into the database
│   ├── database_connection.py # Manages database connections
│   ├── database_setup.py      # Handles database schema creation
│   ├── logger.py              # Custom logging functions
│
├── tests/                     # Unit and integration tests
│   ├── test_data_ingestion.py # Tests for data ingestion
│   ├── test_data_transformation.py # Tests for data transformation
│   ├── test_database.py       # Tests for database-related functionality
│
├── utils/                     # Utility functions
│   ├── __init__.py            # Marks this directory as a package
│   ├── data_utils.py          # Reusable transformation & preprocessing functions
│   ├── validation/            # Validation utilities
│       ├── raw_data_validation.py # Validates raw data
│       ├── processed_data_validation.py # Validates processed data
│   ├── cleaning/              # Data cleaning utilities
│       ├── data_cleaners.py   # Functions for cleaning and formatting data
```

## Setup & Installation

### **1. Install Dependencies**
Ensure you have Python 3.12+ installed. Then, install the required packages:
```bash
pip install -r requirements.txt
```

### **2. Configure Environment**
Create a `.env` file or update `config.py` with the following:
- **API Key:** API key for the stock market data provider.
- **Database Credentials:** PostgreSQL database credentials (host, port, username, password, database name).
- **Data Directories:** Paths for raw and processed data directories.

### **3. Run the Pipeline**
Execute the pipeline by running:
```bash
python main.py
```

## How It Works
1. **Data Ingestion:** Fetch stock data from the API endpoint using the provided stock symbols.
2. **Data Transformation:** Clean and preprocess the data to ensure consistency and usability.
3. **Data Validation:** Validate raw and processed data to ensure integrity and compliance with expected formats.
4. **Data Saving:** Save raw data to JSON files and processed data to CSV files for backup and analysis.
5. **Database Loading:** Insert the transformed data into a PostgreSQL database for long-term storage and querying.
6. **Logging:** Log all operations, including errors, warnings, and informational messages, to dedicated log files.

## Requirements
- Python 3.12+
- PostgreSQL
- API Key for stock data provider

## Future Improvements
- Add support for additional data sources.
- Implement real-time data updates.
- Optimize database performance for large datasets.

## License
This project is open-source and available for modification and contribution.