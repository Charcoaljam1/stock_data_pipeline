# Stock Market Data Pipeline

## Project Overview
This project implements a streamlined data pipeline to fetch stock market data from an API, transform it, and load it into a PostgreSQL database. The pipeline is designed to be modular, ensuring maintainability and scalability. It includes robust error handling and logging to ensure smooth operation.

## Features
- **Automated Data Ingestion:** Fetch stock market data from an API endpoint.
- **Data Transformation:** Clean and preprocess raw data for storage and analysis.
- **Database Loading:** Store transformed data in a PostgreSQL database.

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
│
│
├── scripts/                   # Core scripts for the pipeline
│   ├── __init__.py            # Marks this directory as a package
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
```

## Setup & Installation
### **1. Install Dependencies**
Ensure you have Python 3.12+ installed. Then, install the required packages:
```bash
pip install -r requirements.txt
```

### **2. Configure Environment**
Create a `.env` file or update `config.py` with the following:
- API key for the stock market data provider.
- PostgreSQL database credentials.

### **3. Run the Pipeline**
Execute the pipeline by running:
```bash
python main.py
```

## How It Works
1. **Data Ingestion:** Fetch stock data from the API endpoint.
2. **Data Transformation:** Clean and preprocess the data for consistency.
3. **Database Loading:** Insert the transformed data into a PostgreSQL database.

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