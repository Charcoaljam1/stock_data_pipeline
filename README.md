# Stock Market Data Pipeline

## Project Overview
The **Stock Market Data Pipeline** is a modular data pipeline designed to fetch, transform, validate, and store stock market data. It integrates with a stock market API to retrieve data, preprocesses it for analysis, and stores it in a PostgreSQL database. The pipeline is equipped with error handling, logging, and validation mechanisms to ensure data integrity and operational reliability.

## Features
- **Data Ingestion:** Fetch stock market data from an API endpoint.
- **Data Transformation:** Clean and preprocess raw data for storage and analysis.
- **Data Validation:** Validate raw and processed data to ensure integrity and compliance with expected formats.
- **Database Loading:** Store transformed data in a PostgreSQL database for long-term storage and querying.
- **Custom Logging:** Logging for debugging and monitoring pipeline operations.
- **Modular Design:** Easily extendable and maintainable codebase.
- **Error Handling:** Mechanisms to handle and log errors during pipeline execution.

---

## Folder Structure
```
stock_pipeline_project/
│── main.py                    # Entry point of the project
│── README.md                  # Project documentation
│── requirements.txt           # List of required Python packages
│── .env                       # Environment variables
│── .gitignore                 # Git ignore file
│
├── config/                    # Configuration files
│   ├── config.py              # Stores API keys, database settings & configurations
│
├── data/                      # Data storage
│   ├── raw_data/              # Raw data files
│   ├── processed_data/        # Processed data files
│
├── logs/                      # Log files
│
├── scripts/                   # Core scripts for the pipeline
│   ├── __init__.py            # Marks this directory as a package
│   ├── data_ingestion.py      # Fetches stock data from the API
│   ├── data_transformation.py # Cleans, formats & processes stock data
│   ├── data_saving.py         # Handles data storage
│
├── tests/                     # Unit and integration tests
│   ├── test_data_ingestion.py # Tests for data ingestion
│   ├── test_data_transformation.py # Unit tests for data transformation logic
│   ├── test_saving.py         # Unit tests for data storage functionality
│
├── utils/                     # Utility modules supporting the pipeline
│   ├── __init__.py            # Marks the utils directory as a Python package
│   ├── data_utils.py          # Helper functions for data manipulation and processing
│   ├── cleaning/              # Subpackage for data cleaning utilities
│   │   ├── __init__.py        # Marks the cleaning directory as a Python package
│   │   ├── data_cleaners.py   # Functions for cleaning and preprocessing raw data
│   ├── exceptions/            # Subpackage for custom exception handling
│   │   ├── __init__.py        # Marks the exceptions directory as a Python package
│   │   ├── exception_handling.py # Defines custom exceptions and error-handling mechanisms
│   ├── fetching/              # Subpackage for API data fetching utilities
│   │   ├── __init__.py        # Marks the fetching directory as a Python package
│   │   ├── api_utils.py       # Functions for interacting with external APIs
│   ├── logging/               # Subpackage for logging utilities
│   │   ├── __init__.py        # Marks the logging directory as a Python package
│   │   ├── logger.py          # Implements logging for debugging and monitoring
│   ├── validation/            # Subpackage for data validation utilities
│   │   ├── __init__.py        # Marks the validation directory as a Python package
│   │   ├── raw_data_validation.py # Functions for validating raw input data
│   │   ├── processed_data_validation.py # Functions for validating processed data
```

---

## Setup & Installation

### **1. Prerequisites**
- Python 3.12+
- PostgreSQL installed and running
- API key for the stock market data provider

### **2. Install Dependencies**
Clone the repository and install the required Python packages:
```bash
git clone https://github.com/Charcoaljam1/stock_data_pipeline.git
cd stock_data_pipeline
pip install -r requirements.txt
```

### **3. Configure Environment**
Create a `.env` file or update `config.py` with the following:
- **API Key:** API key for the stock market data provider (Alpha Vantage).
- **Database Credentials:** PostgreSQL database credentials (host, port, username, password, database name).
- **Data Directories:** Paths for raw and processed data directories.

Example `.env` file:
```python
API_KEY=your_api_key_here
DB_HOST=localhost
DB_PORT=5432
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=stock_data
RAW_DATA_DIR=data/raw_data
PROCESSED_DATA_DIR=data/processed_data
```

### **4. Run the Pipeline**
Execute the pipeline by running:
```bash
python main.py
```

---

## How It Works

1. **Data Ingestion:** Fetch stock data from the API endpoint using the provided stock symbols.
2. **Data Transformation:** Clean and preprocess the data to ensure consistency and usability.
3. **Data Validation:** Validate raw and processed data to ensure integrity and compliance with expected formats.
4. **Data Saving:** Save raw data to JSON files and processed data to CSV files for backup and analysis.
5. **Database Loading:** Insert the transformed data into a PostgreSQL database for long-term storage and querying.
6. **Logging:** Log all operations, including errors, warnings, and informational messages, to dedicated log files.

---

## Requirements
- Python 3.12+
- PostgreSQL
- API Key for stock data provider

---

## Logging
The pipeline generates logs for debugging and monitoring:
- **Data Ingestion Logs:** Logs specific to data fetching operations.
- **Data Transformation Logs:** Logs for data cleaning and preprocessing.
- **Database Operations Logs:** Logs for database-related activities.

Logs are stored in the `logs/` directory.

---

## Testing
Unit and integration tests are located in the `tests/` directory. To run the tests:
```bash
pytest tests/
```

---

## Future Improvements
- Add support for additional data sources.
- Implement real-time data updates.
- Optimize database performance for large datasets.
- Add support for cloud-based storage solutions.
- Enhance the pipeline with machine learning-based data analysis.

---

## Contributing
Contributions are welcome! Please follow these steps:
1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Commit your changes and push them to your fork.
4. Submit a pull request with a detailed description of your changes.

---

## License
This project is open-source and available under the MIT License. See the `LICENSE` file for more details.

---

## Contact
For questions or support, please contact:
- **Author:** Charcoal Jam
- **GitHub:** [Charcoaljam1](https://github.com/Charcoaljam1)