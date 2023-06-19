# On-Premise to Cloud Testing Framework

The On-Premise to Cloud Testing Framework is designed to assist in the migration process of an on-premise database to a cloud-based database. The framework includes functionalities to compare data types between the on-premise and cloud databases, as well as create Pandas DataFrames from various data sources.

## Features

- Compare data types between an on-premise database (Oracle) and a specified table.
- Create Pandas DataFrames from an S3 Parquet file and a PySpark table.
- Create a Pandas DataFrame from an RDS (Relational Database Service) table.

## Prerequisites

Before using this testing framework, ensure the following dependencies are installed in your Python environment:

- `pandas`: To work with Pandas DataFrames.
- `psycopg2`: To connect to PostgreSQL databases (for creating Pandas DataFrame from RDS table).
- `cx_Oracle`: To connect to Oracle databases (for comparing data types between RDS and Oracle).
- `pyspark`: To work with PySpark DataFrames (for creating Pandas DataFrame from Parquet file and PySpark table).

## Getting Started

1. Clone the repository:

```bash
git clone https://github.com/your-username/on-premise-to-cloud-testing-framework.git
```

2. Install the required Python dependencies:

```bash
pip install -r requirements.txt
```

3. Update the necessary configuration details:

   - For comparing data types between RDS and Oracle:
     - Open `compare_datatypes.py` and update the host, port, service name, username, password, and table name variables with your specific values.

   - For creating Pandas DataFrame from S3 Parquet file and PySpark table:
     - Open `create_pandas_dataframe.py` and update the S3 Parquet file path and PySpark table name variables with your specific values.

   - For creating Pandas DataFrame from RDS table:
     - Open `create_pandas_dataframe.py` and update the RDS connection details and table name variables with your specific values.

4. Execute the desired testing functionality:

   - For comparing data types between RDS and Oracle:
     - Run `compare_datatypes.py` to compare data types and display any mismatches found.

   - For creating Pandas DataFrame from S3 Parquet file and PySpark table:
     - Run `create_pandas_dataframe.py` to create a Pandas DataFrame from the S3 Parquet file and PySpark table.

   - For creating Pandas DataFrame from RDS table:
     - Run `create_pandas_dataframe.py` to create a Pandas DataFrame from the RDS table.


Enjoy the testing framework and happy migration!
