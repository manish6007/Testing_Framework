# Import the necessary libraries
import pandas as pd
from bs4 import BeautifulSoup
import webbrowser
import os
import cx_Oracle
import boto3
import time
import psycopg2

cx_Oracle.init_oracle_client(
    lib_dir=r"C:\Users\manis\Downloads\instantclient-basic-windows.x64-21.10.0.0.0dbru\instantclient_21_10")


def get_redshift_table_schema(hostname, port, database, username, password, table_name):
    # Create a connection string
    conn_string = f"dbname={database} user={username} password={password} host={hostname} port={port}"

    # Establish a connection
    conn = psycopg2.connect(conn_string)

    # Get a cursor
    cursor = conn.cursor()

    # Get the column data types and field names
    query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"
    cursor.execute(query)

    # Fetch the results
    results = cursor.fetchall()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    # Convert the results to a dictionary
    schema_dict = {field: data_type for field, data_type in results}

    return schema_dict


def redshift_to_oracle_datatypes(redshift_field_datatypes):
    redshift_to_oracle_mapping = {
        'integer': 'number',
        'bigint': 'number',
        'smallint': 'number',
        'decimal': 'number',
        'numeric': 'number',
        'real': 'float',
        'double precision': 'float',
        'char': 'char',
        'character': 'char',
        'varchar': 'varchar2',
        'character varying': 'varchar2',
        'date': 'date',
        'timestamp': 'timestamp',
        'boolean': 'number'
    }


def get_oracle_columns_and_datatypes(user, password, host, port, service_name, table_name):
    """
    Connects to Oracle database and retrieves the columns of a specific table.

    Args:
        user (str): Oracle database username.
        password (str): Oracle database password.
        host (str): Oracle database host.
        port (int): Oracle database port.
        service_name (str): Oracle database service name.
        table_name (str): Name of the table to retrieve columns from.

    Returns:
        list: List of Oracle column descriptions.

    """
    oracle_connection = cx_Oracle.connect(f"{user}/{password}@{host}:{port}/{service_name}")
    oracle_cursor = oracle_connection.cursor()
    oracle_cursor.execute(f"SELECT * FROM {table_name} WHERE 1=0")
    oracle_columns = oracle_cursor.description
    oracle_cursor.close()
    oracle_connection.close()
    return oracle_columns


def oracle_to_redshift_datatype(oracle_datatype):
    datatype_mapping = {
        "<cx_Oracle.DbType DB_TYPE_BFILE>": "bfile",
        "<cx_Oracle.DbType DB_TYPE_BINARY_DOUBLE>": "binary_double",
        "<cx_Oracle.DbType DB_TYPE_BINARY_FLOAT>": "binary_float",
        "<cx_Oracle.DbType DB_TYPE_BLOB>": "blob",
        "<cx_Oracle.DbType DB_TYPE_CHAR>": "char",
        "<cx_Oracle.DbType DB_TYPE_CLOB>": "clob",
        "<cx_Oracle.DbType DB_TYPE_CURSOR>": "cursor",
        "<cx_Oracle.DbType DB_TYPE_DATE>": "date",
        "<cx_Oracle.DbType DB_TYPE_INTERVAL_DS>": "interval_ds",
        "<cx_Oracle.DbType DB_TYPE_JSON>": "json",
        "<cx_Oracle.DbType DB_TYPE_LONG>": "long",
        "<cx_Oracle.DbType DB_TYPE_LONG_RAW>": "long_raw",
        "<cx_Oracle.DbType DB_TYPE_NCHAR>": "nchar",
        "<cx_Oracle.DbType DB_TYPE_NCLOB>": "nclob",
        "<cx_Oracle.DbType DB_TYPE_NUMBER>": "number",
        "<cx_Oracle.DbType DB_TYPE_NVARCHAR>": "nvarchar",
        "<cx_Oracle.DbType DB_TYPE_OBJECT>": "object",
        "<cx_Oracle.DbType DB_TYPE_RAW>": "raw",
        "<cx_Oracle.DbType DB_TYPE_ROWID>": "rowid",
        "<cx_Oracle.DbType DB_TYPE_TIMESTAMP>": "timestamp",
        "<cx_Oracle.DbType DB_TYPE_TIMESTAMP_LTZ>": "timestamp_ltz",
        "<cx_Oracle.DbType DB_TYPE_VARCHAR>": "varchar2"
        # Add more mappings as needed
    }
    return datatype_mapping.get(oracle_datatype, 'unknown')


def convert_oracle_output_to_dict(oracle_output):
    converted_dict = {}
    for column_info in oracle_output:
        column_name = column_info[0]
        oracle_datatype = str(column_info[1])
        redshift_datatype = oracle_to_redshift_datatype(oracle_datatype)
        converted_dict[column_name.lower()] = redshift_datatype
    return converted_dict


def compare_oracle_redshift_datatypes(redshift_datatypes, oracle_output):
    # Convert Redshift datatypes to lowercase for comparison
    redshift_datatypes_lower = {col.lower(): dtype.lower() for col, dtype in redshift_datatypes.items()}

    # Convert Oracle output to lowercase dictionary
    oracle_dict = convert_oracle_output_to_dict(oracle_output)

    comparison_result = []
    for col, oracle_dtype in oracle_dict.items():
        redshift_dtype = redshift_datatypes_lower.get(col, None)
        match = oracle_dtype == redshift_dtype
        comparison_result.append((col, oracle_dtype, redshift_dtype, match))

    df = pd.DataFrame(comparison_result, columns=['column_name', 'oracle_dtype', 'redshift_dtype', 'match'])
    return df


# Function to generate HTML report
def generate_html_report(comparison_results_df, file_path):
    if comparison_results_df.size == 0:
        with open(file_path, "w", encoding="utf-8") as f:
            # Write the HTML content
            f.write(
                "<html><body><h1>Congratulations...No Difference Found between provided datasets. &#128522;</h1></body></html>")
            print("There is no difference in provided datasets.")
            open_html_file(file_path)
    else:
        # Save the differences to an HTML report
        comparison_results_df.to_html(file_path)

        # Modify the HTML report by adding a title tag if it exists or creating a new one
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                # Parse the HTML file using BeautifulSoup
                soup = BeautifulSoup(f, "html.parser")

                # Find the head tag in the parsed HTML
                head_tag = soup.find("head")

                # If head tag exists, add a title tag with the text "Comparison Report"
                if head_tag:
                    title_tag = soup.new_tag("title")
                    title_tag.string = "Comparison Report"
                    head_tag.append(title_tag)
                else:
                    # If head tag doesn't exist, create a new head tag and add the title tag
                    head_tag = soup.new_tag("head")
                    soup.insert(0, head_tag)
                    title_tag = soup.new_tag("title")
                    title_tag.string = "Comparison Report"
                    head_tag.append(title_tag)

                    # Write the modified HTML back to the file
                with open(file_path, "w") as f:
                    f.write(str(soup))
                print(f"Report has been generated at {file_path}")

    return open_html_file(file_path)


# Function to generate CSV report
def generate_csv_report(comparison_results_df, file_path):
    """
    Write a pandas DataFrame to a CSV file.

    Args:
        dataframe (pd.DataFrame): The DataFrame to be written to CSV.
        file_path (str): The file path along with the filename where the CSV file should be saved.
    """
    comparison_results_df.to_csv(file_path, index=False)
    print(f"Report has been generated at {file_path}")


# Function to generate EXCEL report
def generate_excel_report(comparison_results_df, filename):
    # Create a Pandas Excel writer using the filename
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')

    # Write the DataFrame to the Excel file
    comparison_results_df.to_excel(writer, index=True)

    # Save the Excel file
    writer.save()
    print(f"Report has been generated at {filename}")


# Function to open html report in the browser
def open_html_file(file_name):
    webbrowser.open(file_name)


# Function to compare the files having record count mismatch
def compare_files_for_count_diff(df1, df2):
    try:

        # Flag to check if there is a record count difference
        count_check = True

        # Check if there is a record count difference between the two tables
        if len(df1) != len(df2):
            print("There is a record count difference between the two files.")

            # Get the rows that are present in df_oracle but not in df_athena
            df_difference = pd.concat([df2, df1]).drop_duplicates(keep=False)
            return df_difference

        else:
            print("Record counts are matching between the two files.")
            return pd.DataFrame()

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return False


# Function to compare the data between the 2 files.
def compare_files(df1, df2):
    try:
        # Flag to check if there is a record count difference
        count_check = True

        df = compare_files_for_count_diff(df1, df2)
        # Check if there is a record count difference between the two tables
        if len(df) != 0:
            print("There is a record count difference between the two tables.")
            count_check = False
            return df

        # If the record count is the same, compare the data in the DataFrames
        if count_check:
            # Compare the data in df_athena and df_oracle
            df_diff = df1.compare(df2, align_axis=1)
            return df_diff
    except:
        print(f"unexpected error occurred")
        return False


def get_oracle_columns(user, password, host, port, service_name, table_name):
    """
    Connects to Oracle database and retrieves the columns of a specific table.

    Args:
        user (str): Oracle database username.
        password (str): Oracle database password.
        host (str): Oracle database host.
        port (int): Oracle database port.
        service_name (str): Oracle database service name.
        table_name (str): Name of the table to retrieve columns from.

    Returns:
        list: List of Oracle column descriptions.

    """
    oracle_connection = cx_Oracle.connect(f"{user}/{password}@{host}:{port}/{service_name}")
    oracle_cursor = oracle_connection.cursor()
    oracle_cursor.execute(f"SELECT * FROM {table_name} WHERE 1=0")
    oracle_columns = oracle_cursor.description
    oracle_cursor.close()
    oracle_connection.close()
    return oracle_columns


def get_athena_datatype(cx_oracle_datatype):
    """
    Returns the corresponding Athena datatype for the given cx_Oracle datatype.
    Args:
        cx_oracle_datatype: The cx_Oracle datatype.
    Returns:
        The corresponding Athena datatype.
    """
    if cx_oracle_datatype[1].name in ["DB_TYPE_VARCHAR", "DB_TYPE_CHAR", "DB_TYPE_NCHAR", "DB_TYPE_VARCHAR2",
                                      "DB_TYPE_NVARCHAR2", "DB_TYPE_LONG"]:
        return "STRING"
    elif cx_oracle_datatype[1].name in ["DB_TYPE_NUMBER"] and cx_oracle_datatype[5] == 0:
        return "INT"
    elif cx_oracle_datatype[1].name in ["DB_TYPE_NUMBER", "DB_TYPE_FLOAT", "DB_TYPE_DOUBLE", "DB_TYPE_BINARY_FLOAT",
                                        "DB_TYPE_BINARY_DOUBLE"]:
        return "DOUBLE"
    elif cx_oracle_datatype[1].name == "DB_TYPE_DATE":
        return "DATE"
    elif cx_oracle_datatype[1].name == "DB_TYPE_TIMESTAMP":
        return "TIMESTAMP"
    elif cx_oracle_datatype[1].name in ["DB_TYPE_BLOB", "DB_TYPE_CLOB", "DB_TYPE_NCLOB"]:
        return "STRING"  # Consider using "BINARY" for BLOB if necessary
    elif cx_oracle_datatype[1].name in ["DB_TYPE_RAW", "DB_TYPE_LONG_RAW"]:
        return "BINARY"
    elif cx_oracle_datatype[1].name in ["DB_TYPE_ROWID", "DB_TYPE_UROWID"]:
        return "STRING"
    elif cx_oracle_datatype[1].name == "DB_TYPE_BOOLEAN":
        return "BOOLEAN"
    elif cx_oracle_datatype[1].name == "DB_TYPE_INTERVAL":
        return "STRING"
    elif cx_oracle_datatype[1].name in ["DB_TYPE_XML", "DB_TYPE_GEOMETRY", "DB_TYPE_TOPO_GEOMETRY",
                                        "DB_TYPE_GEORASTER"]:
        return "STRING"
    else:
        raise ValueError("Unsupported cx_Oracle datatype: {}".format(cx_oracle_datatype.name))


def oracle_equivalent_athena_datatypes(username, password, host, port, service_name, table_name):
    cx_oracle_datatypes = get_oracle_columns(username, password, host, port, service_name, table_name)

    l = []
    for cx_oracle_datatype in cx_oracle_datatypes:
        athena_datatypes = {}
        athena_datatype = get_athena_datatype(cx_oracle_datatype)
        athena_datatypes['Name'] = cx_oracle_datatype[0].lower()
        athena_datatypes['Type'] = athena_datatype.lower()
        l.append(athena_datatypes)
    return l


def get_athena_columns(region, database, athena_table):
    """
    Connects to Athena and retrieves the columns of a specific table.

    Args:
        region (str): AWS region name.
        database (str): Athena database name.
        athena_table (str): Name of the Athena table to retrieve columns from.

    Returns:
        list: List of Athena column descriptions.

    """
    # Connect to Athena
    athena_client = boto3.client('athena', region_name=region)

    # Get the Athena table schema
    response = athena_client.get_table_metadata(
        CatalogName='AwsDataCatalog',
        DatabaseName=database,
        TableName=athena_table
    )

    athena_columns = response['TableMetadata']['Columns']
    return athena_columns


def write_list_of_dicts_to_text(data, file_path):
    """
    Write a list of dictionaries to a text file.

    Args:
        data (list[dict]): The list of dictionaries to be written.
        file_path (str): The file path along with the filename where the text file should be saved.
    """
    if len(data) == 0:
        with open(file_path, 'w', encoding="utf-8") as file:
            file.write("No difference found in Datatypes.")
    else:
        with open(file_path, 'w', encoding="utf-8") as file:
            for dictionary in data:
                for key, value in dictionary.items():
                    file.write(f"{key}: {value}\n")
                file.write("\n")


def compare_oracle_vs_athena_data_types(ora_user, ora_pwd, host, port, service_name, ora_tab_name, region, athena_db,
                                        athena_table, aws_access_key_id, aws_secret_access_key):
    """
    Compares the data types of columns between Athena and Oracle tables.

    Args:
        athena_columns (list): List of Athena column descriptions.
        oracle_columns (list): List of Oracle column descriptions.

    Returns:
        list: List of differences in column data types.

    """
    athena_columns = get_athena_columns(region, athena_db, athena_table)
    oracle_columns = oracle_equivalent_athena_datatypes(ora_user, ora_pwd, host, port, service_name, ora_tab_name)
    differences = []
    for athena_col, oracle_col in zip(athena_columns, oracle_columns):
        athena_col_name = athena_col['Name']
        athena_data_type = athena_col['Type']

        oracle_col_name = oracle_col['Name']
        oracle_data_type = oracle_col['Type']

        if athena_data_type != oracle_data_type:
            difference = {
                'column': athena_col_name,
                'athena_data_type': athena_data_type,
                'oracle_data_type': oracle_data_type
            }
            differences.append(difference)
    if differences:
        print('Differences found:')
        for diff in differences:
            print(diff)
    else:
        print('No differences found.')
    return differences


def read_oracle_table(username, password, host, port, service_name, table_name):
    """
    Reads data from an Oracle table and returns a pandas DataFrame.

    Args:
        username (str): The username to connect to the Oracle database.
        password (str): The password to connect to the Oracle database.
        host (str): The hostname or IP address of the Oracle database server.
        port (int): The port number to connect to the Oracle database.
        service_name (str): The service name of the Oracle database.
        table_name (str): The name of the table to fetch data from.

    Returns:
        pandas.DataFrame: A DataFrame containing the data from the Oracle table.
    """

    # Establish a connection to the Oracle database
    connection = cx_Oracle.connect(f'{username}/{password}@{host}:{port}/{service_name}')

    # Create a cursor object
    cursor = connection.cursor()

    # Execute a query to fetch data from the Oracle table
    query = f'SELECT * FROM {table_name}'
    cursor.execute(query)

    # Fetch all the rows from the cursor
    rows = cursor.fetchall()

    # Get the column names from the cursor description
    column_names = [desc[0] for desc in cursor.description]

    # Create a pandas DataFrame from the fetched data and column names
    df = pd.DataFrame(rows, columns=column_names)

    # Close the cursor and connection
    cursor.close()
    connection.close()

    return df


def cast_value(value, data_type):
    """
    Casts the value to the specified data type.

    Args:
        value: The value to be casted.
        data_type: The target data type.

    Returns:
        The casted value.
    """
    if value is None:
        return None

    if data_type == 'integer':
        return int(value)
    elif data_type == 'double':
        return float(value)
    elif data_type == 'boolean':
        return bool(value)
    elif data_type == 'date':
        return value
    elif data_type == 'timestamp':
        return value
    else:
        return value


def create_dataframe_from_athena_table(region, database, table, output_location, aws_access_key_id,
                                       aws_secret_access_key):
    """
    Creates a pandas DataFrame by fetching data from an Athena table.

    Args:
        region (str): The AWS region name.
        database (str): The name of the Athena database.
        table (str): The name of the Athena table to fetch data from.
        output_location (str): The S3 bucket location to store the query results.

    Returns:
        pandas.DataFrame: A DataFrame containing the data from the Athena table.
    """

    # Create an Athena client
    athena_client = boto3.client('athena', region_name=region)

    # Execute the query to fetch data from the Athena table
    query = f'SELECT * FROM {table}'
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': output_location
        }
    )

    # Get the query execution ID
    query_execution_id = response['QueryExecutionId']

    # Wait for the query execution to complete
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(5)  # Wait for 5 seconds before checking the query status again

    # Check if the query execution was successful
    if status != 'SUCCEEDED':
        raise ValueError("Athena query execution failed or was cancelled.")

    # Get the query results
    results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

    # Extract the column names and data types
    column_metadata = results['ResultSet']['ResultSetMetadata']['ColumnInfo']
    column_names = [field['Name'].upper() for field in column_metadata]
    data_types = [field['Type'] for field in column_metadata]

    # Extract the rows
    rows = []
    for row in results['ResultSet']['Rows'][1:]:
        values = []
        for i, data in enumerate(row['Data']):
            column_type = data_types[i]

            value = data.get('VarCharValue')

            casted_value = cast_value(value, column_type)
            values.append(casted_value)
        rows.append(values)

    # Create the pandas DataFrame
    df = pd.DataFrame(rows, columns=column_names)

    return df


#################################################################
# Oracle Athena Field Name Comparison
#################################################################
def get_oracle_field_names(username, password, host, port, sid, table_name):
    connection = cx_Oracle.connect(username, password, f"{host}:{port}/{sid}")
    cursor = connection.cursor()

    try:
        # Get column names from the specified table
        query = f"SELECT column_name FROM all_tab_columns WHERE table_name = '{table_name}'"
        cursor.execute(query)

        oracle_field_names = [row[0] for row in cursor.fetchall()]
        return oracle_field_names
    except cx_Oracle.Error as error:
        print("Error:", error)
    finally:
        cursor.close()
        connection.close()


def get_athena_field_names(region, database, athena_table):
    """
    Connects to Athena and retrieves the columns of a specific table.

    Args:
        region (str): AWS region name.
        database (str): Athena database name.
        athena_table (str): Name of the Athena table to retrieve columns from.

    Returns:
        list: List of Athena column descriptions.

    """
    # Connect to Athena
    athena_client = boto3.client('athena', region_name=region)

    # Get the Athena table schema
    response = athena_client.get_table_metadata(
        CatalogName='AwsDataCatalog',
        DatabaseName=database,
        TableName=athena_table
    )

    athena_columns = response['TableMetadata']['Columns']
    fields = []
    for field in athena_columns:
        fields.append(field['Name'].upper())

    return fields


def compare_field_names(oracle_field_names, other_db_field_names):
    all_columns = oracle_field_names + other_db_field_names
    full_set = set(all_columns)

    result_rows = []
    for field in list(full_set):

        if field in oracle_field_names:
            if field in other_db_field_names:
                result_rows.append((field, True, True))

        if field in oracle_field_names:
            if field not in other_db_field_names:
                result_rows.append((field, True, False))

        if field in other_db_field_names:
            if field not in oracle_field_names:
                result_rows.append((field, False, True))

    result_df = pd.DataFrame(result_rows, columns=["column_name", "oracle", "athena"])
    return result_df


def get_redshift_field_names(host, port, dbname, user, password, table_name):
    connection = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    cursor = connection.cursor()

    try:
        # Get column names from the specified table
        query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
        cursor.execute(query)

        redshift_field_names = [row[0].upper() for row in cursor.fetchall()]
        return redshift_field_names
    except psycopg2.Error as error:
        print("Error:", error)
    finally:
        cursor.close()
        connection.close()
