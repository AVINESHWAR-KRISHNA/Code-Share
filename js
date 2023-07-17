import configparser
import pandas as pd
import pyodbc
from sqlalchemy import create_engine

# Read the configuration from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Connection settings for SQL Server
server_name = config.get('DATABASE', 'server_name')
database_name = config.get('DATABASE', 'database_name')
table_name = config.get('DATABASE', 'table_name')
username = config.get('DATABASE', 'username')
password = config.get('DATABASE', 'password')

# Connection string for SQL Server
conn_str = f"DRIVER={{SQL Server}};SERVER={server_name};DATABASE={database_name};UID={username};PWD={password}"
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")

# Path to your large CSV file
csv_file_path = config.get('CSV', 'file_path')

# Chunk size for reading the CSV file
chunk_size = 10000

# Get the column names from the CSV file header
with open(csv_file_path, 'r') as csv_file:
    first_line = csv_file.readline().strip()
    column_names = first_line.split(',')

# Iterate through the CSV file in chunks and insert into the SQL Server table
for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size):
    # Optionally, you can preprocess the data here if needed.
    
    # Convert the chunk to a list of dictionaries (records) for bulk insert
    data = chunk[column_names].to_dict(orient='records')
    
    # Create a connection to the database
    with engine.connect() as conn:
        # Start a transaction
        with conn.begin():
            # Create the SQL Server bulk insert object
            bulk_copy = pyodbc.SqlBulkCopy(conn)
            bulk_copy.destination_table_name = table_name
            
            # Enable fast_executemany for better performance
            bulk_copy.fast_executemany = True
            
            # Perform the bulk insert
            bulk_copy.write_to_server(chunk[column_names])
            
# Data insertion completed
print("Data insertion completed successfully.")
