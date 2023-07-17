
import pandas as pd
import pyodbc
from sqlalchemy import create_engine

# Connection settings for SQL Server
server_name = 'your_server_name'
database_name = 'your_database_name'
table_name = 'your_table_name'
username = 'your_username'
password = 'your_password'

# Connection string for SQL Server
conn_str = f"DRIVER={{SQL Server}};SERVER={server_name};DATABASE={database_name};UID={username};PWD={password}"
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")

# Path to your large CSV file
csv_file_path = 'path/to/your/large_file.csv'

# Chunk size for reading the CSV file
chunk_size = 10000

# Define the column names in the CSV file (replace with your actual column names)
# It's essential to specify the correct order of columns in the CSV file and the table.
column_names = ['column1', 'column2', 'column3', ...]

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
