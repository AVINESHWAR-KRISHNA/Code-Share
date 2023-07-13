import dask.dataframe as dd
import pyodbc

# Define your SQL Server connection details
server = '<server_name>'
database = '<database_name>'
username = '<username>'
password = '<password>'
table_name = '<table_name>'

# Define Dask options for optimization
dask_optimizations = {
    'assume_missing': True,  # Assume missing values
    'low_memory': True,  # Optimize memory usage
    'dtype': str,  # Use string type for all columns
}

# Read the CSV file using Dask
df = dd.read_csv('data.csv', **dask_optimizations)

# Connect to the SQL Server
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
conn = pyodbc.connect(conn_str)

# Create the cursor
cursor = conn.cursor()

# Create the table if it doesn't exist
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (column1 datatype1, column2 datatype2, ...)"
cursor.execute(create_table_query)
conn.commit()

# Insert data into the table
with cursor.fast_executemany = True:
    # Iterate over each partition of the Dask DataFrame
    for partition in df.partitions:
        # Convert the Dask DataFrame partition to a pandas DataFrame
        partition = partition.compute()

        # Convert NaN values to None to match SQL Server NULL semantics
        partition = partition.where(partition.notnull(), None)

        # Convert the pandas DataFrame to a list of tuples
        data = [tuple(row) for row in partition.to_records(index=False)]

        # Generate the insert query with parameter placeholders
        insert_query = f"INSERT INTO {table_name} (column1, column2, ...) VALUES (?, ?, ...)"

        # Execute the insert query with the data
        cursor.executemany(insert_query, data)
        conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
