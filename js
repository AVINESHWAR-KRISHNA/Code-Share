import pandas as pd
import pyodbc
from concurrent.futures import ThreadPoolExecutor

# Number of threads to use for parallel processing
NUM_THREADS = 4  # You can adjust this based on your system's capabilities

def load_data_chunk(chunk_data, table_name, conn_str, columns):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Create a comma-separated list of column names to use in the SQL query
        column_names = ', '.join([f'[{col}]' for col in columns])

        # Prepare the SQL query for bulk insert
        placeholders = ', '.join(['?' for _ in columns])
        query = f'INSERT INTO [{table_name}] ({column_names}) VALUES ({placeholders})'

        # Prepare data for bulk insert
        data = [tuple(row) for _, row in chunk_data[columns].iterrows()]

        # Execute bulk insert
        cursor.fast_executemany = True  # Enable fast_executemany for bulk insert
        cursor.executemany(query, data)

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

    except Exception as e:
        print(f"An error occurred: {str(e)}")


def load_csv_to_sql_server(csv_file, table_name, server, database, delimiter=','):
    try:
        # Load CSV data into a pandas DataFrame
        df = pd.read_csv(csv_file, sep=delimiter, low_memory=False)
        df = df.astype(str)
        df = df.rename(columns=lambda x: x.replace('-', ''))
        df.fillna('NULL', inplace=True)

        # Establish a connection string to SQL Server
        conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

        # Get the list of columns from the DataFrame
        columns = df.columns.tolist()

        # Calculate the chunk size to divide the data into equal parts for parallel processing
        chunk_size = len(df) // NUM_THREADS

        # Divide the DataFrame into chunks for parallel processing
        chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            executor.map(load_data_chunk, chunks, [table_name] * NUM_THREADS, [conn_str] * NUM_THREADS, [columns] * NUM_THREADS)

        print("Data loaded successfully.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    csv_file_path = r'C:\Users\IN10011418\OneDrive - R1\Desktop\MFS-TestData.csv'
    table_name = 'MFS_Export_GenesysRaw'
    server = 'DEVCONTWCOR01.r1rcm.tech'
    database = 'Srdial'
    delimiter = ','  # Change this to the appropriate delimiter used in your CSV file

    load_csv_to_sql_server(csv_file_path, table_name, server, database, delimiter)
