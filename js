import pandas as pd
import pyodbc
from concurrent.futures import ThreadPoolExecutor

# Number of threads to use for parallel processing
NUM_THREADS = 4  # You can adjust this based on your system's capabilities

def load_data_chunk(chunk_data, table_name, conn_str):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Create a comma-separated list of column names to use in the SQL query
        columns = ', '.join([f'[{col}]' for col in chunk_data.columns])

        # Prepare the SQL query with parameter placeholders
        params = ','.join(['?' for _ in chunk_data.columns])
        query = f'INSERT INTO [{table_name}] ({columns}) VALUES ({params})'

        # Prepare data for bulk insert
        data = [tuple(row) for _, row in chunk_data.iterrows()]

        # Execute bulk insert
        cursor.executemany(query, data)

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

    except Exception as e:
        print(f"An error occurred: {str(e)}")


def load_csv_to_sql_server(csv_file, table_name, server, database, username, password):
    try:
        # Load CSV data into a pandas DataFrame
        df = pd.read_csv(csv_file)

        # Establish a connection string to SQL Server
        conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

        # Calculate the chunk size to divide the data into equal parts for parallel processing
        chunk_size = len(df) // NUM_THREADS

        # Divide the DataFrame into chunks for parallel processing
        chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            executor.map(load_data_chunk, chunks, [table_name] * NUM_THREADS, [conn_str] * NUM_THREADS)

        print("Data loaded successfully.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    csv_file_path = "path/to/your/csv_file.csv"
    table_name = "your_table_name"
    server = "your_sql_server_name"
    database = "your_database_name"
    username = "your_username"
    password = "your_password"

    load_csv_to_sql_server(csv_file_path, table_name, server, database, username, password)
