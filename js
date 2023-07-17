from sqlalchemy import create_engine, event
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

SERVER_NAME ='DEVCONTWCOR01.r1rcm.tech'
DATABASE ='Srdial'
DRIVER = 'SQL+Server'
TABLE_NAME = 'MFS_Export_GenesysRaw'
FTP = 'C:/Users/IN10011418/OneDrive - R1/Desktop/MFS-Test.csv'

# Function to insert data into the database
def insert_data(chunk):
    try:
        with engine.connect() as connection:
            chunk.to_sql(TABLE_NAME, connection, index=False, if_exists="append", schema="dbo")
            connection.commit()
    except Exception as e:
        print(f"Error inserting data: {e}")
        connection.rollback()

# Create the database engine
engine = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver=ODBC Driver 17 for SQL Server',fast_executemany=True)

# Path to your CSV file
csv_file_path = FTP

# Read the CSV file into a DataFrame
df = pd.read_csv(csv_file_path, sep=',', low_memory=True)
df = df.astype(str)

# Determine the number of chunks based on the number of CPU cores
num_cores = 4  # You can adjust this based on the number of available CPU cores
chunk_size = len(df) // num_cores
df_chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

# Event listener function for fast_executemany
@event.listens_for(engine, "before_cursor_execute")
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True

# Use ThreadPoolExecutor to process chunks in parallel
with ThreadPoolExecutor(max_workers=num_cores) as executor:
    futures = [executor.submit(insert_data, chunk) for chunk in df_chunks]

# Wait for all tasks to complete
for future in futures:
    future.result()
