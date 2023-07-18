import os
import sys
import time
import pandas as pd
from sqlalchemy import create_engine, event
import concurrent.futures
import gc
gc.enable()

startT = time.time()

SERVER_NAME = 'DEVCONTWCOR01.r1rcm.tech'
DATABASE = 'Srdial'
DRIVER = 'ODBC Driver 17 for SQL Server'
TABLE_NAME = 'MFS_Export_GenesysRaw'
CSV_FILE = 'C:/Users/IN10011418/OneDrive - R1/Desktop/MFS-TestData.csv'
MAX_THREADS = 1
CHUNK_SIZE = 1

try:
    ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}', fast_executemany=True)
    # Set up the event listener for fast_executemany outside the create_chunk function
    @event.listens_for(ENGINE, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True
except Exception as e:
    print(e)
    sys.exit(1)

def insert_records(chunk):
    try:
        with ENGINE.connect() as cnx:
            chunk = chunk.rename(columns=lambda x: x.replace('-', ''))
            chunk = chunk.where(pd.notna(chunk), None)  # Replace NaN with None for NULL values
            columns = ', '.join(chunk.columns)
            placeholders = ', '.join(['%s'] * len(chunk.columns))
            values = [tuple(row) for row in chunk.values]

            stmt = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
            with cnx.begin() as transaction:
                cnx.execute(stmt, values)
                transaction.commit()

    except Exception as e:
        print(e)
        sys.exit(1)

def create_chunk(df):
    chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(insert_records, chunk) for chunk in chunks]

        for future in concurrent.futures.as_completed(futures):
            print(future.result())

if __name__ == '__main__':
    df = pd.read_csv(CSV_FILE, sep=',', low_memory=False)
    create_chunk(df)
