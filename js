
import os
import sys
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, event, text
import concurrent.futures
import gc
gc.enable()

startT = time.time()


SERVER_NAME ='DEVCONTWCOR01.r1rcm.tech'
DATABASE ='Srdial'
DRIVER = 'ODBC Driver 17 for SQL Server' #'SQL Server Native Client 11.0'
TABLE_NAME = 'MFS_Export_GenesysRaw'
CSV_FILE = 'C:/Users/IN10011418/OneDrive - R1/Desktop/MFS-TestData.csv'
MAX_THREADS = 1
CHUNK_SIZE = 1


try:

    ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}',fast_executemany=True)

except Exception as e:
    print(e)
    sys.exit(1)

def insert_records(chunk):

    try:

        cnx = ENGINE.connect()

        chunk = chunk.rename(columns=lambda x: x.replace('-', ''))
        chunk.fillna('NULL', inplace=True)

        stmt = f"INSERT INTO {TABLE_NAME} ({', '.join(chunk.columns)}) VALUES {', '.join(['%s'] * len(chunk.columns))}"

        values = [tuple(x) for x in chunk.values.tolist()]

        with cnx.begin() as transaction:
            cnx.execute(stmt,values)
            transaction.commit()
        
        cnx.close()

    except Exception as e:

        print(e)
        sys.exit(1)

def create_chunk(df):

    chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        @event.listens_for(ENGINE, "before_cursor_execute")
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                print('@event.listens_for')
                cursor.fast_executemany = True

        futures = []

        for chunk in chunks:
            future = executor.submit(insert_records,chunk)
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            print(future)

if __name__ == '__main__':

    df = pd.read_csv(CSV_FILE,sep=',',low_memory=False)
    create_chunk(df)
