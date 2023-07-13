import sys
import pyodbc
import pandas as pd
import numpy as np
import concurrent.futures
import gc
gc.enable()

SERVER_NAME = 'DEVCONTWCOR01.r1rcm.tech'
DATABASE = 'Srdial'
DRIVER = 'SQL Server'
TABLE_NAME = 'MFS_Export_GenesysRaw'
FTP = r'C:\Users\IN10011418\OneDrive - R1\Desktop\MFS-TestData.csv'
MAX_THREADS = 25
CHUNK_SIZE = 1000

insert_records_failure_flag_counter = 0
rows_inserted = 0
insertion_err = ''
insert_records_failure_flag = True

try:
    conn_str = f'DRIVER={{{DRIVER}}};SERVER={SERVER_NAME};DATABASE={DATABASE};Trusted_Connection=yes;'
    cnx = pyodbc.connect(conn_str)
    cursor = cnx.cursor()

except Exception as e:
    print(f"Unable to connect to server :: {SERVER_NAME}. Error message: {e}")
    sys.exit(1)


def insert_records(chunk):
    try:
        global rows_inserted, insert_records_failure_flag, insertion_err, insert_records_failure_flag_counter

        chunk = chunk.rename(columns=lambda x: x.replace('-', ''))
        chunk.fillna('NULL', inplace=True)

        float_columns = chunk.select_dtypes(include='float').columns
        chunk[float_columns] = chunk[float_columns].replace([np.inf, -np.inf], np.nan)
        chunk[float_columns] = chunk[float_columns].astype(pd.Int64Dtype())

        insert_query = f"INSERT INTO {TABLE_NAME} ({', '.join(chunk.columns)}) VALUES ({', '.join(['?'] * len(chunk.columns))})"
        
        for row in chunk.itertuples(index=False):
            cursor.execute(insert_query, row)
        
        cnx.commit()
        rows_inserted += len(chunk)

    except Exception as e:
        print(e)
        insertion_err += str(e)
        insert_records_failure_flag_counter += 1
        print(f"Unable to insert data in table :: {TABLE_NAME}. Error message: {insertion_err}")


def create_chunk(df):
    global insertion_err

    chunks = [df[i:i + CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = []

        for chunk in chunks:
            future = executor.submit(insert_records, chunk)
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            print(future)

    print(f"Data inserted successfully into table :: {TABLE_NAME}.")
    print(f"Total number of rows inserted :: {rows_inserted}.")


if __name__ == '__main__':
    matching_file = FTP

    if matching_file:
        df = pd.read_csv(matching_file, sep=',')
        create_chunk(df)
    else:
        print("No file found. Sys exit.")

cursor.close()
cnx.close()
